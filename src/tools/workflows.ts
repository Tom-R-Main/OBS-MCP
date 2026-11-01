/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { isDeepStrictEqual } from "node:util";
import type { OBSWebSocketClient } from "../client.js";
import { appendInputScreenshot, inputStateResult, readInputState } from "./after-change.js";
import { VIDEO_ONLY_CAPTURE_KINDS, configuredWindow } from "./capture-backend.js";
import { guardCapturePropertyList } from "./protocol-extensions.js";
import { takeSnapshot } from "./snapshots.js";
import { markTake, startTake, stopTake } from "./takes.js";

type JsonObject = Record<string, unknown>;

/** Many MCP clients time a tool call out at 60 seconds. */
export const MAX_CLIP_SECONDS = 50;
const SCK_WINDOW_CAPTURE = 1;
const ALL_TRACKS_OFF = { 1: false, 2: false, 3: false, 4: false, 5: false, 6: false };

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

async function recordClip(
  client: OBSWebSocketClient,
  args: { durationSeconds: number; chapters: { atSeconds: number; name: string }[]; expectSilent: boolean },
): Promise<CallToolResult> {
  const started = await startTake(client, { expectSilent: args.expectSilent, minFreeDiskMb: 1024, lock: true });
  if (!started.ok) return started.result;
  const { take } = started;

  const startedAt = Date.now();
  let result: CallToolResult | undefined;
  try {
    const chapters = [...args.chapters].sort((a, b) => a.atSeconds - b.atSeconds);
    for (const chapter of chapters) {
      await sleep(Math.max(0, startedAt + chapter.atSeconds * 1000 - Date.now()));
      await markTake(client, take, chapter.name);
    }
    await sleep(Math.max(0, startedAt + args.durationSeconds * 1000 - Date.now()));
  } finally {
    // Cleanup reconciles status and reports uncertainty rather than stopping another recording.
    result = await stopTake(client, take, { requestedSeconds: args.durationSeconds });
  }
  return result;
}

type ListItem = { itemName: string; itemValue: unknown; itemEnabled?: boolean };
type CaptureSelector = { inputName: string; inputUuid: string };

async function listItems(client: OBSWebSocketClient, selector: CaptureSelector, propertyName: string): Promise<ListItem[]> {
  const blocker = await guardCapturePropertyList(client, { ...selector, propertyName });
  if (blocker) throw new Error(blocker);
  const response: unknown = await client.sendRequest("GetInputPropertiesListPropertyItems", { ...selector, propertyName });
  const items = isObject(response) && Array.isArray(response.propertyItems) ? response.propertyItems : [];
  return items.filter((item): item is ListItem => (
    isObject(item) && typeof item.itemName === "string" && item.itemEnabled !== false && item.itemName.trim() !== ""
  ));
}

async function captureWindow(
  client: OBSWebSocketClient,
  args: {
    sceneName: string;
    inputName: string;
    backend: "auto" | "x11" | "portal";
    window?: string;
    application?: string;
    displayUuid?: string;
    silent: boolean;
    fit: boolean;
    includeScreenshot: boolean;
    apply: boolean;
  },
): Promise<CallToolResult> {
  if (args.backend !== "portal" && !args.window === !args.application) return errorResult("Give exactly one of window or application");
  await client.connect();
  const mac = client.getConnectionStatus().versionInfo?.platform === "macos";
  if (mac && args.backend !== "auto") return errorResult("capability_unavailable: the requested Linux backend is unavailable on macOS; use backend:auto for ScreenCaptureKit");
  if (mac && args.application) return errorResult("capability_unavailable: Mac application auto-discovery is unsafe because native application string-list serialization can crash OBS. Configure the intended application capture in OBS and bind that existing source; no capture changed");
  if (client.getConnectionStatus().versionInfo?.platform === "windows") return errorResult("OBS is not on macOS or Linux; this workflow supports ScreenCaptureKit, X11 and PipeWire");

  const readScene = async (): Promise<{ sceneName: string; sceneUuid: string }> => {
    const response: unknown = await client.sendRequest("GetSceneList");
    const matches = isObject(response) && Array.isArray(response.scenes)
      ? response.scenes.filter(isObject).filter(({ sceneName }) => sceneName === args.sceneName) : [];
    if (matches.length !== 1 || typeof matches[0]!.sceneUuid !== "string" || !matches[0]!.sceneUuid) {
      throw new Error("Target scene must exist uniquely with an observable UUID; no capture changed");
    }
    return { sceneName: args.sceneName, sceneUuid: matches[0]!.sceneUuid as string };
  };
  const scene = await readScene();
  const readInventory = async (): Promise<JsonObject[]> => {
    const response: unknown = await client.sendRequest("GetInputList");
    if (!isObject(response) || !Array.isArray(response.inputs)) throw new Error("Capture inventory is unavailable; no capture changed");
    return response.inputs.filter(isObject);
  };
  const inputs = await readInventory();
  const existing = inputs.find(({ inputName }) => inputName === args.inputName);
  let kind = "screen_capture";
  if (!mac) {
    const available = await client.sendRequest("GetInputKindList") as { inputKinds?: string[] };
    kind = args.backend === "x11" ? "xcomposite_input"
      : existing && VIDEO_ONLY_CAPTURE_KINDS.has(String(existing.inputKind)) ? String(existing.inputKind)
      : args.backend !== "portal" && available.inputKinds?.includes("xcomposite_input") ? "xcomposite_input" : "pipewire-screen-capture-source";
    if (!available.inputKinds?.includes(kind) && existing?.inputKind !== kind) return errorResult("capability_unavailable: OBS advertises no requested Linux window backend. On Wayland, configure a PipeWire window capture in OBS through the desktop portal; on X11 enable a window manager and XComposite. No capture changed");
    if (kind.startsWith("pipewire-")) {
      if (!existing || existing.inputKind !== kind) return errorResult("capability_unavailable: create and authorize a PipeWire window capture using OBS's desktop portal picker, then call with backend:portal and that inputName. No capture changed");
      if (args.window || args.application) return errorResult("capability_unavailable: the Wayland portal owns window selection. Select the intended window in OBS, inspect its image, then call with backend:portal and omit window/application. No capture changed");
    }
  }
  if (existing && existing.inputKind !== kind) {
    return errorResult(`Input ${args.inputName} exists but is a ${String(existing.inputKind)}, not a ${kind}`);
  }
  const selectorFor = (input: JsonObject): CaptureSelector => {
    if (typeof input.inputName !== "string" || typeof input.inputUuid !== "string" || !input.inputUuid.trim()) {
      throw new Error("Capture discovery requires an observable nonempty input UUID; configure a capture in a UUID-capable OBS instance. No capture changed");
    }
    return { inputName: input.inputName, inputUuid: input.inputUuid };
  };
  if (existing) selectorFor(existing);
  const readMembership = async () => {
    const response: unknown = await client.sendRequest("GetSceneItemList", scene);
    if (!isObject(response) || !Array.isArray(response.sceneItems)) throw new Error("Target-scene membership is unavailable; no capture changed");
    const items = response.sceneItems.filter(isObject).filter((item) => item.sourceName === args.inputName && item.sourceUuid === existing?.inputUuid);
    if (items.length === 0 || items.some(({ sceneItemId }) => !Number.isSafeInteger(sceneItemId))) {
      throw new Error("Existing capture must appear in the requested target scene; no capture changed");
    }
    return items.map(({ sceneItemId }) => Number(sceneItemId)).sort((a, b) => a - b);
  };
  const membership = existing ? await readMembership() : undefined;
  const portal = kind.startsWith("pipewire-");
  const property = mac ? "window" : "capture_window";
  const readSettings = async (selector: CaptureSelector): Promise<JsonObject> => {
    const response: unknown = await client.sendRequest("GetInputSettings", selector);
    if (!isObject(response) || response.inputKind !== kind || !isObject(response.inputSettings)) {
      throw new Error("Capture source identity or settings are unavailable; no capture changed");
    }
    return response.inputSettings;
  };
  // Native enumeration requires an input, but discovery must never create one.
  // Reuse a compatible source as a read-only inventory; an unconfigured
  // XComposite source must pass the shared serializer guard without seeding it.
  let inventory: JsonObject | undefined;
  let inventorySettings: JsonObject | undefined;
  for (const candidate of existing ? [existing] : inputs.filter(({ inputKind }) => inputKind === kind)) {
    if (typeof candidate.inputName !== "string") continue;
    try {
      const selector = selectorFor(candidate);
      const settings = await readSettings(selector);
      if (!portal) {
        const blocker = await guardCapturePropertyList(client, { ...selector, propertyName: property });
        if (blocker) throw new Error(blocker);
      } else if (!configuredWindow(kind, settings)) {
        throw new Error("capability_unavailable: PipeWire capture has no persisted portal selection; authorize it in OBS first");
      }
      inventory = candidate;
      inventorySettings = settings;
      break;
    } catch (error) {
      if (existing) throw error;
    }
  }
  if (!inventory || !inventorySettings) {
    return errorResult("capability_unavailable: no safe existing capture inventory with an observable input UUID is available. Configure a window capture in OBS, then rerun; use obs-prepare-demo for an existing bound capture. No capture changed");
  }
  const inventoryName = String(inventory.inputName);
  const inventorySelector = selectorFor(inventory);
  const wanted = (args.window ?? args.application ?? "").toLowerCase();
  const select = async (): Promise<ListItem> => {
    const items = await listItems(client, inventorySelector, property);
    const matches = items.filter(({ itemName, itemValue }) => (
      itemName.toLowerCase().includes(wanted) || String(itemValue).toLowerCase() === wanted
      || (!mac && Boolean(args.window) && String(itemValue).split("\r\n", 1)[0] === wanted)
      || (!mac && Boolean(args.application) && String(itemValue).toLowerCase().split("\r\n").slice(1).some((part) => part.includes(wanted)))
    ));
    if (matches.length !== 1) {
      const names = (matches.length > 1 ? matches : items).slice(0, 15).map(({ itemName }) => `- ${itemName}`);
      throw new Error(`${matches.length === 0 ? "No" : "More than one"} ${property} matches "${args.window ?? args.application}". ${matches.length > 1 ? "Matches" : "Available"}:\n${names.join("\n")}`);
    }
    const match = matches[0]!;
    if (mac && (!Number.isSafeInteger(match.itemValue) || Number(match.itemValue) <= 0)) {
      throw new Error("The selected Mac window has no positive integer identity; no capture changed");
    }
    if (!mac) {
      const value = match.itemValue;
      const xid = typeof value === "string" ? Number(value.split("\r\n", 1)[0]) : 0;
      if (typeof value !== "string" || !/^\d+(?:\r\n[^\r\n]*\r\n[^\r\n]*)?$/.test(value) || !Number.isSafeInteger(xid) || xid <= 0 || xid > 0xffffffff) {
        throw new Error("The selected X11 window has no well-formed positive XID; no capture changed");
      }
    }
    return match;
  };
  const match = portal ? undefined : await select();
  const identity = (input: JsonObject | undefined) => input ? {
    inputName: input.inputName, inputKind: input.inputKind,
    inputUuid: input.inputUuid,
  } : null;
  const reread = await readInventory();
  if (!isDeepStrictEqual(identity(existing), identity(reread.find(({ inputName }) => inputName === args.inputName)))
    || !isDeepStrictEqual(identity(inventory), identity(reread.find(({ inputName }) => inputName === inventoryName)))
    || !isDeepStrictEqual(inventorySettings, await readSettings(inventorySelector))) {
    return errorResult("Capture source or selection changed during discovery; rerun with a stable window. No capture changed");
  }
  if (!portal && !isDeepStrictEqual(match, await select())) {
    return errorResult("Window selection changed during discovery; rerun with a stable window. No capture changed");
  }
  if (!isDeepStrictEqual(scene, await readScene()) || (existing && !isDeepStrictEqual(membership, await readMembership()))) {
    return errorResult("Target scene or capture membership changed during discovery; no capture changed");
  }
  if (!args.apply) return {
    content: [{ type: "text", text: "Capture plan only; no capture changed. Rendered target is unverified; inspect the image after applying." }],
    structuredContent: {
      applied: false, rendered: "unverified", sceneName: args.sceneName, scene, inputName: args.inputName,
      existing: identity(existing), inventory: identity(inventory),
      selected: match ?? { inputName: args.inputName, kind: "existing-portal-selection", persistedSelection: true },
      changes: { create: !existing, retarget: !portal, silent: args.silent, fit: args.fit },
    },
  };
  // Only resolved, stable selections may create a recovery snapshot or mutate.
  const snapshot = existing
    ? await takeSnapshot(client, { inputs: [args.inputName], scenes: [args.sceneName], label: "before obs-capture-window", auto: true })
    : undefined;
  const selectedSettings = mac ? { type: SCK_WINDOW_CAPTURE, window: match?.itemValue } : { [property]: match?.itemValue };
  let target: CaptureSelector;
  if (!existing) {
    const created: unknown = await client.sendRequest("CreateInput", {
      ...scene, inputName: args.inputName, inputKind: kind,
      inputSettings: { ...selectedSettings, show_cursor: true },
    });
    if (!isObject(created) || typeof created.inputUuid !== "string" || !created.inputUuid.trim()) {
      return errorResult("OBS created the configured capture but did not report its UUID. No further changes were made; inspect OBS before retrying");
    }
    target = { inputName: args.inputName, inputUuid: created.inputUuid };
  } else {
    target = selectorFor(existing);
    if (!portal) await client.sendRequest("SetInputSettings", { ...target, inputSettings: selectedSettings });
  }

  const notes: string[] = [portal ? "Using the existing portal selection; inspect the screenshot for target correctness" : `${mac ? "Capturing" : "Selected"} ${property} ${match?.itemName}`];
  if (snapshot) notes.push(`Saved the previous state as snapshot ${snapshot.id}; obs-restore undoes this change`);
  if (args.silent && !VIDEO_ONLY_CAPTURE_KINDS.has(kind)) {
    // screen_capture carries the application's audio on macOS 13+.
    const silenced = await client.sendBatch([
      { requestType: "SetInputMute", requestData: { ...target, inputMuted: true } },
      { requestType: "SetInputAudioTracks", requestData: { ...target, inputAudioTracks: ALL_TRACKS_OFF } },
    ], { haltOnFailure: true });
    const failure = silenced.find(({ ok }) => !ok) ?? (silenced.length < 2 ? silenced[0] : undefined);
    if (failure) throw new Error(`${failure.requestType} failed with code ${failure.code}${failure.comment ? `: ${failure.comment}` : ""}`);
    notes.push("Muted the capture and removed it from every audio track");
  }

  if (args.silent && VIDEO_ONLY_CAPTURE_KINDS.has(kind)) notes.push("This video-only capture has no audio; inspect other audio inputs with obs-preflight");

  if (args.fit) {
    const [videoResult, itemResult] = await client.sendBatch([
      { requestType: "GetVideoSettings" },
      { requestType: "GetSceneItemId", requestData: { ...scene, sourceName: args.inputName } },
    ]);
    for (const result of [videoResult, itemResult]) {
      if (!result?.ok) throw new Error(`${result?.requestType ?? "Request"} failed${result?.comment ? `: ${result.comment}` : ""}`);
    }
    const video = videoResult!.responseData as JsonObject;
    const { sceneItemId } = itemResult!.responseData as { sceneItemId: number };
    await client.sendRequest("SetSceneItemTransform", {
      ...scene,
      sceneItemId,
      sceneItemTransform: {
        boundsType: "OBS_BOUNDS_SCALE_INNER",
        boundsWidth: video.baseWidth,
        boundsHeight: video.baseHeight,
        boundsAlignment: 0,
        alignment: 5,
        positionX: 0,
        positionY: 0,
      },
    });
    notes.push(`Fitted to the ${String(video.baseWidth)}×${String(video.baseHeight)} canvas`);
  }

  const result = inputStateResult(notes.join(". "), await readInputState(client, args.inputName));
  return args.includeScreenshot ? appendInputScreenshot(client, args.inputName, result) : result;
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-record-clip",
    {
      title: "Record Clip",
      description: `Record a clip of up to ${MAX_CLIP_SECONDS}s in one call: runs obs-preflight, starts recording `
        + "and waits for OBS to confirm, adds chapter markers, stops, then checks the file's length and audio "
        + "with ffprobe/ffmpeg when available. For longer takes use obs-start-record and obs-stop-record",
      inputSchema: z.object({
        durationSeconds: z.number().positive().max(MAX_CLIP_SECONDS).describe("Clip length in seconds"),
        chapters: z.array(z.object({
          atSeconds: z.number().nonnegative().describe("Seconds after the start"),
          name: z.string().min(1).describe("Chapter name"),
        })).default([]).describe("Chapter markers; need the Hybrid MP4 or Hybrid MOV recording format"),
        expectSilent: z.boolean().default(false)
          .describe("Refuse to record with audible inputs, and check the file's audio is silent"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async (args) => {
      // A chapter past the end would keep recording until it, beyond what the client waits for.
      const late = args.chapters.filter(({ atSeconds }) => atSeconds >= args.durationSeconds);
      if (late.length > 0) {
        return errorResult(`Not recording: chapter(s) ${late.map(({ name }) => `"${name}"`).join(", ")} start at or after the `
          + `${args.durationSeconds}s clip ends`);
      }
      try {
        return await recordClip(client, args);
      } catch (error) {
        return errorResult(`Record Clip failed: ${errorMessage(error)}`);
      }
    },
  );

  server.registerTool(
    "obs-capture-window",
    {
      title: "Capture Window",
      description: "Resolve a macOS ScreenCaptureKit or Linux X11 window using an existing compatible capture as read-only inventory, then create/reuse the capture, fit it and optionally silence it. Refuses missing or ambiguous discovery before changes. apply:false returns a plan without snapshots, mutations or screenshots. Mac application auto-discovery is refused; configure and bind an existing application capture in OBS. Linux application matching selects one window. Wayland portal selection requires the user: configure a PipeWire window capture in OBS, then use backend:portal without window/application to fit and inspect it. Returns capture size and optional screenshot; inspect the picture for target correctness.",
      inputSchema: z.object({
        backend: z.enum(["auto", "x11", "portal"]).default("auto").describe("OBS capture backend; portal reuses a user-authorized Wayland window selection"),
        sceneName: z.string().describe("Scene to add the input to if it does not exist"),
        inputName: z.string().describe("Name of the capture input to create or reuse"),
        window: z.string().optional().describe("Case-insensitive part of \"[App] Title\", or the window ID"),
        application: z.string().optional().describe("Linux only: case-insensitive application name identifying one window; Mac application auto-discovery is refused"),
        displayUuid: z.string().optional().describe("Legacy parameter retained for compatibility; ignored for window capture, which does not need a display"),
        silent: z.boolean().default(true).describe("Mute the capture and remove it from every audio track"),
        fit: z.boolean().default(true).describe("Scale the capture to fit the canvas, centered"),
        includeScreenshot: z.boolean().default(false).describe("Also return a screenshot after applying; ignored for apply:false"),
        apply: z.boolean().default(true).describe("Apply the resolved selection; false returns a read-only plan with unverified rendering"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (args) => {
      try {
        return await captureWindow(client, args);
      } catch (error) {
        return errorResult(`Capture Window failed: ${errorMessage(error)}`);
      }
    },
  );
}
