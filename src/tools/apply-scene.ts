/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { BatchRequest, BatchResult, OBSWebSocketClient } from "../client.js";
import { withoutAudioOnly } from "./audio-only.js";
import { readInputs, readItems, same, takeSnapshot, type InputState, type ItemState } from "./snapshots.js";

type JsonObject = Record<string, unknown>;

const TransformSpec = z.object({
  positionX: z.number(),
  positionY: z.number(),
  scaleX: z.number(),
  scaleY: z.number(),
  rotation: z.number(),
  alignment: z.number().int(),
  boundsType: z.enum(["OBS_BOUNDS_NONE", "OBS_BOUNDS_STRETCH", "OBS_BOUNDS_SCALE_INNER", "OBS_BOUNDS_SCALE_OUTER", "OBS_BOUNDS_SCALE_TO_WIDTH", "OBS_BOUNDS_SCALE_TO_HEIGHT", "OBS_BOUNDS_MAX_ONLY"]),
  boundsWidth: z.number(),
  boundsHeight: z.number(),
  boundsAlignment: z.number().int(),
  cropLeft: z.number().int().nonnegative(),
  cropRight: z.number().int().nonnegative(),
  cropTop: z.number().int().nonnegative(),
  cropBottom: z.number().int().nonnegative(),
}).partial();

const SourceSpec = z.object({
  name: z.string().min(1).describe("Input name; an existing input is reused, including one shown in another scene"),
  kind: z.string().optional().describe("Input kind, e.g. screen_capture or color_source_v3; needed only to create the input"),
  settings: z.record(z.string(), z.unknown()).optional().describe("Input settings to set; others are left as they are"),
  visible: z.boolean().optional(),
  locked: z.boolean().optional(),
  muted: z.boolean().optional(),
  volumeDb: z.number().min(-100).max(26).optional(),
  audioTracks: z.array(z.number().int().min(1).max(6)).optional()
    .describe("The tracks it feeds, e.g. [1]; [] removes it from every track"),
  fit: z.enum(["canvas"]).optional().describe("Scale it to fit the canvas, centered, keeping its aspect ratio"),
  transform: TransformSpec.optional().describe("Transform fields to set; applied after fit"),
});

type Source = z.infer<typeof SourceSpec>;
export type SceneSpec = { sceneName: string; sources: Source[]; order: boolean; removeOthers: boolean };

export type SceneAction = {
  target: string;
  change: string;
  phase: "create" | "change";
  /** Absent in a plan for an item that does not exist yet. */
  request?: BatchRequest;
};

const ALL_TRACKS = [1, 2, 3, 4, 5, 6];

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

function toDb(multiplier: number | undefined): number | undefined {
  if (multiplier === undefined) return undefined;
  return multiplier > 0 ? 20 * Math.log10(multiplier) : -100;
}

type Current = {
  sceneExists: boolean;
  inputKinds: Map<string, string>;
  items: ItemState[];
  inputs: InputState[];
  video: { baseWidth: number; baseHeight: number } | null;
};

async function readCurrent(client: OBSWebSocketClient, spec: SceneSpec): Promise<Current> {
  const [sceneList, inputList] = await Promise.all([
    client.sendRequest("GetSceneList") as Promise<JsonObject>,
    client.sendRequest("GetInputList") as Promise<JsonObject>,
  ]);
  const sceneExists = (Array.isArray(sceneList.scenes) ? sceneList.scenes : [])
    .some((scene) => isObject(scene) && scene.sceneName === spec.sceneName);
  const inputKinds = new Map((Array.isArray(inputList.inputs) ? inputList.inputs : [])
    .filter(isObject)
    .map(({ inputName, inputKind }) => [String(inputName), String(inputKind)] as const));
  const [items, inputs, video] = await Promise.all([
    sceneExists ? readItems(client, [spec.sceneName]) : Promise.resolve([] as ItemState[]),
    readInputs(client, spec.sources.map(({ name }) => name).filter((name) => inputKinds.has(name))),
    spec.sources.some(({ fit }) => fit === "canvas")
      ? client.sendRequest("GetVideoSettings") as Promise<{ baseWidth: number; baseHeight: number }>
      : Promise.resolve(null),
  ]);
  return { sceneExists, inputKinds, items, inputs, video };
}

/**
 * Works out what must change for OBS to match the spec. Items that do not
 * exist yet get "create" actions, and their other changes are described
 * without requests: they are planned again once the items exist.
 */
export function planScene(spec: SceneSpec, current: Current): { actions: SceneAction[]; errors: string[] } {
  const actions: SceneAction[] = [];
  const errors: string[] = [];
  const { sceneName } = spec;
  const scene = { sceneName };

  if (!current.sceneExists) {
    actions.push({ target: `scene ${sceneName}`, change: "create", phase: "create", request: { requestType: "CreateScene", requestData: scene } });
  }

  for (const source of spec.sources) {
    const target = source.name;
    const kind = current.inputKinds.get(source.name);
    const item = current.items.find(({ sourceName }) => sourceName === source.name);
    const input = current.inputs.find(({ inputName }) => inputName === source.name);
    const inputName = source.name;

    if (kind === undefined) {
      if (!source.kind) {
        errors.push(`${target} does not exist; give its kind to create it`);
        continue;
      }
      actions.push({
        target,
        change: `create ${source.kind}${source.settings ? " with settings" : ""}`,
        phase: "create",
        request: { requestType: "CreateInput", requestData: { ...scene, inputName, inputKind: source.kind, inputSettings: source.settings ?? {}, sceneItemEnabled: source.visible ?? true } },
      });
    } else {
      if (source.kind && source.kind !== kind) {
        errors.push(`${target} is a ${kind}, not a ${source.kind}; choose another name`);
        continue;
      }
      if (!item) {
        actions.push({
          target,
          change: "add to the scene",
          phase: "create",
          request: { requestType: "CreateSceneItem", requestData: { ...scene, sourceName: inputName, sceneItemEnabled: source.visible ?? true } },
        });
      }
      if (source.settings && input) {
        const changed = Object.keys(source.settings).filter((key) => !same(source.settings![key], input.inputSettings[key]));
        if (changed.length > 0) {
          actions.push({ target, change: `settings (${changed.join(", ")})`, phase: "change", request: { requestType: "SetInputSettings", requestData: { inputName, inputSettings: source.settings, overlay: true } } });
        }
      }
    }

    // Audio: compare with the input when it exists; a new input gets whatever the spec asks for.
    const exists = input !== undefined;
    if (source.muted !== undefined && (!exists || input.muted !== source.muted)) {
      actions.push({ target, change: source.muted ? "mute" : "unmute", phase: "change", request: { requestType: "SetInputMute", requestData: { inputName, inputMuted: source.muted } } });
    }
    if (source.volumeDb !== undefined && (!exists || Math.abs((toDb(input.volumeMul) ?? 0) - source.volumeDb) > 0.1)) {
      actions.push({ target, change: `volume ${source.volumeDb} dB`, phase: "change", request: { requestType: "SetInputVolume", requestData: { inputName, inputVolumeDb: source.volumeDb } } });
    }
    if (source.audioTracks !== undefined) {
      const wanted = Object.fromEntries(ALL_TRACKS.map((track) => [String(track), source.audioTracks!.includes(track)]));
      const differs = !exists || ALL_TRACKS.some((track) => (input.audioTracks?.[String(track)] === true) !== wanted[String(track)]);
      if (differs) {
        actions.push({
          target,
          change: source.audioTracks.length > 0 ? `audio tracks ${source.audioTracks.join(", ")}` : "no audio tracks",
          phase: "change",
          request: { requestType: "SetInputAudioTracks", requestData: { inputName, inputAudioTracks: wanted } },
        });
      }
    }

    // Scene item: needs the item's ID, so a new item's changes wait for the second pass.
    const selector = item ? { ...scene, sceneItemId: item.sceneItemId } : undefined;
    const itemAction = (change: string, requestType: string, data: JsonObject) => {
      actions.push({ target, change, phase: "change", ...(selector ? { request: { requestType, requestData: { ...selector, ...data } } } : {}) });
    };
    if (source.visible !== undefined && item && item.enabled !== source.visible) {
      itemAction(source.visible ? "show" : "hide", "SetSceneItemEnabled", { sceneItemEnabled: source.visible });
    }
    if (source.locked !== undefined && (!item || item.locked !== source.locked)) {
      itemAction(source.locked ? "lock" : "unlock", "SetSceneItemLocked", { sceneItemLocked: source.locked });
    }
    const transform: JsonObject = {
      ...(source.fit === "canvas" && current.video
        ? {
          boundsType: "OBS_BOUNDS_SCALE_INNER",
          boundsWidth: current.video.baseWidth,
          boundsHeight: current.video.baseHeight,
          boundsAlignment: 0,
          alignment: 5,
          positionX: 0,
          positionY: 0,
        }
        : {}),
      ...source.transform,
    };
    const changedFields = Object.keys(transform).filter((key) => !item || !same(transform[key], item.transform[key]));
    if (changedFields.length > 0) {
      itemAction(source.fit && changedFields.includes("boundsType") ? "fit to the canvas" : `transform (${changedFields.join(", ")})`, "SetSceneItemTransform", { sceneItemTransform: transform });
    }
  }

  const wantedNames = new Set(spec.sources.map(({ name }) => name));
  const others = current.items.filter(({ sourceName }) => !wantedNames.has(sourceName));
  if (spec.removeOthers) {
    for (const other of others) {
      actions.push({ target: other.sourceName, change: "remove from the scene", phase: "change", request: { requestType: "RemoveSceneItem", requestData: { ...scene, sceneItemId: other.sceneItemId } } });
    }
  }

  if (spec.order) {
    // Sources are listed back to front; items not in the spec stay behind them.
    const base = spec.removeOthers ? 0 : others.length;
    const allExist = spec.sources.every(({ name }) => current.items.some(({ sourceName }) => sourceName === name));
    const outOfPlace = spec.sources.some(({ name }, index) => current.items.find(({ sourceName }) => sourceName === name)?.index !== base + index);
    if (!allExist) {
      actions.push({ target: `scene ${sceneName}`, change: "order the sources back to front as listed", phase: "change" });
    } else if (outOfPlace) {
      spec.sources.forEach(({ name }, index) => {
        const item = current.items.find(({ sourceName }) => sourceName === name)!;
        actions.push({ target: name, change: `order (to ${base + index})`, phase: "change", request: { requestType: "SetSceneItemIndex", requestData: { ...scene, sceneItemId: item.sceneItemId, sceneItemIndex: base + index } } });
      });
    }
  }
  return { actions, errors };
}

function failures(actions: SceneAction[], results: BatchResult[]): string[] {
  return actions
    .map((action, index) => ({ action, result: results[index] }))
    .filter(({ result }) => !result?.ok)
    .map(({ action, result }) => `${action.target}: ${action.change} failed${result ? `: ${result.comment ?? `code ${result.code}`}` : " (skipped)"}`);
}

function describe(actions: SceneAction[]): string[] {
  return actions.map(({ target, change, request }) => `- ${target}: ${change}${request ? "" : " (once it exists)"}`);
}

async function blankItems(client: OBSWebSocketClient, sceneName: string, names: string[]): Promise<string[]> {
  const items = await client.sendRequest("GetSceneItemList", { sceneName }) as JsonObject;
  const blank = (Array.isArray(items.sceneItems) ? items.sceneItems : [])
    .filter(isObject)
    .filter((item) => names.includes(String(item.sourceName)) && item.sceneItemEnabled !== false)
    .filter((item) => isObject(item.sceneItemTransform) && (item.sceneItemTransform.sourceWidth === 0 || item.sceneItemTransform.sourceHeight === 0))
    .map((item) => ({ name: String(item.sourceName), inputKind: item.inputKind }));
  return withoutAudioOnly(client, blank);
}

export async function applyScene(client: OBSWebSocketClient, spec: SceneSpec, apply: boolean): Promise<CallToolResult> {
  const before = await readCurrent(client, spec);
  const first = planScene(spec, before);
  if (first.errors.length > 0) return errorResult(`Nothing changed:\n${first.errors.map((error) => `- ${error}`).join("\n")}`);
  if (first.actions.length === 0) {
    return { content: [{ type: "text", text: `Scene ${spec.sceneName} already matches` }], structuredContent: { actions: [], applied: false, converged: true } };
  }
  const listed = first.actions.map(({ target, change }) => ({ target, change }));
  if (!apply) {
    return {
      content: [{ type: "text", text: [`To make ${spec.sceneName} match:`, ...describe(first.actions), "Plan only; call again with apply: true"].join("\n") }],
      structuredContent: { actions: listed, applied: false },
    };
  }

  const snapshot = before.sceneExists
    ? await takeSnapshot(client, { scenes: [spec.sceneName], label: "before obs-apply-scene" }).catch(() => undefined)
    : undefined;

  // Pass 1 creates the scene, inputs, and items; pass 2 plans again with their IDs and changes the rest.
  const problems: string[] = [];
  const creations = first.actions.filter(({ phase }) => phase === "create");
  if (creations.length > 0) {
    const results = await client.sendBatch(creations.map(({ request }) => request!), { haltOnFailure: true });
    problems.push(...failures(creations, results));
    if (problems.length > 0) return errorResult([`Stopped while creating:`, ...problems].join("\n"));
  }
  const second = planScene(spec, await readCurrent(client, spec));
  const changes = second.actions.filter((action) => action.request);
  if (changes.length > 0) problems.push(...failures(changes, await client.sendBatch(changes.map(({ request }) => request!))));

  const remaining = planScene(spec, await readCurrent(client, spec)).actions;
  const blank = await blankItems(client, spec.sceneName, spec.sources.map(({ name }) => name)).catch(() => [] as string[]);
  const lines = [
    `Applied ${creations.length + changes.length - problems.length} change(s) to ${spec.sceneName}:`,
    ...describe([...creations, ...changes]),
    ...problems.map((problem) => `Failed: ${problem}`),
    ...(remaining.length > 0 ? ["Still different afterwards:", ...describe(remaining)] : ["OBS now matches the spec"]),
    ...blank.map((name) => `Warning: ${name} renders at 0×0; a capture may need a window, display, or permission`),
    ...(snapshot ? [`Saved the previous state as snapshot ${snapshot.id}; obs-restore undoes changes to existing sources`] : []),
  ];
  return {
    content: [{ type: "text", text: lines.join("\n") }],
    structuredContent: {
      actions: [...creations, ...changes].map(({ target, change }) => ({ target, change })),
      applied: true,
      failures: problems,
      converged: remaining.length === 0,
      remaining: remaining.map(({ target, change }) => ({ target, change })),
      blank,
      ...(snapshot ? { snapshotId: snapshot.id } : {}),
    },
    ...(problems.length > 0 || remaining.length > 0 ? { isError: true } : {}),
  };
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-apply-scene",
    {
      title: "Apply Scene",
      description: "Describe a scene and let the server make OBS match it: creates the scene, inputs, and items "
        + "that are missing, and sets only the settings, visibility, lock, audio, transforms, and order that "
        + "differ. Without apply it returns the plan. Applies in two request batches, checks the result, warns "
        + "about sources that render at 0×0, and saves a snapshot first so obs-restore can undo changes to "
        + "existing sources",
      inputSchema: z.object({
        sceneName: z.string().min(1),
        sources: z.array(SourceSpec).min(1).max(50).describe("Sources in the scene, listed back to front"),
        order: z.boolean().default(false).describe("Stack the sources in the listed order, in front of any others"),
        removeOthers: z.boolean().default(false).describe("Remove items not listed from this scene (their inputs remain)"),
        apply: z.boolean().default(false).describe("Make the changes; otherwise only plan"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    },
    async ({ apply, ...spec }): Promise<CallToolResult> => {
      try {
        return await applyScene(client, spec, apply);
      } catch (error) {
        return errorResult(`Apply Scene failed: ${errorMessage(error)}`);
      }
    },
  );
}
