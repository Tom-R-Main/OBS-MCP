/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { BatchRequest, OBSWebSocketClient } from "../client.js";

type JsonObject = Record<string, unknown>;

/** Saved state of one input: its settings and audio. Audio fields are absent for video-only inputs. */
export type InputState = {
  inputName: string;
  inputKind: string;
  inputSettings: JsonObject;
  muted?: boolean;
  volumeMul?: number;
  audioTracks?: JsonObject;
};

/** Saved state of one scene item. */
export type ItemState = {
  sceneName: string;
  sceneItemId: number;
  sourceName: string;
  index: number;
  enabled: boolean;
  locked: boolean;
  transform: JsonObject;
};

export type Snapshot = {
  id: string;
  label?: string;
  takenAt: string;
  inputs: InputState[];
  items: ItemState[];
};

/** A change obs-restore would make, and the request that makes it. */
export type RestoreChange = { target: string; field: string; request: BatchRequest };

const MAX_SNAPSHOTS = 10;

/** The transform fields SetSceneItemTransform accepts; the rest are computed by OBS. */
const WRITABLE_TRANSFORM = [
  "positionX", "positionY", "rotation", "scaleX", "scaleY", "alignment",
  "boundsType", "boundsAlignment", "boundsWidth", "boundsHeight",
  "cropLeft", "cropRight", "cropTop", "cropBottom", "cropToBounds",
];

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

/** Deep equality for JSON values, with a tolerance for floating-point numbers. */
export function same(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") return Math.abs(a - b) < 1e-6;
  if (Array.isArray(a) && Array.isArray(b)) return a.length === b.length && a.every((value, index) => same(value, b[index]));
  if (isObject(a) && isObject(b)) {
    const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
    return [...keys].every((key) => same(a[key], b[key]));
  }
  return a === b;
}

function pick(object: JsonObject, keys: string[]): JsonObject {
  return Object.fromEntries(keys.filter((key) => key in object).map((key) => [key, object[key]]));
}

const snapshotsByClient = new WeakMap<OBSWebSocketClient, Snapshot[]>();

function snapshots(client: OBSWebSocketClient): Snapshot[] {
  let list = snapshotsByClient.get(client);
  if (!list) {
    list = [];
    snapshotsByClient.set(client, list);
  }
  return list;
}

async function batch(client: OBSWebSocketClient, requests: BatchRequest[]) {
  return client.sendBatch(requests);
}

export async function readItems(client: OBSWebSocketClient, sceneNames: string[]): Promise<ItemState[]> {
  const lists = await batch(client, sceneNames.map((sceneName) => ({ requestType: "GetSceneItemList", requestData: { sceneName } })));
  const items: ItemState[] = [];
  sceneNames.forEach((sceneName, sceneIndex) => {
    const result = lists[sceneIndex];
    if (!result?.ok) throw new Error(`Cannot read scene ${sceneName}: ${result?.comment ?? `code ${result?.code}`}`);
    const sceneItems = isObject(result.responseData) && Array.isArray(result.responseData.sceneItems)
      ? result.responseData.sceneItems.filter(isObject)
      : [];
    for (const item of sceneItems) {
      if (typeof item.sceneItemId !== "number") continue;
      items.push({
        sceneName,
        sceneItemId: item.sceneItemId,
        sourceName: String(item.sourceName ?? ""),
        index: Number(item.sceneItemIndex ?? 0),
        enabled: item.sceneItemEnabled !== false,
        locked: item.sceneItemLocked === true,
        transform: isObject(item.sceneItemTransform) ? pick(item.sceneItemTransform, WRITABLE_TRANSFORM) : {},
      });
    }
  });
  return items;
}

export async function readInputs(client: OBSWebSocketClient, inputNames: string[]): Promise<InputState[]> {
  const per = 4;
  const results = await batch(client, inputNames.flatMap((inputName) => [
    { requestType: "GetInputSettings", requestData: { inputName } },
    { requestType: "GetInputMute", requestData: { inputName } },
    { requestType: "GetInputVolume", requestData: { inputName } },
    { requestType: "GetInputAudioTracks", requestData: { inputName } },
  ]));
  return inputNames.flatMap((inputName, index) => {
    const [settings, mute, volume, tracks] = results.slice(index * per, index * per + per);
    if (!settings?.ok || !isObject(settings.responseData)) return [];
    const data = (result: typeof mute) => (result?.ok && isObject(result.responseData) ? result.responseData : undefined);
    const muteData = data(mute);
    const volumeData = data(volume);
    const trackData = data(tracks);
    return [{
      inputName,
      inputKind: String(settings.responseData.inputKind ?? ""),
      inputSettings: isObject(settings.responseData.inputSettings) ? settings.responseData.inputSettings : {},
      ...(typeof muteData?.inputMuted === "boolean" ? { muted: muteData.inputMuted } : {}),
      ...(typeof volumeData?.inputVolumeMul === "number" ? { volumeMul: volumeData.inputVolumeMul } : {}),
      ...(isObject(trackData?.inputAudioTracks) ? { audioTracks: trackData.inputAudioTracks } : {}),
    }];
  });
}

/**
 * Saves the state of the given scenes' items and the given inputs. With
 * neither, saves the program scene and every input shown in it.
 */
export async function takeSnapshot(
  client: OBSWebSocketClient,
  options: { scenes?: string[]; inputs?: string[]; label?: string },
): Promise<Snapshot> {
  let scenes = options.scenes;
  if (!scenes && !options.inputs) {
    const program = await client.sendRequest("GetCurrentProgramScene") as JsonObject;
    const name = program.currentProgramSceneName ?? program.sceneName;
    if (typeof name !== "string") throw new Error("OBS did not report the program scene");
    scenes = [name];
  }
  const items = await readItems(client, scenes ?? []);
  let inputNames = options.inputs;
  if (!inputNames) {
    // Only sources that are inputs have settings; nested scenes and groups do not.
    const { inputs } = await client.sendRequest("GetInputList") as { inputs?: unknown[] };
    const known = new Set((inputs ?? []).filter(isObject).map(({ inputName }) => String(inputName)));
    inputNames = [...new Set(items.map(({ sourceName }) => sourceName))].filter((name) => known.has(name));
  }
  const snapshot: Snapshot = {
    id: crypto.randomUUID().slice(0, 8),
    ...(options.label ? { label: options.label } : {}),
    takenAt: new Date().toISOString(),
    inputs: await readInputs(client, inputNames),
    items,
  };
  const list = snapshots(client);
  list.push(snapshot);
  if (list.length > MAX_SNAPSHOTS) list.shift();
  return snapshot;
}

/** The requests that return OBS to a snapshot, and what cannot be restored. */
export async function planRestore(
  client: OBSWebSocketClient,
  snapshot: Snapshot,
): Promise<{ changes: RestoreChange[]; missing: string[] }> {
  const sceneNames = [...new Set(snapshot.items.map(({ sceneName }) => sceneName))];
  const [currentItems, currentInputs] = await Promise.all([
    readItems(client, sceneNames).catch(() => [] as ItemState[]),
    readInputs(client, snapshot.inputs.map(({ inputName }) => inputName)),
  ]);
  const changes: RestoreChange[] = [];
  const missing: string[] = [];

  for (const saved of snapshot.inputs) {
    const current = currentInputs.find(({ inputName }) => inputName === saved.inputName);
    const target = `input ${saved.inputName}`;
    if (!current) {
      missing.push(`${target} no longer exists`);
      continue;
    }
    const inputName = saved.inputName;
    if (!same(saved.inputSettings, current.inputSettings)) {
      // Without overlay, OBS resets the settings to defaults before applying these, which is what was saved.
      changes.push({ target, field: "settings", request: { requestType: "SetInputSettings", requestData: { inputName, inputSettings: saved.inputSettings, overlay: false } } });
    }
    if (saved.muted !== undefined && saved.muted !== current.muted) {
      changes.push({ target, field: saved.muted ? "mute" : "unmute", request: { requestType: "SetInputMute", requestData: { inputName, inputMuted: saved.muted } } });
    }
    if (saved.volumeMul !== undefined && !same(saved.volumeMul, current.volumeMul)) {
      changes.push({ target, field: "volume", request: { requestType: "SetInputVolume", requestData: { inputName, inputVolumeMul: saved.volumeMul } } });
    }
    if (saved.audioTracks !== undefined && !same(saved.audioTracks, current.audioTracks)) {
      changes.push({ target, field: "audio tracks", request: { requestType: "SetInputAudioTracks", requestData: { inputName, inputAudioTracks: saved.audioTracks } } });
    }
  }

  for (const saved of snapshot.items) {
    const current = currentItems.find(({ sceneName, sceneItemId }) => sceneName === saved.sceneName && sceneItemId === saved.sceneItemId);
    const target = `${saved.sceneName} › ${saved.sourceName} (#${saved.sceneItemId})`;
    if (!current) {
      missing.push(`${target} was removed`);
      continue;
    }
    const selector = { sceneName: saved.sceneName, sceneItemId: saved.sceneItemId };
    if (!same(saved.transform, current.transform)) {
      changes.push({ target, field: "transform", request: { requestType: "SetSceneItemTransform", requestData: { ...selector, sceneItemTransform: saved.transform } } });
    }
    if (saved.enabled !== current.enabled) {
      changes.push({ target, field: saved.enabled ? "show" : "hide", request: { requestType: "SetSceneItemEnabled", requestData: { ...selector, sceneItemEnabled: saved.enabled } } });
    }
    if (saved.locked !== current.locked) {
      changes.push({ target, field: saved.locked ? "lock" : "unlock", request: { requestType: "SetSceneItemLocked", requestData: { ...selector, sceneItemLocked: saved.locked } } });
    }
  }

  // Reorder last, bottom to top, so earlier moves do not shift later ones.
  const moved = snapshot.items
    .filter((saved) => {
      const current = currentItems.find(({ sceneName, sceneItemId }) => sceneName === saved.sceneName && sceneItemId === saved.sceneItemId);
      return current && current.index !== saved.index;
    })
    .sort((a, b) => a.index - b.index);
  for (const saved of moved) {
    changes.push({
      target: `${saved.sceneName} › ${saved.sourceName} (#${saved.sceneItemId})`,
      field: `order (to ${saved.index})`,
      request: { requestType: "SetSceneItemIndex", requestData: { sceneName: saved.sceneName, sceneItemId: saved.sceneItemId, sceneItemIndex: saved.index } },
    });
  }

  for (const sceneName of sceneNames) {
    const savedIds = new Set(snapshot.items.filter((item) => item.sceneName === sceneName).map(({ sceneItemId }) => sceneItemId));
    for (const item of currentItems.filter((current) => current.sceneName === sceneName && !savedIds.has(current.sceneItemId))) {
      missing.push(`${sceneName} › ${item.sourceName} (#${item.sceneItemId}) was added since; it is left in place`);
    }
  }
  return { changes, missing };
}

function describeSnapshot(snapshot: Snapshot): string {
  const scenes = [...new Set(snapshot.items.map(({ sceneName }) => sceneName))];
  return `${snapshot.id}${snapshot.label ? ` "${snapshot.label}"` : ""} at ${snapshot.takenAt}: `
    + `${snapshot.inputs.length} input(s), ${snapshot.items.length} item(s) in ${scenes.length > 0 ? scenes.join(", ") : "no scenes"}`;
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-snapshot",
    {
      title: "Snapshot Scene State",
      description: "Save the current settings, mute, volume, and audio tracks of inputs, and the transform, "
        + "visibility, lock, and order of scene items, so obs-restore can undo later changes. Defaults to the "
        + "program scene and the inputs in it. Snapshots live in this server's memory (the last 10); call with "
        + "list: true to see them",
      inputSchema: z.object({
        scenes: z.array(z.string()).optional().describe("Scenes whose items to save"),
        inputs: z.array(z.string()).optional().describe("Inputs to save; defaults to the inputs in the saved scenes"),
        label: z.string().optional().describe("A name to recognize the snapshot by"),
        list: z.boolean().default(false).describe("List saved snapshots instead of taking one"),
      }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async (args): Promise<CallToolResult> => {
      if (args.list) {
        const saved = snapshots(client);
        return {
          content: [{ type: "text", text: saved.length > 0 ? saved.map(describeSnapshot).join("\n") : "No snapshots saved" }],
          structuredContent: { snapshots: saved.map(({ id, label, takenAt, inputs, items }) => ({ id, label, takenAt, inputs: inputs.length, items: items.length })) },
        };
      }
      try {
        const snapshot = await takeSnapshot(client, args);
        return {
          content: [{ type: "text", text: `Saved snapshot ${describeSnapshot(snapshot)}. Undo later changes with obs-restore` }],
          structuredContent: { snapshotId: snapshot.id, takenAt: snapshot.takenAt, inputs: snapshot.inputs.map(({ inputName }) => inputName), items: snapshot.items.length },
        };
      } catch (error) {
        return errorResult(`Snapshot failed: ${errorMessage(error)}`);
      }
    },
  );

  server.registerTool(
    "obs-restore",
    {
      title: "Restore Scene State",
      description: "Return inputs and scene items to a snapshot from obs-snapshot, changing only what differs, "
        + "in one request batch. dryRun lists the changes without making them. Removed items and inputs cannot "
        + "be recreated, and items added since are left in place; both are reported",
      inputSchema: z.object({
        snapshotId: z.string().optional().describe("Snapshot to restore; defaults to the most recent"),
        dryRun: z.boolean().default(false).describe("Only list the changes"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    },
    async ({ snapshotId, dryRun }): Promise<CallToolResult> => {
      const saved = snapshots(client);
      const snapshot = snapshotId ? saved.find(({ id }) => id === snapshotId) : saved.at(-1);
      if (!snapshot) {
        return errorResult(snapshotId ? `No snapshot ${snapshotId}; list them with obs-snapshot list: true` : "No snapshots saved; take one with obs-snapshot");
      }
      try {
        const { changes, missing } = await planRestore(client, snapshot);
        const lines = changes.map(({ target, field }) => `- ${target}: ${field}`);
        const notes = missing.map((note) => `Not restored: ${note}`);
        const listed = changes.map(({ target, field }) => ({ target, field }));
        if (changes.length === 0) {
          return {
            content: [{ type: "text", text: [`OBS already matches snapshot ${snapshot.id}`, ...notes].join("\n") }],
            structuredContent: { snapshotId: snapshot.id, changes: [], missing, applied: false },
          };
        }
        if (dryRun) {
          return {
            content: [{ type: "text", text: [`Restoring snapshot ${snapshot.id} would change:`, ...lines, ...notes].join("\n") }],
            structuredContent: { snapshotId: snapshot.id, changes: listed, missing, applied: false },
          };
        }
        const results = await client.sendBatch(changes.map(({ request }) => request));
        const failures = changes
          .map((change, index) => ({ change, result: results[index] }))
          .filter(({ result }) => !result?.ok)
          .map(({ change, result }) => `${change.target}: ${change.field} failed${result?.comment ? `: ${result.comment}` : ""}`);
        return {
          content: [{
            type: "text",
            text: [
              `Restored snapshot ${snapshot.id}: ${changes.length - failures.length} of ${changes.length} change(s)`,
              ...lines,
              ...failures.map((failure) => `Failed: ${failure}`),
              ...notes,
            ].join("\n"),
          }],
          structuredContent: { snapshotId: snapshot.id, changes: listed, missing, failures, applied: true },
          ...(failures.length > 0 ? { isError: true } : {}),
        };
      } catch (error) {
        return errorResult(`Restore failed: ${errorMessage(error)}`);
      }
    },
  );
}
