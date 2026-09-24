/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import { closeSync, mkdirSync, openSync, readFileSync, renameSync, rmSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { BatchRequest, OBSWebSocketClient } from "../client.js";
import { logger } from "../logger.js";
import { stateDirectory } from "../state-dir.js";

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
  /** The OBS WebSocket URL the snapshot was taken from. */
  obsUrl?: string;
  /** Taken automatically before a change, not by a user. */
  auto?: boolean;
  label?: string;
  takenAt: string;
  inputs: InputState[];
  items: ItemState[];
};

/** A change obs-restore would make, and the request that makes it. */
export type RestoreChange = { target: string; field: string; request: BatchRequest };

/** Kept on disk across every OBS instance, newest last: snapshots users take, and automatic ones. */
const MAX_SNAPSHOTS = 20;
const MAX_AUTO_SNAPSHOTS = 10;
const LOCK_WAIT_MS = 2_000;
const LOCK_STALE_MS = 10_000;
const SNAPSHOT_FILE = "snapshots.json";

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
/**
 * OBS keeps transforms as 32-bit floats, so 100.1 reads back as 100.09999847.
 * Two values match when they are the same 32-bit float.
 */
export function sameFloat32(a: unknown, b: unknown): boolean {
  if (typeof a === "number" && typeof b === "number") return Math.fround(a) === Math.fround(b) || Math.abs(a - b) < 1e-6;
  return same(a, b);
}

/** OBS rounds scene item positions to half a pixel (204.37 reads back as 204.5, observed on OBS 32.2.2). */
const POSITION_FIELDS = new Set(["positionX", "positionY"]);
const toHalfPixel = (value: number) => Math.round(value * 2) / 2;

/**
 * Whether two transform values are the same once OBS stores them: positions
 * on the same half pixel, other numbers as the same 32-bit float. A real
 * half-pixel move (204 to 204.5) still counts as a difference.
 */
export function sameTransformValue(key: string, a: unknown, b: unknown): boolean {
  if (POSITION_FIELDS.has(key) && typeof a === "number" && typeof b === "number") return toHalfPixel(a) === toHalfPixel(b);
  return sameFloat32(a, b);
}

export function sameTransform(a: JsonObject, b: JsonObject): boolean {
  const keys = new Set([...Object.keys(a), ...Object.keys(b)]);
  return [...keys].every((key) => sameTransformValue(key, a[key], b[key]));
}

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

/**
 * Snapshots are saved in the state directory so obs-restore works after the
 * server restarts or from another agent's session. The file holds input
 * settings, so it is readable only by its owner.
 */
function snapshotFile(): string {
  return join(stateDirectory(), SNAPSHOT_FILE);
}

function loadAll(): Snapshot[] {
  const path = snapshotFile();
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return [];
  }
  try {
    const parsed: unknown = JSON.parse(text);
    if (!Array.isArray(parsed)) throw new Error("not a list");
    return parsed.filter((entry): entry is Snapshot => isObject(entry) && typeof entry.id === "string");
  } catch (error) {
    // Keep the unreadable file instead of letting the next save replace the whole history.
    const backup = `${path}.unreadable-${Date.now()}`;
    try {
      renameSync(path, backup);
      logger.error(`${path} could not be read (${error instanceof Error ? error.message : String(error)}); moved it to ${backup}`);
    } catch {
      // Another server moved it first.
    }
    return [];
  }
}

/** Automatic snapshots (taken before a change) are capped separately, so they never push out ones a user took. */
function prune(list: Snapshot[]): Snapshot[] {
  const autoKept = new Set(list.filter(({ auto }) => auto).slice(-MAX_AUTO_SNAPSHOTS));
  const userKept = new Set(list.filter(({ auto }) => !auto).slice(-MAX_SNAPSHOTS));
  return list.filter((snapshot) => autoKept.has(snapshot) || userKept.has(snapshot));
}

function sleepSync(ms: number): void {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

/**
 * Runs `change` with the snapshot file locked, so two server processes saving
 * at once cannot lose each other's snapshot. A lock left by a crashed process
 * is broken after LOCK_STALE_MS.
 */
function withFileLock(change: () => void): void {
  const lock = `${snapshotFile()}.lock`;
  mkdirSync(stateDirectory(), { recursive: true, mode: 0o700 });
  const deadline = Date.now() + LOCK_WAIT_MS;
  for (;;) {
    try {
      closeSync(openSync(lock, "wx", 0o600));
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") throw error;
      try {
        if (Date.now() - statSync(lock).mtimeMs > LOCK_STALE_MS) rmSync(lock, { force: true });
      } catch {
        // Released meanwhile.
      }
      if (Date.now() > deadline) throw new Error(`the snapshot file is locked by another process (${lock})`);
      sleepSync(25);
    }
  }
  try {
    change();
  } finally {
    rmSync(lock, { force: true });
  }
}

function saveSnapshot(snapshot: Snapshot): void {
  const path = snapshotFile();
  try {
    withFileLock(() => {
      const list = prune([...loadAll(), snapshot]);
      const temporary = `${path}.${process.pid}.tmp`;
      writeFileSync(temporary, `${JSON.stringify(list, null, 2)}\n`, { mode: 0o600 });
      renameSync(temporary, path);
    });
  } catch (error) {
    logger.error(`Could not save snapshots to ${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
}

/** Snapshots of the OBS instance this client connects to, oldest first. */
function snapshots(client: OBSWebSocketClient): Snapshot[] {
  const url = client.getConnectionStatus().url;
  return loadAll().filter(({ obsUrl }) => obsUrl === undefined || obsUrl === url);
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
  options: { scenes?: string[]; inputs?: string[]; label?: string; auto?: boolean },
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
    obsUrl: client.getConnectionStatus().url,
    ...(options.auto ? { auto: true } : {}),
    ...(options.label ? { label: options.label } : {}),
    takenAt: new Date().toISOString(),
    inputs: await readInputs(client, inputNames),
    items,
  };
  saveSnapshot(snapshot);
  return snapshot;
}

/** The requests that return OBS to a snapshot, and what cannot be restored. */
export async function planRestore(
  client: OBSWebSocketClient,
  snapshot: Snapshot,
): Promise<{ changes: RestoreChange[]; missing: string[] }> {
  const sceneNames = [...new Set(snapshot.items.map(({ sceneName }) => sceneName))];
  // Read each scene on its own, so one deleted scene does not hide the others.
  const [perScene, currentInputs] = await Promise.all([
    Promise.all(sceneNames.map((sceneName) => readItems(client, [sceneName]).then((items) => ({ sceneName, items }), () => ({ sceneName, items: null })))),
    readInputs(client, snapshot.inputs.map(({ inputName }) => inputName)),
  ]);
  const readableScenes = perScene.filter(({ items }) => items !== null).map(({ sceneName }) => sceneName);
  const currentItems = perScene.flatMap(({ items }) => items ?? []);
  const changes: RestoreChange[] = [];
  const missing: string[] = perScene
    .filter(({ items }) => items === null)
    .map(({ sceneName }) => `scene ${sceneName} no longer exists; its items were not restored`);

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

  for (const saved of snapshot.items.filter(({ sceneName }) => readableScenes.includes(sceneName))) {
    const current = currentItems.find(({ sceneName, sceneItemId }) => sceneName === saved.sceneName && sceneItemId === saved.sceneItemId);
    const target = `${saved.sceneName} › ${saved.sourceName} (#${saved.sceneItemId})`;
    if (!current) {
      missing.push(`${target} was removed`);
      continue;
    }
    const selector = { sceneName: saved.sceneName, sceneItemId: saved.sceneItemId };
    if (!sameTransform(saved.transform, current.transform)) {
      changes.push({ target, field: "transform", request: { requestType: "SetSceneItemTransform", requestData: { ...selector, sceneItemTransform: saved.transform } } });
    }
    if (saved.enabled !== current.enabled) {
      changes.push({ target, field: saved.enabled ? "show" : "hide", request: { requestType: "SetSceneItemEnabled", requestData: { ...selector, sceneItemEnabled: saved.enabled } } });
    }
    if (saved.locked !== current.locked) {
      changes.push({ target, field: saved.locked ? "lock" : "unlock", request: { requestType: "SetSceneItemLocked", requestData: { ...selector, sceneItemLocked: saved.locked } } });
    }
  }

  // Order last. The saved items that survive go back into the positions they
  // occupy now, in their saved order, so items added since keep their place.
  // Moving every item of the scene to its final index, bottom to top, gives
  // that order whatever moves OBS makes along the way.
  for (const sceneName of readableScenes) {
    const now = currentItems.filter((item) => item.sceneName === sceneName).sort((a, b) => a.index - b.index);
    const survivors = snapshot.items
      .filter((saved) => saved.sceneName === sceneName && now.some(({ sceneItemId }) => sceneItemId === saved.sceneItemId))
      .sort((a, b) => a.index - b.index);
    const survivorIds = new Set(survivors.map(({ sceneItemId }) => sceneItemId));
    let next = 0;
    const target = now.map((item) => {
      if (!survivorIds.has(item.sceneItemId)) return item;
      const wanted = survivors[next++]!;
      return now.find(({ sceneItemId }) => sceneItemId === wanted.sceneItemId)!;
    });
    if (target.every((item, index) => item.sceneItemId === now[index]!.sceneItemId)) continue;
    target.forEach((item, index) => {
      changes.push({
        target: `${sceneName} › ${item.sourceName} (#${item.sceneItemId})`,
        field: `order (to ${index})`,
        request: { requestType: "SetSceneItemIndex", requestData: { sceneName, sceneItemId: item.sceneItemId, sceneItemIndex: index } },
      });
    });
  }

  for (const sceneName of readableScenes) {
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
        + "program scene and the inputs in it. Snapshots are saved on disk (the last 20) and survive restarts; call "
        + "with list: true to see them",
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
