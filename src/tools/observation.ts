/* SPDX-License-Identifier: GPL-2.0-only */
import { normalizeSceneItem } from "./scene-identity.js";
import crypto from "node:crypto";
import { OBSConnectionChangedError, OBSRequestError, type OBSWebSocketClient } from "../client.js";

export type Obj = Record<string, unknown>;
export const object = (x: unknown): x is Obj => typeof x === "object" && x !== null && !Array.isArray(x);
export const obj = (x: unknown): Obj => { if (!object(x)) throw new ObservationError("evidence_unavailable", "OBS returned incomplete state"); return x; };
export const list = (x: unknown, max = 256): Obj[] => {
  if (!Array.isArray(x) || x.length > max || !x.every(object)) throw new ObservationError("evidence_unavailable", "OBS returned an incomplete or oversized inventory");
  return x;
};
export const identity = (x: unknown): string => {
  if (typeof x !== "string" || !x || x.length > 256) throw new ObservationError("evidence_unavailable", "OBS did not report stable identity");
  return x;
};
export const signature = (x: unknown): string => JSON.stringify(x, (_key, value: unknown) => object(value)
  ? Object.fromEntries(Object.entries(value).sort(([a], [b]) => a.localeCompare(b))) : value);
export const digest = (x: unknown): string => crypto.createHash("sha256").update(signature(x)).digest("hex");
export class ObservationError extends Error {
  constructor(readonly reason: string, message: string) { super(message); }
}

/** Bounds both the number and duration of reads, and fences late replies. Call close in finally. */
export class Observation {
  readonly started = Date.now();
  readonly observedAt = new Date(this.started).toISOString();
  readonly generation: number;
  requests = 0;
  private invalidated = false;
  private closed = false;
  constructor(private readonly client: OBSWebSocketClient, private readonly signal?: AbortSignal) {
    this.generation = client.getConnectionStatus().generation;
    client.on("disconnected", this.invalidate);
    client.on("event", this.onEvent);
  }
  private invalidate = () => { this.invalidated = true; };
  private onEvent = (type: string) => {
    // Meter samples are telemetry, not any field in the structural snapshot.
    // Keep settings/mute/track and other Input changes invalidating evidence.
    if (type === "InputVolumeMeters") return;
    if (/^(CurrentSceneCollection|SceneCollection|Canvas|Scene|Input|SourceFilter|VideoSettings|CurrentProgramScene|CurrentPreviewScene|StudioMode|CurrentProfile|Profile|RecordState|StreamState|ReplayBufferState|VirtualcamState)/.test(type)) this.invalidate();
  };
  check(): void {
    if (this.signal?.aborted) throw new ObservationError("cancelled", "Operation cancelled; inspect OBS before any further action");
    if (this.closed || this.invalidated || !this.client.isConnected() || this.client.getConnectionStatus().generation !== this.generation) {
      throw new ObservationError("observation_invalidated", "OBS changed during observation; run obs-observe again");
    }
    if (Date.now() - this.started >= 12_000) throw new ObservationError("observation_limit", "Observation exceeded its 12 second budget");
  }
  async read(type: string, data?: Obj): Promise<Obj> {
    this.check();
    if (++this.requests > 256) throw new ObservationError("observation_limit", "Observation exceeded its 256 request budget");
    if (this.client.supportsRequest(type) === false) throw new ObservationError("capability_unavailable", `${type} is unavailable; inspect obs-get-version`);
    const timeout = Math.min(2_000, 12_000 - (Date.now() - this.started));
    let result: unknown;
    try { result = await this.client.sendRequest(type, data, timeout); }
    catch (error) { this.check(); throw error; }
    this.check();
    const response = obj(result);
    if (type !== "GetSourceScreenshot" && Buffer.byteLength(JSON.stringify(response)) > 256 * 1024) throw new ObservationError("observation_limit", `Oversized ${type} evidence`);
    return response;
  }
  close(): void {
    this.closed = true;
    this.client.off("disconnected", this.invalidate);
    this.client.off("event", this.onEvent);
  }
}
export function failure(error: unknown): Obj {
  return { reason: error instanceof ObservationError ? error.reason : error instanceof OBSConnectionChangedError ? "observation_invalidated" : error instanceof OBSRequestError && error.code === 604 ? "capability_unavailable" : "evidence_unavailable", message: error instanceof Error ? error.message : String(error), nextCheck: "Run obs-observe and inspect the named target before retrying" };
}

export type Target = { sceneCollection: string; sceneName: string; canvasUuid?: string };
export type SceneIdentity = { sceneName: string; sceneUuid: string };
export function canvas(target: { canvasUuid?: string }): Obj { return target.canvasUuid === undefined ? {} : { canvasUuid: target.canvasUuid }; }
export async function sceneInventory(sample: Observation, target: Target) {
  if (target.canvasUuid !== undefined) throw new ObservationError("capability_unavailable", "This workflow requires the default canvas: the pinned video-settings and transition contracts do not provide canvas-scoped operations");
  const collection = identity((await sample.read("GetSceneCollectionList")).currentSceneCollectionName);
  if (collection !== target.sceneCollection) throw new ObservationError("target_changed", "Selected scene collection is not current");
  const scenes = await sample.read("GetSceneList", canvas(target));
  const inventory = list(scenes.scenes);
  const ids = inventory.map((s) => identity(s.sceneUuid));
  inventory.forEach((s) => identity(s.sceneName));
  if (new Set(ids).size !== ids.length) throw new ObservationError("target_changed", "Duplicate scene identities");
  const selected = inventory.filter((s) => s.sceneName === target.sceneName);
  if (selected.length !== 1) throw new ObservationError("target_changed", "Selected scene is missing or ambiguous");
  return { collection, scenes, selected: { sceneName: identity(selected[0]!.sceneName), sceneUuid: identity(selected[0]!.sceneUuid) } };
}

/** Flat layout inventory, with exact occurrence identity. Never guesses nested bindings. */
export async function flatScene(sample: Observation, target: Target) {
  const inventory = await sceneInventory(sample, target);
  const video = await sample.read("GetVideoSettings", canvas(target));
  const inputs = list((await sample.read("GetInputList")).inputs, 2048);
  const items = list((await sample.read("GetSceneItemList", { ...canvas(target), sceneUuid: inventory.selected.sceneUuid })).sceneItems, 128).map(normalizeSceneItem);
  if (new Set(items.map((item) => item.sceneItemId)).size !== items.length) throw new ObservationError("target_changed", "Duplicate scene item identities");
  for (const item of items) {
    identity(item.sourceName); identity(item.sourceUuid);
    if (!Number.isSafeInteger(item.sceneItemId) || Number(item.sceneItemId) < 0 || typeof item.isGroup !== "boolean") throw new ObservationError("evidence_unavailable", "Incomplete scene item identity");
  }
  return { ...inventory, video, inputs, items };
}
