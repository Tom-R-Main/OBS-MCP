/* SPDX-License-Identifier: GPL-2.0-only */
import crypto from "node:crypto";
import { closeSync, fsyncSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { stateDirectory } from "../state-dir.js";
import { readInputs, same, sameTransformValue, takeSnapshot } from "./snapshots.js";

type Obj = Record<string, unknown>;
type Selection = { sceneCollection: string; sceneName: string; inputName: string };
type State = Selection & { sceneUuid: string; inputUuid: string; sceneItemId: number; settings: Obj; muted: boolean; tracks: Obj; transform: Obj };
type Field = { field: "muted" | "tracks" | "transform"; key?: string; before: unknown; after: unknown };
type Journal = { version: 1; operationId: string; obsUrl: string; snapshotId: string; state: State; changes: Field[]; attempted: number; recovered?: boolean };
const FIT = ["boundsType", "boundsWidth", "boundsHeight", "boundsAlignment", "alignment", "positionX", "positionY", "rotation"];
const busy = new WeakSet<OBSWebSocketClient>();
const object = (x: unknown): x is Obj => typeof x === "object" && x !== null && !Array.isArray(x);
const obj = (x: unknown): Obj => { if (!object(x)) throw new Error("OBS returned incomplete state"); return x; };
const list = (x: unknown): Obj[] => { if (!Array.isArray(x) || !x.every(object)) throw new Error("OBS returned an incomplete list"); return x; };
const string = (x: unknown): string => { if (typeof x !== "string" || !x) throw new Error("OBS did not report stable object identity"); return x; };
const message = (e: unknown) => e instanceof Error ? e.message : String(e);
function result(status: string, details: Obj = {}, error = false): CallToolResult {
  return { content: [{ type: "text", text: JSON.stringify({ status, ...details }) }], structuredContent: { status, ...details }, ...(error ? { isError: true } : {}) };
}
function journalDirectory(): string { return join(stateDirectory(), "demo-recovery"); }
function persist(journal: Journal): void {
  mkdirSync(journalDirectory(), { recursive: true, mode: 0o700 });
  const path = join(journalDirectory(), `${journal.operationId}.json`);
  const temporary = `${path}.${process.pid}.${crypto.randomUUID()}.tmp`;
  const fd = openSync(temporary, "wx", 0o600);
  try { writeFileSync(fd, JSON.stringify(journal)); fsyncSync(fd); } finally { closeSync(fd); }
  renameSync(temporary, path);
  const directory = openSync(journalDirectory(), "r");
  try { fsyncSync(directory); } finally { closeSync(directory); }
  const parent = openSync(stateDirectory(), "r");
  try { fsyncSync(parent); } finally { closeSync(parent); }
}
function readJournal(id: string): Journal {
  if (!z.uuid().safeParse(id).success) throw new Error("Invalid operationId");
  const parsed = JSON.parse(readFileSync(join(journalDirectory(), `${id}.json`), "utf8")) as Journal;
  if (parsed.version !== 1 || parsed.operationId !== id || typeof parsed.obsUrl !== "string" || typeof parsed.snapshotId !== "string"
    || !Array.isArray(parsed.changes) || !Number.isInteger(parsed.attempted) || parsed.attempted < 0 || parsed.attempted > parsed.changes.length || !object(parsed.state)
    || !["sceneCollection", "sceneName", "inputName", "sceneUuid", "inputUuid"].every((key) => typeof (parsed.state as unknown as Obj)[key] === "string")
    || !Number.isInteger(parsed.state.sceneItemId) || !object(parsed.state.settings)
    || typeof parsed.state.muted !== "boolean" || !object(parsed.state.tracks) || !object(parsed.state.transform)
    || parsed.changes.some((change) => !object(change) || !validField(change))) throw new Error("Invalid recovery journal; manual investigation required");
  return parsed;
}
function validField(change: Obj): boolean {
  if (change.field === "muted") return typeof change.before === "boolean" && typeof change.after === "boolean";
  if (change.field === "tracks") return [change.before, change.after].every((value) => object(value)
    && Object.keys(value).length === 6 && [1, 2, 3, 4, 5, 6].every((n) => typeof value[String(n)] === "boolean"));
  return change.field === "transform" && typeof change.key === "string" && FIT.includes(change.key)
    && (change.key === "boundsType" ? [change.before, change.after].every((value) => typeof value === "string" && value.startsWith("OBS_BOUNDS_"))
      : [change.before, change.after].every((value) => typeof value === "number" && Number.isFinite(value)));
}
function pending(client: OBSWebSocketClient, selection: Selection): Journal | undefined {
  let names: string[];
  try { names = readdirSync(journalDirectory()); } catch (e) { if ((e as NodeJS.ErrnoException).code === "ENOENT") return; throw e; }
  for (const name of names.filter((name) => name.endsWith(".json"))) {
    const journal = readJournal(name.slice(0, -5));
    if (!journal.recovered && journal.obsUrl === client.getConnectionStatus().url && journal.state.sceneCollection === selection.sceneCollection
      && journal.state.inputName === selection.inputName) return journal;
  }
}

/** Refuse unsupported or ambiguous selections rather than broaden the restricted recipe. */
async function readState(client: OBSWebSocketClient, selection: Selection): Promise<{ state: State; desired: Obj }> {
  const url = new URL(client.getConnectionStatus().url);
  if (!["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)) throw new Error("V1 requires local OBS on loopback");
  const request = async (type: string, data?: Obj) => obj(await client.sendRequest(type, data));
  if ((await request("GetVersion")).platform !== "macos") throw new Error("V1 requires macOS OBS");
  if ((await request("GetSceneCollectionList")).currentSceneCollectionName !== selection.sceneCollection) throw new Error("Selected scene collection is not current");
  for (const type of ["GetStreamStatus", "GetRecordStatus"]) {
    const status = await request(type);
    if (status.outputActive !== false || status.outputReconnecting === true) throw new Error("Preparation and recovery require inactive recording and streaming");
  }
  const scenes = await request("GetSceneList");
  if (scenes.currentProgramSceneName !== selection.sceneName) throw new Error("Selected scene must already be the current program scene");
  const sceneList = list(scenes.scenes);
  const selectedScene = sceneList.filter((s) => s.sceneName === selection.sceneName);
  if (selectedScene.length !== 1) throw new Error("Scene selection is ambiguous or missing");
  const sceneUuid = string(selectedScene[0]!.sceneUuid);
  const inputs = list((await request("GetInputList")).inputs);
  const selectedInputs = inputs.filter((input) => input.inputName === selection.inputName);
  if (selectedInputs.length !== 1 || selectedInputs[0]!.inputKind !== "screen_capture") throw new Error("Select one existing screen_capture input");
  const inputUuid = string(selectedInputs[0]!.inputUuid);
  let selected: Obj | undefined;
  let occurrences = 0;
  const sceneNames = new Set(sceneList.map((s) => string(s.sceneName)));
  for (const scene of sceneList) {
    for (const item of list((await request("GetSceneItemList", { sceneName: scene.sceneName })).sceneItems)) {
      if (typeof item.isGroup !== "boolean" || typeof item.sourceName !== "string") throw new Error("Incomplete scene membership; source exclusivity unavailable");
      if (item.isGroup === true || sceneNames.has(String(item.sourceName))) throw new Error("V1 requires a collection without nested scenes or groups to prove source exclusivity");
      if (item.sourceName === selection.inputName || item.sourceUuid === inputUuid) {
        occurrences++;
        if (scene.sceneName === selection.sceneName) selected = item;
      }
    }
  }
  if (occurrences !== 1 || !selected) throw new Error("Capture must appear exactly once and may not be shared with another scene");
  if (selected.sourceUuid !== inputUuid || selected.sourceName !== selection.inputName || !Number.isInteger(selected.sceneItemId)) throw new Error("Scene item/source identity is incomplete");
  if (selected.sceneItemEnabled !== true) throw new Error("Capture must already be enabled");
  const transform = obj(selected.sceneItemTransform);
  for (const key of FIT) if (!(key in transform) || (key !== "boundsType" && (typeof transform[key] !== "number" || !Number.isFinite(transform[key])))) throw new Error(`Incomplete snapshot: transform ${key} unavailable`);
  if (typeof transform.boundsType !== "string" || !transform.boundsType.startsWith("OBS_BOUNDS_")) throw new Error("Incomplete snapshot: transform boundsType unavailable");
  if (typeof transform.sourceWidth !== "number" || transform.sourceWidth <= 0 || typeof transform.sourceHeight !== "number" || transform.sourceHeight <= 0) throw new Error("Capture has no reported source dimensions");
  const input = (await readInputs(client, [selection.inputName], ["inputSettings", "muted", "audioTracks"]))[0]!;
  if (input.inputKind !== "screen_capture" || input.inputSettings.type !== 1 || typeof input.inputSettings.window !== "number" || input.inputSettings.window <= 0) throw new Error("Select a preconfigured window capture; V1 never retargets it");
  if (!input.audioTracks || Object.keys(input.audioTracks).length !== 6 || [1, 2, 3, 4, 5, 6].some((n) => typeof input.audioTracks![String(n)] !== "boolean")) throw new Error("Incomplete snapshot: all six audio tracks are required");
  const video = await request("GetVideoSettings");
  if (typeof video.baseWidth !== "number" || video.baseWidth <= 0 || typeof video.baseHeight !== "number" || video.baseHeight <= 0) throw new Error("Canvas dimensions unavailable");
  // Recheck after multi-request discovery. No exclusion of the OBS UI is claimed.
  if ((await request("GetSceneCollectionList")).currentSceneCollectionName !== selection.sceneCollection) throw new Error("Collection changed while reading state");
  return { state: { ...selection, sceneUuid, inputUuid, sceneItemId: selected.sceneItemId as number, settings: input.inputSettings, muted: input.muted!, tracks: input.audioTracks, transform },
    desired: { boundsType: "OBS_BOUNDS_SCALE_INNER", boundsWidth: video.baseWidth, boundsHeight: video.baseHeight, boundsAlignment: 0, alignment: 5, positionX: 0, positionY: 0, rotation: 0 } };
}
function identity(a: State, b: State): boolean {
  return a.sceneUuid === b.sceneUuid && a.inputUuid === b.inputUuid && a.sceneItemId === b.sceneItemId && same(a.settings, b.settings);
}
function preparedState(state: State, desired: Obj): boolean {
  return state.muted === true && Object.values(state.tracks).every((value) => value === false)
    && FIT.every((key) => sameTransformValue(key, state.transform[key], desired[key]));
}
function preparationUnchanged(now: State, expected: State): boolean {
  return now.muted === expected.muted && same(now.tracks, expected.tracks)
    && FIT.every((key) => sameTransformValue(key, now.transform[key], expected.transform[key]));
}
function value(state: State, change: Field): unknown { return change.field === "transform" ? state.transform[change.key!] : state[change.field]; }
function equal(change: Field, a: unknown, b: unknown): boolean { return change.field === "transform" ? sameTransformValue(change.key!, a, b) : same(a, b); }
function matches(state: State, changes: Field[], side: "before" | "after"): boolean { return changes.every((change) => equal(change, value(state, change), change[side])); }
function changesFor(state: State, desired: Obj): Field[] {
  return [
    { field: "muted", before: state.muted, after: true } as Field,
    { field: "tracks", before: state.tracks, after: Object.fromEntries([1, 2, 3, 4, 5, 6].map((n) => [n, false])) } as Field,
    ...FIT.map((key): Field => ({ field: "transform", key, before: state.transform[key], after: desired[key] })),
  ].filter((change) => !equal(change, change.before, change.after));
}
async function writeField(client: OBSWebSocketClient, state: State, change: Field, side: "before" | "after"): Promise<void> {
  if (change.field === "transform") await client.sendRequest("SetSceneItemTransform", { sceneName: state.sceneName, sceneItemId: state.sceneItemId, sceneItemTransform: { [change.key!]: change[side] } });
  else if (change.field === "muted") await client.sendRequest("SetInputMute", { inputName: state.inputName, inputMuted: change[side] });
  else await client.sendRequest("SetInputAudioTracks", { inputName: state.inputName, inputAudioTracks: change[side] });
}
function advance(state: State, change: Field, side: "before" | "after"): void {
  if (change.field === "transform") state.transform[change.key!] = change[side];
  else if (change.field === "muted") state.muted = change[side] as boolean;
  else state.tracks = change[side] as Obj;
}
async function prepare(client: OBSWebSocketClient, selection: Selection, apply: boolean): Promise<CallToolResult> {
  let journal: Journal | undefined;
  let attempted = false;
  try {
    const { state, desired } = await readState(client, selection);
    const existing = pending(client, selection);
    if (existing) {
      if (existing.attempted === existing.changes.length && identity(state, existing.state) && matches(state, existing.changes, "after") && preparedState(state, desired)) return result("already-prepared", { operationId: existing.operationId, snapshotId: existing.snapshotId, applied: false });
      return result("conflict", { operationId: existing.operationId, applied: false, issues: ["An unresolved preparation exists; inspect/recover it before preparing again"] }, true);
    }
    const changes = changesFor(state, desired);
    if (!apply || changes.length === 0) return result(changes.length ? "planned" : "already-prepared", { applied: false, changes });
    const snapshot = await takeSnapshot(client, { scenes: [selection.sceneName], inputs: [selection.inputName], auto: true, label: "before restricted demo", sceneCollection: selection.sceneCollection, requiredInputFields: ["inputSettings", "muted", "audioTracks"] });
    const savedInput = snapshot.inputs[0];
    const savedItem = snapshot.items.find((item) => item.sceneItemId === state.sceneItemId && item.sourceName === state.inputName);
    if (!savedInput || !savedItem || !same(savedInput.inputSettings, state.settings) || savedInput.muted !== state.muted || !same(savedInput.audioTracks, state.tracks)
      || FIT.some((key) => !sameTransformValue(key, savedItem.transform[key], state.transform[key]))) throw new Error("State changed during snapshot; nothing changed");
    journal = { version: 1, operationId: crypto.randomUUID(), obsUrl: client.getConnectionStatus().url, snapshotId: snapshot.id, state, changes, attempted: 0 };
    persist(journal); // Durable before the first OBS mutation, including a crash/uncertain response.
    const expected = structuredClone(state);
    for (const [index, change] of changes.entries()) {
      const now = (await readState(client, selection)).state;
      if (!identity(state, now) || !preparationUnchanged(now, expected)) throw new Error("State conflict during preparation; automatic writes stopped");
      // Persist ownership intent before dispatch. A crash may leave an uncertain
      // attempted field, but fields we never attempted do not become ours.
      persist({ ...journal, attempted: index + 1 });
      journal.attempted = index + 1;
      attempted = true;
      await writeField(client, state, change, "after");
      advance(expected, change, "after");
    }
    const after = (await readState(client, selection)).state;
    if (!identity(state, after) || !matches(after, changes, "after") || !preparedState(after, desired)) throw new Error("Preparation postconditions did not match");
    return result("prepared", { operationId: journal.operationId, snapshotId: snapshot.id, applied: true, changes });
  } catch (e) { return result(attempted ? "partial" : "failed", { applied: attempted, ...(journal ? { operationId: journal.operationId, snapshotId: journal.snapshotId } : {}), issues: [message(e)], recovery: journal ? "Inspect obs-recover-demo; no blind retry" : "Nothing changed" }, true); }
}
async function recover(client: OBSWebSocketClient, operationId: string, apply: boolean): Promise<CallToolResult> {
  let attempted = false;
  try {
    const journal = readJournal(operationId);
    if (journal.obsUrl !== client.getConnectionStatus().url) throw new Error("Journal belongs to a different OBS URL");
    const { state } = await readState(client, journal.state);
    if (!identity(journal.state, state)) return result("conflict", { operationId, applied: false, issues: ["Collection/object identity or capture settings changed"] }, true);
    if (journal.recovered) {
      if (!matches(state, journal.changes, "before")) return result("conflict", { operationId, applied: false, issues: ["State changed after recovery; journal will not be reapplied"] }, true);
      return result("already-recovered", { operationId, applied: false });
    }
    // Admit the whole restore before any write. Only attempted fields may be
    // at 'after'; an unattempted field belongs to the operator even at 'after'.
    const owned = journal.changes.slice(0, journal.attempted);
    const conflicts = journal.changes.filter((change, index) => !equal(change, value(state, change), change.before)
      && (index >= journal.attempted || !equal(change, value(state, change), change.after)));
    if (conflicts.length) return result("conflict", { operationId, applied: false, conflicts }, true);
    const remaining = owned.filter((change) => !equal(change, value(state, change), change.before));
    if (!apply) return result("planned", { operationId, applied: false, changes: remaining });
    const expected = structuredClone(state);
    for (const change of remaining) {
      const now = (await readState(client, journal.state)).state;
      if (!identity(journal.state, now) || !journal.changes.every((c) => equal(c, value(now, c), value(expected, c)))) throw new Error("State conflict during recovery; writes stopped");
      attempted = true;
      await writeField(client, journal.state, change, "before");
      advance(expected, change, "before");
    }
    const after = (await readState(client, journal.state)).state;
    if (!identity(journal.state, after) || !matches(after, journal.changes, "before")) throw new Error("Recovery postconditions did not match");
    persist({ ...journal, recovered: true });
    return result("recovered", { operationId, applied: remaining.length > 0, changes: remaining });
  } catch (e) { return result(attempted ? "partial" : "failed", { operationId, applied: attempted, issues: [message(e)], recovery: "Inspect current state before retrying" }, true); }
}
async function exclusive(client: OBSWebSocketClient, run: () => Promise<CallToolResult>): Promise<CallToolResult> {
  if (busy.has(client)) return result("conflict", { applied: false, issues: ["Another restricted demo operation is running"] }, true);
  busy.add(client);
  try { return await run(); } finally { busy.delete(client); }
}
export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool("obs-prepare-demo", {
    title: "Prepare Existing Demo Capture",
    description: "Plan by default. Fit and silence one existing macOS window capture in the exact current collection/program scene. Refuses live output, shared sources, groups, missing identity/state, and unpersisted recovery. Does not retarget or create anything. No UI exclusion guarantee.",
    inputSchema: z.object({ sceneCollection: z.string().min(1), sceneName: z.string().min(1), inputName: z.string().min(1), apply: z.boolean().default(false) }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
  }, ({ sceneCollection, sceneName, inputName, apply }) => exclusive(client, () => prepare(client, { sceneCollection, sceneName, inputName }, apply)));
  server.registerTool("obs-recover-demo", {
    title: "Recover Restricted Demo Preparation",
    description: "Plan by default. Restore only fields owned by the given durable preparation journal. Refuses conflicting operator changes, different identities/collection, and active output. Rereads postconditions; partial failure needs inspection before retry. Legacy snapshots require manual obs-restore.",
    inputSchema: z.object({ operationId: z.uuid(), apply: z.boolean().default(false) }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
  }, ({ operationId, apply }) => exclusive(client, () => recover(client, operationId, apply)));
}
