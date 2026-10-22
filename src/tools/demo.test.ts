/* SPDX-License-Identifier: GPL-2.0-only */
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { demoSceneState, serveSceneState, type FakeSceneState } from "../../test/support/fake-obs-scene.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;
let state: FakeSceneState;
let collection: string;
let inputUuid: string;
let collectionReads: number;
const selection = { sceneCollection: "Demo collection", sceneName: "Demo", inputName: "Chrome" };
const fields = ["boundsType", "boundsWidth", "boundsHeight", "boundsAlignment", "alignment", "positionX", "positionY", "rotation"];
function serve(): void {
  serveSceneState(harness.fakeObs, state);
  harness.fakeObs.respondWith("GetVersion", () => ({ platform: "macos" }));
  harness.fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: false }));
  harness.fakeObs.respondWith("GetStreamStatus", () => ({ outputActive: false }));
  harness.fakeObs.respondWith("GetSceneCollectionList", () => { collectionReads++; return { currentSceneCollectionName: collection }; });
  harness.fakeObs.respondWith("GetInputList", () => ({ inputs: Object.entries(state.inputs).map(([inputName, input]) => ({ inputName, inputKind: input.kind, inputUuid: inputName === "Chrome" ? inputUuid : "background-uuid" })) }));
  harness.fakeObs.respondWith("GetSceneList", () => ({ currentProgramSceneName: state.program, scenes: Object.keys(state.scenes).map((sceneName) => ({ sceneName, sceneUuid: `${sceneName}-uuid` })) }));
  harness.fakeObs.respondWith("GetSceneItemList", ({ sceneName }) => ({ sceneItems: state.scenes[String(sceneName)]!.map((item, sceneItemIndex) => ({
    ...item, sceneItemIndex, sourceUuid: item.sourceName === "Chrome" ? inputUuid : "background-uuid", isGroup: false,
    sceneItemEnabled: item.enabled, sceneItemLocked: item.locked,
    sceneItemTransform: { ...item.transform, sourceWidth: 1512, sourceHeight: 949 },
  })) }));
}
function mutations() {
  return harness.fakeObs.history().filter(({ frame }) => /^(Set|Create|Remove|Start|Stop)/.test(String(frame.d.requestType)));
}
beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  state = demoSceneState();
  state.inputs.Chrome!.muted = false;
  state.inputs.Chrome!.tracks = { 1: true, 2: false, 3: false, 4: false, 5: false, 6: false };
  Object.assign(state.scenes.Demo![1]!.transform, { boundsWidth: 0, boundsHeight: 0, boundsAlignment: 0 });
  collection = selection.sceneCollection;
  collectionReads = 0;
  inputUuid = "capture-uuid";
  harness = await startMcpHarness();
  serve();
});
afterEach(async () => { await harness.close(); vi.restoreAllMocks(); });
async function prepare() {
  const response = await harness.call("obs-prepare-demo", { ...selection, apply: true });
  expect(response.isError, resultText(response)).toBeFalsy();
  return response.structuredContent as { operationId: string; snapshotId: string; status: string };
}
describe("restricted demo preparation and recovery over MCP", () => {
  it("plans without mutations or state writes; apply persists complete recovery before the first mutation", async () => {
    const before = structuredClone(state);
    const plan = await harness.call("obs-prepare-demo", selection);
    expect(plan.structuredContent).toMatchObject({ status: "planned", applied: false });
    expect(mutations()).toEqual([]);
    expect(state).toEqual(before);
    harness.fakeObs.respondWith("SetInputMute", ({ inputMuted }) => {
      const snapshots = JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR!, "snapshots.json"), "utf8"));
      expect(snapshots[0]).toMatchObject({ sceneCollection: collection, inputs: [{ inputName: "Chrome", muted: false }] });
      state.inputs.Chrome!.muted = inputMuted as boolean;
      return {};
    });
    const prepared = await prepare();
    const journal = JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR!, "demo-recovery", `${prepared.operationId}.json`), "utf8"));
    expect(journal).toMatchObject({ version: 1, state: { sceneUuid: "Demo-uuid", inputUuid, muted: false }, snapshotId: prepared.snapshotId });
    expect(state.inputs.Chrome!.muted).toBe(true);
    expect(Object.values(state.inputs.Chrome!.tracks!)).toEqual([false, false, false, false, false, false]);
    expect(state.scenes.Demo![1]!.transform).toMatchObject({ boundsType: "OBS_BOUNDS_SCALE_INNER", boundsWidth: 1920, boundsHeight: 1080 });
    expect(mutations().every(({ frame }) => ["SetInputMute", "SetInputAudioTracks", "SetSceneItemTransform"].includes(String(frame.d.requestType)))).toBe(true);
  });
  it("does not overwrite the original recovery state on reapply; recovery defaults to plan and is idempotent", async () => {
    const original = structuredClone(state);
    const first = await prepare();
    const count = mutations().length;
    expect((await harness.call("obs-prepare-demo", { ...selection, apply: true })).structuredContent).toMatchObject({ status: "already-prepared", operationId: first.operationId });
    const preview = await harness.call("obs-recover-demo", { operationId: first.operationId });
    expect(preview.structuredContent).toMatchObject({ status: "planned", applied: false });
    expect(mutations()).toHaveLength(count);
    expect((await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true })).structuredContent).toMatchObject({ status: "recovered", applied: true });
    expect(state).toEqual(original);
    const restoredCount = mutations().length;
    expect((await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true })).structuredContent).toMatchObject({ status: "already-recovered", applied: false });
    expect(mutations()).toHaveLength(restoredCount);
  });
  it("recovers using a fresh MCP server and OBS client at the original URL", async () => {
    const first = await prepare();
    const port = Number(new URL(harness.fakeObs.url).port);
    await harness.close();
    harness = await startMcpHarness({ port });
    serve();
    const recovered = await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true });
    expect(recovered.structuredContent).toMatchObject({ status: "recovered" });
    expect(state.inputs.Chrome!.muted).toBe(false);
  });
  it.each(["GetInputSettings", "GetInputMute", "GetInputAudioTracks"])("refuses an incomplete required snapshot when %s fails", async (requestType) => {
    harness.fakeObs.respondWith(requestType, () => { throw new FakeOBSRequestError(604, "unavailable"); });
    const response = await harness.call("obs-prepare-demo", { ...selection, apply: true });
    expect(response.isError).toBe(true);
    expect(resultText(response)).toContain("Incomplete snapshot");
    expect(mutations()).toEqual([]);
  });
  it.each(fields)("refuses missing transform field %s", async (field) => {
    delete state.scenes.Demo![1]!.transform[field];
    expect((await harness.call("obs-prepare-demo", { ...selection, apply: true })).isError).toBe(true);
    expect(mutations()).toEqual([]);
  });
  it.each(["snapshots.json", "demo-recovery"])("makes no mutation if persistence fails at %s", async (path) => {
    if (path === "snapshots.json") mkdirSync(join(process.env.OBS_MCP_STATE_DIR!, path));
    else writeFileSync(join(process.env.OBS_MCP_STATE_DIR!, path), "occupied");
    const response = await harness.call("obs-prepare-demo", { ...selection, apply: true });
    expect(response.isError).toBe(true);
    expect(response.structuredContent).toMatchObject({ applied: false });
    expect(mutations()).toEqual([]);
  });
  it("refuses a shared capture and active output", async () => {
    state.scenes.Other = [structuredClone(state.scenes.Demo![1]!)];
    expect(resultText(await harness.call("obs-prepare-demo", { ...selection, apply: true }))).toContain("shared");
    delete state.scenes.Other;
    harness.fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: true }));
    expect(resultText(await harness.call("obs-prepare-demo", { ...selection, apply: true }))).toContain("inactive");
    expect(mutations()).toEqual([]);
  });
  it("refuses collection drift during discovery and before recovery", async () => {
    harness.fakeObs.respondWith("GetSceneCollectionList", () => ({ currentSceneCollectionName: ++collectionReads > 1 ? "other" : collection }));
    expect((await harness.call("obs-prepare-demo", { ...selection, apply: true })).isError).toBe(true);
    expect(mutations()).toEqual([]);
    serve();
    const first = await prepare();
    const count = mutations().length;
    collection = "other";
    expect((await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true })).isError).toBe(true);
    expect(mutations()).toHaveLength(count);
  });
  it("refuses the entire recovery on a conflicting owned field and preserves unrelated operator fields", async () => {
    const first = await prepare();
    const count = mutations().length;
    state.scenes.Demo![1]!.transform.positionX = 99;
    const conflict = await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true });
    expect(conflict.structuredContent).toMatchObject({ status: "conflict", applied: false });
    expect(mutations()).toHaveLength(count);
    state.scenes.Demo![1]!.transform.positionX = 0;
    state.inputs.Chrome!.volumeMul = 0.4;
    state.scenes.Demo![1]!.transform.cropTop = 27;
    expect((await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true })).isError).toBeFalsy();
    expect(state.inputs.Chrome!.volumeMul).toBe(0.4);
    expect(state.scenes.Demo![1]!.transform.cropTop).toBe(27);
  });
  it("refuses a replaced input with the same name and refuses legacy snapshot ids", async () => {
    const first = await prepare();
    const count = mutations().length;
    inputUuid = "replacement-input";
    expect((await harness.call("obs-recover-demo", { operationId: first.operationId, apply: true })).structuredContent).toMatchObject({ status: "conflict" });
    expect(mutations()).toHaveLength(count);
    expect((await harness.call("obs-recover-demo", { operationId: first.snapshotId, apply: true })).isError).toBe(true);
  });
  it("reports partial preparation and recovers only applied fields after a failed request", async () => {
    harness.fakeObs.respondWith("SetInputAudioTracks", () => { throw new FakeOBSRequestError(500, "write failed"); });
    const response = await harness.call("obs-prepare-demo", { ...selection, apply: true });
    expect(response.structuredContent).toMatchObject({ status: "partial", applied: true });
    expect(state.inputs.Chrome!.muted).toBe(true);
    const operationId = (response.structuredContent as { operationId: string }).operationId;
    serve();
    expect((await harness.call("obs-recover-demo", { operationId, apply: true })).structuredContent).toMatchObject({ status: "recovered" });
    expect(state.inputs.Chrome!.muted).toBe(false);
  });
  it("never takes ownership of unattempted fields after partial preparation", async () => {
    harness.fakeObs.respondWith("SetInputAudioTracks", () => { throw new FakeOBSRequestError(500, "write failed"); });
    const response = await harness.call("obs-prepare-demo", { ...selection, apply: true });
    const operationId = (response.structuredContent as { operationId: string }).operationId;
    const journal = JSON.parse(readFileSync(join(process.env.OBS_MCP_STATE_DIR!, "demo-recovery", `${operationId}.json`), "utf8"));
    expect(journal.attempted).toBe(2);
    expect(journal.changes.slice(0, 2).map((change: { field: string }) => change.field)).toEqual(["muted", "tracks"]);
    state.scenes.Demo![1]!.transform.positionX = 0; // Operator matches planned-after on an unattempted field.
    const count = mutations().length;
    serve();
    expect((await harness.call("obs-recover-demo", { operationId, apply: true })).structuredContent).toMatchObject({ status: "conflict", applied: false });
    expect(mutations()).toHaveLength(count);
    expect(state.scenes.Demo![1]!.transform.positionX).toBe(0);
    journal.attempted = journal.changes.length + 1;
    writeFileSync(join(process.env.OBS_MCP_STATE_DIR!, "demo-recovery", `${operationId}.json`), JSON.stringify(journal));
    expect((await harness.call("obs-recover-demo", { operationId, apply: true })).isError).toBe(true);
    expect(mutations()).toHaveLength(count);
  });
  it("does not claim success when OBS acknowledges but ignores a write", async () => {
    harness.fakeObs.respondWith("SetInputMute", () => ({}));
    expect((await harness.call("obs-prepare-demo", { ...selection, apply: true })).structuredContent).toMatchObject({ status: "partial" });
  });
});
