/* SPDX-License-Identifier: GPL-2.0-only */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startMcpHarness, resultText, type McpHarness } from "../../test/support/mcp-harness.js";
import { serveProduction } from "../../test/support/production-fixture.js";
import { FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { InMemoryTransport, McpServer } from "@modelcontextprotocol/server";
import { Client } from "@modelcontextprotocol/client";
import { writeFileSync } from "node:fs";
import { join } from "node:path";
import { stateDirectory } from "../state-dir.js";
import { OBSWebSocketClient, EventSubscription } from "../client.js";
import { Observation } from "./observation.js";
import { initialize } from "./index.js";

let h: McpHarness;
let state: ReturnType<typeof serveProduction>;
const target = { sceneCollection: "Audit collection", sceneName: "Review" };
const binding = { role: "slides", sourceName: "Application", sourceUuid: "application-source-uuid", containerUuid: "review-scene-uuid", sceneItemId: 7 };
const template = { version: 1, name: "Presentation", canvas: { width: 1920, height: 1080 }, roles: [{ role: "slides", inputKinds: ["screen_capture", "xcomposite_input"], transform: { positionX: 12 }, enabled: true, locked: true, index: 0 }] };
const plan = () => h.call("obs-plan-scene-template", { ...target, template, bindings: [binding] });
const mutations = () => h.fakeObs.history().filter(({ frame }) => /^(Set|Create|Remove|Trigger|Start|Stop)/.test(String(frame.d.requestType)));
beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  h = await startMcpHarness();
  state = serveProduction(h.fakeObs);
});
afterEach(async () => { await h.close(); vi.restoreAllMocks(); });

describe("bounded production observation", () => {
  it.each(["InputVolumeMeters", "InputVolumeChanged", "InputMuteStateChanged", "InputAudioTracksChanged"])("keeps structural evidence for meter samples but invalidates settings: %s", async event => {
    await h.obsClient.connect();
    const unsubscribe = h.obsClient.subscribeHighVolume(EventSubscription.InputVolumeMeters);
    const sample = new Observation(h.obsClient);
    try {
      await h.fakeObs.waitForFrame(({ frame }) => frame.op === 3 && (Number(frame.d.eventSubscriptions) & EventSubscription.InputVolumeMeters) !== 0);
      h.fakeObs.queueSuccess("GetVideoSettings", { baseWidth: 1920, baseHeight: 1080 }, 80);
      const pending = sample.read("GetVideoSettings");
      await h.fakeObs.waitForRequest("GetVideoSettings");
      h.fakeObs.sendRaw({ op: 5, d: { eventType: event, eventIntent: event === "InputVolumeMeters" ? 1 << 16 : 8, eventData: { inputs: [] } } });
      if (event === "InputVolumeMeters") await expect(pending).resolves.toEqual({ baseWidth: 1920, baseHeight: 1080 });
      else await expect(pending).rejects.toMatchObject({ reason: "observation_invalidated" });
    } finally { sample.close(); unsubscribe(); }
  });
  it("shares facts between the tool and resource, including exact identities and next actions", async () => {
    const tool = await h.call("obs-observe");
    expect(tool.isError).toBeFalsy();
    expect(tool.structuredContent).toMatchObject({ status: "observed", generation: 1, collection: state.collection, program: { sceneName: "Main", sceneUuid: "main-scene-uuid" }, preview: { sceneName: "Review" }, availableActions: expect.arrayContaining(["obs-stage-scene"]) });
    const resource = await h.mcpClient.readResource({ uri: "obs://status" });
    const report = JSON.parse((resource.contents[0] as { text: string }).text);
    for (const key of ["program", "preview", "outputs", "control", "recovery", "operationReceipts", "availableActions"]) expect(report[key]).toEqual((tool.structuredContent as Record<string, unknown>)[key]);
    expect(mutations()).toEqual([]);
  });
  it("rejects a same-generation change even without an event", async () => {
    let reads = 0;
    h.fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: ++reads > 1 }));
    expect((await h.call("obs-observe")).structuredContent).toMatchObject({ status: "unknown", reason: "observation_invalidated" });
  });
  it("rejects a collection replacement while a reply is delayed", async () => {
    h.fakeObs.queueSuccess("GetVideoSettings", { baseWidth: 1920, baseHeight: 1080 }, 100);
    const pending = h.call("obs-observe");
    await h.fakeObs.waitForRequest("GetVideoSettings");
    h.fakeObs.sendEvent("CurrentSceneCollectionChanged", { sceneCollectionName: "Replacement" });
    expect((await pending).structuredContent).toMatchObject({ status: "unknown", reason: "observation_invalidated" });
    expect(h.obsClient.listenerCount("event")).toBe(0);
  });
  it("rejects disconnect/reconnect across a delayed response", async () => {
    h.fakeObs.queueSuccess("GetVideoSettings", { baseWidth: 1920, baseHeight: 1080 }, 150);
    const pending = h.call("obs-observe");
    await h.fakeObs.waitForRequest("GetVideoSettings");
    h.fakeObs.disconnect();
    const report = (await pending).structuredContent;
    expect(report).toMatchObject({ status: "unknown", reason: "observation_invalidated" });
    await h.obsClient.connect();
    expect(h.obsClient.getConnectionStatus().generation).toBe(2);
  });
  it("reports unavailable evidence without guessing an output is idle", async () => {
    h.fakeObs.respondWith("GetReplayBufferStatus", () => { throw new FakeOBSRequestError(500, "unavailable"); });
    expect((await h.call("obs-observe")).structuredContent).toMatchObject({ status: "incomplete", outputs: { replayBuffer: null }, missingEvidence: [expect.objectContaining({ request: "GetReplayBufferStatus" })] });
  });
});

describe("portable scene template resolver", () => {
  it("resolves exact occurrence bindings and proposes changes without any writes", async () => {
    const result = await plan();
    expect(result.structuredContent).toMatchObject({ status: "resolved", applied: false, resolved: [binding], changes: [expect.objectContaining({ field: "sceneItemLocked" }), expect.objectContaining({ field: "transform.positionX" })] });
    expect(mutations()).toEqual([]);
  });
  it("accepts OBS's explicit input-source null group flag but keeps unclassified nulls unknown", async () => {
    const item = state.items[0] as Record<string, unknown>;
    item.isGroup = null; item.sourceType = "OBS_SOURCE_TYPE_INPUT";
    expect((await plan()).structuredContent).toMatchObject({ status: "resolved" });
    delete item.sourceType;
    expect((await plan()).structuredContent).toMatchObject({ status: "unknown" });
  });
  it("blocks unsupported named-canvas scoping without dispatch", async () => {
    const report = await h.call("obs-stage-scene", { ...target, canvasUuid: "another-canvas" });
    expect(report.structuredContent).toMatchObject({ status: "blocked", reason: "capability_unavailable" });
    expect(mutations()).toEqual([]);
  });
  it("resolves the same roles against different machine names and UUIDs", async () => {
    state.sceneName = "Linux show"; state.sceneUuid = "linux-scene"; state.kind = "xcomposite_input";
    const result = await h.call("obs-plan-scene-template", { ...target, sceneName: state.sceneName, template, bindings: [{ ...binding, containerUuid: state.sceneUuid }] });
    expect(result.structuredContent).toMatchObject({ status: "resolved", resolved: [expect.objectContaining({ inputKind: "xcomposite_input", containerUuid: "linux-scene" })] });
    expect(mutations()).toEqual([]);
  });
  it.each([
    ["rename", "target_renamed"], ["replacement", "target_replaced"], ["canvas", "canvas_mismatch"], ["kind", "incompatible_kind"], ["duplicate", "duplicate_binding"], ["missing", "missing_binding"],
  ])("distinguishes %s blockers", async (variant, reason) => {
    const localTemplate = structuredClone(template);
    let bindings = [binding];
    if (variant === "rename") state.items[0]!.sourceName = "Renamed";
    if (variant === "replacement") state.items[0]!.sourceUuid = "replacement";
    if (variant === "canvas") localTemplate.canvas.width = 1280;
    if (variant === "kind") state.kind = "browser_source";
    if (variant === "duplicate") bindings = [binding, binding];
    if (variant === "missing") bindings = [];
    const result = await h.call("obs-plan-scene-template", { ...target, template: localTemplate, bindings });
    expect(result.structuredContent).toMatchObject({ status: "blocked", blockers: expect.arrayContaining([expect.objectContaining({ reason })]) });
    expect(mutations()).toEqual([]);
  });
});

describe("guarded preview to program", () => {
  async function stage() {
    const result = await h.call("obs-stage-scene", target);
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(result.content.some((block) => block.type === "image")).toBe(true);
    expect(result.structuredContent).toMatchObject({ status: "staged", renderedComposition: "unverified", sharedSources: ["application-source-uuid"] });
    return (result.structuredContent as Record<string, unknown>).stageId;
  }
  it("stages, inspects and takes once, separating the verified scene from rendered content", async () => {
    state.preview = "Main";
    const stageId = await stage();
    expect(state.program).toBe("Main");
    expect(state.preview).toBe("Review");
    const taken = await h.call("obs-take-scene", { stageId });
    expect(taken.structuredContent).toMatchObject({ status: "verified", requestAccepted: true, intendedSceneObservedOnProgram: true, renderedComposition: "unverified" });
    expect((await h.call("obs-observe")).structuredContent).toMatchObject({ operationReceipts: { status: "observed", operations: expect.arrayContaining([expect.objectContaining({ operationId: (taken.structuredContent as Record<string, unknown>).operationId, kind: "take-scene", phase: "observed", requestAccepted: true, postconditionObserved: true, renderedComposition: "unverified" })]) } });
    expect((await h.call("obs-take-scene", { stageId })).isError).toBe(true);
    expect(mutations().map(({ frame }) => frame.d.requestType)).toEqual(["SetCurrentPreviewScene", "TriggerStudioModeTransition"]);
  });
  it.each(["preview", "settings", "transform", "program", "studio", "audio"])("blocks a human %s change after inspection", async (variant) => {
    const stageId = await stage();
    if (variant === "preview") state.preview = "Main";
    if (variant === "settings") state.setting++;
    if (variant === "transform") state.items[0]!.sceneItemTransform.positionX++;
    if (variant === "program") state.program = "Review";
    if (variant === "studio") state.studio = false;
    if (variant === "audio") h.fakeObs.respondWith("GetInputMute", () => ({ inputMuted: false }));
    const result = await h.call("obs-take-scene", { stageId });
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({ status: "blocked" });
    expect(mutations()).toEqual([]);
  });
  it("retains uncertainty after disconnect following transition dispatch and never replays", async () => {
    const stageId = await stage();
    h.fakeObs.queueDisconnect("TriggerStudioModeTransition");
    const result = await h.call("obs-take-scene", { stageId });
    expect(result.structuredContent).toMatchObject({ status: "unknown", requestAccepted: null, intendedSceneObservedOnProgram: null });
    expect((await h.call("obs-observe")).structuredContent).toMatchObject({ operationReceipts: { operations: expect.arrayContaining([expect.objectContaining({ operationId: (result.structuredContent as Record<string, unknown>).operationId, status: "unknown", requestAccepted: null, postconditionObserved: false })]) } });
    await h.call("obs-take-scene", { stageId });
    expect(mutations()).toHaveLength(1);
  });
  it("cancels delayed inspection before any preview mutation", async () => {
    state.preview = "Main";
    h.fakeObs.queueSuccess("GetSourceFilterList", { filters: [] }, 100);
    const controller = new AbortController();
    const pending = h.mcpClient.callTool({ name: "obs-stage-scene", arguments: target }, { signal: controller.signal });
    const rejected = expect(pending).rejects.toThrow();
    await h.fakeObs.waitForRequest("GetSourceFilterList");
    controller.abort();
    await rejected;
    await vi.waitFor(() => expect(h.obsClient.listenerCount("event")).toBe(0));
    expect(mutations()).toEqual([]);
  });
  it("reconciles an effect whose reply was lost without replaying it", async () => {
    const stageId = await stage();
    h.fakeObs.queueSuccess("TriggerStudioModeTransition", {}, 100);
    const pending = h.call("obs-take-scene", { stageId });
    await h.fakeObs.waitForRequest("TriggerStudioModeTransition");
    state.program = "Review";
    h.fakeObs.disconnect();
    expect((await pending).structuredContent).toMatchObject({ status: "unknown", requestAccepted: null });
    const observed = await h.call("obs-observe");
    expect(observed.structuredContent).toMatchObject({ program: { sceneName: "Review" } });
    await h.call("obs-take-scene", { stageId });
    expect(mutations()).toHaveLength(1);
  });
  it("retains lost-reply custody across a fresh MCP server and separates current agreement from historical proof", async () => {
    const stageId = await stage();
    h.fakeObs.queueSuccess("TriggerStudioModeTransition", {}, 100);
    const pending = h.call("obs-take-scene", { stageId });
    await h.fakeObs.waitForRequest("TriggerStudioModeTransition");
    state.program = "Review";
    h.fakeObs.disconnect();
    const unknown = await pending;
    await h.obsClient.disconnect();
    const obs = new OBSWebSocketClient(h.fakeObs.url);
    const server = new McpServer({ name: "restarted-production", version: "test" });
    const client = new Client({ name: "restarted-client", version: "test" });
    initialize(server, obs);
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    try {
      await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
      const current = await client.callTool({ name: "obs-observe", arguments: {} });
      expect(current.structuredContent).toMatchObject({ program: { sceneName: "Review" }, operationReceipts: { operations: expect.arrayContaining([expect.objectContaining({ operationId: (unknown.structuredContent as Record<string, unknown>).operationId, kind: "take-scene", status: "unknown", postconditionObserved: false })]) } });
      expect((await client.callTool({ name: "obs-take-scene", arguments: { stageId } })).structuredContent).toMatchObject({ status: "blocked", reason: "stage_unavailable" });
      expect(mutations()).toHaveLength(1);
      expect((await client.callTool({ name: "obs-stage-scene", arguments: target })).structuredContent).toMatchObject({ status: "staged" });
    } finally { await client.close(); await server.close(); await obs.disconnect(); }
  });
  it("blocks undispatched preview changes if durable custody cannot be created", async () => {
    state.preview = "Main";
    writeFileSync(join(stateDirectory(), "operation-receipts"), "unavailable");
    expect((await h.call("obs-stage-scene", target)).structuredContent).toMatchObject({ status: "blocked", reason: "receipt_unavailable", requestDispatched: false });
    expect(mutations()).toEqual([]);
    expect((await h.call("obs-observe")).structuredContent).toMatchObject({ operationReceipts: { status: "unknown", reason: "receipt_unavailable" } });
  });
  it("rejects stages across reconnect and expires them", async () => {
    const stageId = await stage();
    await h.obsClient.disconnect(); await h.obsClient.connect();
    expect((await h.call("obs-take-scene", { stageId })).structuredContent).toMatchObject({ status: "blocked", reason: "observation_invalidated" });
    const next = await stage();
    vi.spyOn(Date, "now").mockReturnValue(Date.now() + 11 * 60_000);
    expect((await h.call("obs-take-scene", { stageId: next })).structuredContent).toMatchObject({ status: "blocked", reason: "stage_unavailable" });
  });
});
