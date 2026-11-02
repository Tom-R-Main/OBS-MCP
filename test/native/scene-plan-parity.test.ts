/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../support/fake-obs-server.js";
import { serveProduction } from "../support/production-fixture.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type State = ReturnType<typeof serveProduction>;
type Fixture = (fake: FakeOBSServer, state: State) => void;
type Session = { client: Client; transport: StdioClientTransport; fake: FakeOBSServer; state: State };
const sessions: Session[] = [];
const reads = new Set(["GetVersion", "GetSceneCollectionList", "GetSceneList", "GetVideoSettings", "GetInputList", "GetSceneItemList", "GetSceneItemTransform"]);

function argumentsForPlan() {
  return { sceneCollection: "Audit collection", sceneName: "Review", template: {
    version: 1, name: "Presentation", canvas: { width: 1920, height: 1080 },
    roles: [{ role: "slides", inputKinds: ["screen_capture", "xcomposite_input"], transform: { positionX: 12 }, enabled: true, locked: true, index: 0 }],
  }, bindings: [{ role: "slides", sourceName: "Application", sourceUuid: "application-source-uuid", containerUuid: "review-scene-uuid", sceneItemId: 7 }] };
}
type PlanArguments = ReturnType<typeof argumentsForPlan>;

function children(pid: number): number[] {
  const rows = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number));
  const result: number[] = [];
  const walk = (parent: number) => { for (const [id, ppid] of rows) if (ppid === parent && id) { result.push(id); walk(id); } };
  walk(pid); return result;
}

async function start(native: boolean, legacy: boolean, fixture: Fixture = () => undefined, env: Record<string, string> = {}): Promise<Session> {
  const fake = await FakeOBSServer.start();
  const state = serveProduction(fake);
  fixture(fake, state);
  const directory = join(process.env.OBS_MCP_STATE_DIR!, `scene-plan-${sessions.length}`);
  mkdirSync(directory);
  const client = new Client({ name: "scene-plan-parity", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({ command: native ? binary : process.execPath, args: native ? [] : [compatibility], cwd: root, env: {
    ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory,
    OBS_MCP_COMPAT_ENTRY: compatibility, OBS_MCP_COMPAT_NODE: process.execPath,
    OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false", OBS_MCP_READ_OBS_CONFIG: "false", ...env,
  }, stderr: "pipe" });
  transport.stderr?.on("data", () => undefined);
  const session = { client, transport, fake, state };
  sessions.push(session); await client.connect(transport); return session;
}

function normalizedReport(value: unknown): Record<string, unknown> {
  const result = structuredClone(value) as Record<string, unknown>;
  // Only independent wall times and physical connection generations differ.
  // All evidence, blockers, transforms, ordering and any proof hashes stay exact.
  if ("generation" in result) { expect(Number.isSafeInteger(result.generation)).toBe(true); result.generation = 1; }
  for (const field of ["observedAt", "completedAt"]) if (field in result) {
    expect(result[field]).toEqual(expect.stringMatching(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/));
    result[field] = "2026-10-31T12:00:00.000Z";
  }
  return result;
}

function semanticResult(result: Awaited<ReturnType<Client["callTool"]>>) {
  return { ...result, structuredContent: normalizedReport(result.structuredContent), content: result.content.map(block => {
    if (block.type !== "text") return block;
    const { text, ...metadata } = block;
    return { ...metadata, jsonValue: normalizedReport(JSON.parse(text)) };
  }) };
}

function trace(session: Session) {
  return session.fake.history().filter(record => record.frame.op === 6 && record.frame.d.requestType !== "GetVersion").map(record => ({ requestType: record.frame.d.requestType, requestData: record.frame.d.requestData ?? {} }));
}

function noWrites(session: Session) {
  expect(session.fake.history().filter(record => record.frame.op === 6).every(record => reads.has(String(record.frame.d.requestType)))).toBe(true);
  expect(session.fake.history().some(record => record.frame.op === 8)).toBe(false);
}

async function closedNative(session: Session) {
  await vi.waitFor(() => expect(session.fake.openConnectionCount).toBe(0), { timeout: 2_000 });
  expect(children(session.transport.pid!)).toEqual([]);
  noWrites(session);
}

afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try { await session.client.close(); }
    finally { try { await session.transport.close(); } finally { await session.fake.close(); } }
  }
});

describe("planner deadline compatibility", () => {
  it("joins interrupted native reads before asking the SDK to project a timeout", async () => {
    const fixture: Fixture = fake => {
      // Persistent delay covers both the native attempt and the SDK's later
      // read-only attempt. A one-shot fault would disappear during handoff.
      for (let index = 0; index < 3; index++) fake.queueSuccess("GetVideoSettings", { baseWidth: 1920, baseHeight: 1080 }, 3_000);
    };
    const oracle = await start(false, false, fixture);
    const native = await start(true, false, fixture);
    const call = { name: "obs-plan-scene-template", arguments: argumentsForPlan() };
    const expected = await oracle.client.callTool(call);
    const actual = await native.client.callTool(call);
    expect(semanticResult(actual)).toEqual(semanticResult(expected));
    expect(actual.structuredContent).toMatchObject({ status: "unknown", applied: false });
    expect(native.fake.connectionCount).toBe(2);
    expect(native.fake.openConnectionCount).toBe(1);
    expect(children(native.transport.pid!)).toHaveLength(1);
    noWrites(native); noWrites(oracle);
  });
});

describe.each([false, true])("public native scene planner (legacy=%s)", legacy => {
  it("serves a qualified plan childlessly and preserves exact report and ordered read trace", async () => {
    const oracle = await start(false, legacy);
    const native = await start(true, legacy);
    const call = { name: "obs-plan-scene-template", arguments: argumentsForPlan() };
    const expected = await oracle.client.callTool(call);
    const actual = await native.client.callTool(call);
    expect(actual.structuredContent).toMatchObject({ status: "resolved", applied: false, renderedContent: "unverified" });
    expect(semanticResult(actual)).toEqual(semanticResult(expected));
    expect(trace(native)).toEqual(trace(oracle));
    expect(native.fake.connectionCount).toBe(1);
    expect(native.fake.history().filter(record => record.frame.d.requestType === "GetVersion")).toHaveLength(1);
    expect(native.fake.eventSubscriptions()).toBe(4095);
    noWrites(oracle); await closedNative(native);
  });

  const variants: { name: string; fixture?: Fixture; adjust?: (args: PlanArguments) => void; status: string; reason?: string }[] = [
    { name: "explicit input-source null group", fixture: (_fake, state) => { const item = state.items[0] as unknown as Record<string, unknown>; item.isGroup = null; item.sourceType = "OBS_SOURCE_TYPE_INPUT"; }, status: "resolved" },
    { name: "unclassified null group", fixture: (_fake, state) => { (state.items[0] as unknown as Record<string, unknown>).isGroup = null; }, status: "unknown", reason: "evidence_unavailable" },
    { name: "renamed binding", fixture: (_fake, state) => { state.items[0]!.sourceName = "Renamed"; }, status: "blocked", reason: "target_renamed" },
    { name: "replaced binding", fixture: (_fake, state) => { state.items[0]!.sourceUuid = "replacement"; }, status: "blocked", reason: "target_replaced" },
    { name: "canvas mismatch", adjust: args => { args.template.canvas.width = 1280; }, status: "blocked", reason: "canvas_mismatch" },
    { name: "incompatible kind", fixture: (_fake, state) => { state.kind = "browser_source"; }, status: "blocked", reason: "incompatible_kind" },
    { name: "missing binding", adjust: args => { args.bindings = []; }, status: "blocked", reason: "missing_binding" },
    { name: "duplicate binding", adjust: args => { args.bindings.push(structuredClone(args.bindings[0]!)); }, status: "blocked", reason: "duplicate_binding" },
    { name: "null input kind coercion", fixture: fake => { fake.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: null }] })); }, adjust: args => { args.template.roles[0]!.inputKinds = ["null"]; }, status: "resolved" },
    { name: "event invalidation before reply", fixture: (fake, state) => { fake.respondWith("GetSceneItemTransform", () => { fake.sendEvent("SceneItemTransformChanged", {}); return { sceneItemTransform: state.items[0]!.sceneItemTransform }; }); }, status: "unknown", reason: "observation_invalidated" },
  ];
  it.each(variants)("preserves $name without writes or JS", async ({ fixture, adjust, status, reason }) => {
    const oracle = await start(false, legacy, fixture);
    const native = await start(true, legacy, fixture);
    const args = argumentsForPlan(); adjust?.(args);
    const call = { name: "obs-plan-scene-template", arguments: args };
    const expected = await oracle.client.callTool(call);
    const actual = await native.client.callTool(call);
    expect(actual.structuredContent).toMatchObject({ status, applied: false });
    if (reason) expect(actual.structuredContent).toMatchObject({ blockers: expect.arrayContaining([expect.objectContaining({ reason })]) });
    expect(semanticResult(actual)).toEqual(semanticResult(expected));
    expect(trace(native)).toEqual(trace(oracle));
    noWrites(oracle); await closedNative(native);
  });

  it("matches SDK invalid-argument refusal without scene reads", async () => {
    const oracle = await start(false, legacy);
    const native = await start(true, legacy);
    const args = argumentsForPlan(); args.template.canvas.width = 0;
    const call = { name: "obs-plan-scene-template", arguments: args };
    const expected = await oracle.client.callTool(call);
    const actual = await native.client.callTool(call);
    expect(actual).toEqual(expected);
    expect(actual.isError).toBe(true);
    expect(trace(native)).toEqual([]); expect(trace(oracle)).toEqual([]);
    noWrites(native); noWrites(oracle);
  });

  it("retires native owners before retaining the SDK's explicit OBS error projection", async () => {
    const fixture: Fixture = fake => { fake.respondWith("GetVideoSettings", () => { throw new FakeOBSRequestError(500, "fixture unavailable"); }); };
    const oracle = await start(false, legacy, fixture);
    const native = await start(true, legacy, fixture);
    const call = { name: "obs-plan-scene-template", arguments: argumentsForPlan() };
    const expected = await oracle.client.callTool(call);
    const actual = await native.client.callTool(call);
    expect(semanticResult(actual)).toEqual(semanticResult(expected));
    expect(actual.structuredContent).toMatchObject({ status: "unknown", applied: false });
    expect(native.fake.connectionCount).toBe(2);
    expect(native.fake.openConnectionCount).toBe(1);
    expect(children(native.transport.pid!)).toHaveLength(1);
    noWrites(native); noWrites(oracle);
  });

  it("validates arguments before opening any native OBS connection", async () => {
    const native = await start(true, legacy, undefined, { OBS_MCP_COMPAT_NODE: join(root, "native/target/nonexistent-scene-plan-test-node") });
    const args = argumentsForPlan(); args.template.canvas.width = 0;
    await expect(native.client.callTool({ name: "obs-plan-scene-template", arguments: args })).rejects.toThrow();
    expect(native.fake.connectionCount).toBe(0); expect(native.fake.history()).toEqual([]);
  });

  it("keeps a hidden workflow behind static-filter refusal before native setup", async () => {
    const native = await start(true, legacy, undefined, { OBS_MCP_TOOLS: "obs-get-version", OBS_MCP_TOOLSETS: "", OBS_MCP_COMPAT_NODE: join(root, "native/target/nonexistent-scene-plan-test-node") });
    await expect(native.client.callTool({ name: "obs-plan-scene-template", arguments: argumentsForPlan() })).rejects.toThrow();
    expect(native.fake.connectionCount).toBe(0); expect(native.fake.history()).toEqual([]);
  });

  it("joins the actor and observer before concurrent-request SDK handoff", async () => {
    // Keep the first native read unanswered throughout this15s test; the
    // handoff must cancel/join it before the replacement SDK socket opens.
    const native = await start(true, legacy, fake => { fake.queueSuccess("GetSceneCollectionList", { currentSceneCollectionName: "Audit collection" }, 60_000); });
    const firstReadWait = native.fake.waitForRequest("GetSceneCollectionList", { timeoutMs: 10_000 });
    const plan = native.client.callTool({ name: "obs-plan-scene-template", arguments: argumentsForPlan() });
    void plan.catch(() => undefined);
    const firstRead = await Promise.race([firstReadWait, plan.then(() => { throw new Error("Plan completed before its held fixture read was observed"); })]);
    const nextConnection = native.fake.waitForConnectionCount(2, 10_000).then(() => { expect(native.fake.openConnectionCount).toBe(1); });
    const version = native.client.callTool({ name: "obs-get-version", arguments: {} });
    const [report] = await Promise.all([plan, version, nextConnection]);
    expect(report.structuredContent).toMatchObject({ status: "resolved", applied: false });
    expect(native.fake.history().filter(record => record.connectionId === firstRead.connectionId && record.frame.op === 6).map(record => record.frame.d.requestType)).toEqual(["GetVersion", "GetSceneCollectionList"]);
    expect(native.fake.connectionCount).toBe(2);
    expect(children(native.transport.pid!)).toHaveLength(1);
    noWrites(native);
  });

  it("cancels native work and joins cleanup before starting the sole SDK owner", async () => {
    const native = await start(true, legacy, fake => { fake.queueSuccess("GetSceneCollectionList", { currentSceneCollectionName: "Audit collection" }, 60_000); });
    const controller = new AbortController();
    const firstReadWait = native.fake.waitForRequest("GetSceneCollectionList", { timeoutMs: 10_000 });
    const pending = native.client.callTool({ name: "obs-plan-scene-template", arguments: argumentsForPlan() }, { signal: controller.signal }).then(value => ({ value }), error => ({ error: String(error) }));
    await Promise.race([firstReadWait, pending.then(() => { throw new Error("Plan settled before its held fixture read was observed"); })]);
    const nextConnection = native.fake.waitForConnectionCount(2, 10_000).then(() => { expect(native.fake.openConnectionCount).toBe(1); });
    const settled = Promise.all([pending, nextConnection]);
    controller.abort();
    const [outcome] = await settled;
    expect(outcome).toHaveProperty("error");
    await native.client.callTool({ name: "obs-get-version", arguments: {} });
    expect(native.fake.history().filter(record => record.frame.d.requestType === "GetSceneCollectionList")).toHaveLength(1);
    expect(children(native.transport.pid!)).toHaveLength(1);
    noWrites(native);
  });

  it("joins the native workflow on transport EOF without leaving an OBS socket", async () => {
    const native = await start(true, legacy, fake => { fake.queueSuccess("GetSceneCollectionList", { currentSceneCollectionName: "Audit collection" }, 1_000); });
    const pending = native.client.callTool({ name: "obs-plan-scene-template", arguments: argumentsForPlan() }).then(value => ({ value }), error => ({ error: String(error) }));
    await native.fake.waitForRequest("GetSceneCollectionList");
    expect(children(native.transport.pid!)).toEqual([]);
    await native.transport.close();
    expect(await pending).toHaveProperty("error");
    await vi.waitFor(() => expect(native.fake.openConnectionCount).toBe(0), { timeout: 2_000 });
    expect(native.fake.connectionCount).toBe(1); noWrites(native);
  });

  it("retires the fast read socket before the workflow and keeps later fallback exclusive", async () => {
    const native = await start(true, legacy);
    native.fake.respondWith("GetSceneCollectionList", () => {
      expect(native.fake.openConnectionCount).toBe(1);
      return { currentSceneCollectionName: native.state.collection };
    });
    await native.client.callTool({ name: "obs-get-version", arguments: {} });
    expect(native.fake.connectionCount).toBe(1);
    const plan = await native.client.callTool({ name: "obs-plan-scene-template", arguments: argumentsForPlan() });
    expect(plan.structuredContent).toMatchObject({ status: "resolved" });
    expect(native.fake.connectionCount).toBe(2); await closedNative(native);
    const status = await native.client.callTool({ name: "obs-control-status", arguments: {} });
    expect(status.isError).not.toBe(true);
    expect(children(native.transport.pid!)).toHaveLength(1);
    noWrites(native);
  });
});
