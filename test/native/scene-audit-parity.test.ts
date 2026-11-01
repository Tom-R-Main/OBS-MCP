/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../support/fake-obs-server.js";
import { serveSceneAuditFixture, SCENE_AUDIT_READS } from "../support/scene-audit-fixture.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type State = ReturnType<typeof serveSceneAuditFixture>;
type Fixture = (fake: FakeOBSServer, state: State) => void;
type Session = { client: Client; transport: StdioClientTransport; fake: FakeOBSServer };
const sessions: Session[] = [];

function children(pid: number) {
  return execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number)).filter(([, parent]) => parent === pid).map(([id]) => id);
}
async function start(native: boolean, legacy: boolean, fixture: Fixture = () => undefined, canvas?: string, env: Record<string, string> = {}) {
  const fake = await FakeOBSServer.start();
  const state = serveSceneAuditFixture(fake, canvas); fixture(fake, state);
  const directory = join(process.env.OBS_MCP_STATE_DIR!, `audit-native-${sessions.length}`); mkdirSync(directory);
  const client = new Client({ name: "native-audit-parity", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({ command: native ? binary : process.execPath, args: native ? [] : [compatibility], cwd: root, env: {
    ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory, OBS_MCP_COMPAT_ENTRY: compatibility, OBS_MCP_COMPAT_NODE: process.execPath,
    OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false", OBS_MCP_READ_OBS_CONFIG: "false", ...env,
  }, stderr: "pipe" });
  transport.stderr?.on("data", () => undefined);
  const session = { client, transport, fake }; sessions.push(session); await client.connect(transport); return session;
}
function report(value: unknown) {
  const result = structuredClone(value) as Record<string, unknown>;
  for (const field of ["observedAt", "completedAt"]) if (field in result) {
    expect(result[field]).toEqual(expect.stringMatching(/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/)); result[field] = "2026-10-31T00:00:00.000Z";
  }
  if ("durationMs" in result) { expect(Number.isSafeInteger(result.durationMs)).toBe(true); expect(Number(result.durationMs)).toBeGreaterThanOrEqual(0); result.durationMs = 0; }
  return result;
}
function semantic(result: Awaited<ReturnType<Client["callTool"]>>) {
  return { ...result, structuredContent: result.structuredContent ? report(result.structuredContent) : undefined,
    content: result.content.map(block => block.type === "text" ? { ...block, text: report(JSON.parse(block.text)) } : block) };
}
function trace(session: Session) {
  return session.fake.history().filter(frame => frame.frame.op === 6 && frame.frame.d.requestType !== "GetVersion").map(frame => ({ requestType: frame.frame.d.requestType, requestData: frame.frame.d.requestData ?? {} }));
}
function noWrites(session: Session) {
  expect(session.fake.history().filter(({ frame }) => frame.op === 6).every(({ frame }) => SCENE_AUDIT_READS.has(String(frame.d.requestType)))).toBe(true);
  expect(session.fake.history().some(({ frame }) => frame.op === 8)).toBe(false);
}
async function closedNative(session: Session) {
  await vi.waitFor(() => expect(session.fake.openConnectionCount).toBe(0), { timeout: 2_000 });
  expect(children(session.transport.pid!)).toEqual([]); noWrites(session);
}
afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try { await session.client.close(); } finally { try { await session.transport.close(); } finally { await session.fake.close(); } }
  }
});

describe.each([false, true])("native structural audit (legacy=%s)", legacy => {
  const cases: { name: string; fixture?: Fixture; canvas?: string; options?: Record<string, unknown>; status: string }[] = [
    { name: "flat scene", status: "pass" },
    { name: "named canvas", canvas: "canvas-qualified", status: "pass" },
    { name: "bounded screenshot", options: { includeScreenshot: true }, status: "pass" },
    { name: "missing required binding", options: { requiredItems: [{ sourceName: "Missing" }] }, status: "fail" },
    { name: "disabled required binding", fixture: (_fake, state) => { state.items[0]!.sceneItemEnabled = false; }, options: { requiredItems: [{ sourceName: "Application" }] }, status: "fail" },
    { name: "zero scale", fixture: (_fake, state) => { state.items[0]!.sceneItemTransform.scaleX = 0; }, status: "fail" },
    { name: "null group input identity", fixture: (_fake, state) => { const item = state.items[0] as unknown as Record<string, unknown>; item.isGroup = null; item.sourceType = "OBS_SOURCE_TYPE_INPUT"; }, status: "pass" },
    { name: "unclassified null group", fixture: (_fake, state) => { (state.items[0] as unknown as Record<string, unknown>).isGroup = null; }, status: "unknown" },
    { name: "unrelated program event", fixture: (fake, state) => { fake.respondWith("GetSceneItemTransform", () => { fake.sendEvent("CurrentProgramSceneChanged", { sceneName: "other" }); return { sceneItemTransform: state.items[0]!.sceneItemTransform }; }); }, status: "pass" },
    { name: "relevant event before reply", fixture: (fake, state) => { fake.respondWith("GetSceneItemTransform", () => { fake.sendEvent("SceneItemTransformChanged", {}); return { sceneItemTransform: state.items[0]!.sceneItemTransform }; }); }, status: "unknown" },
  ];
  it.each(cases)("preserves $name report and ordered read trace childlessly", async ({ fixture, canvas, options, status }) => {
    const oracle = await start(false, legacy, fixture, canvas); const native = await start(true, legacy, fixture, canvas);
    const call = { name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review", ...(canvas ? { canvasUuid: canvas } : {}), ...options } };
    const expected = await oracle.client.callTool(call); const actual = await native.client.callTool(call);
    expect(actual.structuredContent).toMatchObject({ status, scope: "structural", renderedContent: "unverified" });
    expect(semantic(actual)).toEqual(semantic(expected)); expect(trace(native)).toEqual(trace(oracle));
    expect(native.fake.connectionCount).toBe(1); await closedNative(native); noWrites(oracle);
  });
  it("joins native owners before SDK projects explicit peer failures", async () => {
    const fixture: Fixture = fake => { fake.respondWith("GetSceneItemTransform", () => { throw new FakeOBSRequestError(500, "qualified peer rejection"); }); };
    const oracle = await start(false, legacy, fixture); const native = await start(true, legacy, fixture);
    const call = { name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review" } };
    const nextConnection = native.fake.waitForConnectionCount(2, 10_000).then(() => { expect(native.fake.openConnectionCount).toBe(1); });
    const [actual] = await Promise.all([native.client.callTool(call), nextConnection]);
    expect(semantic(actual)).toEqual(semantic(await oracle.client.callTool(call)));
    expect(children(native.transport.pid!)).toHaveLength(1); noWrites(native);
  });
  it("matches SDK invalid-input projection without scene reads", async () => {
    const oracle = await start(false, legacy); const native = await start(true, legacy);
    const call = { name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review", includeScreenshot: "yes" } };
    const expected = await oracle.client.callTool(call); const actual = await native.client.callTool(call);
    expect(actual).toEqual(expected); expect(actual.isError).toBe(true); expect(trace(native)).toEqual([]); expect(trace(oracle)).toEqual([]);
  });
  it("validates before opening native OBS even when SDK cannot start", async () => {
    const native = await start(true, legacy, undefined, undefined, { OBS_MCP_COMPAT_NODE: join(root, "native/target/nonexistent-audit-test-node") });
    await expect(native.client.callTool({ name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review", includeScreenshot: "yes" } })).rejects.toThrow();
    expect(native.fake.connectionCount).toBe(0); expect(native.fake.history()).toEqual([]);
  });
  it("filters hidden workflow before any native setup", async () => {
    const native = await start(true, legacy, undefined, undefined, { OBS_MCP_TOOLSETS: "", OBS_MCP_TOOLS: "obs-get-version", OBS_MCP_COMPAT_NODE: join(root, "native/target/nonexistent-audit-test-node") });
    await expect(native.client.callTool({ name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review" } })).rejects.toThrow();
    expect(native.fake.connectionCount).toBe(0); expect(native.fake.history()).toEqual([]);
  });
  it("retires its fast socket before auditing and closes the audit before reply", async () => {
    const native = await start(true, legacy);
    await native.client.callTool({ name: "obs-get-version", arguments: {} });
    const replacement = native.fake.waitForConnectionCount(2).then(() => { expect(native.fake.openConnectionCount).toBe(1); });
    const result = await native.client.callTool({ name: "obs-audit-scene", arguments: { sceneCollection: "Audit collection", sceneName: "Review" } });
    expect(result.structuredContent).toMatchObject({ status: "pass" }); await replacement; await closedNative(native);
  });
});
