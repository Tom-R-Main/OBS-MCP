/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";
import { serveProduction } from "../support/production-fixture.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
const contract = JSON.parse(readFileSync(join(root, "native/assets/tools.json"), "utf8")) as { tools: unknown[]; requests: Record<string, unknown> };
const open: { client: Client; transport: StdioClientTransport }[] = [];
const fakes: FakeOBSServer[] = [];
function children(pid: number): number[] {
  const rows = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number));
  const result: number[] = [];
  const walk = (parent: number) => { for (const [id, ppid] of rows) if (ppid === parent && id) { result.push(id); walk(id); } };
  walk(pid); return result;
}
async function start({ legacy = false, env = {} }: { legacy?: boolean; env?: Record<string, string> } = {}) {
  const fake = await FakeOBSServer.start(); fakes.push(fake);
  const client = new Client({ name: "obs-native-parity", version: "1.0.0" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({ command: binary, args: [], cwd: root, env: {
    ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_COMPAT_ENTRY: compatibility,
    OBS_MCP_STATE_DIR: process.env.OBS_MCP_STATE_DIR!, OBS_MCP_COMPAT_NODE: process.execPath,
    OBS_MCP_READ_OBS_CONFIG: "false", OBS_MCP_TOOLSETS: "all", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false", ...env,
  }, stderr: "pipe" });
  transport.stderr?.on("data", () => undefined);
  open.push({ client, transport }); await client.connect(transport);
  return { fake, client, transport };
}
afterEach(async () => {
  for (const { client, transport } of open.splice(0)) { await client.close(); await transport.close(); }
  for (const fake of fakes.splice(0)) await fake.close();
});

describe("native MCP runtime boundary", () => {
  it.each([false, true])("preserves the full SDK inventory and handles fresh read RPCs without a JS child (legacy=%s)", async (legacy) => {
    const { client, fake, transport } = await start({ legacy });
    expect((await client.listTools()).tools).toEqual(contract.tools);
    const before = fake.history().filter(r => r.frame.d.requestType === "GetVersion").length;
    const first = await client.callTool({ name: "obs-get-version", arguments: {} });
    const second = await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(first.isError).not.toBe(true); expect(second.structuredContent).toEqual(first.structuredContent);
    const requests = fake.history().filter(r => r.frame.d.requestType === "GetVersion");
    expect(requests.length - before).toBe(3); // capability discovery plus two actual public reads
    expect(children(transport.pid!)).toEqual([]);
    expect(fake.eventSubscriptions()).toBe(0);
  });

  it.each([false, true])("hands off once and retains stage tokens, replay refusal and custody (legacy=%s)", async (legacy) => {
    const { client, fake, transport } = await start({ legacy });
    const state = serveProduction(fake);
    await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(children(transport.pid!)).toEqual([]);
    const stage = await client.callTool({ name: "obs-stage-scene", arguments: { sceneCollection: state.collection, sceneName: state.sceneName } });
    expect(stage.isError).not.toBe(true);
    const child = children(transport.pid!); expect(child.length).toBe(1);
    const stageId = (stage.structuredContent as Record<string, unknown>).stageId;
    const taken = await client.callTool({ name: "obs-take-scene", arguments: { stageId } });
    expect(taken.structuredContent).toMatchObject({ intendedSceneObservedOnProgram: true });
    const replay = await client.callTool({ name: "obs-take-scene", arguments: { stageId } });
    expect(replay.isError).toBe(true);
    expect(fake.history().filter(r => r.frame.d.requestType === "TriggerStudioModeTransition")).toHaveLength(1);
    const observed = await client.callTool({ name: "obs-observe", arguments: {} });
    expect(observed.structuredContent).toMatchObject({ operationReceipts: { operations: expect.arrayContaining([expect.objectContaining({ kind: "take-scene", phase: "observed" })]) } });
    await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(children(transport.pid!)).toEqual(child);
    const requestConnections = fake.history().filter(r => r.frame.d.requestType === "GetVersion").map(r => r.connectionId);
    expect(new Set(requestConnections).size).toBe(2); // native session is retired before the sole compatibility session
  });

  it("keeps native serialization guards and input validation ahead of any unsafe request", async () => {
    const { client, fake } = await start();
    fake.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 1, window: 42 } }));
    const invalid = await client.callTool({ name: "obs-get-input-settings", arguments: { inputName: 12 } });
    expect(invalid.isError).toBe(true);
    expect(fake.history().some(r => r.frame.d.requestType === "GetInputSettings")).toBe(false);
    const guarded = await client.callTool({ name: "obs-get-input-property-list-items", arguments: { inputName: "Capture", propertyName: "display_uuid" } });
    expect(guarded.isError).toBe(true);
    expect(fake.history().some(r => r.frame.d.requestType === "GetInputPropertiesListPropertyItems")).toBe(false);
  });

  it("honors a configured read-only tool filter through the SDK owner", async () => {
    const { client } = await start({ env: { OBS_MCP_READ_ONLY: "true" } });
    const tools = (await client.listTools()).tools;
    expect(tools.length).toBeLessThan(contract.tools.length);
    expect(tools.every(tool => tool.annotations?.readOnlyHint === true)).toBe(true);
  });

  it("retains generic response extension fields and strips unknown request fields", async () => {
    const { client, fake, transport } = await start();
    fake.respondWith("GetInputSettings", data => {
      expect(data).toEqual({ inputName: "Capture" });
      return { inputKind: "screen_capture", inputSettings: { type: 1, window: 42 }, futureField: [1, 2] };
    });
    const result = await client.callTool({ name: "obs-get-input-settings", arguments: { inputName: "Capture", extra: "strip me" } });
    expect(result.structuredContent).toMatchObject({ futureField: [1, 2] });
    expect(children(transport.pid!)).toEqual([]);
  });
});
