/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../support/fake-obs-server.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];
function children(pid: number) {
  return execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number)).filter(([, parent]) => parent === pid).map(([id]) => id);
}
async function start(native: boolean, legacy: boolean, response: Record<string, unknown>, restricted = false, rejected = false) {
  const fake = await FakeOBSServer.start({ availableRequests: ["GetSceneList"] });
  fake.respondWith("GetSceneList", () => { if (rejected) throw new FakeOBSRequestError(500, "fixture scene-list refusal"); return response; });
  const client = new Client({ name: "native-resource-list", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({ command: native ? binary : process.execPath, args: native ? [] : [compatibility], cwd: root, env: {
    ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: process.env.OBS_MCP_STATE_DIR!, OBS_MCP_COMPAT_ENTRY: compatibility, OBS_MCP_COMPAT_NODE: process.execPath,
    OBS_MCP_READ_OBS_CONFIG: "false", OBS_MCP_TOOLSETS: restricted ? "general" : "all", OBS_MCP_READ_ONLY: restricted ? "true" : "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false",
  }, stderr: "pipe" }); transport.stderr?.on("data", () => undefined);
  const session = { client, transport, fake }; sessions.push(session); await client.connect(transport); return session;
}
afterEach(async () => { for (const session of sessions.splice(0)) { try { await session.client.close(); } finally { try { await session.transport.close(); } finally { await session.fake.close(); } } } });

describe.each([false, true])("native resources/list (legacy=%s)", legacy => {
  const fixtures: Record<string, unknown>[] = [
    {},
    { scenes: "not an array" },
    { scenes: [null, [], 42, {}, { name: "wrong key" }, { sceneName: 3 }, { sceneName: "Kept" }] },
    { scenes: ["Café /日本語?#%💡", "-_.!~*'()", "line\n\r\t\u0000", "", ".", "..", "__OBS_NATIVE_SCENE_70ba__"].map(sceneName => ({ sceneName })) },
    { scenes: [{ sceneName: "Z" }, { sceneName: "A" }, { sceneName: "Z" }] },
  ];
  it.each(fixtures)("matches complete ordered SDK metadata for %j without a child", async response => {
    const oracle = await start(false, legacy, response); const native = await start(true, legacy, response);
    const expected = await oracle.client.listResources();
    expect(await native.client.listResources()).toEqual(expected);
    expect(await native.client.listResources()).toEqual(expected);
    expect(children(native.transport.pid!)).toEqual([]);
    const reads = native.fake.history().filter(record => record.frame.op === 6).map(record => record.frame.d.requestType);
    expect(reads.filter(type => type === "GetSceneList")).toHaveLength(2);
    expect(reads.every(type => type === "GetVersion" || type === "GetSceneList")).toBe(true);
  });
  it("preserves resource availability independently of tool-group filtering", async () => {
    const response = { scenes: [{ sceneName: "Filtered scene" }] };
    const oracle = await start(false, legacy, response, true); const native = await start(true, legacy, response, true);
    expect(await native.client.listResources()).toEqual(await oracle.client.listResources());
    expect(children(native.transport.pid!)).toEqual([]);
  });
  it("preserves SDK refusal after native read cleanup", async () => {
    const oracle = await start(false, legacy, {}, false, true); const native = await start(true, legacy, {}, false, true);
    const outcome = async (client: Client) => { try { return await client.listResources(); } catch (error) { return { error: (error as Error).message }; } };
    expect(await outcome(native.client)).toEqual(await outcome(oracle.client));
    expect(children(native.transport.pid!)).toHaveLength(1);
  });
  it("hands oversized native admission to the SDK without truncating resources", async () => {
    const response = { scenes: Array.from({ length: 4097 }, (_, index) => ({ sceneName: `Scene ${index}` })) };
    const oracle = await start(false, legacy, response); const native = await start(true, legacy, response);
    expect(await native.client.listResources()).toEqual(await oracle.client.listResources());
    expect(children(native.transport.pid!)).toHaveLength(1);
  });
  it("hands live resource state to one exclusive compatibility owner", async () => {
    const native = await start(true, legacy, { scenes: [] });
    await native.client.listResources(); expect(children(native.transport.pid!)).toEqual([]);
    const take = await native.client.readResource({ uri: "obs://take/current" });
    expect(take.contents).toEqual([{ uri: "obs://take/current", mimeType: "application/json", text: JSON.stringify({ running: false }, null, 2) }]);
    expect(children(native.transport.pid!)).toHaveLength(1);
    expect((await native.client.callTool({ name: "obs-get-version", arguments: {} })).isError).not.toBe(true);
    expect(children(native.transport.pid!)).toHaveLength(1);
  });
});
