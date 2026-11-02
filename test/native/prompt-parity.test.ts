/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";
const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type PromptCall = { name: string; arguments?: Record<string, string> };
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];
function children(pid: number) { return execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number)).filter(([, p]) => p === pid).map(([id]) => id); }
async function start(native: boolean, legacy: boolean, brokenSdk = false) {
 const fake = await FakeOBSServer.start();
 const client = new Client({ name: "native-prompt-parity", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
 const transport = new StdioClientTransport({ command: native ? binary : process.execPath, args: native ? [] : [compatibility], cwd: root, env: {
  ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: process.env.OBS_MCP_STATE_DIR!, OBS_MCP_COMPAT_ENTRY: compatibility, OBS_MCP_COMPAT_NODE: brokenSdk ? join(root, "native/target/nonexistent-prompt-sdk") : process.execPath,
  OBS_MCP_READ_OBS_CONFIG: "false", OBS_MCP_TOOLSETS: "all", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false",
 }, stderr: "pipe" }); transport.stderr?.on("data", () => undefined);
 const session = { client, transport, fake }; sessions.push(session); await client.connect(transport); return session;
}
afterEach(async () => { for (const s of sessions.splice(0)) { try { await s.client.close(); } finally { try { await s.transport.close(); } finally { await s.fake.close(); } } } });
describe.each([false, true])("offline native prompts (legacy=%s)", legacy => {
 it("preserves complete prompt inventory without child or OBS socket", async () => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  expect(await native.client.listPrompts()).toEqual(await oracle.client.listPrompts());
  expect(native.fake.connectionCount).toBe(0); expect(children(native.transport.pid!)).toEqual([]);
 });
 it("lists exact resource templates without child or OBS socket", async () => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  expect(await native.client.listResourceTemplates()).toEqual(await oracle.client.listResourceTemplates());
  expect(native.fake.connectionCount).toBe(0); expect(children(native.transport.pid!)).toEqual([]);
 });
 const cases: PromptCall[] = [
  { name: "software-demo", arguments: { sceneCollection: "collection", sceneName: "scene", inputName: "input" } },
  ...["0x10", "1e2", "+30", "0b10", "0o10", "\uFEFF 3600 \uFEFF"].map(maxDurationSeconds => ({ name: "software-demo", arguments: { sceneCollection: "C", sceneName: "S", inputName: "I", maxDurationSeconds } })),
  { name: "software-demo", arguments: { sceneCollection: '__OBS_NATIVE_SCENE_70ba__"\\\n', sceneName: '章節\u2028', inputName: '__OBS_NATIVE_INPUT_70ba__', extra: "strip" } },
  { name: "record-demo", arguments: { window: "editor" } },
  { name: "record-demo", arguments: { window: '__OBS_NATIVE_SCENE_70ba__', scene: '__OBS_NATIVE_SECONDS_70ba__', seconds: "12\nquotes\"" } },
  { name: "record-demo", arguments: { window: "editor", steps: " first\r\n \n\uFEFF second\uFEFF\n__OBS_NATIVE_SCENE_70ba__", scene: "Fixture" } },
  { name: "record-demo", arguments: { window: "", steps: "  \r\n\uFEFF  " } },
  { name: "record-demo", arguments: { window: "editor", steps: "", seconds: "" } },
  { name: "pre-stream-check" },
  { name: "pre-stream-check", arguments: { ignored: "value" } },
 ];
 it.each(cases)("matches complete $name text for $arguments", async call => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  expect(await native.client.getPrompt(call)).toEqual(await oracle.client.getPrompt(call));
  expect(native.fake.connectionCount).toBe(0); expect(children(native.transport.pid!)).toEqual([]);
 });
 const invalidCases: PromptCall[] = [{ name: "software-demo", arguments: { sceneCollection: "C", sceneName: "S", inputName: "I", maxDurationSeconds: "0" } }, { name: "software-demo", arguments: {} }, { name: "unknown-prompt" }];
 it.each(invalidCases)("keeps invalid/unknown $name with SDK interpretation", async call => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  const outcome = async (client: Client) => { try { return { result: await client.getPrompt(call) }; } catch (error) { return { error: (error as Error).message }; } };
  expect(await outcome(native.client)).toEqual(await outcome(oracle.client)); expect(children(native.transport.pid!)).toHaveLength(1);
 });
 it("refuses unsupported prompt arguments before any native OBS setup", async () => {
  const native = await start(true, legacy, true);
  await expect(native.client.getPrompt({ name: "software-demo", arguments: {} })).rejects.toThrow(); expect(native.fake.history()).toEqual([]);
 });
 it("keeps JavaScript signed-radix refusal instead of Rust integer acceptance", async () => {
  const native = await start(true, legacy);
  await expect(native.client.getPrompt({ name: "software-demo", arguments: { sceneCollection: "C", sceneName: "S", inputName: "I", maxDurationSeconds: "0x+10" } })).rejects.toThrow("maxDurationSeconds must be an integer from 1 to 3600");
  expect(children(native.transport.pid!)).toHaveLength(1);
 });
});
