/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type Lane = "native" | "typescript";
type Environment = Record<string, string>;
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];

function environment(env: Environment = {}): Environment {
  return { ...getDefaultEnvironment(), OBS_MCP_READ_OBS_CONFIG: "false", OBS_MCP_TOOLSETS: "", OBS_MCP_TOOLS: "", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false", OBS_MCP_COMPAT_ENTRY: compatibility, OBS_MCP_COMPAT_NODE: process.execPath, ...env };
}

function children(pid: number): number[] {
  const rows = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number));
  const result: number[] = [];
  const walk = (parent: number) => { for (const [id, ppid] of rows) if (ppid === parent && id) { result.push(id); walk(id); } };
  walk(pid);
  return result;
}

async function start(lane: Lane, env: Environment = {}, legacy = false) {
  const fake = await FakeOBSServer.start();
  fake.respondWith("GetInputSettings", () => ({ inputKind: "image_source", inputSettings: { file: "fixture.png" } }));
  fake.respondWith("GetInputMute", () => ({ inputMuted: false }));
  fake.respondWith("SetInputMute", () => ({}));
  const directory = join(process.env.OBS_MCP_STATE_DIR!, `${lane}-${sessions.length}`);
  mkdirSync(directory);
  const client = new Client({ name: "static-filter-parity", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({ command: lane === "native" ? binary : process.execPath, args: lane === "native" ? [] : [compatibility], cwd: root, env: environment({ OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory, ...env }), stderr: "pipe" });
  transport.stderr?.on("data", () => undefined);
  const session = { client, transport, fake };
  sessions.push(session);
  await client.connect(transport);
  return session;
}

afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try { await session.client.close(); }
    finally { try { await session.transport.close(); } finally { await session.fake.close(); } }
  }
});

const selections: { name: string; env: Environment }[] = [
  { name: "default", env: {} },
  { name: "core", env: { OBS_MCP_TOOLSETS: " core " } },
  { name: "selected groups and regrouped extensions", env: { OBS_MCP_TOOLSETS: "inputs, scenes" } },
  { name: "explicit tools", env: { OBS_MCP_TOOLS: "obs-get-version, obs-get-input-settings" } },
  { name: "group plus explicit tool", env: { OBS_MCP_TOOLSETS: "ui", OBS_MCP_TOOLS: "obs-get-input-settings" } },
  { name: "read-only overrides explicit mutation", env: { OBS_MCP_TOOLSETS: "inputs", OBS_MCP_TOOLS: "obs-start-record", OBS_MCP_READ_ONLY: " YES " } },
  { name: "read-only all", env: { OBS_MCP_READ_ONLY: "true" } },
  { name: "all short-circuits unknown groups", env: { OBS_MCP_TOOLSETS: "all,unknown-group" } },
  { name: "empty selection means all", env: { OBS_MCP_TOOLSETS: " , ", OBS_MCP_TOOLS: " , " } },
  { name: "duplicates preserve registry order", env: { OBS_MCP_TOOLSETS: "inputs,inputs", OBS_MCP_TOOLS: "obs-get-version,obs-get-version" } },
  { name: "JavaScript BOM whitespace", env: { OBS_MCP_TOOLSETS: "\uFEFFcore\uFEFF", OBS_MCP_READ_ONLY: "\uFEFFyes\uFEFF" } },
];

describe.each([false, true])("static native tool filtering (legacy=%s)", legacy => {
  it.each(selections)("matches actual predecessor inventory for $name without starting JS", async ({ env }) => {
    const oracle = await start("typescript", env, legacy);
    const native = await start("native", env, legacy);
    const expected = (await oracle.client.listTools()).tools;
    expect(expected.length).toBeGreaterThan(0);
    expect((await native.client.listTools()).tools).toEqual(expected);
    expect(children(native.transport.pid!)).toEqual([]);
  });

  it("executes an admitted read childlessly and refuses a hidden mutation without dispatch", async () => {
    const env = { OBS_MCP_TOOLS: "obs-get-input-settings" };
    const oracle = await start("typescript", env, legacy);
    const native = await start("native", env, legacy);
    const args = { name: "obs-get-input-settings", arguments: { inputName: "Fixture" } };
    expect((await native.client.callTool(args)).structuredContent).toEqual((await oracle.client.callTool(args)).structuredContent);
    expect(children(native.transport.pid!)).toEqual([]);
    // Preserve the pinned SDK's refusal category and text rather than inventing
    // a second hidden-tool error projection in the Rust adapter.
    // The first refusal is an otherwise native-eligible read, so removing the
    // filter admission check would dispatch it instead of handing off.
    for (const hidden of [
      { name: "obs-get-input-mute", arguments: { inputName: "Fixture" } },
      { name: "obs-set-input-mute", arguments: { inputName: "Fixture", inputMuted: true } },
    ]) {
      const expected = await oracle.client.callTool(hidden).then(value => ({ value }), error => ({ error: String(error) }));
      const actual = await native.client.callTool(hidden).then(value => ({ value }), error => ({ error: String(error) }));
      expect(actual).toEqual(expected);
    }
    expect(oracle.fake.history().some(record => record.frame.d.requestType === "GetInputMute")).toBe(false);
    expect(native.fake.history().some(record => record.frame.d.requestType === "GetInputMute")).toBe(false);
    expect(oracle.fake.history().some(record => record.frame.d.requestType === "SetInputMute")).toBe(false);
    expect(native.fake.history().some(record => record.frame.d.requestType === "SetInputMute")).toBe(false);
    expect(children(native.transport.pid!)).toHaveLength(1);
  });

  it("retains the SDK owner for dynamic toolsets", async () => {
    const env = { OBS_MCP_DYNAMIC_TOOLSETS: "\uFEFFtrue\uFEFF" };
    const oracle = await start("typescript", env, legacy);
    const native = await start("native", env, legacy);
    expect((await native.client.listTools()).tools).toEqual((await oracle.client.listTools()).tools);
    expect(children(native.transport.pid!)).toHaveLength(1);
  });
});

const refusals: { env: Environment; message: string }[] = [
    { env: { OBS_MCP_TOOLSETS: "unknown-group" }, message: 'Unknown OBS_MCP_TOOLSETS entry "unknown-group"' },
    { env: { OBS_MCP_TOOLSETS: "\u0085core\u0085" }, message: 'Unknown OBS_MCP_TOOLSETS entry "\u0085core\u0085"' },
    { env: { OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "unknown-tool" }, message: "Unknown OBS_MCP_TOOLS entries: unknown-tool" },
    { env: { OBS_MCP_TOOLS: "obs-start-record", OBS_MCP_READ_ONLY: "true" }, message: "The tool filter excludes every tool" },
];

describe("static filter startup failures", () => {
  it.each(refusals)("matches predecessor refusal $message", ({ env, message }) => {
    for (const lane of ["typescript", "native"] as const) {
      const result = spawnSync(lane === "native" ? binary : process.execPath, lane === "native" ? [] : [compatibility], { cwd: root, env: environment(env), input: "", encoding: "utf8", timeout: 5_000 });
      expect(result.error).toBeUndefined();
      expect(result.status).toBe(1);
      expect(result.stdout).toBe("");
      expect(result.stderr).toContain(message);
    }
  });
});
