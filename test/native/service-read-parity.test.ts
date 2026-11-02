/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, realpathSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, expect, it } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../support/fake-obs-server.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/debug/obs-mcp-native");
const compat = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type Schema = Record<string, unknown>;
const requests = (JSON.parse(readFileSync(join(root, "native/assets/tools.json"), "utf8")) as {
  requests: Record<string, { requestType: string }>;
}).requests;
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];

function example(schema: Schema): unknown {
  if ("default" in schema) return schema.default;
  if ("const" in schema) return schema.const;
  if (Array.isArray(schema.enum)) return schema.enum[0];
  const choices = schema.anyOf ?? schema.oneOf;
  if (Array.isArray(choices)) return example(choices.find(item => item.type !== "null") ?? choices[0]);
  const type = Array.isArray(schema.type) ? schema.type.find(type => type !== "null") : schema.type;
  if (type === "object" || schema.properties) {
    return Object.fromEntries(Object.entries((schema.properties ?? {}) as Record<string, Schema>).map(([key, value]) => [key, example(value)]));
  }
  if (type === "array") return [];
  if (type === "boolean") return false;
  if (type === "number" || type === "integer") return Math.max(1, Number(schema.minimum ?? 0));
  if (type === "null") return null;
  if (type === "string") return "Fixture";
  return {};
}
function normalize(result: Awaited<ReturnType<Client["callTool"]>>) {
  return { ...result, content: result.content.map(block => {
    if (block.type !== "text") return block;
    try { const { text, ...rest } = block; return { ...rest, jsonValue: JSON.parse(text) as unknown }; }
    catch { return block; }
  }) };
}
async function start(native: boolean, legacy: boolean) {
  // The predecessor connects eagerly during MCP negotiation; advertise the
  // fixed read vocabulary before either process can cache GetVersion.
  const fake = await FakeOBSServer.start({ availableRequests: Object.values(requests).map(entry => entry.requestType) });
  const directory = join(realpathSync(process.env.OBS_MCP_STATE_DIR!), `owned-read-${sessions.length}`);
  mkdirSync(directory, { mode: 0o700 });
  const client = new Client({ name: "owned-read-parity", version: "1" }, {
    versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } },
  });
  const transport = new StdioClientTransport({ command: native ? binary : process.execPath,
    args: native ? [] : [compat], cwd: root, stderr: "pipe", env: {
      ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory,
      OBS_MCP_NATIVE_STATEFUL: native ? "1" : "0", OBS_MCP_READ_OBS_CONFIG: "false",
      OBS_MCP_COMPAT_NODE: join(directory, "nonexistent-node"), OBS_MCP_COMPAT_ENTRY: join(directory, "nonexistent-sdk.js"),
      OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false",
    } });
  let diagnostics = "";
  transport.stderr?.on("data", bytes => { diagnostics = (diagnostics + String(bytes)).slice(-4000); });
  const session = { client, transport, fake }; sessions.push(session);
  try { await client.connect(transport); } catch (error) { throw new Error(`${native ? "native" : "oracle"} startup: ${String(error)}; fixture diagnostics: ${diagnostics}`); }
  return session;
}
afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try { await session.client.close(); await session.transport.close(); }
    finally { await session.fake.close(); }
  }
});

for (const legacy of [false, true]) it(`keeps all 59 qualified reads on one native owner with SDK results (legacy=${legacy})`, async () => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  const actualTools = (await oracle.client.listTools()).tools;
  const owned = (await native.client.listTools()).tools;
  expect(Object.keys(requests)).toHaveLength(59);
  for (const name of Object.keys(requests)) expect(owned.find(tool => tool.name === name), name).toEqual(actualTools.find(tool => tool.name === name));
  const responses = new Map<string, Record<string, unknown>>();
  for (const [name, definition] of Object.entries(requests)) {
    if (definition.requestType === "GetVersion") continue;
    const tool = actualTools.find(tool => tool.name === name)!;
    const response = example(tool.outputSchema ?? { type: "object" }) as Record<string, unknown>;
    responses.set(definition.requestType, { ...responses.get(definition.requestType), ...response, futureField: [1, 2, 3] });
  }
  for (const session of [oracle, native]) for (const [type, response] of responses) session.fake.respondWith(type, () => response);
  for (const [name] of Object.entries(requests)) {
    const tool = actualTools.find(tool => tool.name === name)!;
    const args = example(tool.inputSchema) as Record<string, unknown>;
    const expected = await oracle.client.callTool({ name, arguments: { ...args, ignored: "strip" } });
    const actual = await native.client.callTool({ name, arguments: { ...args, ignored: "strip" } });
    expect(expected.isError, `${name}: ${JSON.stringify(expected)}`).not.toBe(true); expect(actual.isError, `${name}: ${JSON.stringify(actual)}`).not.toBe(true);
    expect(normalize(actual), name).toEqual(normalize(expected));
  }
  for (const session of [oracle, native]) session.fake.respondWith("GetSourceActive", () => { throw new FakeOBSRequestError(500, "fixture private error detail"); });
  const args = { sourceName: "Fixture" };
  expect(normalize(await native.client.callTool({ name: "obs-get-source-active", arguments: args })))
    .toEqual(normalize(await oracle.client.callTool({ name: "obs-get-source-active", arguments: args })));
  const effects = (fake: FakeOBSServer) => fake.history().filter(item => item.frame.op === 6 && item.frame.d.requestType !== "GetVersion")
    .map(item => ({ type: item.frame.d.requestType, data: item.frame.d.requestData ?? {} }));
  expect(effects(native.fake)).toEqual(effects(oracle.fake));
  expect(native.fake.connectionCount).toBe(1);
  const table = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(row => row.trim().split(/\s+/).map(Number));
  expect(table.filter(([, parent]) => parent === native.transport.pid)).toEqual([]);
});

for (const legacy of [false, true]) it(`preserves malformed qualified-read refusal without dispatch (legacy=${legacy})`, async () => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  const outcome = async (client: Client, args: Record<string, unknown>) => {
    try { return { result: normalize(await client.callTool({ name: "obs-get-input-settings", arguments: args })) }; }
    catch (error) { return { threw: String(error) }; }
  };
  for (const args of [{}, { inputName: 42 }, { inputName: null }]) {
    expect(await outcome(native.client, args)).toEqual(await outcome(oracle.client, args));
  }
  for (const session of [oracle, native]) expect(session.fake.history().filter(item => item.frame.op === 6 && item.frame.d.requestType !== "GetVersion")).toEqual([]);
});

for (const legacy of [false, true]) it(`preserves the pinned schema's Unicode control-reason bounds (legacy=${legacy})`, async () => {
  const oracle = await start(false, legacy); const native = await start(true, legacy);
  function lease(result: Awaited<ReturnType<Client["callTool"]>>) {
    const normalized = normalize(result);
    normalized.content = normalized.content.map(block => "text" in block && typeof block.text === "string" ? { ...block, text: block.text.replace(/\d\d:\d\d UTC/g, "TIME UTC") } : block);
    if (normalized.structuredContent) {
      const value = normalized.structuredContent as { since: number; expiresAt: number; reason: string };
      expect(Number.isSafeInteger(value.since)).toBe(true); expect(value.expiresAt - value.since).toBe(60_000);
      normalized.structuredContent = { ...value, since: 0, expiresAt: 60_000 };
    }
    return normalized;
  }
  for (const count of [101, 200, 201]) {
    const args = { minutes: 1, reason: "😀".repeat(count) };
    const expected = await oracle.client.callTool({ name: "obs-claim-control", arguments: args });
    const actual = await native.client.callTool({ name: "obs-claim-control", arguments: args });
    expect(lease(actual), `${count} Unicode code points`).toEqual(lease(expected));
    // Each boundary case gets a fresh lease: renewal intentionally preserves
    // since while extending expiry, so its total lifetime exceeds one minute.
    expect(normalize(await native.client.callTool({ name: "obs-release-control", arguments: {} })))
      .toEqual(normalize(await oracle.client.callTool({ name: "obs-release-control", arguments: {} })));
  }
  expect(native.fake.connectionCount).toBe(0);
});
