/* SPDX-License-Identifier: GPL-2.0-only */
import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { FakeOBSServer, OBS_OP } from "../support/fake-obs-server.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const compatibility = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, "build/index.js");
type Lane = "native" | "typescript";
type Fixture = (fake: FakeOBSServer) => void;
type Session = { lane: Lane; client: Client; transport: StdioClientTransport; fake: FakeOBSServer };
const sessions: Session[] = [];

async function start(lane: Lane, legacy: boolean, fixture: Fixture = () => undefined): Promise<Session> {
  const fake = await FakeOBSServer.start();
  fixture(fake);
  const state = join(process.env.OBS_MCP_STATE_DIR!, `${lane}-${sessions.length}`);
  mkdirSync(state);
  // Identical client identity/configuration; only the launched implementation differs.
  const client = new Client({ name: "contract-parity", version: "1.0.0" }, {
    versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } },
  });
  const transport = new StdioClientTransport({
    command: lane === "native" ? binary : process.execPath,
    args: lane === "native" ? [] : [compatibility],
    cwd: root,
    env: {
      ...getDefaultEnvironment(),
      OBS_WEBSOCKET_URL: fake.url,
      OBS_MCP_STATE_DIR: state,
      OBS_MCP_COMPAT_ENTRY: compatibility,
      OBS_MCP_COMPAT_NODE: process.execPath,
      OBS_MCP_READ_OBS_CONFIG: "false",
      OBS_MCP_TOOLSETS: "all",
      OBS_MCP_READ_ONLY: "false",
      OBS_MCP_DYNAMIC_TOOLSETS: "false",
      OBS_MCP_CONFIRM_LIVE: "false",
    },
    stderr: "pipe",
  });
  transport.stderr?.on("data", () => undefined);
  const session = { lane, client, transport, fake };
  sessions.push(session);
  await client.connect(transport);
  return session;
}

async function pair(legacy: boolean, fixture?: Fixture) {
  // Serial startup keeps setup load from becoming a protocol timing assertion.
  const oracle = await start("typescript", legacy, fixture);
  const native = await start("native", legacy, fixture);
  return [oracle, native] as const;
}

function requests(fake: FakeOBSServer) {
  return fake.history().filter(record => record.frame.op === OBS_OP.Request);
}

function effects(fake: FakeOBSServer) {
  // GetVersion capability probes differ between implementations. Ignore only those
  // probes, not arbitrary reads or writes. Public version freshness has its own test.
  return requests(fake)
    .filter(record => record.frame.d.requestType !== "GetVersion")
    .map(record => ({ requestType: record.frame.d.requestType, requestData: record.frame.d.requestData ?? {} }));
}

function semanticResult(result: Awaited<ReturnType<Client["callTool"]>>) {
  return {
    ...result,
    content: result.content.map(block => {
      if (block.type !== "text") return block;
      try {
        // JSON objects do not promise insertion order. Keep all other text and
        // block metadata exact; do not normalize errors or discard content.
        const { text, ...metadata } = block;
        return { ...metadata, jsonValue: JSON.parse(text) as unknown };
      } catch { return block; }
    }),
  };
}

afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try { await session.client.close(); }
    finally {
      try { await session.transport.close(); }
      finally { await session.fake.close(); }
    }
  }
});

describe.each([false, true])("production contract differential (legacy=%s)", (legacy) => {
  it("matches the actual SDK registry, including schemas, defaults, descriptions and annotations", async () => {
    const [oracle, native] = await pair(legacy);
    const expected = (await oracle.client.listTools()).tools;
    // The oracle is a separately launched predecessor, not tools.json or a copied list.
    expect(expected.length).toBeGreaterThan(0);
    expect((await native.client.listTools()).tools).toEqual(expected);
  });

  it("preserves results and ordered effects across read, mutation, defaulted mutation and subsequent reads", async () => {
    const fixture: Fixture = fake => {
      let muted = false;
      fake.respondWith("GetInputSettings", () => ({
        inputKind: "screen_capture", inputSettings: { type: 1, window: 42 }, futureField: [1, 2],
      }));
      fake.respondWith("SetInputMute", data => {
        expect(fake.openConnectionCount).toBe(1);
        expect(data).toEqual({ inputName: "Capture", inputMuted: true });
        muted = true;
        return {};
      });
      fake.respondWith("GetInputMute", () => ({ inputMuted: muted }));
      fake.respondWith("SetTBarPosition", data => {
        expect(fake.openConnectionCount).toBe(1);
        expect(data).toEqual({ position: 0.25, release: true });
        return {};
      });
    };
    const [oracle, native] = await pair(legacy, fixture);
    const results: unknown[][] = [];
    for (const session of [oracle, native]) {
      const result = [];
      result.push(await session.client.callTool({ name: "obs-get-input-settings", arguments: { inputName: "Capture", ignored: "strip" } }));
      result.push(await session.client.callTool({ name: "obs-set-input-mute", arguments: { inputName: "Capture", inputMuted: true, ignored: "strip" } }));
      result.push(await session.client.callTool({ name: "obs-get-input-mute", arguments: { inputName: "Capture" } }));
      result.push(await session.client.callTool({ name: "obs-set-tbar-position", arguments: { position: 0.25 } }));
      result.push(await session.client.callTool({ name: "obs-get-input-settings", arguments: { inputName: "Capture" } }));
      for (const value of result) expect(value.isError).not.toBe(true);
      expect(result[0]?.structuredContent).toMatchObject({ futureField: [1, 2] });
      expect(result[2]?.structuredContent).toEqual({ inputMuted: true });
      results.push(result.map(semanticResult));
    }
    expect(results[1]).toEqual(results[0]);
    expect(effects(native.fake)).toEqual(effects(oracle.fake));
    expect(effects(native.fake).map(effect => effect.requestType)).toEqual([
      "GetInputSettings", "SetInputMute", "GetInputMute", "SetTBarPosition", "GetInputSettings",
    ]);
    // The modern pinned SDK may launch a disposable negotiation sibling, whose
    // eager TS startup can open a socket before that sibling is reaped. Count
    // admitted tool owners, not every TCP connection attempt during negotiation.
    const oracleCalls = requests(oracle.fake).filter(record => record.frame.d.requestType !== "GetVersion");
    expect(new Set(oracleCalls.map(record => record.connectionId)).size).toBe(1);
    const calls = requests(native.fake).filter(record => record.frame.d.requestType !== "GetVersion");
    expect(new Set(calls.map(record => record.connectionId)).size).toBe(2);
    expect(calls[0]?.connectionId).not.toBe(calls[1]?.connectionId);
    expect(new Set(calls.slice(1).map(record => record.connectionId)).size).toBe(1);
  });

  it("refuses malformed read arguments before any OBS tool dispatch in both implementations", async () => {
    const [oracle, native] = await pair(legacy, fake => {
      fake.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: {} }));
    });
    for (const session of [oracle, native]) {
      for (const args of [{}, { inputName: 12 }, { inputName: null }]) {
        const result = await session.client.callTool({ name: "obs-get-input-settings", arguments: args });
        expect(result.isError).toBe(true);
      }
      // Error text is implementation-specific until the protocol error port;
      // admission and the complete effect trace are the promised boundary here.
      expect(effects(session.fake)).toEqual([]);
    }
  });

  it("retains capture enumeration refusals through dedicated, raw and batch routes with zero writes", async () => {
    const [oracle, native] = await pair(legacy, fake => {
      fake.respondWith("GetInputSettings", () => ({
        inputKind: "screen_capture", inputSettings: { type: 0, display_uuid: "chosen-display" },
      }));
      // Respond if a regression reaches the dangerous request, so it fails quickly.
      fake.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [] }));
      fake.respondWith("SetInputSettings", () => ({}));
    });
    const data = { inputName: "Capture", propertyName: "display_uuid" };
    const argumentsByTool = [
      { name: "obs-get-input-property-list-items", arguments: data },
      { name: "obs-call-request", arguments: { requestType: "GetInputPropertiesListPropertyItems", requestData: data } },
      { name: "obs-batch", arguments: { requests: [
        { requestType: "SetInputSettings", requestData: { inputName: "Capture", inputSettings: { type: 0 } } },
        { requestType: "GetInputPropertiesListPropertyItems", requestData: data },
      ] } },
    ];
    const results: unknown[][] = [];
    for (const session of [oracle, native]) {
      const result = [];
      for (const args of argumentsByTool) {
        const response = await session.client.callTool(args);
        expect(response.isError).toBe(true);
        result.push(response);
      }
      const trace = effects(session.fake);
      expect(trace.length).toBeGreaterThan(0);
      expect(trace.every(effect => effect.requestType === "GetInputSettings")).toBe(true);
      expect(session.fake.history().some(record => record.frame.op === OBS_OP.RequestBatch)).toBe(false);
      results.push(result);
    }
    expect(results[1]).toEqual(results[0]);
    expect(effects(native.fake)).toEqual(effects(oracle.fake));
  });
});
