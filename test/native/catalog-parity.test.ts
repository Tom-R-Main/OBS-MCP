/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
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
type Session = { client: Client; transport: StdioClientTransport; fake: FakeOBSServer };
const sessions: Session[] = [];

async function start(native: boolean, legacy: boolean, env: Record<string, string> = {}): Promise<Session> {
  const fake = await FakeOBSServer.start();
  const state = join(process.env.OBS_MCP_STATE_DIR!, `catalog-${sessions.length}`);
  mkdirSync(state);
  const client = new Client({ name: "catalog-parity", version: "1.0.0" }, {
    versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } },
  });
  const transport = new StdioClientTransport({
    command: native ? binary : process.execPath,
    args: native ? [] : [compatibility],
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
      ...env,
    },
    stderr: "pipe",
  });
  transport.stderr?.on("data", () => undefined);
  const session = { client, transport, fake };
  sessions.push(session);
  await client.connect(transport);
  return session;
}

async function pair(legacy: boolean, env: Record<string, string> = {}) {
  return [await start(false, legacy, env), await start(true, legacy, env)] as const;
}

function semanticResult(result: Awaited<ReturnType<Client["callTool"]>>) {
  return {
    ...result,
    content: result.content.map(block => {
      if (block.type !== "text") return block;
      try {
        // Normalize valid JSON object key order only. Arrays stay ordered;
        // non-JSON error text, block metadata and all result fields stay exact.
        const { text, ...metadata } = block;
        return { ...metadata, jsonValue: JSON.parse(text) as unknown };
      } catch { return block; }
    }),
  };
}

function assertLocal(session: Session) {
  expect(session.fake.connectionCount).toBe(0);
  expect(session.fake.history()).toEqual([]);
  const rows = execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" })
    .trim().split("\n").map(line => line.trim().split(/\s+/).map(Number));
  expect(rows.filter(([_pid, parent]) => parent === session.transport.pid)).toEqual([]);
}

async function callPair(oracle: Session, native: Session, args: Record<string, unknown>) {
  const expected = await oracle.client.callTool({ name: "obs-describe-request", arguments: args });
  const actual = await native.client.callTool({ name: "obs-describe-request", arguments: args });
  expect(semanticResult(actual)).toEqual(semanticResult(expected));
  return actual;
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

describe.each([false, true])("native protocol catalog differential (legacy=%s)", legacy => {
  it("keeps an explicitly selected read-only catalog local through both admission layers", async () => {
    const [oracle, native] = await pair(legacy, {
      OBS_MCP_TOOLSETS: "", OBS_MCP_TOOLS: "obs-describe-request", OBS_MCP_READ_ONLY: "true",
    });
    const expected = await oracle.client.listTools();
    expect((await native.client.listTools()).tools).toEqual(expected.tools);
    expect(expected.tools.map(tool => tool.name)).toEqual(["obs-describe-request"]);
    const result = await callPair(oracle, native, { requestType: "StartRecord" });
    expect(result.structuredContent).toMatchObject({ requestType: "StartRecord" });
    assertLocal(native);
  });
  it("lists the predecessor's complete ordered catalog locally", async () => {
    const [oracle, native] = await pair(legacy);
    const result = await callPair(oracle, native, {});
    expect(result.isError).not.toBe(true);
    const entries = (result.structuredContent as Record<string, unknown>).result as Record<string, unknown>[];
    expect(entries.length).toBeGreaterThan(0);
    for (const entry of entries) expect(Object.keys(entry).sort()).toEqual([
      "category", "deprecated", "description", "initialVersion", "requestType",
    ]);
    assertLocal(native);
  });

  it("preserves exact request fields and gives nonempty requestType precedence over category", async () => {
    const [oracle, native] = await pair(legacy);
    const result = await callPair(oracle, native, { requestType: "GetVersion", category: "no-such-category", ignored: "strip" });
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent).toMatchObject({ requestType: "GetVersion", requestFields: [], responseFields: expect.any(Array) });
    expect(((result.structuredContent as Record<string, unknown>).responseFields as unknown[]).length).toBeGreaterThan(0);
    assertLocal(native);
  });

  it("retains category case folding, empty-string truthiness and whitespace behavior", async () => {
    const [oracle, native] = await pair(legacy);
    const all = await callPair(oracle, native, {});
    const general = await callPair(oracle, native, { category: "gEnErAl" });
    const entries = (general.structuredContent as Record<string, unknown>).result as Record<string, unknown>[];
    expect(entries.length).toBeGreaterThan(0);
    expect(entries.every(entry => String(entry.category).toLowerCase() === "general")).toBe(true);
    expect(semanticResult(await callPair(oracle, native, { requestType: "", category: "GENERAL" }))).toEqual(semanticResult(general));
    expect(semanticResult(await callPair(oracle, native, { requestType: "", category: "" }))).toEqual(semanticResult(all));
    for (const category of ["no-such-category", " General "]) {
      const result = await callPair(oracle, native, { category });
      expect(result.structuredContent).toEqual({ result: [] });
    }
    assertLocal(native);
  });

  it("preserves exact unknown-request errors without a structured-content envelope", async () => {
    const [oracle, native] = await pair(legacy);
    for (const requestType of ["NoSuchRequest", "getversion", " GetVersion "]) {
      const result = await callPair(oracle, native, { requestType, category: "General" });
      expect(result.isError).toBe(true);
      expect(result.structuredContent).toBeUndefined();
      expect(result.content).toEqual([{ type: "text", text: `Unknown OBS WebSocket request: ${requestType}` }]);
    }
    assertLocal(native);
  });

  it("keeps invalid arguments behind SDK validation without OBS tool effects", async () => {
    const [oracle, native] = await pair(legacy);
    for (const args of [{ requestType: 12 }, { category: null }, { requestType: false }]) {
      const result = await callPair(oracle, native, args);
      expect(result.isError).toBe(true);
    }
    // The SDK owner may perform its eager capability probe. No catalog request,
    // mutation, batch or other OBS operation is permitted by these arguments.
    for (const session of [oracle, native]) {
      expect(session.fake.history().filter(record => record.frame.op === OBS_OP.Request)
        .every(record => record.frame.d.requestType === "GetVersion")).toBe(true);
      expect(session.fake.history().some(record => record.frame.op === OBS_OP.RequestBatch)).toBe(false);
    }
  });
});
