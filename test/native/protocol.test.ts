/* SPDX-License-Identifier: GPL-2.0-only */
import { nativeDefaultFixture } from "../support/native-default-fixture.js";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it, vi } from "vitest";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const modernParams = { _meta: {
  "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  "io.modelcontextprotocol/clientInfo": { name: "protocol-probe", version: "1" },
  "io.modelcontextprotocol/clientCapabilities": {},
} };
const legacyParams = { protocolVersion: "2025-11-25", clientInfo: { name: "protocol-probe", version: "1" }, capabilities: {} };
const children: ChildProcessWithoutNullStreams[] = [];

function start(extraEnv: Record<string, string> = {}) {
  const child = spawn(binary, [], { cwd: root, stdio: ["pipe", "pipe", "pipe"], env: {
    PATH: process.env.PATH, ...nativeDefaultFixture(root), OBS_MCP_TOOLSETS: "all", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false",
    ...extraEnv,
  } });
  children.push(child);
  const messages: Record<string, unknown>[] = [];
  let pending = "";
  let diagnostics = "";
  child.stdout.on("data", (chunk: Buffer) => {
    pending += chunk.toString();
    let newline: number;
    while ((newline = pending.indexOf("\n")) >= 0) {
      messages.push(JSON.parse(pending.slice(0, newline)) as Record<string, unknown>);
      pending = pending.slice(newline + 1);
    }
  });
  child.stderr.on("data", bytes => { diagnostics += String(bytes); });
  child.stdin.on("error", () => undefined);
  return {
    child,
    messages,
    send(method: string, id?: unknown, params?: unknown) {
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, ...(id === undefined ? {} : { id }), ...(params === undefined ? {} : { params }) })}\n`);
    },
    async response(id: unknown) {
      await vi.waitFor(() => expect(messages.some(message => message.id === id)).toBe(true), { timeout: 2_000 });
      return messages.find(message => message.id === id)!;
    },
    diagnostics: () => diagnostics,
    async refusedStartup() {
      await vi.waitFor(() => expect(child.exitCode).not.toBeNull(), { timeout: 2_000 });
      expect(child.exitCode).toBe(1);
    },
  };
}

afterEach(async () => {
  for (const child of children.splice(0)) {
    if (child.exitCode !== null || child.signalCode !== null) continue;
    child.stdin.end();
    await vi.waitFor(() => expect(child.exitCode !== null || child.signalCode !== null).toBe(true), { timeout: 2_000 });
  }
});

describe("native protocol admission", () => {
  it.each(["wss://localhost:4455", " WSS://localhost:4455 "])("refuses feature-off encrypted endpoint %s without compatibility", async url => {
    const server = start({ OBS_WEBSOCKET_URL: url });
    await server.refusedStartup();
    expect(server.messages).toEqual([]);
    expect(server.diagnostics().trim()).toBe("OBS MCP native: Native configuration requires a supported WS/WSS endpoint; unavailable transports cannot activate");
    expect(server.diagnostics()).not.toContain(url);
  });

  it("refuses a modern to legacy switch and retains the original modern session", async () => {
    const server = start();
    server.send("server/discover", 1, modernParams);
    expect(await server.response(1)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
    server.send("initialize", 2, legacyParams);
    expect(await server.response(2)).toHaveProperty("error.code", -32601);
    server.send("server/discover", 3, modernParams);
    expect(await server.response(3)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
    expect(server.child.exitCode).toBeNull();
  });

  it("refuses a legacy to modern switch and retains the original legacy session", async () => {
    const server = start();
    server.send("initialize", 1, legacyParams); await server.response(1); server.send("notifications/initialized");
    server.send("server/discover", 2, modernParams);
    expect(await server.response(2)).toHaveProperty("error.code", -32601);
    server.send("ping", 3); expect(await server.response(3)).toHaveProperty("result", {});
  });

  it("keeps a legacy session usable after refusing duplicate initialization", async () => {
    const server = start();
    server.send("initialize", 1, legacyParams); await server.response(1); server.send("notifications/initialized");
    server.send("ping", 2); expect(await server.response(2)).toHaveProperty("result", {});
    server.send("initialize", 3, legacyParams);
    expect(await server.response(3)).toHaveProperty("error.code", -32601);
    server.send("ping", 4); expect(await server.response(4)).toHaveProperty("result", {});
  });

  it.each([null, true, 1.5, {}, []].map(id => ({ id })))("refuses invalid request identity $id without admitting discovery", async ({ id }) => {
    const server = start(); server.send("server/discover", id, modernParams);
    await vi.waitFor(() => expect(server.messages).toHaveLength(1));
    expect(server.messages[0]).toHaveProperty("error.code", -32601);
    expect(server.messages[0]).not.toHaveProperty("result");
    server.send("server/discover", 10, modernParams);
    expect(await server.response(10)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
  });

  it("refuses unowned progress metadata without replacing the session", async () => {
    const server = start(); server.send("server/discover", 1, { _meta: { ...modernParams._meta, progressToken: "progress" } });
    expect(await server.response(1)).toHaveProperty("error.code", -32601);
    server.send("server/discover", 2, modernParams);
    expect(await server.response(2)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
  });

  it("admits an unused valid elicitation offer without invoking or granting it", async () => {
    const server = start(); server.send("server/discover", 1, { _meta: { ...modernParams._meta, "io.modelcontextprotocol/clientCapabilities": { elicitation: { form: {} } } } });
    expect(await server.response(1)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
    expect(server.messages).toHaveLength(1);
    expect(server.child.exitCode).toBeNull();
  });
});
