/* SPDX-License-Identifier: GPL-2.0-only */
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
    PATH: process.env.PATH,
    OBS_MCP_COMPAT_NODE: join(root, "native/target/nonexistent-protocol-test-node"),
    // A native admission failure must attempt handoff and exit; it cannot be
    // hidden by another working native-looking response from the SDK child.
    OBS_MCP_COMPAT_ENTRY: join(root, "native/target/nonexistent-protocol-test-entry.js"),
    ...extraEnv,
  } });
  children.push(child);
  const messages: Record<string, unknown>[] = [];
  let pending = "";
  child.stdout.on("data", (chunk: Buffer) => {
    pending += chunk.toString();
    let newline: number;
    while ((newline = pending.indexOf("\n")) >= 0) {
      messages.push(JSON.parse(pending.slice(0, newline)) as Record<string, unknown>);
      pending = pending.slice(newline + 1);
    }
  });
  child.stderr.on("data", () => undefined);
  child.stdin.on("error", () => undefined);
  return {
    child,
    messages,
    send(method: string, id?: unknown, params?: unknown) {
      child.stdin.write(`${JSON.stringify({ jsonrpc: "2.0", method, ...(id === undefined ? {} : { id }), ...(params === undefined ? {} : { params }) })}\n`);
    },
    async response(id: number) {
      await vi.waitFor(() => expect(messages.some(message => message.id === id)).toBe(true), { timeout: 2_000 });
      return messages.find(message => message.id === id)!;
    },
    async handedOff() {
      await vi.waitFor(() => expect(child.exitCode).not.toBeNull(), { timeout: 2_000 });
      expect(child.exitCode).not.toBe(0);
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
  it.each(["wss://localhost:4455", " WSS://localhost:4455 "])("keeps encrypted endpoint %s with the SDK owner", async url => {
    const server = start({ OBS_WEBSOCKET_URL: url });
    server.send("server/discover", 1, modernParams);
    await server.handedOff();
    expect(server.messages).toEqual([]);
  });

  it("retires native admission instead of switching from modern to legacy", async () => {
    const server = start();
    server.send("server/discover", 1, modernParams);
    expect(await server.response(1)).toHaveProperty("result.supportedVersions", ["2026-07-28"]);
    server.send("initialize", 2, legacyParams);
    await server.handedOff();
    expect(server.messages.some(message => message.id === 2)).toBe(false);
  });

  it("retires native admission instead of switching from legacy to modern", async () => {
    const server = start();
    server.send("initialize", 1, legacyParams);
    await server.response(1);
    server.send("notifications/initialized");
    server.send("server/discover", 2, modernParams);
    await server.handedOff();
    expect(server.messages.some(message => message.id === 2)).toBe(false);
  });

  it("keeps a legacy session usable and delegates duplicate initialization", async () => {
    const server = start();
    server.send("initialize", 1, legacyParams);
    await server.response(1);
    server.send("notifications/initialized");
    server.send("ping", 2);
    expect(await server.response(2)).toHaveProperty("result", {});
    server.send("initialize", 3, legacyParams);
    await server.handedOff();
    expect(server.messages.some(message => message.id === 3)).toBe(false);
  });

  it.each([null, true, 1.5, {}, []].map(id => ({ id })))("delegates invalid native request identity $id", async ({ id }) => {
    const server = start();
    server.send("server/discover", id, modernParams);
    await server.handedOff();
    expect(server.messages).toEqual([]);
  });

  it.each([
    { ...modernParams._meta, progressToken: "progress" },
    { ...modernParams._meta, "io.modelcontextprotocol/clientCapabilities": { elicitation: { form: {} } } },
  ])("keeps richer metadata behind the SDK owner", async meta => {
    const server = start();
    server.send("server/discover", 1, { _meta: meta });
    await server.handedOff();
    expect(server.messages).toEqual([]);
  });
});
