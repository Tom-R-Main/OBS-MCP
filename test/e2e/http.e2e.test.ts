/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { spawn, type ChildProcess } from "node:child_process";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { Client, StreamableHTTPClientTransport, type CallToolResult } from "@modelcontextprotocol/client";
import { getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";

const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const cliPath = fileURLToPath(new URL("../../build/index.js", import.meta.url));

const children: ChildProcess[] = [];
const clients: Client[] = [];
const fakes: FakeOBSServer[] = [];

/** Starts the compiled server in HTTP mode on a free port and returns its MCP URL. */
async function startHttpServer(fake: FakeOBSServer, env: Record<string, string> = {}): Promise<string> {
  const child = spawn(process.execPath, [cliPath], {
    cwd: projectRoot,
    env: { ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_HTTP_PORT: "0", ...env },
    stdio: ["ignore", "ignore", "pipe"],
  });
  children.push(child);
  let stderr = "";
  return await new Promise<string>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`No HTTP URL in stderr:\n${stderr}`)), 5_000);
    child.stderr!.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
      const match = /Serving MCP over HTTP at (\S+)/.exec(stderr);
      if (match?.[1]) {
        clearTimeout(timer);
        resolve(match[1]);
      }
    });
  });
}

async function agent(url: string, name: string, headers: Record<string, string> = {}) {
  const client = new Client({ name, version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  await client.connect(new StreamableHTTPClientTransport(new URL(url), { requestInit: { headers } }));
  clients.push(client);
  return async (tool: string, args: Record<string, unknown> = {}) => await client.callTool({ name: tool, arguments: args }) as CallToolResult;
}

/** A raw request, so the Host header can be set (fetch forbids it). */
function rawPost(url: string, headers: Record<string, string>): Promise<number> {
  const target = new URL(url);
  return new Promise((resolve, reject) => {
    const request = http.request({ host: target.hostname, port: target.port, path: target.pathname, method: "POST", headers: { "content-type": "application/json", ...headers } }, (response) => {
      response.resume();
      resolve(response.statusCode ?? 0);
    });
    request.on("error", reject);
    request.end(JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/list", params: {} }));
  });
}

async function postJson(url: string, body: unknown, headers: Record<string, string> = {}) {
  const target = new URL(url);
  return await new Promise<{ status: number; body: string }>((resolve, reject) => {
    const request = http.request({
      host: target.hostname,
      port: target.port,
      path: target.pathname,
      method: "POST",
      headers: { "content-type": "application/json", accept: "application/json, text/event-stream", ...headers },
    }, (response) => {
      let body = "";
      response.setEncoding("utf8");
      response.on("data", (chunk: string) => { body += chunk; });
      response.on("end", () => resolve({ status: response.statusCode ?? 0, body }));
      response.on("error", reject);
    });
    request.on("error", reject);
    request.setTimeout(5_000, () => request.destroy(new Error("HTTP test request timed out")));
    request.end(JSON.stringify(body));
  });
}

const modernMeta = {
  "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  "io.modelcontextprotocol/clientCapabilities": {},
  "io.modelcontextprotocol/clientInfo": { name: "http-regression-test", version: "1.0.0" },
};

function text(result: CallToolResult): string {
  return result.content.map((block) => (block.type === "text" ? block.text : "")).join("\n");
}

afterEach(async () => {
  await Promise.allSettled(clients.splice(0).map((client) => client.close()));
  for (const child of children.splice(0)) child.kill("SIGTERM");
  await Promise.allSettled(fakes.splice(0).map((fake) => fake.close()));
});

describe("compiled HTTP server", () => {
  it.each(["modern", "legacy"])("rejects oversized %s request bodies and remains available", async (era) => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    const url = await startHttpServer(fake);
    const body = {
      jsonrpc: "2.0", id: 1, method: "tools/list",
      params: { ...(era === "modern" ? { _meta: modernMeta } : {}), padding: "x".repeat(4 * 1024 * 1024) },
    };
    const headers: Record<string, string> = era === "modern"
      ? { "mcp-protocol-version": "2026-07-28", "mcp-method": "tools/list" }
      : { "mcp-protocol-version": "2025-11-25" };

    // A chunked upload exercises the bounded reader without a Content-Length shortcut.
    const response = await postJson(url, body, { ...headers, "transfer-encoding": "chunked" });
    expect(response.status).toBe(413);
    expect(JSON.parse(response.body).error.message).toContain("Payload Too Large");

    const call = await agent(url, `after-large-${era}`);
    expect((await call("obs-control-status")).isError).toBeFalsy();
    await vi.waitFor(async () => expect(text(await call("obs-get-status"))).toContain('"connected": true'), { timeout: 3_000 });
  });

  it("rejects legacy batches exceeding 100 messages and remains available", async () => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    const url = await startHttpServer(fake);
    const batch = Array.from({ length: 101 }, (_, id) => ({ jsonrpc: "2.0", id, method: "tools/list", params: {} }));
    const response = await postJson(url, batch, { "mcp-protocol-version": "2025-11-25" });
    expect(response.status).toBe(400);
    expect(JSON.parse(response.body).error).toMatchObject({ code: -32600, message: expect.stringContaining("100 messages") });

    const call = await agent(url, "after-large-batch");
    expect((await call("obs-control-status")).isError).toBeFalsy();
    await vi.waitFor(async () => expect(text(await call("obs-get-status"))).toContain('"connected": true'), { timeout: 3_000 });
  });

  it("serves concurrent discovery requests with independent server instances", async () => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    const url = await startHttpServer(fake);
    const responses = await Promise.all(Array.from({ length: 8 }, (_, id) => postJson(url, {
      jsonrpc: "2.0", id, method: "server/discover", params: { _meta: modernMeta },
    }, { "mcp-protocol-version": "2026-07-28", "mcp-method": "server/discover" })));

    for (const [id, response] of responses.entries()) {
      expect(response.status).toBe(200);
      // Discovery is an ordinary response stream; consume its complete result before reconnecting.
      const event = response.body.split("\n").find((line) => line.startsWith("data: "));
      const result = JSON.parse(event ? event.slice(6) : response.body);
      expect(result.id).toBe(id);
      expect(result.error).toBeUndefined();
      expect(result.result.supportedVersions).toContain("2026-07-28");
    }

    const call = await agent(url, "after-concurrent-discovery");
    expect((await call("obs-control-status")).isError).toBeFalsy();
    await vi.waitFor(async () => expect(text(await call("obs-get-status"))).toContain('"connected": true'), { timeout: 3_000 });
  });

  it("lets two agents share one server and one OBS connection, with a control lease", async () => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    fake.respondWith("SetCurrentProgramScene", () => ({}));
    const url = await startHttpServer(fake);
    expect(url).toMatch(/^http:\/\/127\.0\.0\.1:\d+\/mcp$/);

    const codex = await agent(url, "codex-mcp-client");
    const claude = await agent(url, "claude-code");
    // A 2025-era client over HTTP is named by its URL.
    const legacy = new Client({ name: "legacy", version: "1.0.0" }, { versionNegotiation: { mode: "legacy" } });
    await legacy.connect(new StreamableHTTPClientTransport(new URL(`${url}?client=codex-legacy`)));
    clients.push(legacy);
    expect(text(await legacy.callTool({ name: "obs-control-status", arguments: {} }) as CallToolResult))
      .toContain("any client may change OBS");
    expect(text(await legacy.callTool({ name: "obs-claim-control", arguments: {} }) as CallToolResult)).toMatch(/^codex-legacy has control/);
    await legacy.callTool({ name: "obs-release-control", arguments: {} });
    await vi.waitFor(async () => expect(text(await claude("obs-get-status"))).toContain('"connected": true'), { timeout: 3_000 });

    await codex("obs-claim-control", { reason: "recording" });
    const refused = await claude("obs-set-current-scene", { sceneName: "B" });
    const allowed = await codex("obs-set-current-scene", { sceneName: "A" });

    expect(text(refused)).toContain("codex-mcp-client has control (recording)");
    expect(allowed.isError).toBeFalsy();
    expect(fake.connectionCount).toBe(1);
  });

  it("rejects other Host headers, other paths, and missing tokens", async () => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    const url = await startHttpServer(fake, { OBS_MCP_HTTP_TOKEN: "local-secret" });
    const port = new URL(url).port;

    expect(await rawPost(url, { host: "evil.example", authorization: "Bearer local-secret" })).toBe(403);
    expect(await rawPost(url, { host: `127.0.0.1:${port}`, origin: "https://evil.example", authorization: "Bearer local-secret" })).toBe(403);
    expect(await rawPost(url, { host: `127.0.0.1:${port}` })).toBe(401);
    expect(await rawPost(url, { host: `127.0.0.1:${port}`, authorization: "Bearer wrong-secret!" })).toBe(401);
    expect(await rawPost(url.replace("/mcp", "/other"), { host: `127.0.0.1:${port}` })).toBe(404);

    const call = await agent(url, "claude-code", { authorization: "Bearer local-secret" });
    expect((await call("obs-control-status")).isError).toBeFalsy();
  });
});
