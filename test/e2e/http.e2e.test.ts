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

function text(result: CallToolResult): string {
  return result.content.map((block) => (block.type === "text" ? block.text : "")).join("\n");
}

afterEach(async () => {
  await Promise.allSettled(clients.splice(0).map((client) => client.close()));
  for (const child of children.splice(0)) child.kill("SIGTERM");
  await Promise.allSettled(fakes.splice(0).map((fake) => fake.close()));
});

describe("compiled HTTP server", () => {
  it("lets two agents share one server and one OBS connection, with a control lease", async () => {
    const fake = await FakeOBSServer.start();
    fakes.push(fake);
    fake.respondWith("SetCurrentProgramScene", () => ({}));
    const url = await startHttpServer(fake);
    expect(url).toMatch(/^http:\/\/127\.0\.0\.1:\d+\/mcp$/);

    const codex = await agent(url, "codex-mcp-client");
    const claude = await agent(url, "claude-code");
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
    expect(await rawPost(url, { host: `127.0.0.1:${port}` })).toBe(401);
    expect(await rawPost(url, { host: `127.0.0.1:${port}`, authorization: "Bearer wrong-secret!" })).toBe(401);
    expect(await rawPost(url.replace("/mcp", "/other"), { host: `127.0.0.1:${port}` })).toBe(404);

    const call = await agent(url, "claude-code", { authorization: "Bearer local-secret" });
    expect((await call("obs-control-status")).isError).toBeFalsy();
  });
});
