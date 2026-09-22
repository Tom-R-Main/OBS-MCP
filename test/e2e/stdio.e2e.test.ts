/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { EventEmitter, once } from "node:events";
import { createServer } from "node:net";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import {
  StdioClientTransport,
  getDefaultEnvironment,
} from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeOBSServer } from "../support/fake-obs-server.js";
import { TOOL_COUNT } from "../support/tool-count.js";

type JsonObject = Record<string, unknown>;
type JsonRpcMessage = JsonObject & { id?: string | number };

const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const cliPath = fileURLToPath(new URL("../../build/index.js", import.meta.url));
const testPassword = "stdio-e2e-password";
const modernMeta = {
  "io.modelcontextprotocol/protocolVersion": "2026-07-28",
  "io.modelcontextprotocol/clientInfo": { name: "obs-mcp-raw-e2e", version: "1.0.0" },
  "io.modelcontextprotocol/clientCapabilities": {},
};

const fakeServers: FakeOBSServer[] = [];
const mcpClients: Client[] = [];
const rawChildren: RawMcpChild[] = [];

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textContent(result: { content: readonly unknown[] }): string {
  return result.content
    .filter((item): item is { type: "text"; text: string } => (
      isObject(item) && item.type === "text" && typeof item.text === "string"
    ))
    .map(({ text }) => text)
    .join("\n");
}

async function reservePort(): Promise<number> {
  const server = createServer();
  server.listen({ host: "127.0.0.1", port: 0 });
  await once(server, "listening");
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("Expected a TCP port");
  await new Promise<void>((resolve, reject) => server.close((error) => {
    if (error) reject(error);
    else resolve();
  }));
  return address.port;
}

function childEnvironment(
  obsUrl: string,
  password?: string,
  additional: Record<string, string> = {},
): Record<string, string> {
  return {
    ...getDefaultEnvironment(),
    OBS_WEBSOCKET_URL: obsUrl,
    ...(password ? { OBS_WEBSOCKET_PASSWORD: password } : {}),
    ...additional,
  };
}

async function processIsGone(pid: number): Promise<boolean> {
  try {
    process.kill(pid, 0);
    return false;
  } catch (error) {
    return isObject(error) && error.code === "ESRCH";
  }
}

async function connectClient(
  fake: FakeOBSServer,
  mode: "modern" | "legacy",
  environment: Record<string, string> = {},
): Promise<{ client: Client; transport: StdioClientTransport; stderr: string[] }> {
  const client = new Client(
    { name: `obs-mcp-${mode}-e2e`, version: "1.0.0" },
    mode === "modern"
      ? { versionNegotiation: { mode: { pin: "2026-07-28" } } }
      : { versionNegotiation: { mode: "legacy" } },
  );
  const transport = new StdioClientTransport({
    command: process.execPath,
    args: [cliPath],
    cwd: projectRoot,
    env: childEnvironment(fake.url, fake.password, environment),
    stderr: "pipe",
  });
  const stderr: string[] = [];
  transport.stderr?.on("data", (chunk: Buffer) => {
    const redacted = chunk.toString().replaceAll(testPassword, "[REDACTED]");
    stderr.push(redacted);
    if (stderr.length > 100) stderr.shift();
  });
  await client.connect(transport);
  mcpClients.push(client);
  return { client, transport, stderr };
}

async function waitForObsConnected(client: Client): Promise<void> {
  await vi.waitFor(async () => {
    const status = await client.callTool({ name: "obs-get-status", arguments: {} });
    expect(textContent(status)).toContain('"connected": true');
  }, { timeout: 3_000 });
}

class RawMcpChild {
  readonly child: ChildProcessWithoutNullStreams;
  readonly messages: JsonRpcMessage[] = [];
  readonly invalidStdout: string[] = [];
  readonly stderr: string[] = [];

  private readonly events = new EventEmitter();
  private stdoutBuffer = "";

  constructor(obsUrl: string, password?: string) {
    this.child = spawn(process.execPath, [cliPath], {
      cwd: projectRoot,
      env: childEnvironment(obsUrl, password),
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.child.stdout.on("data", (chunk: Buffer) => this.consumeStdout(chunk));
    this.child.stderr.on("data", (chunk: Buffer) => {
      const redacted = chunk.toString().replaceAll(testPassword, "[REDACTED]");
      this.stderr.push(redacted);
      if (this.stderr.length > 100) this.stderr.shift();
      this.events.emit("stderr");
    });
  }

  send(message: JsonRpcMessage): void {
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  async response(id: string | number, timeoutMs = 2_000): Promise<JsonRpcMessage> {
    const existing = this.messages.find((message) => message.id === id);
    if (existing) return existing;
    return this.waitFor("message", () => this.messages.find((message) => message.id === id), timeoutMs);
  }

  async waitForStderr(pattern: RegExp, timeoutMs = 2_000): Promise<void> {
    if (pattern.test(this.stderr.join(""))) return;
    await this.waitFor(
      "stderr",
      () => pattern.test(this.stderr.join("")) ? true : undefined,
      timeoutMs,
    );
  }

  async endStdinAndWait(timeoutMs = 2_000): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    this.child.stdin.end();
    return this.waitForExit(timeoutMs);
  }

  async waitForExit(timeoutMs = 2_000): Promise<{ code: number | null; signal: NodeJS.Signals | null }> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) {
      return { code: this.child.exitCode, signal: this.child.signalCode };
    }
    return new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.child.off("exit", onExit);
        reject(new Error(`Child did not exit within ${timeoutMs}ms\n${this.stderr.join("")}`));
      }, timeoutMs);
      const onExit = (code: number | null, signal: NodeJS.Signals | null) => {
        clearTimeout(timeout);
        resolve({ code, signal });
      };
      this.child.once("exit", onExit);
    });
  }

  async close(): Promise<void> {
    if (this.child.exitCode !== null || this.child.signalCode !== null) return;
    this.child.stdin.end();
    try {
      await this.waitForExit(500);
      return;
    } catch {
      this.child.kill("SIGTERM");
    }
    try {
      await this.waitForExit(500);
    } catch {
      this.child.kill("SIGKILL");
      await this.waitForExit(500);
    }
  }

  private consumeStdout(chunk: Buffer): void {
    this.stdoutBuffer += chunk.toString();
    while (true) {
      const newline = this.stdoutBuffer.indexOf("\n");
      if (newline === -1) return;
      const line = this.stdoutBuffer.slice(0, newline);
      this.stdoutBuffer = this.stdoutBuffer.slice(newline + 1);
      if (!line) continue;
      try {
        const parsed: unknown = JSON.parse(line);
        if (!isObject(parsed)) throw new Error("MCP line was not a JSON object");
        this.messages.push(parsed);
        this.events.emit("message");
      } catch {
        this.invalidStdout.push(line);
        this.events.emit("message");
      }
    }
  }

  private async waitFor<T>(
    event: "message" | "stderr",
    value: () => T | undefined,
    timeoutMs: number,
  ): Promise<T> {
    const immediate = value();
    if (immediate !== undefined) return immediate;
    return new Promise<T>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.events.off(event, onEvent);
        reject(new Error(`Timed out waiting for child ${event}\n${this.stderr.join("")}`));
      }, timeoutMs);
      const onEvent = () => {
        const result = value();
        if (result === undefined) return;
        clearTimeout(timeout);
        this.events.off(event, onEvent);
        resolve(result);
      };
      this.events.on(event, onEvent);
    });
  }
}

function spawnRawChild(obsUrl: string, password?: string): RawMcpChild {
  const child = new RawMcpChild(obsUrl, password);
  rawChildren.push(child);
  return child;
}

function sendModernDiscover(child: RawMcpChild, id: string): void {
  child.send({
    jsonrpc: "2.0",
    id,
    method: "server/discover",
    params: { _meta: modernMeta },
  });
}

afterEach(async () => {
  await Promise.allSettled(mcpClients.splice(0).map((client) => client.close()));
  await Promise.allSettled(rawChildren.splice(0).map((child) => child.close()));
  await Promise.allSettled(fakeServers.splice(0).map((fake) => fake.close()));
});

describe("compiled stdio MCP boundary", () => {
  it("negotiates modern MCP and calls through to fake OBS", async () => {
    const fake = await FakeOBSServer.start({
      password: testPassword,
      availableRequests: ["SetCurrentProgramScene"],
    });
    fakeServers.push(fake);
    const { client, transport } = await connectClient(fake, "modern");

    expect(client.getProtocolEra()).toBe("modern");
    expect(client.getNegotiatedProtocolVersion()).toBe("2026-07-28");
    const tools = await client.listTools();
    expect(tools.tools).toHaveLength(TOOL_COUNT);
    expect(new Set(tools.tools.map(({ name }) => name)).size).toBe(TOOL_COUNT);
    await waitForObsConnected(client);

    const versionCursor = fake.cursor();
    const version = await client.callTool({ name: "obs-get-version", arguments: {} });
    const versionRequest = await fake.waitForRequest("GetVersion", { after: versionCursor });
    expect(versionRequest.frame.op).toBe(6);
    expect(version.isError).not.toBe(true);
    expect(textContent(version)).toContain('"obsVersion": "32.2.2"');

    fake.queueSuccess("SetCurrentProgramScene");
    const mutationCursor = fake.cursor();
    const mutation = client.callTool({
      name: "obs-set-current-scene",
      arguments: { sceneName: "E2E Program" },
    });
    const mutationRequest = await fake.waitForRequest(
      "SetCurrentProgramScene",
      { after: mutationCursor },
    );
    expect(mutationRequest.frame.d.requestData).toEqual({ sceneName: "E2E Program" });
    expect((await mutation).isError).not.toBe(true);

    const pid = transport.pid;
    expect(pid).not.toBeNull();
    await client.close();
    mcpClients.splice(mcpClients.indexOf(client), 1);
    if (pid !== null) {
      await vi.waitFor(async () => expect(await processIsGone(pid)).toBe(true));
    }
    await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0));
  });

  it("returns bounded screenshot image content and remains live after rejection", async () => {
    const transparentPng =
      "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";
    const fake = await FakeOBSServer.start({ availableRequests: ["GetSourceScreenshot"] });
    fakeServers.push(fake);
    const { client } = await connectClient(fake, "modern", {
      OBS_MCP_MAX_SCREENSHOT_BYTES: "68",
    });
    await waitForObsConnected(client);

    fake.queueSuccess("GetSourceScreenshot", {
      imageData: `data:image/png;base64,${transparentPng}`,
    });
    const screenshot = await client.callTool({
      name: "obs-get-source-screenshot",
      arguments: { sourceName: "Program", imageFormat: "png" },
    });
    expect(screenshot.isError).not.toBe(true);
    expect(screenshot.structuredContent).toEqual({ mimeType: "image/png", sizeBytes: 68 });
    expect(screenshot.content).toContainEqual({
      type: "image",
      mimeType: "image/png",
      data: transparentPng,
    });
    expect(textContent(screenshot)).not.toContain(transparentPng);

    fake.queueSuccess("GetSourceScreenshot", {
      imageData: `data:image/png;base64,${Buffer.alloc(69).toString("base64")}`,
    });
    const oversized = await client.callTool({
      name: "obs-get-source-screenshot",
      arguments: { sourceName: "Program", imageFormat: "png" },
    });
    expect(oversized.isError).toBe(true);
    expect(textContent(oversized)).toContain("69 bytes; limit is 68 bytes");

    const invalidDimensions = await client.callTool({
      name: "obs-get-source-screenshot",
      arguments: { sourceName: "Program", imageFormat: "png", imageWidth: 7 },
    });
    expect(invalidDimensions.isError).toBe(true);

    const version = await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(version.isError).not.toBe(true);
    expect(version.structuredContent).toEqual(expect.objectContaining({ obsVersion: "32.2.2" }));
  });

  it("keeps discovery online and reconnects when fake OBS returns", async () => {
    const port = await reservePort();
    const offlineUrl = `ws://127.0.0.1:${port}`;
    const client = new Client(
      { name: "obs-mcp-offline-e2e", version: "1.0.0" },
      { versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: [cliPath],
      cwd: projectRoot,
      env: childEnvironment(offlineUrl),
      stderr: "pipe",
    });
    transport.stderr?.on("data", () => undefined);
    await client.connect(transport);
    mcpClients.push(client);

    expect((await client.listTools()).tools).toHaveLength(TOOL_COUNT);
    const unavailable = await client.callTool({ name: "obs-get-scene-list", arguments: {} });
    expect(unavailable.isError).toBe(true);
    expect(textContent(unavailable)).toContain("Unable to connect to OBS WebSocket server");

    const first = await FakeOBSServer.start({ port, availableRequests: ["GetSceneList"] });
    fakeServers.push(first);
    first.queueSuccess("GetSceneList", { currentProgramSceneName: "Program", scenes: [] });
    const recovered = await client.callTool({ name: "obs-get-scene-list", arguments: {} });
    expect(recovered.isError).not.toBe(true);
    expect(textContent(recovered)).toContain('"scenes": []');

    await first.close();
    fakeServers.splice(fakeServers.indexOf(first), 1);
    await vi.waitFor(async () => {
      const status = await client.callTool({ name: "obs-get-status", arguments: {} });
      expect(textContent(status)).toContain('"connected": false');
    });

    const second = await FakeOBSServer.start({ port, availableRequests: ["GetSceneList"] });
    fakeServers.push(second);
    second.queueSuccess("GetSceneList", { currentProgramSceneName: "Recovered", scenes: [] });
    const recoveredAgain = await client.callTool({ name: "obs-get-scene-list", arguments: {} });
    expect(recoveredAgain.isError).not.toBe(true);
    expect(textContent(recoveredAgain)).toContain("Recovered");
  }, 15_000);

  it("serves the explicit 2025 legacy era without a probe sibling", async () => {
    const fake = await FakeOBSServer.start();
    fakeServers.push(fake);
    const { client } = await connectClient(fake, "legacy");
    await fake.waitForConnectionCount(1);

    expect(client.getProtocolEra()).toBe("legacy");
    expect(client.getNegotiatedProtocolVersion()).toBe("2025-11-25");
    expect((await client.listTools()).tools).toHaveLength(TOOL_COUNT);
    await waitForObsConnected(client);
    const result = await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(result.isError).not.toBe(true);
    expect(fake.connectionCount).toBe(1);
  });

  it("supports modern probe followed by legacy initialize in one process", async () => {
    const fake = await FakeOBSServer.start();
    fakeServers.push(fake);
    const child = spawnRawChild(fake.url);

    sendModernDiscover(child, "discover");
    expect(await child.response("discover")).toHaveProperty("result");
    child.send({
      jsonrpc: "2.0",
      id: "initialize",
      method: "initialize",
      params: {
        protocolVersion: "2025-11-25",
        capabilities: {},
        clientInfo: { name: "raw-legacy-client", version: "1.0.0" },
      },
    });
    const initialized = await child.response("initialize");
    expect(initialized).toHaveProperty("result.protocolVersion", "2025-11-25");
    child.send({ jsonrpc: "2.0", method: "notifications/initialized" });
    child.send({ jsonrpc: "2.0", id: "tools", method: "tools/list", params: {} });
    const tools = await child.response("tools");
    expect(tools).toHaveProperty("result.tools");
    const result = tools.result;
    expect(isObject(result) && Array.isArray(result.tools) ? result.tools : []).toHaveLength(TOOL_COUNT);
    expect(child.invalidStdout).toEqual([]);
    expect(child.stderr.join("")).not.toContain("Unhandled");
  });

  it("exits on stdin EOF while OBS is identified and a request is active", async () => {
    const fake = await FakeOBSServer.start({ availableRequests: ["GetStats"] });
    fakeServers.push(fake);
    const child = spawnRawChild(fake.url);
    sendModernDiscover(child, "discover-active");
    await child.response("discover-active");
    fake.queueSuccess("GetStats", { activeFps: 60 }, 5_000);
    const cursor = fake.cursor();
    child.send({
      jsonrpc: "2.0",
      id: "call-active",
      method: "tools/call",
      params: { name: "obs-get-stats", arguments: {}, _meta: modernMeta },
    });
    await fake.waitForRequest("GetStats", { after: cursor });

    const exit = await child.endStdinAndWait();
    expect(exit).toEqual({ code: 0, signal: null });
    expect(child.invalidStdout).toEqual([]);
    await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0));
  });

  it("registers only the configured tool groups in read-only mode", async () => {
    const fake = await FakeOBSServer.start();
    fakeServers.push(fake);
    const { client, stderr } = await connectClient(fake, "modern", {
      OBS_MCP_TOOLSETS: "core",
      OBS_MCP_READ_ONLY: "true",
    });

    const { tools } = await client.listTools();
    const names = tools.map(({ name }) => name);
    expect(tools.length).toBeGreaterThan(0);
    expect(tools.length).toBeLessThan(TOOL_COUNT);
    expect(tools.every(({ annotations }) => annotations?.readOnlyHint === true)).toBe(true);
    expect(names).toEqual(expect.arrayContaining(["obs-get-status", "obs-preflight"]));
    expect(names).not.toContain("obs-start-record");
    expect(names).not.toContain("obs-get-stream-status");
    await vi.waitFor(() => expect(stderr.join("")).toMatch(/Registering \d+ of \d+ tools \(read-only\)/));
  });

  it("refuses to start with an unknown tool group or tool name", async () => {
    const environments: Record<string, string>[] = [
      { OBS_MCP_TOOLSETS: "scenez" },
      { OBS_MCP_TOOLS: "obs-start-recording" },
    ];
    for (const environment of environments) {
      const child = spawn(process.execPath, [cliPath], {
        cwd: projectRoot,
        env: childEnvironment("ws://127.0.0.1:1", undefined, environment),
      });
      let stderr = "";
      child.stderr.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
      const [code] = await once(child, "exit") as [number | null];

      expect(code).toBe(1);
      expect(stderr).toMatch(/scenez|obs-start-recording/);
    }
  });

  it("exits on stdin EOF while OBS is offline", async () => {
    const port = await reservePort();
    const child = spawnRawChild(`ws://127.0.0.1:${port}`);
    await child.waitForStderr(/Server startup complete/);

    const exit = await child.endStdinAndWait();
    expect(exit).toEqual({ code: 0, signal: null });
    expect(child.invalidStdout).toEqual([]);
  });

  it.skipIf(process.platform === "win32")("handles SIGTERM through the graceful path", async () => {
    const port = await reservePort();
    const child = spawnRawChild(`ws://127.0.0.1:${port}`);
    await child.waitForStderr(/Server startup complete/);
    child.child.kill("SIGTERM");

    expect(await child.waitForExit()).toEqual({ code: 0, signal: null });
    expect(child.invalidStdout).toEqual([]);
  });
});
