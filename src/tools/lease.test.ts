/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createMcpHandler, McpServer, type McpHttpHandler } from "@modelcontextprotocol/server";
import { Client, StreamableHTTPClientTransport, type CallToolResult } from "@modelcontextprotocol/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { FakeOBSServer } from "../../test/support/fake-obs-server.js";
import { healthyObsState, servePreflightState } from "../../test/support/fake-obs-state.js";
import { initialize } from "./index.js";
import { activeLease, claim } from "./lease.js";

let fakeObs: FakeOBSServer;
let obsClient: OBSWebSocketClient;
let handler: McpHttpHandler;
let directory: string;
const clients: Client[] = [];

/** One MCP client, named as an agent would name itself, sharing the server with the others. */
async function agent(name: string): Promise<(tool: string, args?: Record<string, unknown>) => Promise<CallToolResult>> {
  const client = new Client({ name, version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  await client.connect(new StreamableHTTPClientTransport(
    new URL("http://obs-mcp.test/mcp"),
    { fetch: (input, init) => handler.fetch(new Request(input, init)) },
  ));
  clients.push(client);
  return async (tool, args = {}) => await client.callTool({ name: tool, arguments: args }) as CallToolResult;
}

function text(result: CallToolResult): string {
  return result.content.map((block) => (block.type === "text" ? block.text : "")).join("\n");
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  directory = mkdtempSync(join(tmpdir(), "obs-mcp-lease-"));
  fakeObs = await FakeOBSServer.start();
  servePreflightState(fakeObs, () => healthyObsState(directory));
  fakeObs.respondWith("SetCurrentProgramScene", () => ({}));
  fakeObs.respondWith("CreateRecordChapter", () => ({}));
  fakeObs.respondWith("StartRecord", () => {
    setTimeout(() => fakeObs.sendEvent("RecordStateChanged", { outputActive: true, outputState: "OBS_WEBSOCKET_OUTPUT_STARTED", outputPath: join(directory, "take.mp4") }), 5);
    return {};
  });
  fakeObs.respondWith("StopRecord", () => ({ outputPath: join(directory, "take.mp4") }));
  obsClient = new OBSWebSocketClient(fakeObs.url);
  handler = createMcpHandler(() => {
    const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
    initialize(server, obsClient, { sessionNotifications: false });
    return server;
  }, { legacy: "reject" });
});

afterEach(async () => {
  for (const client of clients.splice(0)) await client.close();
  await handler.close();
  await obsClient.disconnect();
  await fakeObs.close();
  rmSync(directory, { recursive: true, force: true });
  vi.useRealTimers();
  vi.restoreAllMocks();
});

describe("control lease", () => {
  it("lets only the holder change OBS, while anyone may read and stop", async () => {
    const codex = await agent("codex-mcp-client");
    const claude = await agent("claude-code");

    const claimed = await codex("obs-claim-control", { minutes: 10, reason: "recording the demo" });
    expect(text(claimed)).toMatch(/^codex-mcp-client has control \(recording the demo\) until \d\d:\d\d UTC/);

    const refused = await claude("obs-set-current-scene", { sceneName: "Other" });
    expect(refused.isError).toBe(true);
    expect(text(refused)).toContain("Not changed: codex-mcp-client has control");
    expect((await claude("obs-get-version")).isError).toBeFalsy();
    expect(text(await claude("obs-control-status"))).toContain("codex-mcp-client has control");
    expect(text(await claude("obs-claim-control"))).toContain("Not claimed");
    expect(text(await claude("obs-release-control"))).toContain("Not released");

    expect((await codex("obs-set-current-scene", { sceneName: "Other" })).isError).toBeFalsy();
    expect(text(await codex("obs-control-status"))).toContain("(this client)");

    expect(text(await codex("obs-release-control"))).toBe("Released control of OBS");
    expect((await claude("obs-set-current-scene", { sceneName: "Other" })).isError).toBeFalsy();
  });

  it("never blocks stopping a recording", async () => {
    const codex = await agent("codex-mcp-client");
    const claude = await agent("claude-code");
    await codex("obs-claim-control");

    expect((await claude("obs-stop-record")).isError).toBeFalsy();
  });

  it("expires", () => {
    vi.useFakeTimers({ toFake: ["Date"] });
    expect(claim(obsClient, "codex-mcp-client", 1, "").ok).toBe(true);
    expect(claim(obsClient, "claude-code", 1, "").ok).toBe(false);

    vi.setSystemTime(Date.now() + 61_000);

    expect(activeLease(obsClient)).toBeUndefined();
    expect(claim(obsClient, "claude-code", 1, "").ok).toBe(true);
  });

  it("is claimed for the length of a take by the client that started it", async () => {
    const codex = await agent("codex-mcp-client");
    const claude = await agent("claude-code");

    expect((await codex("obs-take-start")).isError).toBeFalsy();
    expect(text(await claude("obs-control-status"))).toMatch(/codex-mcp-client has control \(recording take \w+\)/);
    expect((await claude("obs-take-mark", { name: "Sneaky" })).isError).toBe(true);
    expect((await codex("obs-take-mark", { name: "Step 1" })).isError).toBeFalsy();

    await codex("obs-take-stop");
    expect(text(await claude("obs-control-status"))).toContain("No client has control");
  });
});
