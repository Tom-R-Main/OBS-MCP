/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { createMcpHandler, McpServer } from "@modelcontextprotocol/server";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { afterEach, describe, expect, it, vi } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { FakeOBSServer } from "../../test/support/fake-obs-server.js";
import { initialize } from "./index.js";
import { resultText, startMcpHarness, type HarnessOptions, type McpHarness } from "../../test/support/mcp-harness.js";
import { liveConfirmationEnabled } from "./confirm.js";

let harness: McpHarness | undefined;

async function start(options: HarnessOptions): Promise<McpHarness> {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness(options);
  harness.fakeObs.respondWith("StopStream", () => ({}));
  harness.fakeObs.respondWith("StopRecord", () => ({ outputPath: "/tmp/take.mp4" }));
  return harness;
}

function sent(requestType: string): boolean {
  return harness!.fakeObs.history().some(({ frame }) => frame.d.requestType === requestType);
}

afterEach(async () => {
  await harness?.close();
  harness = undefined;
  vi.restoreAllMocks();
});

describe("live action confirmation", () => {
  it("is off unless OBS_MCP_CONFIRM_LIVE is set", async () => {
    expect(liveConfirmationEnabled({})).toBe(false);
    expect(liveConfirmationEnabled({ OBS_MCP_CONFIRM_LIVE: "true" })).toBe(true);

    const { call, mcpClient } = await start({});
    const { tools } = await mcpClient.listTools();
    expect(tools.find(({ name }) => name === "obs-stop-stream")?.inputSchema.properties).not.toHaveProperty("confirm");
    expect((await call("obs-stop-stream")).isError).toBeFalsy();
    expect(sent("StopStream")).toBe(true);
  });

  it("asks the user through elicitation and stops only after they accept", async () => {
    const questions: string[] = [];
    const { call } = await start({
      confirmLive: true,
      onElicit: (message) => {
        questions.push(message);
        return { action: "accept", content: { confirm: true } };
      },
    });

    const result = await call("obs-stop-stream");

    expect(questions).toEqual(["Stop the live stream?"]);
    expect(result.isError).toBeFalsy();
    expect(sent("StopStream")).toBe(true);
  });

  it.each([
    ["declines", { action: "decline" as const }],
    ["answers no", { action: "accept" as const, content: { confirm: false } }],
  ])("does nothing when the user %s", async (_label, answer) => {
    const { call } = await start({ confirmLive: true, onElicit: () => answer });

    const result = await call("obs-stop-record");

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("Cancelled");
    expect(sent("StopRecord")).toBe(false);
  });

  it("asks the model to get consent when the client cannot elicit", async () => {
    const { call } = await start({ confirmLive: true });

    const refused = await call("obs-stop-stream");
    expect(refused.isError).toBe(true);
    expect(resultText(refused)).toContain("call again with confirm: true");
    expect(sent("StopStream")).toBe(false);

    const confirmed = await call("obs-stop-stream", { confirm: true });
    expect(confirmed.isError).toBeFalsy();
    expect(sent("StopStream")).toBe(true);
  });

  it("does not pass confirm through to tools that take arguments", async () => {
    const { call, mcpClient } = await start({ confirmLive: true });
    const { tools } = await mcpClient.listTools();

    expect(tools.find(({ name }) => name === "obs-stop-record")?.inputSchema.properties).toHaveProperty("confirm");
    expect(tools.find(({ name }) => name === "obs-remove-scene")?.inputSchema.properties).not.toHaveProperty("confirm");
    expect((await call("obs-stop-record", { confirm: true })).isError).toBeFalsy();
  });

  it("confirms through the 2026-07-28 input_required round trip", async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    const fakeObs = await FakeOBSServer.start();
    fakeObs.respondWith("StopStream", () => ({}));
    const obsClient = new OBSWebSocketClient(fakeObs.url);
    const handler = createMcpHandler(() => {
      const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
      initialize(server, obsClient, { confirmLive: true });
      return server;
    }, { legacy: "reject" });
    const client = new Client(
      { name: "obs-mcp-modern-confirm", version: "0.0.0" },
      { capabilities: { elicitation: { form: {} } }, versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const questions: string[] = [];
    client.setRequestHandler("elicitation/create", async (request) => {
      questions.push(request.params.message);
      return { action: "accept", content: { confirm: true } };
    });

    try {
      await client.connect(new StreamableHTTPClientTransport(
        new URL("http://obs-mcp.test/mcp"),
        { fetch: (input, init) => handler.fetch(new Request(input, init)) },
      ));
      expect(client.getNegotiatedProtocolVersion()).toBe("2026-07-28");

      const result = await client.callTool({ name: "obs-stop-stream", arguments: {} });

      expect(result.isError).toBeFalsy();
      expect(questions).toEqual(["Stop the live stream?"]);
      expect(fakeObs.history().some(({ frame }) => frame.d.requestType === "StopStream")).toBe(true);
    } finally {
      await client.close();
      await handler.close();
      await obsClient.disconnect();
      await fakeObs.close();
    }
  });
});
