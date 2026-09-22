/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { InMemoryTransport, McpServer } from "@modelcontextprotocol/server";
import { Client, type CallToolResult } from "@modelcontextprotocol/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { FakeOBSServer } from "../../test/support/fake-obs-server.js";
import { initialize } from "./index.js";
import { RECORD_OUTPUT, startOutputAndConfirm } from "./output-start.js";

const REQUESTS = [
  "StartRecord",
  "GetRecordStatus",
  "GetVideoSettings",
  "SetProfileParameter",
];

let fakeObs: FakeOBSServer;
let obsClient: OBSWebSocketClient;
let mcpServer: McpServer;
let mcpClient: Client;

function text(result: CallToolResult): string {
  return result.content
    .map((block) => (block.type === "text" ? block.text : ""))
    .join("\n");
}

async function callTool(name: string, args: Record<string, unknown> = {}): Promise<CallToolResult> {
  return await mcpClient.callTool({ name, arguments: args }) as CallToolResult;
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  fakeObs = await FakeOBSServer.start({ availableRequests: REQUESTS });
  obsClient = new OBSWebSocketClient(fakeObs.url);
  mcpServer = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
  mcpClient = new Client({ name: "obs-mcp-test-client", version: "0.0.0" });
  initialize(mcpServer, obsClient);
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([mcpServer.connect(serverTransport), mcpClient.connect(clientTransport)]);
});

afterEach(async () => {
  await mcpClient.close();
  await mcpServer.close();
  await obsClient.disconnect();
  await fakeObs.close();
  vi.restoreAllMocks();
});

describe("connection-dependent general tools", () => {
  it("connects on demand instead of rejecting calls made before the first handshake", async () => {
    expect(obsClient.isConnected()).toBe(false);

    const version = await callTool("obs-get-version");
    const connection = await callTool("obs-test-connection");

    expect(version.isError).toBeFalsy();
    expect(version.structuredContent).toMatchObject({ obsVersion: "32.2.2" });
    expect(connection.isError).toBeFalsy();
  });
});

describe("obs-start-record", () => {
  it("reports success with the output path once OBS confirms the output started", async () => {
    fakeObs.queueSuccess("StartRecord");
    const cursor = fakeObs.cursor();
    const pending = callTool("obs-start-record");

    await fakeObs.waitForRequest("StartRecord", { after: cursor });
    fakeObs.sendEvent("RecordStateChanged", {
      outputActive: true,
      outputState: "OBS_WEBSOCKET_OUTPUT_STARTED",
      outputPath: "/tmp/demo.mp4",
    });

    const result = await pending;
    expect(result.isError).toBeFalsy();
    expect(text(result)).toBe("Recording started, writing to: /tmp/demo.mp4");
  });

  it("reports a request failure without waiting for a state change", async () => {
    fakeObs.queueError("StartRecord", 500, "Output already active");

    const result = await callTool("obs-start-record");

    expect(result.isError).toBe(true);
    expect(text(result)).toContain("Output already active");
  });
});

describe("startOutputAndConfirm", () => {
  const fastRecord = { ...RECORD_OUTPUT, timeoutMs: 50 };

  it("fails when OBS acknowledges the start but the output never becomes active", async () => {
    fakeObs.queueSuccess("StartRecord");
    fakeObs.queueSuccess("GetRecordStatus", { outputActive: false });

    const result = await startOutputAndConfirm(obsClient, fastRecord);

    expect(result.isError).toBe(true);
    expect(text(result)).toContain("did not become active within 0.05s");
    expect(text(result)).toContain("error dialog");
  });

  it("falls back to the status request when no state event arrives", async () => {
    fakeObs.queueSuccess("StartRecord");
    fakeObs.queueSuccess("GetRecordStatus", { outputActive: true });

    const result = await startOutputAndConfirm(obsClient, fastRecord);

    expect(result.isError).toBeFalsy();
    expect(text(result)).toBe("Recording started");
  });

  it("fails when OBS stops the output right after starting it", async () => {
    await obsClient.connect();
    fakeObs.queueSuccess("StartRecord");
    fakeObs.queueSuccess("GetRecordStatus", { outputActive: false });
    const cursor = fakeObs.cursor();
    const pending = startOutputAndConfirm(obsClient, { ...RECORD_OUTPUT, timeoutMs: 1_000 });

    await fakeObs.waitForRequest("StartRecord", { after: cursor });
    fakeObs.sendEvent("RecordStateChanged", {
      outputActive: false,
      outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED",
    });

    const result = await pending;
    expect(result.isError).toBe(true);
    expect(text(result)).toContain("stopped the output immediately");
  });

  it("removes its event listener after completing", async () => {
    fakeObs.queueSuccess("StartRecord");
    fakeObs.queueSuccess("GetRecordStatus", { outputActive: true });

    await startOutputAndConfirm(obsClient, fastRecord);

    expect(obsClient.listenerCount("RecordStateChanged")).toBe(0);
  });
});

describe("obs-sleep", () => {
  it("waits in the server instead of sending a standalone Sleep request", async () => {
    const cursor = fakeObs.cursor();
    const result = await callTool("obs-sleep", { sleepMillis: 20 });

    expect(result.isError).toBeFalsy();
    expect(text(result)).toBe("Slept for 20ms");
    expect(fakeObs.history().filter(({ cursor: c, frame }) => (
      c > cursor && frame.d.requestType === "Sleep"
    ))).toHaveLength(0);
  });

  it("converts frames to milliseconds using the OBS frame rate", async () => {
    fakeObs.queueSuccess("GetVideoSettings", { fpsNumerator: 30, fpsDenominator: 1 });

    const result = await callTool("obs-sleep", { sleepFrames: 3 });

    expect(text(result)).toBe("Slept for 100ms");
  });

  it("requires exactly one duration", async () => {
    const neither = await callTool("obs-sleep");
    const both = await callTool("obs-sleep", { sleepMillis: 1, sleepFrames: 1 });

    expect(neither.isError).toBe(true);
    expect(both.isError).toBe(true);
  });
});

describe("obs-set-profile-parameter", () => {
  it("warns that output settings need an OBS restart", async () => {
    fakeObs.queueSuccess("SetProfileParameter");
    fakeObs.queueSuccess("SetProfileParameter");

    const output = await callTool("obs-set-profile-parameter", {
      parameterCategory: "SimpleOutput",
      parameterName: "RecFormat2",
      parameterValue: "hybrid_mp4",
    });
    const other = await callTool("obs-set-profile-parameter", {
      parameterCategory: "General",
      parameterName: "Name",
      parameterValue: "Demo",
    });

    expect(text(output)).toContain("restart OBS");
    expect(text(other)).not.toContain("restart OBS");
  });
});
