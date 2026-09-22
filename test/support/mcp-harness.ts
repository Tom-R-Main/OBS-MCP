/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { InMemoryTransport, McpServer } from "@modelcontextprotocol/server";
import { Client, type CallToolResult } from "@modelcontextprotocol/client";
import { OBSWebSocketClient } from "../../src/client.js";
import { initialize } from "../../src/tools/index.js";
import { FakeOBSServer, type FakeOBSOptions } from "./fake-obs-server.js";

/** An MCP client wired in memory to the real tool set and a fake OBS. */
export type McpHarness = {
  fakeObs: FakeOBSServer;
  obsClient: OBSWebSocketClient;
  mcpClient: Client;
  call(name: string, args?: Record<string, unknown>): Promise<CallToolResult>;
  close(): Promise<void>;
};

export function resultText(result: CallToolResult): string {
  return result.content
    .map((block) => (block.type === "text" ? block.text : ""))
    .join("\n");
}

export type HarnessOptions = FakeOBSOptions & {
  /** Register tools with OBS_MCP_CONFIRM_LIVE behavior. */
  confirmLive?: boolean;
  /** Declare form elicitation and answer every elicitation with this. */
  onElicit?: (message: string) => {
    action: "accept" | "decline" | "cancel";
    content?: Record<string, string | number | boolean | string[]>;
  };
};

export async function startMcpHarness(options: HarnessOptions = {}): Promise<McpHarness> {
  const { confirmLive = false, onElicit, ...fakeOptions } = options;
  const fakeObs = await FakeOBSServer.start(fakeOptions);
  const obsClient = new OBSWebSocketClient(fakeObs.url);
  const mcpServer = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
  const mcpClient = new Client(
    { name: "obs-mcp-test-client", version: "0.0.0" },
    onElicit ? { capabilities: { elicitation: { form: {} } } } : {},
  );
  if (onElicit) {
    mcpClient.setRequestHandler("elicitation/create", async (request) => onElicit(request.params.message));
  }
  initialize(mcpServer, obsClient, { confirmLive });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([mcpServer.connect(serverTransport), mcpClient.connect(clientTransport)]);

  return {
    fakeObs,
    obsClient,
    mcpClient,
    call: async (name, args = {}) => (
      await mcpClient.callTool({ name, arguments: args }) as CallToolResult
    ),
    close: async () => {
      await mcpClient.close();
      await mcpServer.close();
      await obsClient.disconnect();
      await fakeObs.close();
    },
  };
}
