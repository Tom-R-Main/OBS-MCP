/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { InMemoryTransport, McpServer } from "@modelcontextprotocol/server";
import { Client } from "@modelcontextprotocol/client";
import { describe, expect, it } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { initialize } from "./index.js";

// Pins every tool's public contract so refactors (such as generating tools from
// docs/protocol.json) cannot silently rename tools or change their arguments.
describe("tool contract snapshot", () => {
  it("keeps tool names, input schemas, and annotations stable", async () => {
    const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
    const client = new Client({ name: "obs-mcp-test-client", version: "0.0.0" });
    initialize(server, new OBSWebSocketClient("ws://127.0.0.1:1"));
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);

    try {
      const { tools } = await client.listTools();
      const contract = tools
        .map(({ name, title, inputSchema, annotations }) => ({ name, title, inputSchema, annotations }))
        .sort((a, b) => a.name.localeCompare(b.name));
      await expect(`${JSON.stringify(contract, null, 2)}\n`)
        .toMatchFileSnapshot("./__snapshots__/tool-contract.json");
    } finally {
      await client.close();
      await server.close();
    }
  });
});
