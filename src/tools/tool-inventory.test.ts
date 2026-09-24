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
// Tools that end a broadcast, finalize a recording, or cut off consumers of an
// output must be marked destructive so clients can ask before calling them.
const DESTRUCTIVE_NAME = /^obs-(remove-.+|take-stop|restore|stop-(stream|record|output|virtual-cam|replay-buffer)|toggle-(stream|record|output|virtual-cam|replay-buffer))$/;

describe("tool contract snapshot", () => {
  it("keeps tool names, input and output schemas, and annotations stable", async () => {
    const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
    const client = new Client({ name: "obs-mcp-test-client", version: "0.0.0" });
    initialize(server, new OBSWebSocketClient("ws://127.0.0.1:1"));
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);

    try {
      const { tools } = await client.listTools();
      const contract = tools
        .map(({ name, title, inputSchema, outputSchema, annotations }) => ({ name, title, inputSchema, outputSchema, annotations }))
        .sort((a, b) => a.name.localeCompare(b.name));
      const unsafe = contract
        .filter(({ name, annotations }) => DESTRUCTIVE_NAME.test(name) && !annotations?.destructiveHint)
        .map(({ name }) => name);
      expect(unsafe).toEqual([]);
      const readOnlyMutators = contract
        .filter(({ name, annotations }) => annotations?.readOnlyHint && !/^obs-(get|list|test|describe)-|^obs-(sleep|preflight|take-status|contact-sheet|snapshot|control-status)$/.test(name))
        .map(({ name }) => name);
      expect(readOnlyMutators).toEqual([]);
      await expect(`${JSON.stringify(contract, null, 2)}\n`)
        .toMatchFileSnapshot("./__snapshots__/tool-contract.json");
    } finally {
      await client.close();
      await server.close();
    }
  });
});
