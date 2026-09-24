/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { createMcpHandler, InMemoryTransport, McpServer } from "@modelcontextprotocol/server";
import { Client, StreamableHTTPClientTransport } from "@modelcontextprotocol/client";
import { describe, expect, it } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { initialize } from "./index.js";
import { TOOL_COUNT } from "../../test/support/tool-count.js";

describe("MCP tool inventory", () => {
  it("publishes deterministic, fully annotated tools through MCP", async () => {
    const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
    const client = new Client({ name: "obs-mcp-test-client", version: "0.0.0" });
    const obsClient = new OBSWebSocketClient("ws://127.0.0.1:1");
    await initialize(server, obsClient);

    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await Promise.all([
      server.connect(serverTransport),
      client.connect(clientTransport),
    ]);

    try {
      const first = await client.listTools();
      const second = await client.listTools();
      const names = first.tools.map(({ name }) => name);

      expect(first.tools).toHaveLength(TOOL_COUNT);
      expect(new Set(names).size).toBe(names.length);
      expect(second.tools.map(({ name }) => name)).toEqual(names);

      for (const tool of first.tools) {
        expect(tool.title, tool.name).toBeTruthy();
        expect(tool.description, tool.name).toBeTruthy();
        expect(tool.inputSchema.type, tool.name).toBe("object");
        expect(tool.annotations, tool.name).toEqual(expect.objectContaining({
          readOnlyHint: expect.any(Boolean),
          destructiveHint: expect.any(Boolean),
          idempotentHint: expect.any(Boolean),
          openWorldHint: expect.any(Boolean),
        }));
      }

      expect(first.tools.find(({ name }) => name === "obs-call-request")?.annotations)
        .toMatchObject({ destructiveHint: true, openWorldHint: true });
      expect(first.tools.find(({ name }) => name === "obs-get-canvas-list")?.annotations)
        .toMatchObject({ readOnlyHint: true, openWorldHint: false });

      const status = await client.callTool({ name: "obs-get-status", arguments: {} });
      expect(status.structuredContent).toEqual(expect.objectContaining({
        server: expect.objectContaining({ name: "obs-mcp", status: "running" }),
        obs: expect.objectContaining({ connected: false }),
      }));
    } finally {
      await client.close();
      await server.close();
    }
  });

  it("negotiates the released 2026-07-28 protocol and exposes the full tool set", async () => {
    const obsClient = new OBSWebSocketClient("ws://127.0.0.1:1");
    const handler = createMcpHandler(() => {
      const server = new McpServer({ name: "obs-mcp-test", version: "0.0.0" });
      initialize(server, obsClient);
      return server;
    }, { legacy: "reject" });
    const client = new Client(
      { name: "obs-mcp-modern-test-client", version: "0.0.0" },
      { versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const transport = new StreamableHTTPClientTransport(
      new URL("http://obs-mcp.test/mcp"),
      { fetch: (input, init) => handler.fetch(new Request(input, init)) },
    );

    try {
      await client.connect(transport);

      expect(client.getProtocolEra()).toBe("modern");
      expect(client.getNegotiatedProtocolVersion()).toBe("2026-07-28");
      expect((await client.listTools()).tools).toHaveLength(TOOL_COUNT);
    } finally {
      await client.close();
      await handler.close();
    }
  });
});
