#!/usr/bin/env node
/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { Client } from "@modelcontextprotocol/client";
import { McpServer, InMemoryTransport } from "@modelcontextprotocol/server";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "path";
import { fileURLToPath, pathToFileURL } from "url";

export async function listTools(root) {
  const mockClient = {
    isConnected: () => false,
    getConnectionStatus: () => ({}),
    on: () => {},
  };

  const { version } = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  const server = new McpServer({ name: "obs-mcp", version });
  const client = new Client({ name: "obs-mcp-tool-inspector", version });

  const { initialize } = await import(pathToFileURL(resolve(root, "build/tools/index.js")).href);
  await initialize(server, mockClient);

  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();

  try {
    await Promise.all([
      server.connect(serverTransport),
      client.connect(clientTransport),
    ]);
    const { tools } = await client.listTools();

    return tools.map(({ name, description }) => ({ name, description }));
  } finally {
    await client.close();
    await server.close();
  }
}

// Allow running directly: node scripts/list-tools.mjs
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  console.log(JSON.stringify(await listTools(root), null, 2));
}
