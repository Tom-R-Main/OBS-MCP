/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { fileURLToPath } from "node:url";
import { Client, type CallToolResult } from "@modelcontextprotocol/client";
import {
  StdioClientTransport,
  getDefaultEnvironment,
} from "@modelcontextprotocol/client/stdio";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

type JsonObject = Record<string, unknown>;

const enabled = process.env.OBS_MCP_LIVE_TEST === "1";
const mutationsEnabled = process.env.OBS_MCP_LIVE_MUTATION === "1";
const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const cliPath = fileURLToPath(new URL("../../build/index.js", import.meta.url));

let client: Client | undefined;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textContent(result: CallToolResult): string {
  return result.content
    .filter((block): block is { type: "text"; text: string } => (
      isObject(block) && block.type === "text" && typeof block.text === "string"
    ))
    .map(({ text }) => text)
    .join("\n");
}

function structuredResult(result: CallToolResult, toolName: string): JsonObject {
  if (result.isError) throw new Error(`${toolName} failed: ${textContent(result)}`);
  if (!isObject(result.structuredContent)) {
    throw new Error(`${toolName} did not return object structuredContent`);
  }
  return result.structuredContent;
}

async function call(toolName: string, args: JsonObject = {}): Promise<JsonObject> {
  if (!client) throw new Error("Live MCP client is not connected");
  return structuredResult(
    await client.callTool({ name: toolName, arguments: args }),
    toolName,
  );
}

describe.skipIf(!enabled)("live OBS through compiled MCP stdio", () => {
  beforeAll(async () => {
    client = new Client(
      { name: "obs-mcp-live-test", version: "1.0.0" },
      { versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: [cliPath],
      cwd: projectRoot,
      env: {
        ...getDefaultEnvironment(),
        OBS_WEBSOCKET_URL: process.env.OBS_WEBSOCKET_URL ?? "ws://localhost:4455",
        ...(process.env.OBS_WEBSOCKET_PASSWORD
          ? { OBS_WEBSOCKET_PASSWORD: process.env.OBS_WEBSOCKET_PASSWORD }
          : {}),
      },
      stderr: "pipe",
    });
    transport.stderr?.on("data", () => undefined);
    await client.connect(transport);
    await vi.waitFor(async () => {
      const status = await call("obs-get-status");
      expect(status.obs).toEqual(expect.objectContaining({ connected: true }));
    }, { timeout: 20_000, interval: 500 });
  });

  afterAll(async () => {
    await client?.close();
    client = undefined;
  });

  it("reads version, scenes, collection, stream, and recording state", async () => {
    const [version, scenes, collection, stream, record] = await Promise.all([
      call("obs-get-version"),
      call("obs-get-scene-list"),
      call("obs-get-scene-collection-list"),
      call("obs-get-stream-status"),
      call("obs-get-record-status"),
    ]);

    expect(version.obsVersion).toEqual(expect.any(String));
    expect(scenes.scenes).toEqual(expect.any(Array));
    expect(collection.currentSceneCollectionName).toEqual(expect.any(String));
    expect(collection.sceneCollections).toEqual(expect.any(Array));
    expect(stream.outputActive).toEqual(expect.any(Boolean));
    expect(record.outputActive).toEqual(expect.any(Boolean));
  });

  it.skipIf(!mutationsEnabled)("creates and removes one scene in the approved collection", async () => {
    const expectedCollection = process.env.OBS_MCP_LIVE_SCENE_COLLECTION;
    if (!expectedCollection) {
      throw new Error(
        "OBS_MCP_LIVE_SCENE_COLLECTION is required when OBS_MCP_LIVE_MUTATION=1",
      );
    }

    const collection = await call("obs-get-scene-collection-list");
    expect(collection.currentSceneCollectionName).toBe(expectedCollection);

    const [stream, record] = await Promise.all([
      call("obs-get-stream-status"),
      call("obs-get-record-status"),
    ]);
    expect(stream.outputActive, "Streaming must be inactive for live mutation tests").toBe(false);
    expect(record.outputActive, "Recording must be inactive for live mutation tests").toBe(false);

    const sceneName = `__obs_mcp_live_test_${process.pid}_${Date.now()}`;
    const before = await call("obs-get-scene-list");
    const beforeScenes = Array.isArray(before.scenes) ? before.scenes : [];
    expect(beforeScenes).not.toContainEqual(expect.objectContaining({ sceneName }));

    let created = false;
    try {
      await call("obs-create-scene", { sceneName });
      created = true;
      const afterCreate = await call("obs-get-scene-list");
      expect(afterCreate.scenes).toEqual(
        expect.arrayContaining([expect.objectContaining({ sceneName })]),
      );
    } finally {
      if (created) await call("obs-remove-scene", { sceneName });
    }

    const afterRemove = await call("obs-get-scene-list");
    const remainingScenes = Array.isArray(afterRemove.scenes) ? afterRemove.scenes : [];
    expect(remainingScenes).not.toContainEqual(expect.objectContaining({ sceneName }));
  });
});
