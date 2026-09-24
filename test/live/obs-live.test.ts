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
const recordingEnabled = mutationsEnabled && process.env.OBS_MCP_LIVE_RECORD === "1";
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
        ...(process.env.OBS_MCP_READ_OBS_CONFIG
          ? { OBS_MCP_READ_OBS_CONFIG: process.env.OBS_MCP_READ_OBS_CONFIG }
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

  it("runs a read-only recording preflight against real OBS", async () => {
    const preflight = await call("obs-preflight");
    const checks = Array.isArray(preflight.checks) ? preflight.checks : [];

    expect(preflight.ready).toEqual(expect.any(Boolean));
    expect(checks).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: "connection", status: "pass" }),
      expect.objectContaining({ id: "record-directory" }),
      expect.objectContaining({ id: "encoder" }),
    ]));
    for (const check of checks) {
      expect(check, JSON.stringify(check)).toMatchObject({
        status: expect.stringMatching(/^(pass|warn|fail|unknown)$/),
      });
    }
  });

  it("returns results that match every read-only tool's declared output schema", async () => {
    if (!client) throw new Error("Live MCP client is not connected");
    const { tools } = await client.listTools();
    const candidates = tools.filter(({ annotations, outputSchema, inputSchema }) => (
      annotations?.readOnlyHint === true
      && outputSchema !== undefined
      && (inputSchema.required ?? []).length === 0
    ));
    expect(candidates.length).toBeGreaterThan(20);

    const mismatches: string[] = [];
    for (const tool of candidates) {
      const result = await client.callTool({ name: tool.name, arguments: {} });
      const text = textContent(result as CallToolResult);
      if (/Output validation error/.test(text)) mismatches.push(`${tool.name}: ${text}`);
    }
    expect(mismatches).toEqual([]);
  }, 60_000);

  it("runs read-only request batches, including Sleep, against real OBS", async () => {
    const batch = async (args: JsonObject) => await client!.callTool({ name: "obs-batch", arguments: args }) as CallToolResult;

    // A missing input fails on its own without failing the rest.
    const realtime = await batch({
      requests: [
        { requestType: "GetVersion" },
        { requestType: "GetStats" },
        { requestType: "GetInputMute", requestData: { inputName: "obs-mcp-no-such-input" } },
      ],
    });
    expect(realtime.structuredContent).toMatchObject({ succeeded: 2, failed: 1, skipped: 0 });
    const results = (realtime.structuredContent as { results: { requestType: string; ok: boolean }[] }).results;
    expect(results.map(({ requestType, ok }) => `${requestType}:${ok}`)).toEqual(["GetVersion:true", "GetStats:true", "GetInputMute:false"]);

    const started = Date.now();
    const framed = await batch({
      executionType: "serial-frame",
      requests: [
        { requestType: "GetCurrentProgramScene" },
        { requestType: "Sleep", requestData: { sleepFrames: 6 } },
        { requestType: "GetRecordStatus" },
      ],
    });
    expect(framed.isError).toBeFalsy();
    expect(framed.structuredContent).toMatchObject({ succeeded: 3, failed: 0 });
    expect(Date.now() - started).toBeGreaterThanOrEqual(60);
  });

  it("reads status, scene, and scene item resources from real OBS", async () => {
    if (!client) throw new Error("Live MCP client is not connected");
    const read = async (uri: string) => {
      const { contents } = await client!.readResource({ uri });
      const [first] = contents as { text?: string }[];
      return JSON.parse(first?.text ?? "null") as JsonObject;
    };

    const status = await read("obs://status");
    const scenes = await read("obs://scenes");
    const { resources } = await client.listResources();
    const itemsUri = resources.map(({ uri }) => uri).find((uri) => uri.endsWith("/items"));

    expect(status.connection).toEqual(expect.objectContaining({ connected: true }));
    expect(scenes.scenes).toEqual(expect.any(Array));
    expect(itemsUri, "expected at least one scene").toBeDefined();
    expect((await read(itemsUri!)).sceneItems).toEqual(expect.any(Array));
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

  it.skipIf(!recordingEnabled)("records a short clip only after OBS confirms the output", async () => {
    const collection = await call("obs-get-scene-collection-list");
    expect(collection.currentSceneCollectionName).toBe(process.env.OBS_MCP_LIVE_SCENE_COLLECTION);
    const record = await call("obs-get-record-status");
    expect(record.outputActive, "Recording must be inactive for the live recording test").toBe(false);
    const preflight = await call("obs-preflight");
    expect(preflight.ready, JSON.stringify(preflight.checks)).toBe(true);

    const started = await call("obs-start-record");
    try {
      expect(started.message).toMatch(/^Recording started/);
      expect((await call("obs-get-record-status")).outputActive).toBe(true);
      await call("obs-sleep", { sleepMillis: 1_000 });
    } finally {
      const stopped = await call("obs-stop-record");
      expect(stopped.message).toMatch(/^Recording stopped, saved to: /);
    }
  }, 20_000);
});
