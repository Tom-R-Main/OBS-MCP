/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import { TakeMonitor, takeUpdates } from "./take-monitor.js";

const JPEG = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/wAALCAABAAEBAREA/8QAFAABAAAAAAAAAAAAAAAAAAAACv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AN//Z";

let harness: McpHarness;
let closed = false;

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  closed = false;
  harness = await startMcpHarness();
  const { fakeObs } = harness;
  fakeObs.respondWith("GetSceneList", () => ({
    currentProgramSceneName: "Demo",
    scenes: [{ sceneName: "Demo", sceneIndex: 0 }, { sceneName: "Be Right Back", sceneIndex: 1 }],
  }));
  fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: true, outputDuration: 1200 }));
  fakeObs.respondWith("GetStreamStatus", () => ({ outputActive: false }));
  fakeObs.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: "Demo" }));
  fakeObs.respondWith("GetSceneItemList", ({ sceneName }) => ({
    sceneItems: [{ sceneItemId: 1, sourceName: `${String(sceneName)} source` }],
  }));
  fakeObs.respondWith("GetSourceScreenshot", () => ({ imageData: JPEG }));
});

afterEach(async () => {
  if (!closed) await harness.close();
  vi.restoreAllMocks();
});

function textOf(contents: unknown[]): unknown {
  const [first] = contents as { text?: string }[];
  return JSON.parse(first?.text ?? "null");
}

describe("resources", () => {
  it("lists fixed resources, per-scene item resources, and templates", async () => {
    const { resources } = await harness.mcpClient.listResources();
    const { resourceTemplates } = await harness.mcpClient.listResourceTemplates();

    expect(resources.map(({ uri }) => uri)).toEqual(expect.arrayContaining([
      "obs://status",
      "obs://scenes",
      "obs://take/current",
      "obs://scene/Demo/items",
      "obs://scene/Be%20Right%20Back/items",
    ]));
    expect(resourceTemplates.map(({ uriTemplate }) => uriTemplate)).toEqual([
      "obs://scene/{sceneName}/items",
      "obs://scene/{sceneName}/screenshot",
    ]);
  });

  it("reads status and scene items, decoding scene names", async () => {
    const status = await harness.mcpClient.readResource({ uri: "obs://status" });
    const items = await harness.mcpClient.readResource({ uri: "obs://scene/Be%20Right%20Back/items" });

    expect(textOf(status.contents)).toMatchObject({
      connection: { connected: true },
      record: { outputActive: true },
      currentProgramSceneName: "Demo",
    });
    expect(textOf(items.contents)).toMatchObject({ sceneItems: [{ sourceName: "Be Right Back source" }] });
  });

  it("returns a scene screenshot as a JPEG blob", async () => {
    const { contents } = await harness.mcpClient.readResource({ uri: "obs://scene/Demo/screenshot" });

    expect(contents[0]).toMatchObject({ mimeType: "image/jpeg", blob: expect.any(String) });
    const request = harness.fakeObs.history().find(({ frame }) => frame.d.requestType === "GetSourceScreenshot");
    expect(request?.frame.d.requestData).toMatchObject({ sourceName: "Demo", imageFormat: "jpeg", imageWidth: 960 });
  });

  it("notifies subscribers when OBS reports a change, and only for subscribed resources", async () => {
    const updated: string[] = [];
    harness.mcpClient.setNotificationHandler("notifications/resources/updated", async (notification) => {
      updated.push(notification.params.uri);
    });
    await harness.mcpClient.readResource({ uri: "obs://status" }); // Connects OBS.
    await harness.mcpClient.subscribeResource({ uri: "obs://status" });
    await harness.mcpClient.subscribeResource({ uri: "obs://scene/Be%20Right%20Back/items" });

    harness.fakeObs.sendEvent("RecordStateChanged", { outputActive: false, outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED" });
    harness.fakeObs.sendEvent("SceneItemEnableStateChanged", { sceneName: "Be Right Back", sceneItemId: 1 });
    harness.fakeObs.sendEvent("SceneCreated", { sceneName: "New" });

    await vi.waitFor(() => expect(updated).toEqual(["obs://status", "obs://scene/Be%20Right%20Back/items"]));

    await harness.mcpClient.unsubscribeResource({ uri: "obs://status" });
    harness.fakeObs.sendEvent("StreamStateChanged", { outputActive: true });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(updated).toHaveLength(2);
  });

  it("serves the running take and notifies subscribers when it changes", async () => {
    const updated: string[] = [];
    harness.mcpClient.setNotificationHandler("notifications/resources/updated", async (notification) => {
      updated.push(notification.params.uri);
    });
    const read = async () => JSON.parse(String(
      ((await harness.mcpClient.readResource({ uri: "obs://take/current" })).contents[0] as { text: string }).text,
    )) as Record<string, unknown>;
    expect(await read()).toEqual({ running: false });

    await harness.obsClient.connect();
    const take = new TakeMonitor(harness.obsClient, { sampleIntervalMs: 60_000, statsIntervalMs: 60_000 });
    await take.start();
    await harness.mcpClient.subscribeResource({ uri: "obs://take/current" });
    take.mark("Intro");

    await vi.waitFor(() => expect(updated).toContain("obs://take/current"));
    expect(await read()).toMatchObject({ running: true, chapters: [{ name: "Intro" }] });
    await take.stop();
    expect(await read()).toEqual({ running: false });
  });

  it("removes its OBS event listeners when the session closes", async () => {
    expect(harness.obsClient.listenerCount("RecordStateChanged")).toBeGreaterThan(0);
    const { obsClient } = harness;

    await harness.close();
    closed = true;

    expect(obsClient.listenerCount("RecordStateChanged")).toBe(0);
    expect(takeUpdates.listenerCount("update")).toBe(0);
  });
});

describe("prompts", () => {
  it("lists the workflow prompts", async () => {
    const { prompts } = await harness.mcpClient.listPrompts();

    expect(prompts.map(({ name }) => name).sort()).toEqual(["pre-stream-check", "record-demo"]);
  });

  it("fills record-demo with the window, scene, and length", async () => {
    const prompt = await harness.mcpClient.getPrompt({
      name: "record-demo",
      arguments: { window: "ChatGPT", scene: "Review", seconds: "20" },
    });
    const [message] = prompt.messages;
    const text = message?.content.type === "text" ? message.content.text : "";

    expect(text).toContain('"ChatGPT" window in the OBS scene "Review"');
    expect(text).toContain("obs-capture-window");
    expect(text).toContain("obs-record-clip with durationSeconds 20 and expectSilent true");
  });

  it("turns record-demo steps into a take with a chapter per step", async () => {
    const prompt = await harness.mcpClient.getPrompt({
      name: "record-demo",
      arguments: { window: "ChatGPT", steps: "Open settings\n\nAsk about tasks" },
    });
    const [message] = prompt.messages;
    const text = message?.content.type === "text" ? message.content.text : "";

    expect(text).toContain("obs-take-start with expectSilent true");
    expect(text).toContain("   - Open settings\n   - Ask about tasks");
    expect(text).not.toContain("obs-record-clip");
  });
});
