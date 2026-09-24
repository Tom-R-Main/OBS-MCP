/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

const PNG_1X1 = "data:image/jpeg;base64,/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQEBAQH/wAALCAABAAEBAREA/8QAFAABAAAAAAAAAAAAAAAAAAAACv/EABQQAQAAAAAAAAAAAAAAAAAAAAD/2gAIAQEAAD8AN//Z";

let harness: McpHarness;
let size: { sourceWidth: number; sourceHeight: number };
let settings: Record<string, unknown>;

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness();
  size = { sourceWidth: 1512, sourceHeight: 949 };
  settings = { type: 1, window: 31875 };
  const { fakeObs } = harness;
  fakeObs.respondWith("SetInputSettings", (data) => {
    settings = { ...settings, ...(data.inputSettings as Record<string, unknown>) };
    return {};
  });
  fakeObs.respondWith("CreateInput", () => ({ inputUuid: "uuid", sceneItemId: 7 }));
  fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: settings }));
  fakeObs.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: "Demo" }));
  fakeObs.respondWith("GetSceneItemList", () => ({
    sceneItems: [
      { sceneItemId: 1, sourceName: "Background", sceneItemEnabled: true, sceneItemTransform: { sourceWidth: 1920, sourceHeight: 1080 } },
      { sceneItemId: 2, sourceName: "Chrome", sceneItemEnabled: true, sceneItemTransform: { ...size } },
    ],
  }));
  fakeObs.respondWith("GetSourceScreenshot", () => ({ imageData: PNG_1X1 }));
  fakeObs.respondWith("SetSceneItemTransform", () => ({}));
  fakeObs.respondWith("GetSceneItemTransform", () => ({
    sceneItemTransform: { ...size, positionX: 10, cropTop: 372 },
  }));
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

describe("obs-set-input-settings", () => {
  it("returns the resulting settings and the input's size in the program scene", async () => {
    const result = await harness.call("obs-set-input-settings", { inputName: "Chrome", inputSettings: { show_cursor: false } });

    expect(result.isError).toBeFalsy();
    expect(result.structuredContent).toMatchObject({
      inputSettings: { type: 1, window: 31875, show_cursor: false },
      appearances: [{ sceneName: "Demo", sceneItemId: 2, sourceWidth: 1512, sourceHeight: 949 }],
      warnings: [],
    });
    expect(result.content.some(({ type }) => type === "image")).toBe(false);
  });

  it("waits for a capture to produce its first frame before reporting its size", async () => {
    size = { sourceWidth: 0, sourceHeight: 0 };
    setTimeout(() => { size = { sourceWidth: 1512, sourceHeight: 949 }; }, 300);

    const result = await harness.call("obs-set-input-settings", { inputName: "Chrome", inputSettings: { type: 2 } });

    expect(result.structuredContent).toMatchObject({ appearances: [{ sourceWidth: 1512 }], warnings: [] });
  });

  it("warns when the input still renders at 0×0", async () => {
    size = { sourceWidth: 0, sourceHeight: 0 };

    const result = await harness.call("obs-set-input-settings", { inputName: "Chrome", inputSettings: { type: 2 } });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("Chrome renders at 0×0");
    expect(resultText(result)).toContain("display_uuid");
  });

  it("adds a bounded screenshot on request", async () => {
    const result = await harness.call("obs-set-input-settings", {
      inputName: "Chrome",
      inputSettings: {},
      includeScreenshot: true,
    });

    expect(result.content).toEqual(expect.arrayContaining([
      expect.objectContaining({ type: "image", mimeType: "image/jpeg" }),
    ]));
    const request = harness.fakeObs.history().find(({ frame }) => frame.d.requestType === "GetSourceScreenshot");
    expect(request?.frame.d.requestData).toMatchObject({ sourceName: "Chrome", imageFormat: "jpeg", imageWidth: 960 });
  });

  it("still reports success when the screenshot fails", async () => {
    harness.fakeObs.respondWith("GetSourceScreenshot", () => {
      throw new FakeOBSRequestError(702, "Failed to render screenshot");
    });

    const result = await harness.call("obs-set-input-settings", { inputName: "Chrome", inputSettings: {}, includeScreenshot: true });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toMatch(/No screenshot: .*Failed to render screenshot/);
  });
});

describe("obs-create-input", () => {
  it("returns the new scene item ID with the input's state", async () => {
    const result = await harness.call("obs-create-input", {
      sceneName: "Demo",
      inputName: "Chrome",
      inputKind: "screen_capture",
    });

    expect(resultText(result)).toContain("with ID 7");
    expect(result.structuredContent).toMatchObject({ appearances: [{ sceneItemId: 2 }] });
  });
});

describe("obs-set-scene-item-transform", () => {
  it("returns the transform OBS applied", async () => {
    const result = await harness.call("obs-set-scene-item-transform", { sceneName: "Demo", sceneItemId: 2, cropTop: 372 });

    expect(result.structuredContent).toMatchObject({
      sceneItemTransform: { cropTop: 372, sourceWidth: 1512 },
    });
    expect(result.structuredContent).not.toHaveProperty("warnings");
  });

  it("reports success with a warning when the transform cannot be read back", async () => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => {
      throw new FakeOBSRequestError(600, "No scene item was found");
    });

    const result = await harness.call("obs-set-scene-item-transform", { sceneName: "Demo", sceneItemId: 2, cropTop: 372 });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("Could not read the applied transform back");
  });
});
