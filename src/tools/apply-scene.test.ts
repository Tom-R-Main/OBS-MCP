/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OBS_OP } from "../../test/support/fake-obs-server.js";
import { demoSceneState, serveSceneState, type FakeSceneState } from "../../test/support/fake-obs-scene.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;
let state: FakeSceneState;

const SETS = ["CreateScene", "CreateInput", "CreateSceneItem", "RemoveSceneItem", "SetInputSettings", "SetInputMute", "SetInputVolume", "SetInputAudioTracks", "SetSceneItemTransform", "SetSceneItemEnabled", "SetSceneItemLocked", "SetSceneItemIndex"];

function sent(): string[] {
  return harness.fakeObs.history()
    .map(({ frame }) => String(frame.d.requestType))
    .filter((type) => SETS.includes(type));
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  state = demoSceneState();
  harness = await startMcpHarness();
  serveSceneState(harness.fakeObs, state);
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

describe("obs-apply-scene", () => {
  it("reports that an existing scene already matches", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Background", visible: true }, { name: "Chrome", muted: true, audioTracks: [] }],
      apply: true,
    });

    expect(resultText(result)).toBe("Scene Demo already matches");
    expect(sent()).toEqual([]);
  });

  it("plans without changing anything, including changes to items that do not exist yet", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Review",
      sources: [
        { name: "Background", visible: true },
        { name: "Slides", kind: "image_source", settings: { file: "/tmp/slide.png" }, fit: "canvas", locked: true },
      ],
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result).split("\n")).toEqual([
      "To make Review match:",
      "- scene Review: create",
      "- Background: add to the scene",
      "- Slides: create image_source with settings",
      "- Slides: lock (once it exists)",
      "- Slides: fit to the canvas (once it exists)",
      "Plan only; call again with apply: true",
    ]);
    expect(sent()).toEqual([]);
  });

  it("creates what is missing, then sets the rest with the new IDs, and converges", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Review",
      sources: [
        { name: "Background" },
        { name: "Mic", kind: "coreaudio_input_capture", muted: true, audioTracks: [] },
        { name: "Chrome", fit: "canvas" },
      ],
      order: true,
      apply: true,
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("OBS now matches the spec");
    expect(state.scenes.Review!.map(({ sourceName }) => sourceName)).toEqual(["Background", "Mic", "Chrome"]);
    expect(state.inputs.Mic).toMatchObject({ kind: "coreaudio_input_capture", muted: true, tracks: { 1: false, 2: false } });
    expect(state.scenes.Review![2]!.transform).toMatchObject({ boundsType: "OBS_BOUNDS_SCALE_INNER", boundsWidth: 1920, boundsHeight: 1080 });
    const batches = harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch).length;
    expect(batches).toBeGreaterThanOrEqual(2);
    expect(result.structuredContent).toMatchObject({ applied: true, converged: true, failures: [] });
    // A new scene has nothing to undo.
    expect(result.structuredContent).not.toHaveProperty("snapshotId");
  });

  it("changes only what differs in an existing scene, and saves a snapshot first", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [
        { name: "Background", locked: true },
        { name: "Chrome", settings: { type: 1, window: 40000 }, muted: true, volumeDb: -6, transform: { cropTop: 40, positionX: 0 } },
      ],
      apply: true,
    });

    expect(sent()).toEqual(["SetInputSettings", "SetInputVolume", "SetSceneItemTransform"]);
    expect(resultText(result)).toContain("- Chrome: settings (window)");
    expect(resultText(result)).toContain("- Chrome: transform (positionX)");
    expect(resultText(result)).toMatch(/snapshot \w+; obs-restore undoes/);
    expect(state.inputs.Chrome!.settings).toEqual({ type: 1, window: 40000 });

    const snapshotId = (result.structuredContent as { snapshotId: string }).snapshotId;
    await harness.call("obs-restore", { snapshotId });
    expect(state.inputs.Chrome!.settings).toEqual({ type: 1, window: 31875 });
    expect(state.inputs.Chrome!.volumeMul).toBeCloseTo(1);
  });

  it("removes unlisted items only when asked", async () => {
    const keep = await harness.call("obs-apply-scene", { sceneName: "Demo", sources: [{ name: "Chrome" }], apply: true });
    expect(resultText(keep)).toBe("Scene Demo already matches");

    const remove = await harness.call("obs-apply-scene", { sceneName: "Demo", sources: [{ name: "Chrome" }], removeOthers: true, apply: true });

    expect(resultText(remove)).toContain("- Background: remove from the scene");
    expect(state.scenes.Demo!.map(({ sourceName }) => sourceName)).toEqual(["Chrome"]);
    expect(state.inputs.Background).toBeDefined();
  });

  it("refuses specs it cannot apply before changing anything", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Webcam" }, { name: "Chrome", kind: "browser_source" }],
      apply: true,
    });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("Webcam does not exist; give its kind to create it");
    expect(resultText(result)).toContain("Chrome is a screen_capture, not a browser_source");
    expect(sent()).toEqual([]);
  });

  it("warns when a source renders at 0×0", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Window", kind: "window_capture" }],
      apply: true,
    });

    expect(resultText(result)).toContain("Warning: Window renders at 0×0");
    expect(result.structuredContent).toMatchObject({ blank: ["Window"] });
  });
});
