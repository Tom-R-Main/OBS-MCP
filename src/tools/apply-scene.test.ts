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
  // Only readCurrent asks for the scene inventory: snapshot capture already
  // has explicit scene/input names. Change state at a whole-workflow read,
  // rather than relying on timing or internal request counts.
  function onCurrentRead(ordinal: number, change: () => void): void {
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneList", () => {
      if (++reads === ordinal) change();
      return { scenes: Object.keys(state.scenes).map((sceneName) => ({ sceneName })) };
    });
  }

  it("does not retry an acknowledged creation omitted from the next inventory", async () => {
    let omitSlides = false;
    onCurrentRead(2, () => { omitSlides = true; });
    harness.fakeObs.respondWith("GetInputList", () => ({
      inputs: Object.entries(state.inputs)
        .filter(([name]) => !omitSlides || name !== "Slides")
        .map(([inputName, { kind }]) => ({ inputName, inputKind: kind })),
    }));

    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Chrome", muted: false }, { name: "Slides", kind: "image_source" }],
      apply: true,
    });

    expect(sent()).toEqual(["CreateInput"]);
    expect(state.inputs.Slides).toBeDefined();
    expect(state.inputs.Chrome!.muted).toBe(true);
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/^Stopped before applying changes:\n- Slides: create image_source is still needed; it was not retried\nCreated before stopping:\n- Slides: create image_source\nSnapshot \w+ holds the previous state of existing sources$/);
  });

  it("does not recreate an existing input removed between planning reads", async () => {
    onCurrentRead(2, () => {
      delete state.inputs.Chrome;
      state.scenes.Demo = state.scenes.Demo!.filter(({ sourceName }) => sourceName !== "Chrome");
    });

    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo", sources: [{ name: "Chrome", kind: "screen_capture", muted: false }], apply: true,
    });

    expect(sent()).toEqual([]);
    expect(state.inputs.Chrome).toBeUndefined();
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/^Stopped before applying changes:\n- Chrome: create screen_capture is still needed; it was not retried\nSnapshot \w+ holds the previous state of existing sources$/);
  });

  it("stops on a second-plan kind error while retaining known creations and snapshot", async () => {
    onCurrentRead(2, () => { state.inputs.Chrome!.kind = "browser_source"; });

    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Chrome", kind: "screen_capture", muted: false }, { name: "Slides", kind: "image_source" }],
      apply: true,
    });

    expect(sent()).toEqual(["CreateInput"]);
    expect(state.inputs.Slides).toBeDefined();
    expect(state.inputs.Chrome!.muted).toBe(true);
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/^Stopped before applying changes:\n- Chrome is a browser_source, not a screen_capture; choose another name\nCreated before stopping:\n- Slides: create image_source\nSnapshot \w+ holds the previous state of existing sources$/);
  });

  it("reports final-plan errors separately from known change failures and convergence", async () => {
    onCurrentRead(3, () => { state.inputs.Chrome!.kind = "browser_source"; });

    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo", sources: [{ name: "Chrome", kind: "screen_capture", muted: false }], apply: true,
    });

    expect(sent()).toEqual(["SetInputMute"]);
    expect(state.inputs.Chrome!.muted).toBe(false);
    expect(result.isError).toBe(true);
    expect(result.structuredContent).toMatchObject({
      actions: [{ target: "Chrome", change: "unmute" }], applied: true,
      failures: [], converged: false, remaining: [], blank: ["Chrome"],
      verificationErrors: ["Chrome is a browser_source, not a screen_capture; choose another name"],
    });
    expect(result.structuredContent).toHaveProperty("snapshotId");
    expect(resultText(result)).toContain("Could not verify afterwards:\n- Chrome is a browser_source, not a screen_capture; choose another name");
    expect(resultText(result)).not.toContain("OBS now matches the spec");
  });

  it("does not replay a creation whose applied effect loses its batch reply", async () => {
    harness.fakeObs.respondWith("CreateInput", () => {
      const sceneItemId = state.nextItemId++;
      state.inputs.Slides = { kind: "image_source", settings: {} };
      state.scenes.Demo!.push({ sceneItemId, sourceName: "Slides", enabled: true, locked: false, transform: {} });
      harness.fakeObs.disconnect();
      return { sceneItemId };
    });

    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo", sources: [{ name: "Slides", kind: "image_source", locked: true }], apply: true,
    });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/^Apply Scene failed:/);
    expect(resultText(result)).not.toContain("Nothing changed");
    expect(sent()).toEqual(["CreateInput"]);
    expect(state.inputs.Slides).toBeDefined();
    expect(state.scenes.Demo!.find(({ sourceName }) => sourceName === "Slides")?.locked).toBe(false);
    expect(harness.fakeObs.history().filter(({ frame }) => frame.d.requestType === "GetSceneList")).toHaveLength(1);
  });

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
    expect(result.structuredContent).not.toHaveProperty("verificationErrors");
    // The scene is new, but it reuses existing inputs, whose state is saved.
    expect(result.structuredContent).toHaveProperty("snapshotId");
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

  it("stacks the listed sources in front, in order, from any starting order, in one call", async () => {
    state.inputs.Webcam = { kind: "image_source", settings: {} };
    state.inputs.Slides = { kind: "image_source", settings: {} };
    const all = ["Background", "Chrome", "Webcam", "Slides"];
    const permutations = (list: string[]): string[][] => list.length <= 1 ? [list]
      : list.flatMap((head, index) => permutations([...list.slice(0, index), ...list.slice(index + 1)]).map((rest) => [head, ...rest]));
    for (const order of permutations(all)) {
      state.scenes.Demo = order.map((sourceName, index) => ({ sceneItemId: index + 1, sourceName, enabled: true, locked: false, transform: {} }));

      const result = await harness.call("obs-apply-scene", {
        sceneName: "Demo",
        sources: [{ name: "Slides" }, { name: "Chrome" }],
        order: true,
        apply: true,
      });

      const names = state.scenes.Demo.map(({ sourceName }) => sourceName);
      expect(names.slice(-2), `from ${order.join(",")}`).toEqual(["Slides", "Chrome"]);
      expect(result.isError, `from ${order.join(",")}: ${resultText(result)}`).toBeFalsy();
    }
  });

  it("refuses a source listed twice or named like a scene, before changing anything", async () => {
    state.scenes["Be Right Back"] = [];

    const twice = await harness.call("obs-apply-scene", { sceneName: "Demo", sources: [{ name: "Chrome" }, { name: "Chrome" }], apply: true });
    const scene = await harness.call("obs-apply-scene", { sceneName: "Demo", sources: [{ name: "Be Right Back", kind: "image_source" }], apply: true });

    expect(resultText(twice)).toContain("Chrome is listed more than once");
    expect(resultText(scene)).toContain("Be Right Back is a scene, not an input");
    expect(sent()).toEqual([]);
  });

  it("converges on decimal transforms, which OBS keeps as 32-bit floats", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Chrome", transform: { positionX: 100.1, scaleX: 0.3333, rotation: 12.34 } }],
      apply: true,
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("OBS now matches the spec");
    expect(resultText(await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Chrome", transform: { positionX: 100.1, scaleX: 0.3333, rotation: 12.34 } }],
    }))).toBe("Scene Demo already matches");
  });

  it("does not warn about an audio file, which has no picture", async () => {
    const result = await harness.call("obs-apply-scene", {
      sceneName: "Demo",
      sources: [{ name: "Test Tone", kind: "ffmpeg_source", settings: { is_local_file: true, local_file: "/tmp/tone.wav" }, muted: true }],
      apply: true,
    });

    expect(resultText(result)).not.toContain("0×0");
    expect(result.structuredContent).toMatchObject({ blank: [] });
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
