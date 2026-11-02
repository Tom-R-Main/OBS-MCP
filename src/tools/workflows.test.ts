/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError, OBS_OP } from "../../test/support/fake-obs-server.js";
import { healthyObsState, servePreflightState, serveRecordOutput, type FakeObsState } from "../../test/support/fake-obs-state.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

function hasFfmpeg(): boolean {
  try {
    execFileSync("ffmpeg", ["-version"], { stdio: "ignore" });
    execFileSync("ffprobe", ["-version"], { stdio: "ignore" });
    return true;
  } catch {
    return false;
  }
}

const ffmpegAvailable = hasFfmpeg();

let harness: McpHarness;
let recordDirectory: string;
let state: FakeObsState;
let clipPath: string;

function requestTypes(): unknown[] {
  return harness.fakeObs.history().map(({ frame }) => frame.d.requestType).filter(Boolean);
}

function requestData(requestType: string): Record<string, unknown>[] {
  return harness.fakeObs.history()
    .filter(({ frame }) => frame.d.requestType === requestType)
    .map(({ frame }) => frame.d.requestData as Record<string, unknown>);
}

function makeClip(seconds: number, audio: "none" | "silent" | "tone"): void {
  const inputs = ["-f", "lavfi", "-i", `color=c=black:s=64x36:d=${seconds}`];
  if (audio === "silent") inputs.push("-f", "lavfi", "-i", `anullsrc=r=48000:cl=mono`);
  if (audio === "tone") inputs.push("-f", "lavfi", "-i", `sine=frequency=440:duration=${seconds}`);
  execFileSync("ffmpeg", ["-y", "-v", "error", ...inputs, "-t", String(seconds), "-shortest", clipPath]);
}

async function startHarness(platform = "macos"): Promise<void> {
  harness = await startMcpHarness({ platform });
  servePreflightState(harness.fakeObs, () => state);
  const { fakeObs } = harness;
  serveRecordOutput(fakeObs, () => clipPath, (active) => { state.recording = active; });
  fakeObs.respondWith("CreateRecordChapter", () => ({}));
}

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  recordDirectory = mkdtempSync(join(tmpdir(), "obs-mcp-workflows-"));
  clipPath = join(recordDirectory, "clip.mp4");
  state = healthyObsState(recordDirectory);
});

afterEach(async () => {
  await harness.close();
  rmSync(recordDirectory, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("obs-record-clip", () => {
  it("refuses to start when preflight fails", async () => {
    await startHarness();
    state.recording = true;

    const result = await harness.call("obs-record-clip", { durationSeconds: 1 });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("record-idle");
    expect(requestTypes()).not.toContain("StartRecord");
  });

  it("refuses audible inputs when silence is expected", async () => {
    await startHarness();
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: true } };

    const result = await harness.call("obs-record-clip", { durationSeconds: 1, expectSilent: true });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("Chrome (track 1)");
  });

  it("records, adds chapters in order, stops, and reports the file", async () => {
    await startHarness();

    const result = await harness.call("obs-record-clip", {
      durationSeconds: 0.3,
      chapters: [{ atSeconds: 0.2, name: "Second" }, { atSeconds: 0.05, name: "First" }],
    });

    expect(resultText(result)).toContain(`to ${clipPath}`);
    expect(requestData("CreateRecordChapter").map(({ chapterName }) => chapterName)).toEqual(["First", "Second"]);
    const order = requestTypes();
    expect(order.indexOf("StartRecord")).toBeLessThan(order.indexOf("CreateRecordChapter"));
    expect(order.lastIndexOf("CreateRecordChapter")).toBeLessThan(order.indexOf("StopRecord"));
  });

  it("keeps recording when a chapter is rejected", async () => {
    await startHarness();
    harness.fakeObs.respondWith("CreateRecordChapter", () => {
      throw new FakeOBSRequestError(501, "Chapters need Hybrid MP4");
    });

    const result = await harness.call("obs-record-clip", {
      durationSeconds: 0.1,
      chapters: [{ atSeconds: 0, name: "Intro" }],
    });

    expect(resultText(result)).toMatch(/Chapter "Intro" not added: .*Hybrid MP4/);
    expect(requestTypes()).toContain("StopRecord");
  });

  it("reports uncertainty without a stop request after connection loss", async () => {
    await startHarness();
    harness.fakeObs.respondWith("CreateRecordChapter", () => {
      harness.fakeObs.disconnect();
      return {};
    });

    const result = await harness.call("obs-record-clip", { durationSeconds: 0.1, chapters: [{ atSeconds: 0, name: "x" }] });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("unconfirmed");
    expect(requestTypes()).not.toContain("StopRecord");
  });

  it.skipIf(!ffmpegAvailable)("verifies the file's length and that it is silent", async () => {
    await startHarness();
    makeClip(1, "silent");

    const result = await harness.call("obs-record-clip", { durationSeconds: 0.5, expectSilent: true });

    expect(result.isError).toBeFalsy();
    expect(result.structuredContent).toMatchObject({ audioStreams: 1, problems: [] });
    // Encoded digital silence measures about -91 dB rather than -inf.
    expect((result.structuredContent as { maxVolumeDb: number | null }).maxVolumeDb ?? -Infinity).toBeLessThan(-60);
    expect(resultText(result)).toMatch(/File checks pass: 1\.0s, 1 audio stream/);
  });

  it.skipIf(!ffmpegAvailable)("flags audible audio and a short file", async () => {
    await startHarness();
    makeClip(1, "tone");

    const result = await harness.call("obs-record-clip", { durationSeconds: 4, expectSilent: true });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("Expected silence, but the audio peaks at");
    expect(resultText(result)).toContain("shorter than the requested 4s");
  }, 10_000);

  it("flags an input that becomes audible during the take, and writes a take log", async () => {
    await startHarness();
    // Muted at the start, so preflight passes; unmuted by someone mid-take.
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: true, tracks: { 1: true } };

    const pending = harness.call("obs-record-clip", {
      durationSeconds: 0.5,
      expectSilent: true,
      chapters: [{ atSeconds: 0, name: "Start" }],
    });
    await harness.fakeObs.waitForFrame(({ frame }) => frame.op === OBS_OP.Reidentify, { timeoutMs: 2_000 });
    harness.fakeObs.sendEvent("InputVolumeMeters", {
      inputs: [{ inputName: "Chrome", inputLevelsMul: [[0.2, 0.5, 0.5]] }],
    });
    const result = await pending;

    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/Problem: At [\d.]+s: Chrome is audible in the recording at -6 dB/);
    const takeLog = join(recordDirectory, "clip.take.json");
    expect(resultText(result)).toContain(`Take log: ${takeLog}`);
    expect(existsSync(takeLog)).toBe(true);
    expect(JSON.parse(readFileSync(takeLog, "utf8"))).toMatchObject({
      recording: clipPath,
      chapters: [{ name: "Start" }],
      warnings: [{ kind: "audio" }],
    });
  });

  it("refuses a chapter at or after the end, which would keep it recording", async () => {
    await startHarness();

    const result = await harness.call("obs-record-clip", { durationSeconds: 5, chapters: [{ atSeconds: 3600, name: "Late" }] });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain('"Late" start at or after the 5s clip ends');
    expect(requestTypes()).not.toContain("StartRecord");
  });

  it("rejects clips longer than a client timeout allows", async () => {
    await startHarness();

    const result = await harness.call("obs-record-clip", { durationSeconds: 120 });

    expect(result.isError).toBe(true);
    expect(requestTypes()).not.toContain("StartRecord");
  });
});

describe("obs-capture-window", () => {
  let inputs: { inputName: string; inputKind: string; inputUuid?: string }[];

  async function startCaptureHarness(platform = "macos"): Promise<void> {
    await startHarness(platform);
    inputs = [{ inputName: "Window inventory", inputUuid: "inventory-source", inputKind: platform === "macos" ? "screen_capture" : "xcomposite_input" }];
    const { fakeObs } = harness;
    fakeObs.respondWith("GetInputList", () => ({ inputs }));
    fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: state.sceneItems.map((item) => ({ ...item, sourceUuid: inputs.find(({ inputName }) => inputName === item.sourceName)?.inputUuid, sceneItemTransform: { sourceWidth: item.sourceWidth, sourceHeight: item.sourceHeight } })) }));
    fakeObs.respondWith("GetSceneList", () => ({ scenes: [{ sceneName: "Demo", sceneUuid: "scene-demo" }] }));
    fakeObs.respondWith("CreateInput", (data) => {
      inputs.push({ inputName: String(data.inputName), inputKind: String(data.inputKind), inputUuid: "created-source" });
      return { sceneItemId: 2, inputUuid: "created-source" };
    });
    fakeObs.respondWith("GetInputPropertiesListPropertyItems", ({ propertyName }) => ({
      propertyItems: propertyName === "display_uuid"
        ? [{ itemName: " ", itemValue: "", itemEnabled: true }, { itemName: "Built-in Display", itemValue: "37D8832A", itemEnabled: true }]
        : [
          { itemName: " ", itemValue: 0, itemEnabled: true },
          { itemName: "[Google Chrome] ChatGPT", itemValue: 31875, itemEnabled: true },
          { itemName: "[Google Chrome] YouTube", itemValue: 31876, itemEnabled: true },
          { itemName: "[Terminal] zsh", itemValue: 402, itemEnabled: true },
        ],
    }));
    for (const request of ["SetInputSettings", "SetInputMute", "SetInputAudioTracks", "SetSceneItemTransform"]) {
      fakeObs.respondWith(request, () => ({}));
    }
    fakeObs.respondWith("GetVideoSettings", () => ({ baseWidth: 1920, baseHeight: 1080 }));
    fakeObs.respondWith("GetSceneItemId", () => ({ sceneItemId: 2 }));
    fakeObs.respondWith("GetInputVolume", () => ({ inputVolumeMul: 1 }));
    fakeObs.respondWith("GetInputSettings", ({ inputName }) => ({
      inputKind: inputs.find((input) => input.inputName === inputName)?.inputKind,
      inputSettings: platform === "macos" ? { type: 1, window: 31875 } : { capture_window: "42\r\nEditor\r\nCode" },
    }));
  }

  async function expectNoCaptureWrites(): Promise<void> {
    expect(requestTypes().filter((type) => /^(Create|Set)/.test(String(type)))).toEqual([]);
    expect((await harness.call("obs-snapshot", { list: true })).structuredContent).toMatchObject({ snapshots: [] });
  }

  it.each([["inventory", undefined], ["inventory", ""], ["inventory", " "], ["existing", undefined], ["existing", ""], ["existing", " "]] as const)("refuses a capture %s without a nonempty UUID (%s) before saving or changing anything", async (source, inputUuid) => {
    await startCaptureHarness();
    if (source === "inventory") inputs[0]!.inputUuid = inputUuid;
    else inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", fit: false });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/UUID/i);
    await expectNoCaptureWrites();
  });

  it("stops after creation if OBS omits the new source UUID, without further writes", async () => {
    await startCaptureHarness();
    harness.fakeObs.respondWith("CreateInput", () => ({ sceneItemId: 2 }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/created.*UUID.*inspect OBS/i);
    expect(requestTypes().filter((type) => /^(Create|Set)/.test(String(type)))).toEqual(["CreateInput"]);
    expect((await harness.call("obs-snapshot", { list: true })).structuredContent).toMatchObject({ snapshots: [] });
  });

  it.each([undefined, "another-source"])("refuses target-scene membership with source UUID %s", async (sourceUuid) => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ sceneItemId: 2, sourceName: "Chrome", ...(sourceUuid ? { sourceUuid } : {}) }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", fit: false });
    expect(result.isError).toBe(true);
    await expectNoCaptureWrites();
  });

  it.each(["missing", "duplicate", "missing-uuid", "disappearing", "replaced"])("refuses a %s target scene before capture writes or snapshots", async (defect) => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneList", () => {
      reads += 1;
      if (defect === "missing" || (defect === "disappearing" && reads > 1)) return { scenes: [] };
      if (defect === "duplicate") return { scenes: [{ sceneName: "Demo", sceneUuid: "one" }, { sceneName: "Demo", sceneUuid: "two" }] };
      if (defect === "missing-uuid") return { scenes: [{ sceneName: "Demo" }] };
      return { scenes: [{ sceneName: "Demo", sceneUuid: defect === "replaced" && reads > 1 ? "replacement" : "scene-demo" }] };
    });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", fit: false });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/scene/i);
    await expectNoCaptureWrites();
  });

  it.each(["absent", "disappearing"])("refuses an existing input with %s target-scene membership", async (defect) => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: defect === "absent" || ++reads > 1 ? [] : [{ sceneItemId: 2, sourceName: "Chrome", sourceUuid: "chrome-source" }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", fit: false });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/scene|membership/i);
    await expectNoCaptureWrites();
  });

  it.each([true, false])("refuses absent safe inventory without creating a discovery placeholder (apply=%s)", async (apply) => {
    await startCaptureHarness();
    inputs = [];
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "New capture", window: "chatgpt", apply });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/configure.*OBS|inventory/i);
    await expectNoCaptureWrites();
  });

  it.each([["missing", true], ["missing", false], ["ambiguous", true], ["ambiguous", false]] as const)("does not save or change an existing capture on %s discovery (apply=%s)", async (selection, apply) => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: selection === "missing" ? "not running" : "google chrome", apply });
    expect(result.isError).toBe(true);
    await expectNoCaptureWrites();
  });

  it("returns a read-only capture plan with explicit source and selected window", async () => {
    await startCaptureHarness();
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt", apply: false, includeScreenshot: true });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(result.structuredContent).toMatchObject({ applied: false, rendered: "unverified", scene: { sceneName: "Demo", sceneUuid: "scene-demo" }, existing: null, inventory: { inputName: "Window inventory", inputUuid: "inventory-source", inputKind: "screen_capture" }, selected: { itemName: "[Google Chrome] ChatGPT", itemValue: 31875 } });
    await expectNoCaptureWrites();
    expect(requestTypes()).not.toContain("GetSourceScreenshot");
  });

  it("plans an existing capture without saving its snapshot or retargeting it", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "owned-source" });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", apply: false });
    expect(result.structuredContent).toMatchObject({ applied: false, rendered: "unverified", existing: { inputUuid: "owned-source" }, inventory: { inputUuid: "owned-source" }, selected: { itemValue: 402 } });
    await expectNoCaptureWrites();
  });

  it.each(["new-target", "replaced-inventory", "settings"])("refuses %s changes between discovery and apply", async (change) => {
    await startCaptureHarness();
    inputs[0]!.inputUuid = "initial";
    let inventories = 0;
    harness.fakeObs.respondWith("GetInputList", () => {
      if (++inventories === 2) {
        if (change === "new-target") inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
        if (change === "replaced-inventory") inputs[0]!.inputUuid = "replacement";
      }
      return { inputs };
    });
    if (change === "settings") {
      let reads = 0;
      harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 1, window: ++reads === 1 ? 31875 : 402 } }));
    }
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("changed during discovery");
    await expectNoCaptureWrites();
  });

  it("refuses a changed selection during readback before saving or mutating", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    let reads = 0;
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Owned", itemValue: ++reads === 1 ? 88 : 89 }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "Owned" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/changed|stable/i);
    await expectNoCaptureWrites();
  });

  it("refuses null XComposite selection without seeding a discovery value", async () => {
    await startCaptureHarness("linux");
    inputs.push({ inputName: "Editor", inputKind: "xcomposite_input", inputUuid: "editor-source" });
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ sceneItemId: 2, sourceName: "Editor", sourceUuid: "editor-source" }] }));
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["xcomposite_input"] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "xcomposite_input", inputSettings: { capture_window: null } }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor", window: "Editor" });
    expect(result.isError).toBe(true);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
    await expectNoCaptureWrites();
  });

  it("creates the capture, lists only integer windows, silences it, and fits it", async () => {
    await startCaptureHarness();

    const result = await harness.call("obs-capture-window", {
      sceneName: "Demo",
      inputName: "Chrome",
      window: "chatgpt",
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("Capturing window [Google Chrome] ChatGPT");
    expect(requestData("CreateInput")).toEqual([{ sceneName: "Demo", sceneUuid: "scene-demo", inputName: "Chrome", inputKind: "screen_capture", inputSettings: { type: 1, window: 31875, show_cursor: true } }]);
    expect(requestData("SetInputSettings")).toEqual([]);
    const order = requestTypes();
    expect(requestData("GetInputPropertiesListPropertyItems")).toEqual([
      { inputName: "Window inventory", inputUuid: "inventory-source", propertyName: "window" }, { inputName: "Window inventory", inputUuid: "inventory-source", propertyName: "window" },
    ]);
    expect(order.indexOf("CreateInput")).toBeGreaterThan(order.lastIndexOf("GetInputPropertiesListPropertyItems"));
    expect(requestData("SetInputMute")).toEqual([{ inputName: "Chrome", inputUuid: "created-source", inputMuted: true }]);
    expect(requestData("SetInputAudioTracks")[0]?.inputUuid).toBe("created-source");
    expect(requestData("SetInputAudioTracks")[0]?.inputAudioTracks).toEqual({
      1: false, 2: false, 3: false, 4: false, 5: false, 6: false,
    });
    expect(requestData("SetSceneItemTransform")[0]?.sceneItemTransform).toMatchObject({
      boundsType: "OBS_BOUNDS_SCALE_INNER",
      boundsWidth: 1920,
      boundsHeight: 1080,
    });
    expect(requestData("SetSceneItemTransform")[0]).toMatchObject({ sceneName: "Demo", sceneUuid: "scene-demo" });
    expect(result.structuredContent).toMatchObject({ appearances: [{ sourceWidth: 1512 }], warnings: [] });
  });

  it("lists the candidates when the window name is ambiguous", async () => {
    await startCaptureHarness();

    const result = await harness.call("obs-capture-window", {
      sceneName: "Demo",
      inputName: "Chrome",
      window: "google chrome",
    });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("More than one window");
    expect(resultText(result)).toContain("[Google Chrome] YouTube");
    await expectNoCaptureWrites();
  });

  it("ignores legacy displayUuid for window capture and leaves audio alone when asked", async () => {
    await startCaptureHarness();

    await harness.call("obs-capture-window", {
      sceneName: "Demo",
      inputName: "Terminal",
      window: "402",
      displayUuid: "ABC",
      silent: false,
      fit: false,
    });

    expect(requestData("GetInputPropertiesListPropertyItems").map(({ propertyName }) => propertyName)).toEqual(["window", "window"]);
    expect(requestData("CreateInput")[0]?.inputSettings).toEqual({ type: 1, window: 402, show_cursor: true });
    expect(requestData("SetInputSettings")).toEqual([]);
    expect(requestTypes()).not.toContain("SetInputMute");
    expect(requestTypes()).not.toContain("SetSceneItemTransform");
  });

  it.each([undefined, "explicit-display"])("refuses Mac application auto-discovery before any mutation (display=%s)", async (displayUuid) => {
    await startCaptureHarness();
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "App", application: "com.example.App", ...(displayUuid ? { displayUuid } : {}) });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/application.*discovery|application.*selection/i);
    expect(requestTypes().filter((type) => /^(Create|Set)/.test(String(type)))).toEqual([]);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
  });

  it("does not enumerate unsafe strings even if native string-property serialization fails", async () => {
    await startCaptureHarness();
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", ({ propertyName }) => {
      if (propertyName !== "window") throw new FakeOBSRequestError(500, "UNSAFE_STRING_PROPERTY");
      return { propertyItems: [{ itemName: "Owned window", itemValue: 88, itemEnabled: true }] };
    });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Fixture", window: "Owned window", silent: false, fit: false });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(requestData("GetInputPropertiesListPropertyItems").map(({ propertyName }) => propertyName)).toEqual(["window", "window"]);
    expect(requestData("CreateInput")[0]?.inputSettings).toEqual({ type: 1, window: 88, show_cursor: true });
    expect(requestData("SetInputSettings")).toEqual([]);
  });

  it("refuses a nonpositive Mac window binding instead of selecting no window", async () => {
    await startCaptureHarness();
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "None", itemValue: 0, itemEnabled: true }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Fixture", window: "None", silent: false, fit: false });
    expect(result.isError).toBe(true);
    expect(requestData("SetInputSettings")).toEqual([]);
  });

  it("saves a snapshot before changing an existing capture", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    harness.fakeObs.respondWith("GetInputMute", () => ({ inputMuted: false }));
    harness.fakeObs.respondWith("GetInputVolume", () => ({ inputVolumeMul: 1 }));
    harness.fakeObs.respondWith("GetInputAudioTracks", () => ({ inputAudioTracks: { 1: true } }));

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402" });

    expect(resultText(result)).toMatch(/Saved the previous state as snapshot \w+; obs-restore undoes this change/);
    expect(requestData("SetInputSettings")).toEqual([{ inputName: "Chrome", inputUuid: "chrome-source", inputSettings: { type: 1, window: 402 } }]);
    expect(requestData("GetInputSettings").filter(({ inputName }) => inputName === "Chrome").some(({ inputUuid }) => inputUuid === "chrome-source")).toBe(true);
    const order = requestTypes();
    expect(order.indexOf("GetInputSettings")).toBeLessThan(order.indexOf("SetInputSettings"));
  });

  it("refuses an existing input of another kind", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "browser_source", inputUuid: "browser-source" });

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });

    expect(resultText(result)).toContain("is a browser_source, not a screen_capture");
  });

  it.each(["window", "application"])("selects an X11 %s using OBS's encoded property value, without audio mutations", async (selector) => {
    await startCaptureHarness("ubuntu");
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["xcomposite_input"] }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Editor", itemValue: "42\r\nEditor\r\nCode", itemEnabled: true }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor capture", [selector]: selector === "application" ? "Code" : "Editor" });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(requestData("CreateInput")[0]).toMatchObject({ inputKind: "xcomposite_input", inputSettings: { capture_window: "42\r\nEditor\r\nCode", show_cursor: true } });
    expect(requestData("SetInputSettings")).toEqual([]);
    expect(requestTypes()).not.toContain("SetInputMute");
    expect(requestTypes()).not.toContain("SetInputAudioTracks");
    expect(requestData("SetSceneItemTransform")).toHaveLength(1);
  });

  it.each([null, "0", "-1", "42bad", "42\r\nmissing-class", "4294967296\r\nEditor\r\nCode"])("refuses invalid X11 identity %s without any change", async (itemValue) => {
    await startCaptureHarness("linux");
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["xcomposite_input"] }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Editor", itemValue }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor capture", window: "Editor" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("positive XID");
    await expectNoCaptureWrites();
  });

  it("uses an existing nonnull XComposite no-selection inventory without changing its sentinel", async () => {
    await startCaptureHarness("linux");
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["xcomposite_input"] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "xcomposite_input", inputSettings: { capture_window: "0" } }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Editor", itemValue: "42\r\nEditor\r\nCode" }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor capture", window: "Editor", fit: false });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(requestData("CreateInput")[0]?.inputSettings).toMatchObject({ capture_window: "42\r\nEditor\r\nCode" });
    expect(requestData("SetInputSettings")).toEqual([]);
  });
  it("explains unavailable Wayland selection before making any mutation", async () => {
    await startCaptureHarness("linux");
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["pipewire-screen-capture-source"] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor", window: "Editor" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("desktop portal picker");
    expect(requestTypes().filter((type) => /^(Set|Create)/.test(String(type)))).toEqual([]);
  });
  it.each([true, false])("reuses an existing Wayland selection without retargeting it (apply=%s)", async (apply) => {
    await startCaptureHarness("linux");
    inputs.push({ inputName: "Editor", inputKind: "pipewire-screen-capture-source", inputUuid: "editor-source" });
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ sceneItemId: 2, sourceName: "Editor", sourceUuid: "editor-source", sceneItemEnabled: true, sceneItemTransform: { sourceWidth: 1512, sourceHeight: 949 } }] }));
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["pipewire-screen-capture-source"] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "pipewire-screen-capture-source", inputSettings: { RestoreToken: "granted" } }));
    harness.fakeObs.respondWith("GetInputVolume", () => ({ inputVolumeMul: 1 }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor", backend: "portal", apply });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(requestTypes().filter((type) => /^(Create|SetInput)/.test(String(type)))).toEqual([]);
    expect(requestData("SetSceneItemTransform")).toHaveLength(apply ? 1 : 0);
    if (!apply) {
      expect(result.structuredContent).toMatchObject({ applied: false, rendered: "unverified", selected: { kind: "existing-portal-selection", persistedSelection: true } });
      await expectNoCaptureWrites();
    }
  });

  it.each([undefined, "Editor"])("refuses explicit portal on an existing X11 capture without discovery or writes (window=%s)", async (window) => {
    await startCaptureHarness("linux");
    inputs.push({ inputName: "Editor", inputKind: "xcomposite_input", inputUuid: "editor-source" });
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: ["xcomposite_input", "pipewire-screen-capture-source"] }));
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ sceneItemId: 2, sourceName: "Editor", sourceUuid: "editor-source" }] }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Editor", itemValue: "42\r\nEditor\r\nCode" }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor", backend: "portal", ...(window ? { window } : {}), includeScreenshot: true });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/portal|PipeWire/);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
    expect(requestTypes()).not.toContain("GetSourceScreenshot");
    await expectNoCaptureWrites();
  });

  it.each(["pipewire-window-capture-source", "pipewire-screen-capture-source"])("requires an existing persisted portal selection for %s", async (kind) => {
    await startCaptureHarness("linux");
    inputs.push({ inputName: "Editor", inputKind: kind, inputUuid: "editor-source" });
    harness.fakeObs.respondWith("GetInputKindList", () => ({ inputKinds: [kind] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: kind, inputSettings: {} }));
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ sceneItemId: 2, sourceName: "Editor", sourceUuid: "editor-source" }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Editor", backend: "portal" });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/persisted portal selection|authorize/i);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
    await expectNoCaptureWrites();
  });

  it.each(["empty", "first-only", "second-only", "wrong-types", "extra", "rejected"] as const)("stops after an incomplete or failed silence ACK (%s)", async (shape) => {
    await startCaptureHarness();
    const row = (requestType: string, requestId: string, ok = true) => ({ requestType, requestId, requestStatus: { result: ok, code: ok ? 100 : 501, ...(ok ? {} : { comment: "SILENCE_REFUSED" }) } });
    const first = row("SetInputMute", "0");
    const second = row("SetInputAudioTracks", "1");
    const rows = shape === "empty" ? [] : shape === "first-only" ? [first] : shape === "second-only" ? [second]
      : shape === "wrong-types" ? [row("SetInputAudioTracks", "0"), row("SetInputMute", "1")]
        : shape === "extra" ? [first, second, row("SetInputMute", "2")] : [row("SetInputMute", "0", false)];
    harness.fakeObs.respondWith("SetInputMute", () => {
      const batch = harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch).at(-1)!;
      // Exercise real websocket parsing and client ID normalization. This wire
      // reply precedes the fake's ordinary full ACK; no production method is mocked.
      harness.fakeObs.sendRaw({ op: OBS_OP.RequestBatchResponse, d: { requestId: batch.frame.d.requestId, results: rows } }, batch.connectionId);
      return {};
    });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402", includeScreenshot: true });
    expect(result.isError).toBe(true);
    expect(resultText(result)).not.toContain("Muted the capture and removed it from every audio track");
    expect(requestData("CreateInput")).toHaveLength(1);
    expect(requestTypes()).not.toContain("GetVideoSettings");
    expect(requestTypes()).not.toContain("SetSceneItemTransform");
    expect(requestTypes()).not.toContain("GetCurrentProgramScene");
    expect(requestTypes()).not.toContain("GetSourceScreenshot");
    if (shape === "rejected") expect(resultText(result)).toContain("SILENCE_REFUSED");
  });

  it("accepts a complete silence ACK returned out of order with original request IDs", async () => {
    await startCaptureHarness();
    harness.fakeObs.respondWith("SetInputMute", () => {
      const batch = harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch).at(-1)!;
      harness.fakeObs.sendRaw({ op: OBS_OP.RequestBatchResponse, d: { requestId: batch.frame.d.requestId, results: [
        { requestType: "SetInputAudioTracks", requestId: "1", requestStatus: { result: true, code: 100 } },
        { requestType: "SetInputMute", requestId: "0", requestStatus: { result: true, code: 100 } },
      ] } }, batch.connectionId);
      return {};
    });
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "402" });
    expect(result.isError, resultText(result)).toBeFalsy();
    expect(resultText(result)).toContain("Muted the capture and removed it from every audio track");
    expect(requestData("SetSceneItemTransform")).toHaveLength(1);
  });

  it.each(["input-uuid", "input-kind", "settings", "scene-uuid", "missing-scene", "membership", "selection"] as const)("rechecks %s after snapshot publication and refuses further capture effects", async (change) => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture", inputUuid: "chrome-source" });
    let snapshotStarted = false;
    harness.fakeObs.respondWith("GetInputMute", () => { snapshotStarted = true; return { inputMuted: false }; });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: inputs.map((input) => input.inputName === "Chrome" && snapshotStarted ? {
      ...input,
      ...(change === "input-uuid" ? { inputUuid: "replacement-source" } : {}),
      ...(change === "input-kind" ? { inputKind: "browser_source" } : {}),
    } : input) }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 1, window: snapshotStarted && change === "settings" ? 402 : 31875 } }));
    harness.fakeObs.respondWith("GetSceneList", () => ({ scenes: snapshotStarted && change === "missing-scene" ? [] : [{ sceneName: "Demo", sceneUuid: snapshotStarted && change === "scene-uuid" ? "replacement-scene" : "scene-demo" }] }));
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: snapshotStarted && change === "membership" ? [] : [{ sceneItemId: 2, sourceName: "Chrome", sourceUuid: "chrome-source", sceneItemTransform: { sourceWidth: 1512, sourceHeight: 949 } }] }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Owned", itemValue: snapshotStarted && change === "selection" ? 31876 : 31875 }] }));
    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "Owned", includeScreenshot: true });
    const snapshots = (await harness.call("obs-snapshot", { list: true })).structuredContent as { snapshots: { id: string }[] };
    expect(snapshotStarted).toBe(true);
    // The snapshot is an already-completed file effect, not rollback authority.
    expect(snapshots.snapshots).toHaveLength(1);
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain(`snapshot ${snapshots.snapshots[0]!.id}`);
    expect(requestTypes().filter((type) => /^(Create|Set)/.test(String(type)))).toEqual([]);
    expect(requestTypes()).not.toContain("GetCurrentProgramScene");
    expect(requestTypes()).not.toContain("GetSourceScreenshot");
  });

  it("refuses when OBS is not on macOS", async () => {
    await startCaptureHarness("windows");

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("not on macOS");
  });
});
