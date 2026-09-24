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
  serveRecordOutput(fakeObs, () => clipPath);
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

  it("stops the recording even when waiting fails", async () => {
    await startHarness();
    harness.fakeObs.respondWith("CreateRecordChapter", () => {
      harness.fakeObs.disconnect();
      return {};
    });

    await harness.call("obs-record-clip", { durationSeconds: 0.1, chapters: [{ atSeconds: 0, name: "x" }] });

    await vi.waitFor(() => expect(requestTypes()).toContain("StopRecord"));
  });

  it.skipIf(!ffmpegAvailable)("verifies the file's length and that it is silent", async () => {
    await startHarness();
    makeClip(1, "silent");

    const result = await harness.call("obs-record-clip", { durationSeconds: 0.5, expectSilent: true });

    expect(result.isError).toBeFalsy();
    expect(result.structuredContent).toMatchObject({ audioStreams: 1, problems: [] });
    // Encoded digital silence measures about -91 dB rather than -inf.
    expect((result.structuredContent as { maxVolumeDb: number | null }).maxVolumeDb ?? -Infinity).toBeLessThan(-60);
    expect(resultText(result)).toMatch(/Verified: 1\.0s, 1 audio stream/);
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
  let inputs: { inputName: string; inputKind: string }[];

  async function startCaptureHarness(platform = "macos"): Promise<void> {
    await startHarness(platform);
    inputs = [];
    const { fakeObs } = harness;
    fakeObs.respondWith("GetInputList", () => ({ inputs }));
    fakeObs.respondWith("CreateInput", (data) => {
      inputs.push({ inputName: String(data.inputName), inputKind: String(data.inputKind) });
      return { sceneItemId: 2 };
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
    fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 1, window: 31875 } }));
  }

  it("creates the capture, selects a display before listing windows, silences it, and fits it", async () => {
    await startCaptureHarness();

    const result = await harness.call("obs-capture-window", {
      sceneName: "Demo",
      inputName: "Chrome",
      window: "chatgpt",
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("Capturing window [Google Chrome] ChatGPT");
    expect(requestData("SetInputSettings")).toEqual([
      { inputName: "Chrome", inputSettings: { type: 1, display_uuid: "37D8832A" } },
      { inputName: "Chrome", inputSettings: { window: 31875 } },
    ]);
    const order = requestTypes();
    expect(order.indexOf("SetInputSettings")).toBeLessThan(
      order.lastIndexOf("GetInputPropertiesListPropertyItems"),
    );
    expect(requestData("SetInputMute")).toEqual([{ inputName: "Chrome", inputMuted: true }]);
    expect(requestData("SetInputAudioTracks")[0]?.inputAudioTracks).toEqual({
      1: false, 2: false, 3: false, 4: false, 5: false, 6: false,
    });
    expect(requestData("SetSceneItemTransform")[0]?.sceneItemTransform).toMatchObject({
      boundsType: "OBS_BOUNDS_SCALE_INNER",
      boundsWidth: 1920,
      boundsHeight: 1080,
    });
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
    expect(requestTypes()).not.toContain("SetInputMute");
  });

  it("uses a given display without listing displays, and leaves audio alone when asked", async () => {
    await startCaptureHarness();

    await harness.call("obs-capture-window", {
      sceneName: "Demo",
      inputName: "Terminal",
      window: "402",
      displayUuid: "ABC",
      silent: false,
      fit: false,
    });

    expect(requestData("GetInputPropertiesListPropertyItems").map(({ propertyName }) => propertyName)).toEqual(["window"]);
    expect(requestData("SetInputSettings")[1]).toEqual({ inputName: "Terminal", inputSettings: { window: 402 } });
    expect(requestTypes()).not.toContain("SetInputMute");
    expect(requestTypes()).not.toContain("SetSceneItemTransform");
  });

  it("saves a snapshot before changing an existing capture", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "screen_capture" });
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [] }));
    harness.fakeObs.respondWith("GetInputMute", () => ({ inputMuted: false }));
    harness.fakeObs.respondWith("GetInputVolume", () => ({ inputVolumeMul: 1 }));
    harness.fakeObs.respondWith("GetInputAudioTracks", () => ({ inputAudioTracks: { 1: true } }));

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });

    expect(resultText(result)).toMatch(/Saved the previous state as snapshot \w+; obs-restore undoes this change/);
    const order = requestTypes();
    expect(order.indexOf("GetInputSettings")).toBeLessThan(order.indexOf("SetInputSettings"));
  });

  it("refuses an existing input of another kind", async () => {
    await startCaptureHarness();
    inputs.push({ inputName: "Chrome", inputKind: "browser_source" });

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });

    expect(resultText(result)).toContain("is a browser_source, not a screen_capture");
  });

  it("refuses when OBS is not on macOS", async () => {
    await startCaptureHarness("windows");

    const result = await harness.call("obs-capture-window", { sceneName: "Demo", inputName: "Chrome", window: "chatgpt" });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("not on macOS");
  });
});
