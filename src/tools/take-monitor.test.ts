/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EventSubscription, OBSWebSocketClient } from "../client.js";
import { FakeOBSServer } from "../../test/support/fake-obs-server.js";
import { healthyObsState, servePreflightState, type FakeObsState } from "../../test/support/fake-obs-state.js";
import { encodePpm } from "./frame-sample.js";
import { activeTake, takeLogPath, TakeMonitor, writeTakeLog, type TakeMonitorOptions } from "./take-monitor.js";

const SAMPLE_MS = 20;

let fakeObs: FakeOBSServer;
let client: OBSWebSocketClient;
let state: FakeObsState;
let directory: string;
let stats: { renderSkippedFrames: number; renderTotalFrames: number; outputSkippedFrames: number; outputTotalFrames: number };
let picture: () => Buffer;
let monitor: TakeMonitor | undefined;

const page = () => encodePpm(32, 18, (pixel) => (pixel % 3 ? [230, 230, 230] : [30, 30, 30]));
const black = () => encodePpm(32, 18, () => [0, 0, 0]);
let frameCounter = 0;
const moving = () => encodePpm(32, 18, (pixel) => ((pixel + frameCounter++) % 5 ? [230, 230, 230] : [20, 20, 20]));

async function startMonitor(options: TakeMonitorOptions = {}): Promise<TakeMonitor> {
  monitor = new TakeMonitor(client, {
    expectSilent: true,
    sampleIntervalMs: SAMPLE_MS,
    statsIntervalMs: SAMPLE_MS,
    stillAfterSeconds: 0.1,
    ...options,
  });
  await monitor.start();
  return monitor;
}

function meters(inputs: { inputName: string; peak: number; inputPeak?: number }[]): void {
  fakeObs.sendEvent("InputVolumeMeters", {
    inputs: inputs.map(({ inputName, peak, inputPeak = peak }) => ({
      inputName,
      inputLevelsMul: [[peak / 2, peak, inputPeak], [0, 0, 0]],
    })),
  });
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  directory = mkdtempSync(join(tmpdir(), "obs-mcp-take-"));
  state = healthyObsState(directory);
  state.inputs = [
    { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: true } },
    { inputName: "Muted Chrome", inputKind: "screen_capture", muted: true, tracks: { 1: true } },
    { inputName: "Off-track Mic", inputKind: "coreaudio_input_capture", muted: false, tracks: { 1: false, 2: true } },
  ];
  stats = { renderSkippedFrames: 0, renderTotalFrames: 0, outputSkippedFrames: 0, outputTotalFrames: 0 };
  picture = moving;
  fakeObs = await FakeOBSServer.start({ supportedImageFormats: ["png", "ppm"] });
  servePreflightState(fakeObs, () => state);
  fakeObs.respondWith("GetStats", () => ({ ...stats, availableDiskSpace: 50_000 }));
  fakeObs.respondWith("GetSourceScreenshot", ({ imageFormat }) => {
    expect(imageFormat).toBe("ppm");
    return { imageData: `data:image/ppm;base64,${picture().toString("base64")}` };
  });
  client = new OBSWebSocketClient(fakeObs.url);
  await client.connect();
});

afterEach(async () => {
  await monitor?.stop();
  monitor = undefined;
  await client.disconnect();
  await fakeObs.close();
  rmSync(directory, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("TakeMonitor", () => {
  it("subscribes to volume meters only while it runs, and is the active take", async () => {
    const take = await startMonitor();

    await vi.waitFor(() => expect(fakeObs.eventSubscriptions()).toBe(EventSubscription.All | EventSubscription.InputVolumeMeters));
    expect(activeTake(client)).toBe(take);

    await take.stop();

    await vi.waitFor(() => expect(fakeObs.eventSubscriptions()).toBe(EventSubscription.All));
    expect(activeTake(client)).toBeUndefined();
  });

  it("warns once when a recorded input becomes audible, ignoring muted and off-track inputs", async () => {
    const take = await startMonitor();

    meters([
      { inputName: "Chrome", peak: 0.5 },
      { inputName: "Muted Chrome", peak: 0, inputPeak: 0.8 },
      { inputName: "Off-track Mic", peak: 0.9 },
    ]);
    meters([{ inputName: "Chrome", peak: 0.4 }]);
    await vi.waitFor(() => expect(take.summary().warnings).toHaveLength(1));
    meters([{ inputName: "Chrome", peak: 0 }]);
    await vi.waitFor(() => expect(take.summary().events.map(({ type }) => type)).toContain("audio-end"));

    const summary = await take.stop();
    expect(summary.warnings[0]).toMatchObject({ kind: "audio", message: expect.stringContaining("Chrome is audible in the recording at -6 dB") });
    expect(summary.audio).toEqual([
      { inputName: "Chrome", peakDb: -6, inputPeakDb: -6, recorded: true },
      { inputName: "Muted Chrome", peakDb: -Infinity, inputPeakDb: -1.9, recorded: true },
      { inputName: "Off-track Mic", peakDb: -0.9, inputPeakDb: -0.9, recorded: false },
    ].sort((a, b) => b.peakDb - a.peakDb));
  });

  it("records audible inputs without warning when silence is not expected", async () => {
    const take = await startMonitor({ expectSilent: false });

    meters([{ inputName: "Chrome", peak: 0.5 }]);
    await vi.waitFor(() => expect(take.summary().events.map(({ type }) => type)).toContain("audio-start"));

    expect((await take.stop()).warnings).toEqual([]);
  });

  it("follows track changes made during the take", async () => {
    const take = await startMonitor();

    fakeObs.sendEvent("InputAudioTracksChanged", { inputName: "Chrome", inputAudioTracks: { 1: false } });
    await new Promise((resolve) => setTimeout(resolve, 20));
    meters([{ inputName: "Chrome", peak: 0.5 }]);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect((await take.stop()).warnings).toEqual([]);
  });

  it("warns about skipped frames and totals them", async () => {
    const take = await startMonitor();

    stats = { renderSkippedFrames: 3, renderTotalFrames: 120, outputSkippedFrames: 0, outputTotalFrames: 118 };
    await vi.waitFor(() => expect(take.summary().warnings.map(({ kind }) => kind)).toContain("frames"));

    const summary = await take.stop();
    expect(summary.frames).toMatchObject({ renderSkipped: 3, renderTotal: 120, outputSkipped: 0 });
    expect(summary.warnings.filter(({ kind }) => kind === "frames")).toHaveLength(1);
  });

  it("reports black stretches as warnings and unchanged stretches as stills", async () => {
    const take = await startMonitor();
    await vi.waitFor(() => expect(take.summary().framesSampled).toBeGreaterThan(2));

    picture = page;
    await new Promise((resolve) => setTimeout(resolve, 200));
    picture = black;
    await vi.waitFor(() => expect(take.summary().warnings.map(({ kind }) => kind)).toContain("blank"));
    picture = moving;
    await vi.waitFor(() => expect(take.summary().blanks.length).toBe(1));

    const summary = await take.stop();
    expect(summary.pictureChecks).toBe("on");
    expect(summary.warnings.filter(({ kind }) => kind === "blank")).toHaveLength(1);
    expect(summary.stills.length).toBeGreaterThanOrEqual(1);
    const still = summary.stills[0]!;
    expect(still.endSeconds - still.startSeconds).toBeGreaterThanOrEqual(0.1);
  });

  it("skips picture checks when OBS cannot return PPM", async () => {
    await client.disconnect();
    await fakeObs.close();
    fakeObs = await FakeOBSServer.start();
    servePreflightState(fakeObs, () => state);
    client = new OBSWebSocketClient(fakeObs.url);
    await client.connect();

    const take = await startMonitor();
    await new Promise((resolve) => setTimeout(resolve, 60));

    const summary = await take.stop();
    expect(summary.pictureChecks).toBe("off");
    expect(fakeObs.history().some(({ frame }) => frame.d.requestType === "GetSourceScreenshot")).toBe(false);
  });

  it("warns when the recording stops before it was asked to, but not after expectStop", async () => {
    const take = await startMonitor();
    fakeObs.sendEvent("RecordStateChanged", { outputActive: false, outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED" });
    await vi.waitFor(() => expect(take.summary().warnings.map(({ kind }) => kind)).toContain("output"));
    await take.stop();

    const second = await startMonitor();
    second.expectStop();
    fakeObs.sendEvent("RecordStateChanged", { outputActive: false, outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED" });
    await vi.waitFor(() => expect(second.summary().events.map(({ type }) => type)).toContain("RecordStateChanged"));
    expect(second.summary().warnings).toEqual([]);
  });

  it("leaves paused time out of take times, and ignores audio while paused", async () => {
    const take = await startMonitor();
    fakeObs.sendEvent("RecordStateChanged", { outputActive: true, outputState: "OBS_WEBSOCKET_OUTPUT_PAUSED" });
    await vi.waitFor(() => expect(take.isPaused()).toBe(true));
    meters([{ inputName: "Chrome", peak: 0.5 }]);
    await new Promise((resolve) => setTimeout(resolve, 300));
    fakeObs.sendEvent("RecordStateChanged", { outputActive: true, outputState: "OBS_WEBSOCKET_OUTPUT_RESUMED" });
    await vi.waitFor(() => expect(take.isPaused()).toBe(false));

    const summary = await take.stop();

    expect(summary.durationSeconds).toBeLessThan(0.25);
    expect(summary.warnings.filter(({ kind }) => kind === "audio")).toEqual([]);
  });

  it("writes a take log next to the recording with chapters and without -Infinity", async () => {
    const take = await startMonitor();
    take.mark("Intro");
    meters([{ inputName: "Muted Chrome", peak: 0, inputPeak: 0.8 }]);
    await new Promise((resolve) => setTimeout(resolve, 20));
    const summary = await take.stop();

    const recording = join(directory, "2026-09-23 10-00-00.mp4");
    const path = await writeTakeLog(recording, summary);

    expect(path).toBe(join(directory, "2026-09-23 10-00-00.take.json"));
    const log = JSON.parse(readFileSync(path!, "utf8")) as Record<string, unknown>;
    expect(log).toMatchObject({ recording, running: false, chapters: [{ name: "Intro" }] });
    expect(log.audio).toEqual([expect.objectContaining({ inputName: "Muted Chrome", peakDb: null })]);
    expect(takeLogPath("/a.b/clip")).toBe("/a.b/clip.take.json");
  });
});
