/* SPDX-License-Identifier: MIT */
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { audioOnsets, flashTimes, measureSync, toneLevelDb } from "../support/media-analysis.js";
import {
  GAME_TONE_HZ,
  MIC_TONE_HZ,
  SYNC_BEEP_HZ,
  generateScenarioFixtures,
  hasMediaTools,
  inspectRecording,
  syncPulseTimes,
  type ScenarioFixtures,
} from "../support/scenario-media.js";

const RATE = 48_000;

function sine(frequency: number, seconds: number, amplitude = 0.5): Float32Array {
  return Float32Array.from({ length: Math.round(seconds * RATE) }, (_, index) => amplitude * Math.sin((2 * Math.PI * frequency * index) / RATE));
}

describe("scenario signal checks", () => {
  it("measures a present tone near its amplitude and an absent one far below it", () => {
    const tone = sine(440, 1);
    expect(toneLevelDb(tone, RATE, 440)).toBeCloseTo(20 * Math.log10(0.5), 0);
    expect(toneLevelDb(tone, RATE, 880)).toBeLessThan(-60);
    expect(toneLevelDb(new Float32Array(0), RATE, 440)).toBe(-Infinity);
  });

  it("finds burst onsets and ignores the rest of each burst", () => {
    const samples = new Float32Array(RATE * 3);
    for (const start of [0.5, 1.5, 2.5]) samples.set(sine(1000, 0.06), Math.round(start * RATE));
    const onsets = audioOnsets(samples, RATE);
    expect(onsets).toHaveLength(3);
    onsets.forEach((onset, index) => expect(onset).toBeCloseTo(0.5 + index, 2));
  });

  it("finds flash edges, skipping a flash already showing on the first frame", () => {
    const frames = Array.from({ length: 90 }, (_, frame) => ({ time: frame / 30, luma: frame % 30 < 2 ? 235 : 16 }));
    expect(flashTimes(frames)).toEqual([1, 2]);
    expect(flashTimes(frames.map(({ time }) => ({ time, luma: 40 })))).toEqual([]);
  });

  it("pairs each flash with its nearest beep and reports the median offset", () => {
    expect(measureSync([1, 2, 3], [1.02, 2.03, 3.02, 3.5])).toEqual({
      medianOffset: expect.closeTo(0.02, 5), worstOffset: expect.closeTo(0.03, 5), pairs: 3, spread: expect.closeTo(0.01, 5),
    });
    expect(measureSync([1, 2], [5])).toBeUndefined();
  });

  it("reports the worst pair when errors cancel out in the median", () => {
    // Alternating ±250 ms: the median is zero, but every pair is out of sync.
    const sync = measureSync([1, 2, 3, 4, 5, 6], [1.25, 1.75, 3.25, 3.75, 5.25, 5.75])!;
    expect(sync.pairs).toBe(6);
    expect(sync.medianOffset).toBeCloseTo(0, 5);
    expect(sync.worstOffset).toBeCloseTo(0.25, 5);
  });

  it("spaces sync pulses unevenly so a one-pulse slip cannot line up", () => {
    const times = syncPulseTimes(10);
    expect(times[0]).toBe(0);
    const gaps = times.slice(1).map((time, index) => time - times[index]!);
    expect(Math.min(...gaps)).toBeCloseTo(0.7, 5);
    expect(Math.max(...gaps) - Math.min(...gaps)).toBeGreaterThan(0.5);
  });

});

describe.skipIf(!hasMediaTools)("multi-track recording inspection (synthetic media, no OBS)", () => {
  let dir: string;
  let fixtures: ScenarioFixtures;
  let shifted: ScenarioFixtures;
  let slipped: ScenarioFixtures;

  beforeAll(() => {
    dir = mkdtempSync(join(tmpdir(), "obs-mcp-scenario-media-"));
    fixtures = generateScenarioFixtures(dir, { seconds: 4 });
    shifted = generateScenarioFixtures(mkdtempSync(join(dir, "shifted-")), { seconds: 4, syncAudioDelayMs: 200 });
    // About one pulse late: with a beep every second, this would pair each flash with the next beep and read as in sync.
    slipped = generateScenarioFixtures(mkdtempSync(join(dir, "slipped-")), { seconds: 4, syncAudioDelayMs: 1000 });
  });

  afterAll(() => rmSync(dir, { recursive: true, force: true }));

  /**
   * Mux the fixtures the way OBS would write tracks 1-3: sync beeps, mic, game.
   * The sync clip sits in the corner of a 1080p canvas, as an unscaled OBS source does.
   */
  function muxTracks(source: ScenarioFixtures, name: string, container = "mp4"): string {
    const output = join(dir, `${name}.${container}`);
    execFileSync("ffmpeg", [
      "-v", "error", "-y",
      "-i", source.syncClip, "-i", source.micTone, "-i", source.gameTone,
      "-vf", "pad=1920:1080:0:0:black",
      "-map", "0:v", "-map", "0:a", "-map", "1:a", "-map", "2:a",
      "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
      "-c:a", "aac", "-b:a", "160k", "-shortest", output,
    ], { stdio: ["ignore", "ignore", "pipe"], timeout: 60_000 });
    return output;
  }

  it("separates each track's tone and measures sync near zero on an aligned file", () => {
    const report = inspectRecording(muxTracks(fixtures, "aligned"));
    expect(report.tracks).toHaveLength(3);
    const [sync, mic, game] = report.tracks;
    expect(mic!.levels[MIC_TONE_HZ]).toBeGreaterThan(-10);
    expect(mic!.levels[GAME_TONE_HZ]).toBeLessThan(-50);
    expect(game!.levels[GAME_TONE_HZ]).toBeGreaterThan(-10);
    expect(game!.levels[MIC_TONE_HZ]).toBeLessThan(-50);
    expect(sync!.levels[SYNC_BEEP_HZ]).toBeGreaterThan(sync!.levels[MIC_TONE_HZ]! + 25);
    expect(report.flashes).toBeGreaterThanOrEqual(2);
    expect(report.sync?.pairs).toBeGreaterThanOrEqual(2);
    expect(Math.abs(report.sync!.medianOffset)).toBeLessThan(0.035);
    expect(report.sync!.worstOffset).toBeLessThan(0.035);
  });

  it("detects a known 200 ms audio delay in MKV as well", () => {
    const report = inspectRecording(muxTracks(shifted, "late", "mkv"));
    expect(report.formatName).toMatch(/matroska/);
    expect(report.sync!.medianOffset).toBeCloseTo(0.2, 1);
  });

  it("flags audio that slipped by about one pulse, even where the median looks in sync", () => {
    const report = inspectRecording(muxTracks(slipped, "slipped"));
    expect(report.sync?.pairs).toBeGreaterThanOrEqual(2);
    expect(report.sync!.worstOffset).toBeGreaterThan(0.15);
  });
});
