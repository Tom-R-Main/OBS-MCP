/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { healthyObsState, servePreflightState } from "../../test/support/fake-obs-state.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import { planCuts, remapTime } from "./trim.js";

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

describe("planCuts", () => {
  const base = { duration: 60, chapters: [], minIdleSeconds: 4, keepSeconds: 1, holdAfterChapterSeconds: 1.5, holdBeforeChapterSeconds: 0.25 };

  it("cuts the middle of long idle stretches and keeps the rest", () => {
    const { cuts, keep } = planCuts({ ...base, idle: [{ start: 10, end: 20 }, { start: 30, end: 32 }] });

    expect(cuts).toEqual([{ start: 11, end: 19 }]);
    expect(keep).toEqual([{ start: 0, end: 11 }, { start: 19, end: 60 }]);
  });

  it("never cuts the moment around a chapter start", () => {
    const { cuts } = planCuts({ ...base, idle: [{ start: 10, end: 30 }], chapters: [{ at: 20, name: "Step" }] });

    expect(cuts).toEqual([{ start: 11, end: 19.75 }, { start: 21.5, end: 29 }]);
  });

  it("drops fragments too short to be worth a seam and runs to the end", () => {
    const { cuts, keep } = planCuts({ ...base, idle: [{ start: 50, end: 60 }], chapters: [{ at: 51.6, name: "x" }] });

    expect(cuts).toEqual([{ start: 53.1, end: 59 }]);
    expect(keep.at(-1)).toEqual({ start: 59, end: 60 });
  });

  it("holds the previous step's result before each chapter and before the end", () => {
    const { cuts } = planCuts({
      ...base,
      holdBeforeChapterSeconds: 3,
      idle: [{ start: 10, end: 30 }, { start: 40, end: 60 }],
      chapters: [{ at: 30, name: "Next" }],
    });

    expect(cuts).toEqual([{ start: 11, end: 27 }, { start: 41, end: 57 }]);
  });

  it("maps times in the original to the trimmed file", () => {
    const cuts = [{ start: 10, end: 20 }, { start: 30, end: 35 }];

    expect(remapTime(5, cuts)).toBe(5);
    expect(remapTime(15, cuts)).toBe(10);
    expect(remapTime(25, cuts)).toBe(15);
    expect(remapTime(40, cuts)).toBe(25);
  });
});

describe.skipIf(!ffmpegAvailable)("obs-trim-take and obs-contact-sheet", () => {
  let directory: string;
  let clip: string;
  let harness: McpHarness;

  /** 3s of changing color, 8s of flat gray, 3s of changing color, with chapters at 0s and 11.5s. */
  function makeClip(path: string, audio: "none" | "silent" | "tone"): void {
    const withAudio = audio !== "none";
    const metadata = join(directory, "meta.txt");
    writeFileSync(metadata, ";FFMETADATA1\n[CHAPTER]\nTIMEBASE=1/1000\nSTART=0\nEND=11500\ntitle=Intro\n"
      + "[CHAPTER]\nTIMEBASE=1/1000\nSTART=11500\nEND=14000\ntitle=Answer\n");
    const color = (name: string, seconds: number) => ["-f", "lavfi", "-i", `color=c=${name}:s=160x90:r=10:d=${seconds}`];
    execFileSync("ffmpeg", [
      "-v", "error", "-y", ...color("red", 3), ...color("gray", 8), ...color("blue", 3),
      ...(audio === "silent" ? ["-f", "lavfi", "-i", "anullsrc=r=48000:cl=mono"] : []),
      ...(audio === "tone" ? ["-f", "lavfi", "-i", "sine=frequency=440:sample_rate=48000"] : []),
      "-f", "ffmetadata", "-i", metadata,
      "-filter_complex", "[0]hue=H=t*6,format=yuv420p[a];[1]format=yuv420p[b];[2]hue=H=t*6,format=yuv420p[c];[a][b][c]concat=n=3:v=1[v]",
      "-map", "[v]", ...(withAudio ? ["-map", "3:a", "-shortest"] : []), "-map_chapters", withAudio ? "4" : "3",
      "-c:v", "libx264", "-preset", "ultrafast", path,
    ]);
  }

  beforeAll(() => {
    directory = mkdtempSync(join(tmpdir(), "obs-mcp-trim-"));
    clip = join(directory, "take.mp4");
    makeClip(clip, "none");
  });

  afterAll(() => {
    rmSync(directory, { recursive: true, force: true });
  });

  beforeEach(async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    harness = await startMcpHarness();
    servePreflightState(harness.fakeObs, () => healthyObsState(directory));
    return async () => {
      await harness.close();
      vi.restoreAllMocks();
    };
  });

  it("plans the cut without writing anything", async () => {
    const result = await harness.call("obs-trim-take", { path: "take.mp4" });

    expect(result.isError).toBeFalsy();
    const plan = result.structuredContent as { cuts: { start: number; end: number }[]; removedSeconds: number; chapters: unknown[] };
    expect(plan.cuts).toHaveLength(1);
    // Gray from 3s to 11s, less 1s kept at each side and 3s held before the chapter at 11.5s.
    expect(plan.cuts[0]!.start).toBeCloseTo(4, 0);
    expect(plan.cuts[0]!.end).toBeCloseTo(8.5, 0);
    expect(plan.chapters).toEqual([{ atSeconds: 0, name: "Intro" }, { atSeconds: expect.closeTo(7, 0), name: "Answer" }]);
    expect(resultText(result)).toContain("Plan only");
    expect(existsSync(join(directory, "take.trimmed.mp4"))).toBe(false);
  });

  it("exports the trimmed copy with moved chapters and a contact sheet of the seam", async () => {
    const result = await harness.call("obs-trim-take", { path: clip, apply: true });

    expect(result.isError).toBeFalsy();
    const output = join(directory, "take.trimmed.mp4");
    expect(existsSync(output)).toBe(true);
    const structured = result.structuredContent as { outputDurationSeconds: number; outputChapters: number };
    expect(structured.outputDurationSeconds).toBeCloseTo(9.5, 0);
    expect(structured.outputChapters).toBe(2);
    expect(result.content.some((block) => block.type === "image")).toBe(true);

    const again = await harness.call("obs-trim-take", { path: clip, apply: true });
    expect(resultText(again)).toContain("already exists");
  }, 30_000);

  it("keeps idle stretches that have sound, and ignores a silent audio track", async () => {
    makeClip(join(directory, "silent.mp4"), "silent");
    makeClip(join(directory, "tone.mp4"), "tone");

    const silent = (await harness.call("obs-trim-take", { path: "silent.mp4" })).structuredContent as { cuts: unknown[]; audioConsidered: boolean };
    const tone = (await harness.call("obs-trim-take", { path: "tone.mp4" })).structuredContent as { cuts: unknown[]; audioConsidered: boolean };

    expect(silent).toMatchObject({ cuts: [expect.anything()], audioConsidered: false });
    expect(tone).toMatchObject({ cuts: [], audioConsidered: true });
  }, 30_000);

  it("keeps chapter names with control characters from breaking the chapter list", async () => {
    const tricky = join(directory, "tricky.mp4");
    const metadata = join(directory, "tricky.txt");
    // A title holding \r would otherwise end the line and start a forged chapter.
    writeFileSync(metadata, ";FFMETADATA1\n[CHAPTER]\nTIMEBASE=1/1000\nSTART=0\nEND=11500\ntitle=Intro\\\r[CHAPTER]\n"
      + "[CHAPTER]\nTIMEBASE=1/1000\nSTART=11500\nEND=14000\ntitle=Answer\n");
    execFileSync("ffmpeg", ["-v", "error", "-y", "-i", clip, "-f", "ffmetadata", "-i", metadata, "-map", "0", "-map_chapters", "1", "-c", "copy", tricky]);
    writeFileSync(join(directory, "tricky.take.json"), "{}");

    const result = await harness.call("obs-trim-take", { path: "tricky.mp4", apply: true, contactSheet: false });

    expect(result.structuredContent).toMatchObject({ outputChapters: 2 });
  }, 30_000);

  it("refuses an output path that is a dangling symlink", async () => {
    symlinkSync(join(tmpdir(), "obs-mcp-nowhere", "escape.mp4"), join(directory, "link.mp4"));

    const result = await harness.call("obs-trim-take", { path: "take.mp4", apply: true, outputPath: "link.mp4", contactSheet: false });

    expect(resultText(result)).toContain("already exists");
  }, 30_000);

  it("never deletes a recording whose name looks like a temporary export", async () => {
    // Exported to fresh.mp4, the old temporary name was exactly this file.
    const lookalike = join(directory, ".fresh.mp4.partial.mp4");
    execFileSync("cp", [clip, lookalike]);

    await harness.call("obs-trim-take", { path: ".fresh.mp4.partial.mp4", apply: true, outputPath: "fresh.mp4", contactSheet: false });

    expect(existsSync(lookalike)).toBe(true);
  }, 30_000);

  it("refuses files outside the OBS recording directory", async () => {
    const outside = mkdtempSync(join(tmpdir(), "obs-mcp-outside-"));
    try {
      const path = join(outside, "x.mp4");
      writeFileSync(path, "");

      const result = await harness.call("obs-contact-sheet", { path });

      expect(result.isError).toBe(true);
      expect(resultText(result)).toContain("outside the OBS recording directory");
    } finally {
      rmSync(outside, { recursive: true, force: true });
    }
  });

  it("tiles a frame after each chapter start", async () => {
    const result = await harness.call("obs-contact-sheet", { path: "take.mp4" });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("1. 1s Intro");
    expect(resultText(result)).toContain("2. 12.5s Answer");
    const image = result.content.find((block) => block.type === "image");
    expect(image?.type === "image" && image.mimeType).toBe("image/jpeg");
  }, 15_000);
});
