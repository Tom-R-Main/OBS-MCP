/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFileSync } from "node:child_process";
import { createReadStream, existsSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, beforeEach, describe, expect, it, vi } from "vitest";
import { healthyObsState, servePreflightState } from "../../test/support/fake-obs-state.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import { contactSheet, planCuts, publishTrimmedExport, qualifiedEncoderArgs, remapTime, requireSdrInput, seamSampleTimes, validateTrimmedMetadata, type MediaProbe } from "./trim.js";
import { runBoundedMediaProcess } from "./media-verification.js";

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

describe("verified export boundary", () => {
  const expected = { duration: 9.5, width: 160, height: 90, audioStreams: 0, chapters: [{ at: 0, name: "Intro" }, { at: 7, name: "Answer" }] };
  const valid: MediaProbe = { ...expected, videoStreams: 1, codec: "h264", pixelFormat: "yuv420p" };

  it.each([
    ["truncated duration", { duration: 2 }], ["missing video", { videoStreams: 0 }],
    ["wrong dimensions", { width: 640 }], ["missing chapter", { chapters: [] }],
    ["wrong chapter time", { chapters: [{ at: 0, name: "Intro" }, { at: 8, name: "Answer" }] }],
    ["unexpected audio", { audioStreams: 1 }], ["unqualified codec", { codec: "mpeg4" }],
  ])("withholds publication for %s", async (_label, change) => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-invalid-"));
    const partial = join(dir, "partial.mp4"), output = join(dir, "final.mp4");
    try {
      await expect(publishTrimmedExport(partial, output, {
        encode: async () => { writeFileSync(partial, "encoded"); },
        verify: async () => validateTrimmedMetadata({ ...valid, ...change }, expected),
      })).rejects.toThrow();
      expect(readdirSync(dir)).toEqual([]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("withholds publication on strict decode failure", async () => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-decode-"));
    try {
      await expect(publishTrimmedExport(join(dir, "partial"), join(dir, "final"), {
        encode: async () => { writeFileSync(join(dir, "partial"), "encoded"); },
        verify: async () => { throw new Error("strict decode failed"); },
      })).rejects.toThrow("strict decode failed");
      expect(readdirSync(dir)).toEqual([]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("withholds publication after apparent encoder completion followed by a terminal trailer error", async () => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-trailer-"));
    const partial = join(dir, "partial"), output = join(dir, "final");
    const verify = vi.fn(async () => valid);
    try {
      await expect(publishTrimmedExport(partial, output, {
        encode: () => runBoundedMediaProcess(process.execPath, ["-e", "require('fs').writeFileSync(process.argv[1],'encoded packets'); console.error('progress=end'); console.error('Error writing trailer'); process.exitCode=1", partial]),
        verify,
      })).rejects.toThrow("Error writing trailer");
      expect(verify).not.toHaveBeenCalled();
      expect(existsSync(output)).toBe(false);
      expect(readdirSync(dir)).toEqual([]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it.each(["after-encode", "after-verification"])("refuses publication when cancelled %s", async (when) => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-abort-"));
    const abort = new AbortController();
    try {
      await expect(publishTrimmedExport(join(dir, "partial"), join(dir, "final"), {
        encode: async () => { writeFileSync(join(dir, "partial"), "encoded"); if (when === "after-encode") abort.abort(); },
        verify: async () => { abort.signal.throwIfAborted(); if (when === "after-verification") abort.abort(); return valid; },
      }, abort.signal)).rejects.toThrow();
      expect(readdirSync(dir)).toEqual([]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("does not overwrite a destination that appears during verification", async () => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-race-"));
    const output = join(dir, "final");
    try {
      await expect(publishTrimmedExport(join(dir, "partial"), output, {
        encode: async () => { writeFileSync(join(dir, "partial"), "ours"); },
        verify: async () => { writeFileSync(output, "theirs"); return valid; },
      })).rejects.toMatchObject({ code: "EEXIST" });
      expect(readFileSync(output, "utf8")).toBe("theirs");
      expect(readdirSync(dir)).toEqual(["final"]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it("retains committed output identity when contact sheet generation fails", async () => {
    const dir = mkdtempSync(join(tmpdir(), "obs-export-preview-"));
    const output = join(dir, "final");
    try {
      const result = await publishTrimmedExport(join(dir, "partial"), output, {
        encode: async () => { writeFileSync(join(dir, "partial"), "ours"); },
        verify: async () => { expect(existsSync(output)).toBe(false); return valid; },
        preview: async () => { expect(readFileSync(output, "utf8")).toBe("ours"); throw new Error("preview failed"); },
      });
      expect(result).toMatchObject({ verified: valid, sha256: expect.stringMatching(/^[a-f0-9]{64}$/), previewWarning: expect.stringContaining("Export committed") });
      expect(readdirSync(dir)).toEqual(["final"]);
    } finally { rmSync(dir, { recursive: true, force: true }); }
  });

  it.each(["smpte2084", "arib-std-b67"])("refuses %s without tone mapping", (colorTransfer) => {
    expect(() => requireSdrInput({ ...valid, colorTransfer })).toThrow("PQ/HLG");
  });
  it.each([
    { pixelFormat: "yuv420p10le" }, { pixelFormat: "gbrp16be" },
    { pixelFormat: "yuv420p", bitDepth: 10 }, { pixelFormat: undefined },
  ])("refuses high or unknown pixel depth %j", (change) => {
    expect(() => requireSdrInput({ ...valid, ...change })).toThrow("8-bit");
  });
  it("accepts known 8-bit input without inventing color tags", () => {
    expect(() => requireSdrInput(valid)).not.toThrow();
    expect(valid.colorTransfer).toBeUndefined();
  });
  it("fails clearly when no qualified encoder exists", () => {
    expect(() => qualifiedEncoderArgs(" V..... mpeg4 ")).toThrow("no qualified H.264 encoder");
    expect(qualifiedEncoderArgs(" V..... libx264 ")).toContain("yuv420p");
  });
  it("keeps both seam preview samples inside an export with a short tail", () => {
    const times = seamSampleTimes([2], 2.11);
    expect(times).toHaveLength(2);
    expect(times[0]).toBeLessThan(2);
    expect(times[1]).toBeGreaterThan(2);
    expect(times[1]).toBeLessThan(2.11);
  });
});

describe("decoder process lifetime", () => {
  it.each(["timeout", "cancel"])("waits for child close after %s, including an ignored TERM", async (reason) => {
    const dir = mkdtempSync(join(tmpdir(), "obs-decode-child-"));
    const pidFile = join(dir, "pid");
    const abort = new AbortController();
    let cancelTimer: NodeJS.Timeout | undefined;
    try {
      const pending = runBoundedMediaProcess(process.execPath, ["-e", "process.on('SIGTERM',()=>{}); require('fs').writeFileSync(process.argv[1],String(process.pid)); setInterval(()=>{},1000)", pidFile], { timeoutMs: reason === "cancel" ? 10_000 : 1000, signal: abort.signal });
      if (reason === "cancel") cancelTimer = setTimeout(() => abort.abort(), 1000);
      await expect(pending).rejects.toThrow(reason === "cancel" ? /cancelled/ : /timed out/);
      const pid = Number(readFileSync(pidFile, "utf8"));
      expect(() => process.kill(pid, 0)).toThrow();
    } finally { if (cancelTimer) clearTimeout(cancelTimer); rmSync(dir, { recursive: true, force: true }); }
  });
});

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

  it("rejects local HLS/concat inputs before any secondary network or file input", async () => {
    const manifest = join(directory, "untrusted.m3u8");
    execFileSync("ffmpeg", ["-v", "error", "-i", clip, "-c", "copy", "-f", "hls", "-hls_time", "2", "-hls_list_size", "0", manifest]);
    const segment = join(directory, readdirSync(directory).find((name) => name.startsWith("untrusted") && name.endsWith(".ts"))!);
    let connections = 0;
    const sockets = new Set<import("node:net").Socket>();
    const server = createServer((_request, response) => {
      response.writeHead(200, { "Content-Type": "video/mp2t", "Content-Length": statSync(segment).size, Connection: "close" });
      const stream = createReadStream(segment);
      response.once("close", () => stream.destroy());
      stream.once("error", () => response.destroy());
      stream.pipe(response);
    });
    server.on("connection", (socket) => { connections++; sockets.add(socket); socket.on("close", () => sockets.delete(socket)); });
    await new Promise<void>((resolve, reject) => { server.once("error", reject); server.listen(0, "127.0.0.1", resolve); });
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Owned HTTP fixture did not bind");
    const original = readFileSync(manifest, "utf8");
    writeFileSync(manifest, original.split("\n").map((line) => line && !line.startsWith("#") ? `http://127.0.0.1:${address.port}/segment.ts` : line).join("\n"));
    const disguised = join(directory, "looks-like-recording.mp4");
    writeFileSync(disguised, readFileSync(manifest));
    const outside = mkdtempSync(join(tmpdir(), "obs-trim-external-reference-"));
    const concat = join(directory, "untrusted.ffconcat");
    const external = join(outside, "external.mp4");
    writeFileSync(external, readFileSync(clip));
    writeFileSync(concat, `ffconcat version 1.0\nfile '${external}'\n`);
    try {
      // Explicitly permissive non-product control proves this fixture can fetch
      // the task-owned HTTP segment; no external host is used.
      await runBoundedMediaProcess("ffprobe", ["-v", "error", "-protocol_whitelist", "file,http,tcp", "-show_format", manifest], { timeoutMs: 5000 });
      expect(connections).toBeGreaterThan(0);
      connections = 0;
      for (const input of [manifest, disguised, concat]) {
        const exported = join(directory, `${input === manifest ? "hls" : "concat"}.blocked.mp4`);
        const trim = await harness.call("obs-trim-take", { path: input, apply: true, outputPath: exported, contactSheet: false });
        expect(trim.isError).toBe(true);
        expect(resultText(trim)).toContain("whitelist");
        expect(existsSync(exported)).toBe(false);
        const sheet = await harness.call("obs-contact-sheet", { path: input, times: [0.2] });
        expect(sheet.isError).toBe(true);
        await expect(contactSheet(input, [0.2], 1, 80)).rejects.toThrow(/whitelist/);
      }
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
      expect(connections).toBe(0);
    } finally {
      for (const socket of sockets) socket.destroy();
      await new Promise<void>((resolve) => server.close(() => resolve()));
      rmSync(outside, { recursive: true, force: true });
    }
  }, 15_000);

  it("exports the trimmed copy with moved chapters and a contact sheet of the seam", async () => {
    const result = await harness.call("obs-trim-take", { path: clip, apply: true });

    expect(result.isError).toBeFalsy();
    const output = join(directory, "take.trimmed.mp4");
    expect(existsSync(output)).toBe(true);
    const structured = result.structuredContent as { outputDurationSeconds: number; outputChapters: number };
    expect(structured.outputDurationSeconds).toBeCloseTo(9.5, 0);
    expect(structured.outputChapters).toBe(2);
    expect(result.structuredContent).toMatchObject({ committed: true, sha256: expect.stringMatching(/^[a-f0-9]{64}$/), verification: { metadata: "pass", strictDecode: "pass" }, format: { codec: "h264", pixelFormat: "yuv420p", colorFidelity: "unverified" } });
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

  it("keeps fractional-FPS video, audio markers, and chapters aligned across shared cuts", async () => {
    const input = join(directory, "fractional-markers.mp4"), output = join(directory, "fractional-markers.trimmed.mp4");
    const metadata = join(directory, "fractional-chapters.txt");
    writeFileSync(metadata, ";FFMETADATA1\n[CHAPTER]\nTIMEBASE=1/1000\nSTART=0\nEND=4000\ntitle=Before\n[CHAPTER]\nTIMEBASE=1/1000\nSTART=4000\nEND=8000\ntitle=Marker\n");
    execFileSync("ffmpeg", [
      "-v", "error", "-f", "lavfi", "-i", "color=red:s=160x90:r=30000/1001:d=8",
      "-f", "lavfi", "-i", "sine=frequency=1000:sample_rate=48000:duration=0.25,adelay=4000,apad=whole_dur=8",
      "-f", "ffmetadata", "-i", metadata,
      "-vf", "drawbox=x=0:y=0:w=iw:h=ih:color=blue:t=fill:enable=gte(t\\,4)",
      "-map", "0:v", "-map", "1:a", "-map_chapters", "2", "-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p", "-c:a", "aac", input,
    ]);
    const args = { path: input, outputPath: output, minIdleSeconds: 1.5, keepSeconds: 0.25, holdBeforeChapterSeconds: 0.15, holdAfterChapterSeconds: 0.35, contactSheet: false };
    const planned = await harness.call("obs-trim-take", args);
    expect(planned.isError).toBeFalsy();
    const plan = planned.structuredContent as { cuts: { start: number; end: number }[]; chapters: { atSeconds: number; name: string }[]; trimmedSeconds: number };
    expect(plan.cuts.length).toBeGreaterThan(0);
    expect(plan.cuts.some((cut) => cut.end < 4)).toBe(true);
    // Independent half-open interval arithmetic; do not call the implementation's remapTime.
    const mapSourceTime = (at: number) => at - plan.cuts.reduce((removed, cut) => removed + Math.max(0, Math.min(at, cut.end) - cut.start), 0);
    const frameTolerance = 1001 / 30000;
    const audioTolerance = 1024 / 48000; // one AAC-LC block; the source and export both use AAC
    const blueOnset = (path: string): number => {
      const pixels = execFileSync("ffmpeg", ["-v", "error", "-i", path, "-map", "0:v:0", "-vf", "scale=1:1,format=rgb24", "-fps_mode", "passthrough", "-f", "rawvideo", "pipe:1"]);
      const probe = JSON.parse(execFileSync("ffprobe", ["-v", "error", "-select_streams", "v:0", "-show_frames", "-show_entries", "frame=best_effort_timestamp_time", "-of", "json", path], { encoding: "utf8" })) as { frames: { best_effort_timestamp_time: string }[] };
      for (let i = 0; i < pixels.length / 3; i++) {
        if (pixels[i * 3 + 2]! > pixels[i * 3]! * 2) return Number(probe.frames[i]!.best_effort_timestamp_time);
      }
      throw new Error("Blue marker was not decoded");
    };
    const toneOnset = (path: string): number => {
      const pcm = execFileSync("ffmpeg", ["-v", "error", "-i", path, "-map", "0:a:0", "-ac", "1", "-ar", "48000", "-f", "f32le", "pipe:1"], { maxBuffer: 4 * 1024 * 1024 });
      for (let i = 0; i < pcm.length / 4; i++) if (Math.abs(pcm.readFloatLE(i * 4)) > 0.05) return i / 48000;
      throw new Error("Audio marker was not decoded");
    };
    const sourceVideo = blueOnset(input), sourceAudio = toneOnset(input);
    expect(Math.abs(sourceVideo - 4)).toBeLessThanOrEqual(frameTolerance);
    expect(Math.abs(sourceAudio - 4)).toBeLessThanOrEqual(audioTolerance);
    const applied = await harness.call("obs-trim-take", { ...args, apply: true });
    expect(applied.isError, resultText(applied)).toBeFalsy();
    expect(applied.structuredContent).toMatchObject({ committed: true, verification: { metadata: "pass", strictDecode: "pass" } });
    const exportedVideo = blueOnset(output), exportedAudio = toneOnset(output);
    expect(Math.abs(exportedVideo - mapSourceTime(sourceVideo))).toBeLessThanOrEqual(frameTolerance);
    expect(Math.abs(exportedAudio - mapSourceTime(sourceAudio))).toBeLessThanOrEqual(frameTolerance + audioTolerance);
    expect(Math.abs(exportedVideo - exportedAudio)).toBeLessThanOrEqual(frameTolerance + audioTolerance);
    const probe = JSON.parse(execFileSync("ffprobe", ["-v", "error", "-show_format", "-show_streams", "-show_chapters", "-of", "json", output], { encoding: "utf8" })) as { format: { duration: string }; streams: { codec_type: string; avg_frame_rate: string }[]; chapters: { start_time: string; tags: { title: string } }[] };
    expect(probe.streams.find((stream) => stream.codec_type === "video")!.avg_frame_rate).toBe("30000/1001");
    expect(Math.abs(Number(probe.format.duration) - plan.trimmedSeconds)).toBeLessThanOrEqual(frameTolerance + audioTolerance);
    const chapterAt = Number(probe.chapters.find((chapter) => chapter.tags.title === "Marker")!.start_time);
    expect(Math.abs(chapterAt - mapSourceTime(4))).toBeLessThanOrEqual(0.01);
    expect(Math.abs(chapterAt - exportedVideo)).toBeLessThanOrEqual(frameTolerance);
  }, 30_000);

  it.each(["smpte2084", "arib-std-b67", "high-depth"])("refuses %s through the public trim tool before export", async (kind) => {
    const input = join(directory, `unsupported-${kind}.mkv`);
    execFileSync("ffmpeg", ["-v", "error", "-i", clip,
      ...(kind === "high-depth" ? ["-c:v", "ffv1", "-pix_fmt", "yuv420p10le"] : ["-c:v", "copy", "-color_trc", kind]), input]);
    const metadata = JSON.parse(execFileSync("ffprobe", ["-v", "error", "-show_streams", "-of", "json", input], { encoding: "utf8" })) as { streams: { pix_fmt: string; color_transfer?: string }[] };
    if (kind === "high-depth") expect(metadata.streams[0]!.pix_fmt).toBe("yuv420p10le");
    else expect(metadata.streams[0]!.color_transfer).toBe(kind);
    const output = join(directory, `refused-${kind}.mp4`);
    const result = await harness.call("obs-trim-take", { path: input, apply: true, outputPath: output });
    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain(kind === "high-depth" ? "8-bit" : "PQ/HLG");
    expect(existsSync(output)).toBe(false);
    expect(readdirSync(directory).some((name) => name.startsWith(`.refused-${kind}`))).toBe(false);
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

  it("passes the tool call's cancellation to ffmpeg", async () => {
    // The trim tools hand ctx.mcpReq.signal, which the SDK aborts when the client cancels, to every child process.
    await expect(contactSheet(clip, [1], 1, 80, AbortSignal.abort())).rejects.toThrow(/abort/i);
  });

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
