/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFile, spawn } from "node:child_process";
import crypto from "node:crypto";
import { lstatSync, mkdtempSync, readFileSync, realpathSync, renameSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, extname, isAbsolute, join, relative } from "node:path";
import { promisify } from "node:util";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { StillTracker } from "./frame-sample.js";
import { takeLogPath } from "./take-monitor.js";

type JsonObject = Record<string, unknown>;
export type Span = { start: number; end: number };
export type Chapter = { at: number; name: string };

const execFileAsync = promisify(execFile);

/**
 * Idle detection samples the video at SAMPLE_FPS, 96px wide, and compares
 * frames the way the take monitor does. On a 1080p ChatGPT recording,
 * ffmpeg's freezedetect either called streaming text frozen (-50 dB) or saw
 * encoder noise in a still screen as change (-70 dB); the downscaled
 * changed-pixel rule separated them.
 */
const SAMPLE_FPS = 2;
const SAMPLE_WIDTH = 96;
/** Cut fragments shorter than this are not worth a seam. */
const MIN_CUT_SECONDS = 0.5;
const MAX_SHEET_FRAMES = 24;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

/** ENOENT from spawning ffmpeg or ffprobe, as opposed to a missing recording. */
function missingTool(error: unknown): boolean {
  return isObject(error) && error.code === "ENOENT" && typeof error.syscall === "string" && error.syscall.startsWith("spawn");
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

const round = (seconds: number) => Math.round(seconds * 100) / 100;

function pathTaken(path: string): boolean {
  try {
    lstatSync(path);
    return true;
  } catch {
    return false;
  }
}

function mergeSpans(spans: Span[]): Span[] {
  const sorted = [...spans].sort((a, b) => a.start - b.start);
  const merged: Span[] = [];
  for (const span of sorted) {
    const last = merged[merged.length - 1];
    if (last && span.start <= last.end) last.end = Math.max(last.end, span.end);
    else merged.push({ ...span });
  }
  return merged;
}

function subtract(spans: Span[], holes: Span[]): Span[] {
  let result = spans;
  for (const hole of holes) {
    result = result.flatMap((span) => {
      if (hole.end <= span.start || hole.start >= span.end) return [span];
      return [
        ...(hole.start > span.start ? [{ start: span.start, end: hole.start }] : []),
        ...(hole.end < span.end ? [{ start: hole.end, end: span.end }] : []),
      ];
    });
  }
  return result;
}

export type CutOptions = {
  duration: number;
  idle: Span[];
  chapters: Chapter[];
  /** Idle stretches shorter than this are kept. */
  minIdleSeconds: number;
  /** Seconds of each idle stretch kept on both sides of a cut. */
  keepSeconds: number;
  /** Seconds after each chapter start that are never cut. */
  holdAfterChapterSeconds: number;
  /**
   * Seconds before each chapter start, and before the end, that are never
   * cut: the finished result of the previous step, held so it can be read.
   */
  holdBeforeChapterSeconds: number;
};

/**
 * Plans the cuts: each idle stretch of minIdleSeconds or more loses its
 * middle, keeping keepSeconds at each side. Nothing is cut around a chapter
 * start or just before the end, where the previous step's result is on screen.
 */
export function planCuts(options: CutOptions): { cuts: Span[]; keep: Span[] } {
  const candidates = options.idle
    .filter(({ start, end }) => end - start >= options.minIdleSeconds)
    .map(({ start, end }) => ({ start: start + options.keepSeconds, end: end - options.keepSeconds }))
    .filter(({ start, end }) => end - start >= MIN_CUT_SECONDS);
  const protectedSpans = [
    ...options.chapters.map(({ at }) => ({
      start: at - options.holdBeforeChapterSeconds,
      end: at + options.holdAfterChapterSeconds,
    })),
    { start: options.duration - options.holdBeforeChapterSeconds, end: options.duration },
  ];
  const cuts = mergeSpans(subtract(candidates, protectedSpans))
    .map(({ start, end }) => ({ start: Math.max(0, start), end: Math.min(options.duration, end) }))
    .filter(({ start, end }) => end - start >= MIN_CUT_SECONDS)
    .map(({ start, end }) => ({ start: round(start), end: round(end) }));

  const keep: Span[] = [];
  let cursor = 0;
  for (const cut of cuts) {
    if (cut.start > cursor) keep.push({ start: round(cursor), end: cut.start });
    cursor = cut.end;
  }
  if (options.duration > cursor) keep.push({ start: round(cursor), end: round(options.duration) });
  return { cuts, keep };
}

/** Where a time in the original lands in the trimmed file. Times inside a cut land on its seam. */
export function remapTime(time: number, cuts: Span[]): number {
  let removed = 0;
  for (const cut of cuts) {
    if (time >= cut.end) removed += cut.end - cut.start;
    else if (time > cut.start) removed += time - cut.start;
  }
  return round(time - removed);
}

function parseSilence(stderr: string, duration: number): Span[] {
  const spans: Span[] = [];
  let start: number | null = null;
  for (const match of stderr.matchAll(/silence_(start|end):\s*(-?[\d.]+)/g)) {
    const value = Number(match[2]);
    if (match[1] === "start") start = Math.max(0, value);
    else if (start !== null) {
      spans.push({ start, end: value });
      start = null;
    }
  }
  if (start !== null) spans.push({ start, end: duration });
  return spans;
}

type Probe = { duration: number; audioStreams: number; chapters: Chapter[]; width?: number; height?: number };

async function probe(path: string): Promise<Probe> {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v", "error", "-print_format", "json", "-show_format", "-show_streams", "-show_chapters", path,
  ], { timeout: 15_000 });
  const data = JSON.parse(stdout) as JsonObject;
  const streams = Array.isArray(data.streams) ? data.streams.filter(isObject) : [];
  const video = streams.find((stream) => stream.codec_type === "video");
  const chapters = (Array.isArray(data.chapters) ? data.chapters.filter(isObject) : []).map((chapter, index) => ({
    at: Number(chapter.start_time),
    name: isObject(chapter.tags) && typeof chapter.tags.title === "string" ? chapter.tags.title : `Chapter ${index + 1}`,
  }));
  return {
    duration: Number(isObject(data.format) ? data.format.duration : NaN),
    audioStreams: streams.filter((stream) => stream.codec_type === "audio").length,
    chapters,
    ...(video ? { width: Number(video.width), height: Number(video.height) } : {}),
  };
}

/** Chapters from the file, or from the take log when the file has none. */
function takeLogChapters(path: string): Chapter[] {
  try {
    const log = JSON.parse(readFileSync(takeLogPath(path), "utf8")) as JsonObject;
    return (Array.isArray(log.chapters) ? log.chapters.filter(isObject) : [])
      .map(({ atSeconds, name }) => ({ at: Number(atSeconds), name: String(name) }))
      .filter(({ at }) => Number.isFinite(at));
  } catch {
    return [];
  }
}

/** Streams SAMPLE_FPS grayscale frames and returns the stretches where the picture did not change. */
function stillSpans(path: string, info: Probe, minIdleSeconds: number, silenceArgs: string[]): Promise<{ spans: Span[]; stderr: string }> {
  const aspect = info.width && info.height ? info.height / info.width : 9 / 16;
  const height = Math.max(2, Math.round((SAMPLE_WIDTH * aspect) / 2) * 2);
  const frameBytes = SAMPLE_WIDTH * height;
  return new Promise((resolvePromise, reject) => {
    const child = spawn("ffmpeg", [
      "-hide_banner", "-nostats", "-i", path,
      "-map", "0:v:0", "-vf", `fps=${SAMPLE_FPS},scale=${SAMPLE_WIDTH}:${height},format=gray`, "-f", "rawvideo", "pipe:1",
      ...silenceArgs,
    ], { stdio: ["ignore", "pipe", "pipe"] });
    let timedOut = false;
    const timer = setTimeout(() => {
      timedOut = true;
      child.kill("SIGKILL");
    }, 300_000);
    const tracker = new StillTracker(minIdleSeconds);
    let stderr = "";
    let pending: Buffer = Buffer.alloc(0);
    let frame = 0;

    child.stdout.on("data", (chunk: Buffer) => {
      pending = pending.length === 0 ? chunk : Buffer.concat([pending, chunk]);
      while (pending.length >= frameBytes) {
        tracker.push(frame / SAMPLE_FPS, new Uint8Array(pending.subarray(0, frameBytes)));
        pending = pending.subarray(frameBytes);
        frame += 1;
      }
    });
    child.stderr.on("data", (chunk: Buffer) => {
      stderr += chunk.toString();
      if (stderr.length > 4_000_000) stderr = stderr.slice(-2_000_000);
    });
    child.on("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (timedOut) {
        reject(new Error("Finding still stretches took longer than 5 minutes; try a shorter recording"));
        return;
      }
      if (code !== 0) {
        reject(new Error(`ffmpeg exited with ${code}: ${stderr.trim().split("\n").slice(-2).join(" ")}`));
        return;
      }
      resolvePromise({ spans: tracker.finish(Math.min(info.duration, frame / SAMPLE_FPS)), stderr });
    });
  });
}

async function detectIdle(path: string, info: Probe, minIdleSeconds: number): Promise<{ idle: Span[]; audible: boolean }> {
  // Silence spans only matter if the recording has sound; they keep narration from being cut.
  const withAudio = info.audioStreams > 0;
  const silenceArgs = withAudio
    ? ["-map", "0:a:0", "-af", `silencedetect=n=-50dB:d=${minIdleSeconds}`, "-f", "null", "-"]
    : [];
  const { spans: still, stderr } = await stillSpans(path, info, minIdleSeconds, silenceArgs);
  if (!withAudio) return { idle: still, audible: false };
  const silent = parseSilence(stderr, info.duration);
  const whollySilent = silent.length === 1 && silent[0]!.start <= 0.1 && silent[0]!.end >= info.duration - 0.1;
  if (whollySilent) return { idle: still, audible: false };
  // Idle only where the picture is still and the sound is silent.
  const idle = still.flatMap((f) => silent
    .map((s) => ({ start: Math.max(f.start, s.start), end: Math.min(f.end, s.end) }))
    .filter(({ start, end }) => end > start));
  return { idle, audible: true };
}

let cachedEncoder: string[] | null = null;
async function videoEncoderArgs(): Promise<string[]> {
  if (cachedEncoder) return cachedEncoder;
  const { stdout } = await execFileAsync("ffmpeg", ["-hide_banner", "-encoders"], { timeout: 10_000 });
  cachedEncoder = /\blibx264\b/.test(stdout)
    ? ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p"]
    : /\bh264_videotoolbox\b/.test(stdout)
      ? ["-c:v", "h264_videotoolbox", "-q:v", "65", "-pix_fmt", "yuv420p"]
      : [];
  return cachedEncoder;
}

function ffmetadata(chapters: Chapter[], duration: number): string {
  // ffmetadata ends a line at \r as well as \n: drop control characters, then escape its special characters.
  // eslint-disable-next-line no-control-regex
  const escape = (text: string) => text.replace(/[\u0000-\u001f\u007f]/g, " ").replace(/([=;#\\])/g, "\\$1");
  const lines = [";FFMETADATA1"];
  chapters.forEach((chapter, index) => {
    const end = chapters[index + 1]?.at ?? duration;
    lines.push("[CHAPTER]", "TIMEBASE=1/1000", `START=${Math.round(chapter.at * 1000)}`, `END=${Math.round(end * 1000)}`, `title=${escape(chapter.name)}`);
  });
  return `${lines.join("\n")}\n`;
}

async function exportTrimmed(
  path: string,
  outputPath: string,
  keep: Span[],
  audioStreams: number,
  chapters: Chapter[],
  duration: number,
): Promise<void> {
  const work = mkdtempSync(join(tmpdir(), "obs-mcp-trim-"));
  try {
    const metadata = join(work, "chapters.txt");
    writeFileSync(metadata, ffmetadata(chapters, duration));
    const withAudio = audioStreams > 0;
    const parts = keep.map(({ start, end }, index) => [
      `[0:v:0]trim=start=${start}:end=${end},setpts=PTS-STARTPTS[v${index}]`,
      ...(withAudio ? [`[0:a:0]atrim=start=${start}:end=${end},asetpts=PTS-STARTPTS[a${index}]`] : []),
    ].join(";"));
    const inputs = keep.map((_span, index) => `[v${index}]${withAudio ? `[a${index}]` : ""}`).join("");
    const filter = `${parts.join(";")};${inputs}concat=n=${keep.length}:v=1:a=${withAudio ? 1 : 0}[v]${withAudio ? "[a]" : ""}`;
    await execFileAsync("ffmpeg", [
      "-hide_banner", "-nostats", "-y", "-i", path, "-f", "ffmetadata", "-i", metadata,
      "-filter_complex", filter, "-map", "[v]", ...(withAudio ? ["-map", "[a]", "-c:a", "aac", "-b:a", "160k"] : []),
      "-map_metadata", "1", "-map_chapters", "1", ...(await videoEncoderArgs()), "-movflags", "+faststart", outputPath,
    ], { timeout: 600_000, maxBuffer: 32 * 1024 * 1024 });
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

/** Grabs one frame per time and tiles them into one JPEG. */
export async function contactSheet(path: string, times: number[], columns: number, width: number): Promise<Buffer> {
  const work = mkdtempSync(join(tmpdir(), "obs-mcp-sheet-"));
  try {
    for (const [index, time] of times.entries()) {
      await execFileAsync("ffmpeg", [
        "-hide_banner", "-v", "error", "-y", "-ss", String(Math.max(0, time)), "-i", path,
        "-frames:v", "1", "-vf", `scale=${width}:-2`, "-q:v", "4", join(work, `${String(index).padStart(3, "0")}.jpg`),
      ], { timeout: 30_000 });
    }
    const rows = Math.ceil(times.length / columns);
    const sheet = join(work, "sheet.jpg");
    await execFileAsync("ffmpeg", [
      "-hide_banner", "-v", "error", "-y", "-framerate", "1", "-i", join(work, "%03d.jpg"),
      "-vf", `tile=${Math.min(columns, times.length)}x${rows}:padding=4:color=white`, "-frames:v", "1", "-q:v", "4", sheet,
    ], { timeout: 30_000 });
    return readFileSync(sheet);
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

/**
 * Resolves a recording path and checks it lies in OBS's recording directory,
 * so these tools cannot be pointed at arbitrary files.
 */
async function recordingFile(client: OBSWebSocketClient, path: string, mustExist = true): Promise<string> {
  const { recordDirectory } = await client.sendRequest("GetRecordDirectory") as { recordDirectory?: unknown };
  if (typeof recordDirectory !== "string" || !recordDirectory) throw new Error("OBS has no recording directory set");
  const root = realpathSync(recordDirectory);
  const absolute = isAbsolute(path) ? path : join(root, path);
  const resolved = mustExist ? realpathSync(absolute) : join(realpathSync(dirname(absolute)), basename(absolute));
  const inside = relative(root, resolved);
  if (inside.startsWith("..") || isAbsolute(inside)) {
    throw new Error(`${path} is outside the OBS recording directory ${root}`);
  }
  return resolved;
}

function describeSpans(spans: Span[]): string {
  return spans.map(({ start, end }) => `${start}–${end}s`).join(", ");
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-trim-take",
    {
      title: "Trim Dead Time",
      description: "Plan, and optionally export, a copy of a recording with its dead time cut: stretches where "
        + "the picture does not change (and the sound, if any, is silent). A spinner or shimmering label counts "
        + "as unchanged; text appearing does not. Keeps a little of each stretch, holds each step's result before "
        + "the next chapter and the end, and moves chapters to match. Without apply it only returns the "
        + "plan. Needs ffmpeg; works on files in the OBS recording directory",
      inputSchema: z.object({
        path: z.string().describe("Recording to trim; absolute, or relative to the OBS recording directory"),
        apply: z.boolean().default(false).describe("Export the trimmed copy; otherwise only plan"),
        outputPath: z.string().optional().describe("Where to write the copy; defaults to <name>.trimmed.mp4 next to it"),
        minIdleSeconds: z.number().min(1).max(60).default(4).describe("Shortest unchanged stretch to cut"),
        keepSeconds: z.number().min(0).max(5).default(1).describe("Seconds kept at each side of a cut"),
        holdAfterChapterSeconds: z.number().min(0).max(10).default(1.5).describe("Seconds after each chapter start never cut"),
        holdBeforeChapterSeconds: z.number().min(0).max(10).default(3)
          .describe("Seconds before each chapter start and before the end never cut, so each step's result stays readable"),
        contactSheet: z.boolean().default(true).describe("With apply, also return frames either side of each seam"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (args): Promise<CallToolResult> => {
      try {
        const path = await recordingFile(client, args.path);
        const info = await probe(path);
        if (!Number.isFinite(info.duration)) return errorResult(`ffprobe could not read the length of ${path}`);
        const chapters = info.chapters.length > 0 ? info.chapters : takeLogChapters(path);
        const { idle, audible } = await detectIdle(path, info, args.minIdleSeconds);
        const { cuts, keep } = planCuts({ ...args, duration: info.duration, idle, chapters });
        const removed = round(cuts.reduce((total, { start, end }) => total + end - start, 0));
        const newChapters = chapters.map(({ at, name }) => ({ at: remapTime(at, cuts), name }));
        const seams = cuts.map(({ start }) => remapTime(start, cuts));

        const plan = {
          path,
          durationSeconds: round(info.duration),
          trimmedSeconds: round(info.duration - removed),
          removedSeconds: removed,
          idle: idle.map(({ start, end }) => ({ start: round(start), end: round(end) })),
          cuts,
          keep,
          seams,
          chapters: newChapters.map(({ at, name }) => ({ atSeconds: at, name })),
          audioConsidered: audible,
        };
        const lines = [
          cuts.length === 0
            ? `Nothing to cut: no unchanged stretch of ${args.minIdleSeconds}s or more outside the chapter starts`
            : `Cut ${cuts.length} stretch(es), ${removed}s of ${plan.durationSeconds}s: ${describeSpans(cuts)}`,
          `Trimmed length: ${plan.trimmedSeconds}s`,
          ...(newChapters.length > 0 ? [`Chapters after trimming: ${newChapters.map(({ at, name }) => `${at}s ${name}`).join(", ")}`] : []),
        ];
        if (!args.apply || cuts.length === 0) {
          if (!args.apply && cuts.length > 0) lines.push("Plan only; call again with apply: true to export");
          return { content: [{ type: "text", text: lines.join("\n") }], structuredContent: plan };
        }

        const outputPath = await recordingFile(
          client,
          args.outputPath ?? join(dirname(path), `${basename(path, extname(path))}.trimmed.mp4`),
          false,
        );
        if (outputPath === path) return errorResult("The trimmed copy cannot replace the original");
        // lstat also sees a dangling symlink, which ffmpeg would write through, possibly outside the directory.
        if (pathTaken(outputPath)) return errorResult(`${outputPath} already exists; choose another outputPath`);
        // Export under a temporary name so a failed or interrupted export leaves nothing that looks finished.
        // A unique name, so it can never be the recording being trimmed, which the finally block would delete.
        const partial = join(dirname(outputPath), `.${basename(outputPath)}.${process.pid}-${crypto.randomUUID().slice(0, 8)}.partial${extname(outputPath)}`);
        if (partial === path || pathTaken(partial)) return errorResult("Could not choose a temporary name for the export; try again");
        try {
          await exportTrimmed(path, partial, keep, info.audioStreams, newChapters, plan.trimmedSeconds);
          if (pathTaken(outputPath)) return errorResult(`${outputPath} appeared while exporting; the export is discarded`);
          renameSync(partial, outputPath);
        } finally {
          rmSync(partial, { force: true });
        }
        const result = await probe(outputPath);
        lines.push(`Exported ${outputPath}: ${round(result.duration)}s, ${result.chapters.length} chapter(s)`);

        const content: CallToolResult["content"] = [];
        if (args.contactSheet && seams.length > 0) {
          const times = seams.flatMap((seam) => [Math.max(0, seam - 0.5), seam + 0.2]).slice(0, MAX_SHEET_FRAMES);
          const sheet = await contactSheet(outputPath, times, 4, 320);
          lines.push(`Contact sheet: frames 0.5s before and 0.2s after each seam, in pairs, left to right: ${times.map((t) => `${round(t)}s`).join(", ")}`);
          content.push({ type: "image", data: sheet.toString("base64"), mimeType: "image/jpeg" });
        }
        return {
          content: [{ type: "text", text: lines.join("\n") }, ...content],
          structuredContent: { ...plan, outputPath, outputDurationSeconds: round(result.duration), outputChapters: result.chapters.length },
        };
      } catch (error) {
        return errorResult(missingTool(error)
          ? "obs-trim-take needs ffmpeg and ffprobe on PATH"
          : `Trim failed: ${errorMessage(error)}`);
      }
    },
  );

  server.registerTool(
    "obs-contact-sheet",
    {
      title: "Contact Sheet",
      description: "Return one image of frames from a recording, tiled in time order: at each chapter start by "
        + "default, or at the given times. Use it to check a take or an edit without watching it. Needs ffmpeg; "
        + "works on files in the OBS recording directory",
      inputSchema: z.object({
        path: z.string().describe("Recording; absolute, or relative to the OBS recording directory"),
        times: z.array(z.number().nonnegative()).max(MAX_SHEET_FRAMES).optional()
          .describe("Seconds to grab; defaults to 1s after each chapter start, or evenly spaced frames"),
        columns: z.number().int().min(1).max(8).default(4),
        width: z.number().int().min(80).max(640).default(320).describe("Width of each frame in pixels"),
      }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (args): Promise<CallToolResult> => {
      try {
        const path = await recordingFile(client, args.path);
        const info = await probe(path);
        const chapters = info.chapters.length > 0 ? info.chapters : takeLogChapters(path);
        const times = (args.times ?? (chapters.length > 0
          ? chapters.map(({ at }) => at + 1)
          : Array.from({ length: 8 }, (_unused, index) => ((index + 0.5) * info.duration) / 8)))
          .filter((time) => time < info.duration)
          .slice(0, MAX_SHEET_FRAMES);
        if (times.length === 0) return errorResult("No requested time falls inside the recording");
        const sheet = await contactSheet(path, times, args.columns, args.width);
        const legend = times.map((time, index) => {
          const chapter = !args.times ? chapters[index]?.name : undefined;
          return `${index + 1}. ${round(time)}s${chapter ? ` ${chapter}` : ""}`;
        });
        return {
          content: [
            { type: "text", text: `${times.length} frame(s) from ${path}, left to right, top to bottom:\n${legend.join("\n")}` },
            { type: "image", data: sheet.toString("base64"), mimeType: "image/jpeg" },
          ],
        };
      } catch (error) {
        return errorResult(missingTool(error)
          ? "obs-contact-sheet needs ffmpeg and ffprobe on PATH"
          : `Contact sheet failed: ${errorMessage(error)}`);
      }
    },
  );
}

