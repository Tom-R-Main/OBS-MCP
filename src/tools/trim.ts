/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: MIT
 */
import { execFile } from "node:child_process";
import crypto from "node:crypto";
import { closeSync, constants, fstatSync, linkSync, lstatSync, mkdtempSync, openSync, readFileSync, readSync, readdirSync, realpathSync, rmdirSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import type { BigIntStats } from "node:fs";
import { tmpdir } from "node:os";
import { basename, dirname, extname, isAbsolute, join, relative } from "node:path";
import { promisify } from "node:util";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { StillTracker } from "./frame-sample.js";
import { takeLogPath } from "./take-monitor.js";
import { localMediaInputOptions, runStreamingMediaProcess, strictDecode } from "./media-verification.js";

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

/** Trusted conceptual controls only; production reads private ametadata files. */
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

export type AudioTrack = { codec: string; sampleRate: number; channels: number; channelLayout: string; language: string; title: string | null; dispositions: Record<string, number> | null; startSeconds: number | null };
export type MediaProbe = {
  duration: number; audioStreams: number; videoStreams: number; chapters: Chapter[];
  width?: number; height?: number; codec?: string; pixelFormat?: string; bitDepth?: number;
  colorTransfer?: string; colorPrimaries?: string; colorSpace?: string; colorRange?: string;
  frameRate?: string;
  audioTracks?: AudioTrack[];
  formatStartSeconds?: number;
  videoStartSeconds?: number;
};
type Probe = MediaProbe;
function timestamp(value: unknown): number | null {
  return typeof value === 'string' && /^[+-]?(?:\d+\.?\d*|\.\d+)(?:e[+-]?\d+)?$/i.test(value) && Number.isFinite(Number(value)) ? Number(value) : null;
}

function rationalFrameRate(value: unknown): string | undefined {
  if (typeof value !== "string" || !/^[1-9][0-9]{0,9}\/[1-9][0-9]{0,9}$/.test(value)) return undefined;
  const [numerator, denominator] = value.split("/").map(Number) as [number, number];
  if (numerator > 2147483647 || denominator > 2147483647 || numerator > denominator * 1000) return undefined;
  const gcd = (a: number, b: number): number => b === 0 ? a : gcd(b, a % b);
  const divisor = gcd(numerator, denominator);
  return `${numerator / divisor}/${denominator / divisor}`;
}

async function probe(path: string, signal?: AbortSignal): Promise<Probe> {
  const { stdout } = await execFileAsync("ffprobe", [
    "-v", "error", "-print_format", "json", "-show_format", "-show_streams", "-show_chapters", ...localMediaInputOptions(), path,
  ], { timeout: 15_000, signal });
  const data = JSON.parse(stdout) as JsonObject;
  const streams = Array.isArray(data.streams) ? data.streams.filter(isObject) : [];
  const video = streams.find((stream) => stream.codec_type === "video");
  const audioTracks = streams.filter(stream => stream.codec_type === "audio").map((stream): AudioTrack => {
    const tag = (name: string) => isObject(stream.tags) ? Object.entries(stream.tags).find(([key]) => key.toLowerCase() === name)?.[1] : undefined;
    const explicitTitle = tag('title');
    const rawTitle = typeof explicitTitle === 'string' && explicitTitle !== '' ? explicitTitle : tag('handler_name');
    const title = typeof rawTitle === 'string' && rawTitle !== '' && (rawTitle !== 'SoundHandler' || rawTitle === explicitTitle) ? rawTitle : null;
    return { codec: typeof stream.codec_name === 'string' ? stream.codec_name : '', sampleRate: typeof stream.sample_rate === 'string' && /^[0-9]+$/.test(stream.sample_rate) && Number(stream.sample_rate) <= 4294967295 ? Number(stream.sample_rate) : 0, channels: typeof stream.channels === 'number' && Number.isInteger(stream.channels) && stream.channels >= 0 && stream.channels <= 255 ? stream.channels : 0, channelLayout: typeof stream.channel_layout === 'string' ? stream.channel_layout : '', language: typeof tag('language') === 'string' ? tag('language') as string : 'und', title, dispositions: isObject(stream.disposition) && Object.values(stream.disposition).every(value => value === 0 || value === 1) ? stream.disposition as Record<string, number> : null, startSeconds: timestamp(stream.start_time) };
  });
  const averageRate = rationalFrameRate(video?.avg_frame_rate);
  // Retain an agreed declared cadence. Conflicting or unavailable rate
  // metadata supplies no fixed-rate target; it does not prove packet timing.
  const frameRate = averageRate && averageRate === rationalFrameRate(video?.r_frame_rate) ? averageRate : undefined;
  const chapters = (Array.isArray(data.chapters) ? data.chapters.filter(isObject) : []).map((chapter, index) => ({
    at: Number(chapter.start_time),
    name: isObject(chapter.tags) && typeof chapter.tags.title === "string" ? chapter.tags.title : `Chapter ${index + 1}`,
  }));
  return {
    duration: Number(isObject(data.format) ? data.format.duration : NaN),
    audioStreams: streams.filter((stream) => stream.codec_type === "audio").length,
    ...(audioTracks.length ? { audioTracks } : {}),
    ...(isObject(data.format) && timestamp(data.format.start_time) !== null ? {formatStartSeconds:timestamp(data.format.start_time)!} : {}),
    ...(timestamp(video?.start_time) !== null ? {videoStartSeconds:timestamp(video?.start_time)!} : {}),
    videoStreams: streams.filter((stream) => stream.codec_type === "video").length,
    chapters,
    ...(video ? {
      width: Number(video.width), height: Number(video.height),
      codec: typeof video.codec_name === "string" ? video.codec_name : undefined,
      pixelFormat: typeof video.pix_fmt === "string" ? video.pix_fmt : undefined,
      bitDepth: Number(video.bits_per_raw_sample) || undefined,
      colorTransfer: typeof video.color_transfer === "string" ? video.color_transfer : undefined,
      colorPrimaries: typeof video.color_primaries === "string" ? video.color_primaries : undefined,
      colorSpace: typeof video.color_space === "string" ? video.color_space : undefined,
      colorRange: typeof video.color_range === "string" ? video.color_range : undefined,
      ...(frameRate ? { frameRate } : {}),
    } : {}),
  };
}

const EIGHT_BIT_FORMATS = new Set([
  "yuv420p", "yuvj420p", "yuv422p", "yuvj422p", "yuv444p", "yuvj444p", "nv12", "nv21",
  "uyvy422", "yuyv422", "rgb24", "bgr24", "rgba", "bgra", "argb", "abgr", "gbrp", "gray", "pal8",
  "0rgb", "rgb0", "0bgr", "bgr0",
]);

export function requireSdrInput(info: MediaProbe): void {
  if (["smpte2084", "arib-std-b67"].includes(info.colorTransfer ?? "")) {
    throw new Error("Trim export supports 8-bit SDR only; PQ/HLG input requires an explicit tone-mapping workflow");
  }
  if ((info.bitDepth ?? 0) > 8 || !EIGHT_BIT_FORMATS.has(info.pixelFormat ?? "")) {
    throw new Error(`Trim export requires a known 8-bit pixel format; got ${info.pixelFormat ?? "unknown"}`);
  }
}

const chapterTitle = (name: string) => name.replace(/[\u0000-\u001f\u007f]/g, " ");

function requireSingleAudioTrack(audioStreams: number): void {
  if (audioStreams > 1) throw new Error("Trim supports at most one audio track; multitrack input requires an explicit track-preserving workflow");
}
function qualifiedAudioTracks(info: Pick<MediaProbe, 'audioStreams' | 'audioTracks' | 'formatStartSeconds' | 'videoStartSeconds'>, checkTiming = true): AudioTrack[] {
  const tracks = info.audioTracks;
  if (info.audioStreams < 2 || info.audioStreams > 6 || !tracks || tracks.length !== info.audioStreams) throw new Error('Multitrack trim requires complete metadata for two through six audio tracks');
  const nearZero=(value:unknown):value is number=>typeof value==='number'&&Number.isFinite(value)&&Math.abs(value)<=0.05;
  if(tracks.some(track=>!nearZero(track.startSeconds))||(checkTiming&&(!nearZero(info.formatStartSeconds)||!nearZero(info.videoStartSeconds)||tracks.some(track=>Math.abs(track.startSeconds!-info.videoStartSeconds!)>0.05))))throw new Error('Multitrack trim requires declared audio/video/format starts within 50 ms of zero and each other');
  let defaults = 0;
  for (const track of tracks) {
    if (!['aac','flac','opus','pcm_s16le','pcm_s24le','pcm_s32le','pcm_f32le'].includes(track.codec) || ![44100,48000].includes(track.sampleRate) || !((track.channels === 1 && track.channelLayout === 'mono') || (track.channels === 2 && track.channelLayout === 'stereo')) || !/^[a-z]{3}$/.test(track.language) || (track.title !== null && (track.title === 'SoundHandler' || Buffer.byteLength(track.title) > 1024 || /[\u0000-\u001f\u007f-\u009f]/.test(track.title)))) throw new Error('Multitrack trim requires supported codecs, mono/stereo 44.1/48 kHz audio and representable track metadata');
    if (!track.dispositions) throw new Error('Multitrack audio dispositions are unavailable');
    if (Object.entries(track.dispositions).some(([name,value]) => value !== 0 && (value !== 1 || name !== 'default'))) throw new Error('Multitrack trim supports only the default audio disposition');
    if (track.dispositions.default === 1) defaults++;
  }
  // OBS marks every recorded track default; the export copies each flag.
  if (defaults === 0) throw new Error('Multitrack MP4 trim requires at least one default audio track');
  return tracks;
}

export function validateTrimmedMetadata(actual: MediaProbe, expected: { duration: number; width?: number; height?: number; audioStreams: number; audioTracks?: AudioTrack[]; chapters: Chapter[]; frameRate?: string }): void {
  if (expected.audioStreams > 1 && !expected.audioTracks) requireSingleAudioTrack(expected.audioStreams);
  if (!Number.isFinite(actual.duration) || Math.abs(actual.duration - expected.duration) > 0.25) throw new Error("Staged export duration differs from the cut plan");
  if (actual.videoStreams !== 1 || actual.codec !== "h264" || actual.pixelFormat !== "yuv420p") throw new Error("Staged export must contain one H.264 yuv420p video stream");
  if (actual.width !== expected.width || actual.height !== expected.height) throw new Error("Staged export dimensions differ from the source");
  if (expected.frameRate !== undefined && actual.frameRate !== expected.frameRate) throw new Error("Staged export frame rate differs from the source");
  if (expected.audioStreams > 1) {
    const tracks = qualifiedAudioTracks(expected, false);
    if (actual.audioStreams !== tracks.length || actual.audioTracks?.length !== tracks.length || tracks.some((track,index) => { const value = actual.audioTracks![index]!; return value.codec !== 'aac' || value.startSeconds===null || !Number.isFinite(value.startSeconds) || Math.abs(value.startSeconds)>0.05 || value.sampleRate !== track.sampleRate || value.channels !== track.channels || value.channelLayout !== track.channelLayout || value.language !== track.language || value.title !== track.title || !value.dispositions || Object.entries(value.dispositions).some(([name,flag]) => flag !== 0 && name !== 'default') || (value.dispositions.default ?? 0) !== (track.dispositions?.default ?? 0); })) throw new Error('Staged export audio track order or descriptors differ from the source');
  } else if (actual.audioStreams !== Math.min(1, expected.audioStreams)) throw new Error("Staged export audio stream count differs from the first-track policy");
  if (actual.chapters.length !== expected.chapters.length || expected.chapters.some((chapter, i) => {
    const observed = actual.chapters[i];
    return !observed || observed.name !== chapterTitle(chapter.name) || !Number.isFinite(observed.at) || Math.abs(observed.at - chapter.at) > 0.05;
  })) throw new Error("Staged export chapters differ from the remapped cut plan");
}

function mediaHash(path: string): string {
  const hash = crypto.createHash("sha256");
  const fd = openSync(path, "r");
  try {
    const buffer = Buffer.alloc(64 * 1024);
    let count: number;
    while ((count = readSync(fd, buffer)) > 0) hash.update(buffer.subarray(0, count));
    return hash.digest("hex");
  } finally { closeSync(fd); }
}

/** Encode/verify before a no-clobber publication. Optional preview cannot erase a committed outcome. */
export async function publishTrimmedExport<T>(
  partial: string, outputPath: string,
  steps: { encode: () => Promise<void>; verify: () => Promise<T>; preview?: () => Promise<Buffer> },
  signal?: AbortSignal,
): Promise<{ verified: T; sha256: string; preview?: Buffer; previewWarning?: string; cleanupWarning?: string }> {
  let committed = false;
  let cleanupWarning: string | undefined;
  let verified: T;
  let sha256: string;
  try {
    signal?.throwIfAborted();
    await steps.encode();
    signal?.throwIfAborted();
    verified = await steps.verify();
    sha256 = mediaHash(partial);
    signal?.throwIfAborted();
    // link is atomic and fails if ANY destination appeared; rename would overwrite it.
    linkSync(partial, outputPath);
    committed = true;
  } finally {
    try { rmSync(partial, { force: true }); }
    catch (error) {
      if (!committed) throw error;
      cleanupWarning = `Export committed; temporary-file cleanup failed: ${errorMessage(error)}`;
    }
  }
  let preview: Buffer | undefined;
  let previewWarning: string | undefined;
  if (steps.preview) {
    try { preview = await steps.preview(); }
    catch (error) { previewWarning = `Export committed; contact sheet unavailable: ${errorMessage(error)}`; }
  }
  return { verified, sha256, ...(preview ? { preview } : {}), ...(previewWarning ? { previewWarning } : {}), ...(cleanupWarning ? { cleanupWarning } : {}) };
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

const AUDIO_EVIDENCE_BYTES = 1024 * 1024;
const AUDIO_EVIDENCE_EVENTS = 100_000;
/** Two FFmpeg parsing layers: graph separators, then the file option value. */
export function audioEvidenceFilterPath(path: string): string {
  return path.replace(/\\/g, "\\\\\\\\").replace(/'/g, "\\\\\\'").replace(/:/g, "\\\\:").replace(/[,;\[\]]/g, "\\$&");
}
/** Only a joined, private producer file may be passed here, never stderr. */
export function parseAudioEvidence(text: string, duration: number): Span[] {
  if (!Number.isFinite(duration) || duration <= 0 || Buffer.byteLength(text) > AUDIO_EVIDENCE_BYTES || (text !== "" && !text.endsWith("\n"))) throw Error('Audio evidence is incomplete or exceeds its bound');
  const numeric = /^-?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$/;
  let frame = -1, fields = new Map<string,number>(), start: number | null = null, events = 0;
  const spans: Span[] = [];
  const finishFrame = () => {
    if (frame < 0) return;
    if (fields.size === 0 || fields.has('end') !== fields.has('duration')) throw Error('Audio evidence frame is incomplete');
    if (fields.has('duration') && fields.get('duration')! < 0) throw Error('Audio evidence silence duration is negative');
    if (fields.has('start')) { if (start !== null) throw Error('Audio evidence has overlapping silence starts'); start = Math.max(0,fields.get('start')!); }
    if (fields.has('end')) { const end = fields.get('end')!; if (start === null || end < start) throw Error('Audio evidence has an unmatched silence end'); spans.push({start,end:Math.min(duration,end)}); start = null; }
  };
  for (const line of text.split('\n').slice(0, -1)) {
    const header = /^frame:\s*(\d+)\s+pts:\s*(-?\d+|NOPTS)\s+pts_time:\s*(\S+)\s*$/.exec(line);
    if (header) {
      const next = Number(header[1]), time = Number(header[3]);
      if (!Number.isSafeInteger(next) || next <= frame || !numeric.test(header[3]!) || !Number.isFinite(time)) throw Error('Audio evidence frame sequence is invalid');
      finishFrame();frame = next;fields = new Map();continue;
    }
    const entry = /^lavfi\.silence_(start|end|duration)=([^\s]+)$/.exec(line);
    if (frame < 0 || !entry || fields.has(entry[1]!) || ++events > AUDIO_EVIDENCE_EVENTS) throw Error('Audio evidence contains unexpected or duplicate metadata');
    const value = Number(entry[2]);
    if (!numeric.test(entry[2]!) || !Number.isFinite(value) || value < -0.1 || value > duration + 0.1) throw Error('Audio evidence timestamp is outside the recording');
    fields.set(entry[1]!,value);
  }
  finishFrame();
  if (start !== null) spans.push({start,end:duration});
  if (spans.some((span,index) => span.end < span.start || (index > 0 && spans[index-1]!.end > span.start))) throw Error('Audio silence evidence overlaps');
  return spans;
}

/** One sampling child owns private audio evidence until both pipes are joined. */
async function stillSpans(path: string, info: Probe, minIdleSeconds: number, signal?: AbortSignal): Promise<{ spans: Span[]; audio: Span[][] }> {
  signal?.throwIfAborted();
  const aspect = info.width && info.height ? info.height / info.width : 9 / 16;
  const height = Math.max(2, Math.round((SAMPLE_WIDTH * aspect) / 2) * 2), frameBytes = SAMPLE_WIDTH * height;
  if (info.audioStreams < 0 || info.audioStreams > 6) throw Error('Audio analysis supports at most six tracks');
  const root = realpathSync(mkdtempSync(join(tmpdir(), 'obs-mcp-audio-evidence-'))), rootIdentity = lstatSync(root, {bigint:true});
  const files: {path: string; fd: number; identity: BigIntStats}[] = [];
  const stop = new AbortController(), combined = signal ? AbortSignal.any([signal, stop.signal]) : stop.signal;
  let retained = false, issue: Error | undefined, monitor: ReturnType<typeof setInterval> | undefined;
  const same = (a:BigIntStats,b:BigIntStats) => a.dev === b.dev && a.ino === b.ino;
  const check = () => {
    const current = lstatSync(root, {bigint:true});
    if (!same(rootIdentity,current) || current.mode !== rootIdentity.mode || !current.isDirectory() || current.isSymbolicLink()) throw Error('Private audio evidence directory identity changed');
    let total = 0n;
    for (const file of files) {
      const named = lstatSync(file.path,{bigint:true}), opened = fstatSync(file.fd,{bigint:true});
      if (!same(named,file.identity) || !same(opened,file.identity) || !named.isFile() || named.isSymbolicLink() || named.nlink !== 1n || opened.nlink !== 1n || named.mode !== file.identity.mode) throw Error('Private audio evidence file identity changed');
      total += opened.size;
      if (opened.size > BigInt(AUDIO_EVIDENCE_BYTES) || total > BigInt(6 * AUDIO_EVIDENCE_BYTES)) throw Error('Private audio evidence exceeds its byte bound');
    }
  };
  try {
    for (let index = 0; index < info.audioStreams; index++) {
      const output = join(root, `track-${index}.txt`), fd = openSync(output, constants.O_CREAT | constants.O_EXCL | constants.O_RDWR | constants.O_NOFOLLOW, 0o600);
      try { files.push({path:output,fd,identity:fstatSync(fd,{bigint:true})}); } catch (error) { closeSync(fd); throw error; }
    }
    check();
    const silenceArgs = files.flatMap((file,index) => ['-map',`0:a:${index}`,'-af',`ametadata=mode=delete,silencedetect=n=-50dB:d=${minIdleSeconds},ametadata=mode=print:file=${audioEvidenceFilterPath(file.path)}`,'-f','null','-']);
    const tracker = new StillTracker(minIdleSeconds); let pending:Buffer = Buffer.alloc(0), frame = 0;
    // Sampled extent checks stop the owned producer; this is not a disk sandbox.
    monitor = setInterval(() => { try { check(); } catch (error) { issue = error instanceof Error ? error : Error(String(error)); stop.abort(); } }, 25);
    await runStreamingMediaProcess('ffmpeg', ['-hide_banner','-nostats','-xerror','-err_detect','explode',...localMediaInputOptions(),'-i',path,'-map','0:v:0','-vf',`fps=${SAMPLE_FPS},scale=${SAMPLE_WIDTH}:${height},format=gray`,'-f','rawvideo','pipe:1',...silenceArgs], chunk => {
      check(); pending = pending.length === 0 ? Buffer.from(chunk) : Buffer.concat([pending,chunk]);
      while (pending.length >= frameBytes) { tracker.push(frame / SAMPLE_FPS, new Uint8Array(pending.subarray(0,frameBytes))); pending = pending.subarray(frameBytes); frame++; }
    }, {signal:combined,timeoutMs:300_000});
    if (issue) throw issue;
    if (pending.length !== 0) throw Error('Grayscale sampling ended with an incomplete frame');
    check();
    const audio = files.map(file => {
      const before = fstatSync(file.fd,{bigint:true}), bytes = Buffer.alloc(Number(before.size) + 1), count = readSync(file.fd,bytes,0,bytes.length,0), after = fstatSync(file.fd,{bigint:true});
      if (count !== Number(before.size) || before.size !== after.size || before.mtimeNs !== after.mtimeNs || before.ctimeNs !== after.ctimeNs) throw Error('Private audio evidence changed during read');
      return parseAudioEvidence(bytes.subarray(0,count).toString('utf8'),info.duration);
    });
    check(); return {spans:tracker.finish(Math.min(info.duration,frame / SAMPLE_FPS)),audio};
  } catch (error) {
    retained = error instanceof Error && /cleanup unconfirmed/i.test(error.message);
    if (retained) throw error;
    throw issue ?? error;
  } finally {
    if (monitor) clearInterval(monitor);
    try {
      if (!retained) {
        check(); const expected = files.map(file => basename(file.path)).sort(), names = readdirSync(root).sort();
        if (JSON.stringify(names) !== JSON.stringify(expected)) throw Error('Private audio evidence directory contains unowned entries');
        for (const file of files) unlinkSync(file.path);
        rmdirSync(root);
      }
    } catch (error) { retained = true; throw Error(`Audio evidence cleanup unconfirmed; private files retained: ${errorMessage(error)}`); }
    finally { for (const file of files) closeSync(file.fd); }
  }
}

async function detectIdle(path: string, info: Probe, minIdleSeconds: number, signal?: AbortSignal): Promise<{ idle: Span[]; audible: boolean }> {
  if (info.audioStreams > 1) qualifiedAudioTracks(info);
  const {spans:still,audio} = await stillSpans(path, info, minIdleSeconds, signal);
  if (audio.length === 0) return {idle:still,audible:false};
  let common:Span[] = [{start:0,end:info.duration}], allSilent = true;
  for (const silent of audio) {
    allSilent &&= silent.length === 1 && silent[0]!.start <= 0.1 && silent[0]!.end >= info.duration - 0.1;
    common = intersectSpans(common,silent);
  }
  return {idle:intersectSpans(still,common),audible:!allSilent};
}
function intersectSpans(a: Span[], b: Span[]): Span[] {
  let left=0,right=0; const out:Span[]=[];
  while(left<a.length&&right<b.length){const start=Math.max(a[left]!.start,b[right]!.start),end=Math.min(a[left]!.end,b[right]!.end);if(end>start)out.push({start,end});if(a[left]!.end<b[right]!.end)left++;else right++;}
  return out;
}

let cachedEncoder: string[] | null = null;
export function qualifiedEncoderArgs(encoders: string): string[] {
  if (/\blibx264\b/.test(encoders)) return ["-c:v", "libx264", "-preset", "veryfast", "-crf", "18", "-pix_fmt", "yuv420p"];
  if (/\bh264_videotoolbox\b/.test(encoders)) return ["-c:v", "h264_videotoolbox", "-q:v", "65", "-pix_fmt", "yuv420p"];
  throw new Error("Trim export requires libx264 or h264_videotoolbox; no qualified H.264 encoder is available");
}
async function videoEncoderArgs(): Promise<string[]> {
  if (cachedEncoder) return cachedEncoder;
  const { stdout } = await execFileAsync("ffmpeg", ["-hide_banner", "-encoders"], { timeout: 10_000 });
  cachedEncoder = qualifiedEncoderArgs(stdout);
  return cachedEncoder;
}

function ffmetadata(chapters: Chapter[], duration: number): string {
  // ffmetadata ends a line at \r as well as \n: drop control characters, then escape its special characters.
  // eslint-disable-next-line no-control-regex
  const escape = (text: string) => chapterTitle(text).replace(/([=;#\\])/g, "\\$1");
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
  signal?: AbortSignal,
  frameRate?: string,
  audioTracks?: AudioTrack[],
): Promise<void> {
  if (audioStreams > 1 && !audioTracks) requireSingleAudioTrack(audioStreams);
  const tracks = audioStreams > 1 ? qualifiedAudioTracks({audioStreams,audioTracks}, false) : [];
  if (tracks.length && keep.length>256) throw new Error('Track-preserving export plan exceeds its qualified bounds');
  const rate = rationalFrameRate(frameRate);
  if (frameRate !== undefined && !rate) throw new Error("Invalid export frame rate");
  const work = mkdtempSync(join(tmpdir(), "obs-mcp-trim-"));
  try {
    const metadata = join(work, "chapters.txt");
    writeFileSync(metadata, ffmetadata(chapters, duration));
    const withAudio = audioStreams > 0;
    const parts = keep.map(({ start, end }, index) => [
      `[0:v:0]trim=start=${start}:end=${end},setpts=PTS-STARTPTS[v${index}]`,
      ...(tracks.length ? tracks.map((_,track) => `[0:a:${track}]atrim=start=${start}:end=${end},asetpts=PTS-STARTPTS[a${index}_${track}]`) : withAudio ? [`[0:a:0]atrim=start=${start}:end=${end},asetpts=PTS-STARTPTS[a${index}]`] : []),
    ].join(";"));
    const inputs = keep.map((_span, index) => `[v${index}]${tracks.length ? tracks.map((_,track) => `[a${index}_${track}]`).join('') : withAudio ? `[a${index}]` : ""}`).join("");
    // Audio lengths can move concat seams off the source frame grid. Restore
    // the declared cadence after concatenation, before encoding/muxing.
    const filter = `${parts.join(";")};${inputs}concat=n=${keep.length}:v=1:a=${tracks.length || (withAudio ? 1 : 0)}${rate ? "[joined]" : "[v]"}${tracks.length ? tracks.map((_,index) => `[a${index}]`).join('') : withAudio ? "[a]" : ""}${rate ? `;[joined]fps=fps=${rate}[v]` : ""}`;
    const audioArgs = tracks.length ? [...tracks.flatMap((track,index) => ['-map',`[a${index}]`,`-ar:a:${index}`,String(track.sampleRate),`-ac:a:${index}`,String(track.channels),`-channel_layout:a:${index}`,track.channelLayout,`-metadata:s:a:${index}`,`language=${track.language}`,`-metadata:s:a:${index}`,`title=${track.title??''}`,`-metadata:s:a:${index}`,`handler_name=${track.title??''}`,`-disposition:a:${index}`,track.dispositions?.default===1?'default':'0']),'-c:a','aac','-b:a','160k'] : withAudio ? ['-map','[a]','-c:a','aac','-b:a','160k'] : [];
    await execFileAsync("ffmpeg", [
      "-hide_banner", "-nostats", "-y", ...localMediaInputOptions(), "-i", path, "-f", "ffmetadata", ...localMediaInputOptions("metadata"), "-i", metadata,
      "-filter_complex", filter, "-map", "[v]", ...audioArgs,
      "-map_metadata", "1", "-map_chapters", "1", ...(await videoEncoderArgs()), "-movflags", "+faststart", outputPath,
    ], { timeout: 600_000, maxBuffer: 32 * 1024 * 1024, signal });
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

/** Grabs one frame per time and tiles them into one JPEG. */
export async function contactSheet(path: string, times: number[], columns: number, width: number, signal?: AbortSignal): Promise<Buffer> {
  const work = mkdtempSync(join(tmpdir(), "obs-mcp-sheet-"));
  try {
    for (const [index, time] of times.entries()) {
      await execFileAsync("ffmpeg", [
        "-hide_banner", "-v", "error", "-y", "-ss", String(Math.max(0, time)), ...localMediaInputOptions(), "-i", path,
        "-frames:v", "1", "-vf", `scale=${width}:-2`, "-q:v", "4", join(work, `${String(index).padStart(3, "0")}.jpg`),
      ], { timeout: 30_000, signal });
    }
    const rows = Math.ceil(times.length / columns);
    const sheet = join(work, "sheet.jpg");
    await execFileAsync("ffmpeg", [
      "-hide_banner", "-v", "error", "-y", "-framerate", "1", ...localMediaInputOptions("images"), "-i", join(work, "%03d.jpg"),
      "-vf", `tile=${Math.min(columns, times.length)}x${rows}:padding=4:color=white`, "-frames:v", "1", "-q:v", "4", sheet,
    ], { timeout: 30_000, signal });
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

export function seamSampleTimes(seams: number[], duration: number): number[] {
  const last = Math.max(0, duration - Math.min(0.1, duration / 2));
  return seams.flatMap((seam) => [Math.max(0, seam - 0.5), Math.min(last, seam + 0.2)]).slice(0, MAX_SHEET_FRAMES);
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
        + "plan. Export requires 8-bit SDR input and a qualified H.264 encoder; validates metadata and full "
        + "decode before publishing a new copy. Needs ffmpeg; works on files in the OBS recording directory. "
        + "Preserves up to six qualified mono/stereo 44.1/48 kHz audio tracks with near-zero timestamps as lossy AAC in MP4; unsupported metadata is refused",
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
    async (args, ctx): Promise<CallToolResult> => {
      // Cancelling the tool call stops ffmpeg instead of leaving it running for minutes.
      const signal = (ctx as { mcpReq?: { signal?: AbortSignal } } | undefined)?.mcpReq?.signal;
      try {
        const path = await recordingFile(client, args.path);
        const info = await probe(path, signal);
        if (!Number.isFinite(info.duration)) return errorResult(`ffprobe could not read the length of ${path}`);
        if (args.apply) requireSdrInput(info);
        if (info.audioStreams > 1) qualifiedAudioTracks(info);
        const chapters = info.chapters.length > 0 ? info.chapters : takeLogChapters(path);
        const { idle, audible } = await detectIdle(path, info, args.minIdleSeconds, signal);
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
        if (info.audioStreams > 1 && extname(outputPath).toLowerCase() !== '.mp4') return errorResult('Trim failed: Track-preserving trim requires an MP4 output');
        // lstat also sees a dangling symlink, which ffmpeg would write through, possibly outside the directory.
        if (pathTaken(outputPath)) return errorResult(`${outputPath} already exists; choose another outputPath`);
        // Export under a temporary name so a failed or interrupted export leaves nothing that looks finished.
        // A unique name, so it can never be the recording being trimmed, which the finally block would delete.
        const partial = join(dirname(outputPath), `.${basename(outputPath)}.${process.pid}-${crypto.randomUUID().slice(0, 8)}.partial${extname(outputPath)}`);
        if (partial === path || pathTaken(partial)) return errorResult("Could not choose a temporary name for the export; try again");
        const times = seamSampleTimes(seams, plan.trimmedSeconds);
        const exported = await publishTrimmedExport(partial, outputPath, {
          encode: () => exportTrimmed(path, partial, keep, info.audioStreams, newChapters, plan.trimmedSeconds, signal, info.frameRate, info.audioTracks),
          verify: async () => {
            const result = await probe(partial, signal);
            validateTrimmedMetadata(result, { duration: plan.trimmedSeconds, width: info.width, height: info.height, audioStreams: info.audioStreams, ...(info.audioStreams>1?{audioTracks:info.audioTracks}:{}), chapters: newChapters, frameRate: info.frameRate });
            await strictDecode(partial, signal);
            return result;
          },
          ...(args.contactSheet && times.length > 0 ? { preview: () => contactSheet(outputPath, times, 4, 320, signal) } : {}),
        }, signal);
        const result = exported.verified;
        lines.push(`Exported ${outputPath}: ${round(result.duration)}s, ${result.chapters.length} chapter(s)`);

        const content: CallToolResult["content"] = [];
        if (exported.preview) {
          lines.push(`Contact sheet: frames around each seam (clamped near the end), in pairs, left to right: ${times.map((t) => `${round(t)}s`).join(", ")}`);
          content.push({ type: "image", data: exported.preview.toString("base64"), mimeType: "image/jpeg" });
        }
        const warnings = [exported.previewWarning, exported.cleanupWarning].filter((value): value is string => !!value);
        lines.push(...warnings);
        return {
          content: [{ type: "text", text: lines.join("\n") }, ...content],
          structuredContent: {
            ...plan, committed: true, outputPath, sha256: exported.sha256,
            outputDurationSeconds: round(result.duration), outputChapters: result.chapters.length,
            verification: { metadata: "pass", strictDecode: "pass", scope: info.audioStreams>1 ? "Cut-plan duration (0.25s tolerance), dimensions, ordered audio track descriptors, chapters and complete all-stream decode; lossy H.264/AAC transcode, editorial content requires review" : "Cut-plan duration (0.25s tolerance), dimensions, first audio track, chapters and complete decode; editorial content requires review" },
            format: { codec: result.codec, pixelFormat: result.pixelFormat, colorTransfer: result.colorTransfer ?? "unknown", colorPrimaries: result.colorPrimaries ?? "unknown", colorSpace: result.colorSpace ?? "unknown", colorFidelity: "unverified", ...(info.audioStreams>1?{audioTracks:result.audioTracks}:{}) },
            previewStatus: exported.preview ? "available" : exported.previewWarning ? "unavailable" : "not-requested",
            warnings,
          },
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
    async (args, ctx): Promise<CallToolResult> => {
      const signal = (ctx as { mcpReq?: { signal?: AbortSignal } } | undefined)?.mcpReq?.signal;
      try {
        const path = await recordingFile(client, args.path);
        const info = await probe(path, signal);
        const chapters = info.chapters.length > 0 ? info.chapters : takeLogChapters(path);
        const times = (args.times ?? (chapters.length > 0
          ? chapters.map(({ at }) => at + 1)
          : Array.from({ length: 8 }, (_unused, index) => ((index + 0.5) * info.duration) / 8)))
          .filter((time) => time < info.duration)
          .slice(0, MAX_SHEET_FRAMES);
        if (times.length === 0) return errorResult("No requested time falls inside the recording");
        const sheet = await contactSheet(path, times, args.columns, args.width, signal);
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

