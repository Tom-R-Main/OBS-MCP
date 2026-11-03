/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync, spawnSync } from "node:child_process";
import { join } from "node:path";
import {
  audioOnsets,
  flashTimes,
  measureSync,
  toneLevels,
  type LumaFrame,
  type SyncMeasurement,
} from "./media-analysis.js";

export const hasMediaTools = !spawnSync("ffmpeg", ["-version"]).error && !spawnSync("ffprobe", ["-version"]).error;

/** Stand-in sources: two distinct tones for a mic and a game, and a flash-and-beep clip for sync. */
export const MIC_TONE_HZ = 440;
export const GAME_TONE_HZ = 880;
export const SYNC_BEEP_HZ = 1000;
export const SCENARIO_FREQUENCIES = [MIC_TONE_HZ, GAME_TONE_HZ, SYNC_BEEP_HZ] as const;

export interface ScenarioFixtures {
  micTone: string;
  gameTone: string;
  syncClip: string;
}

const MAX_BUFFER = 256 * 1024 * 1024;

function ffmpeg(args: string[]): void {
  execFileSync("ffmpeg", ["-v", "error", "-y", ...args], { stdio: ["ignore", "ignore", "pipe"], timeout: 60_000 });
}

/**
 * Writes the fixtures into `dir`. The sync clip shows a white frame pair and
 * plays a 60 ms 1 kHz beep at the start of every second; otherwise it is black
 * and silent. `syncAudioDelayMs` shifts its beeps, to build known-bad inputs.
 */
export function generateScenarioFixtures(dir: string, { seconds = 10, syncAudioDelayMs = 0 } = {}): ScenarioFixtures {
  const micTone = join(dir, `tone-${MIC_TONE_HZ}.wav`);
  const gameTone = join(dir, `tone-${GAME_TONE_HZ}.wav`);
  const syncClip = join(dir, "sync-flash-beep.mov");
  for (const [file, frequency] of [[micTone, MIC_TONE_HZ], [gameTone, GAME_TONE_HZ]] as const) {
    ffmpeg(["-f", "lavfi", "-i", `sine=frequency=${frequency}:sample_rate=48000:duration=${seconds}`, "-af", "volume=4", "-c:a", "pcm_s16le", file]);
  }
  const delay = syncAudioDelayMs / 1000;
  ffmpeg([
    "-f", "lavfi", "-i", `color=black:s=320x180:r=30:d=${seconds}`,
    "-f", "lavfi", "-i",
    `aevalsrc='0.5*sin(2*PI*${SYNC_BEEP_HZ}*t)*lt(mod(t-${delay},1),0.06)*gte(t,${delay})':s=48000:d=${seconds}`,
    "-filter_complex", "[0:v]geq=lum='if(lt(mod(N,30),2),235,16)':cb=128:cr=128,format=yuv420p[v]",
    "-map", "[v]", "-map", "1:a",
    "-c:v", "mpeg4", "-q:v", "2", "-g", "30",
    "-c:a", "pcm_s16le",
    syncClip,
  ]);
  return { micTone, gameTone, syncClip };
}

export interface ProbedStream {
  index: number;
  codec_type: string;
  codec_name?: string;
  start_time?: string;
  duration?: string;
}

export function probeStreams(file: string): { streams: ProbedStream[]; formatName: string; duration: number } {
  const info = JSON.parse(execFileSync("ffprobe", [
    "-v", "error", "-show_streams", "-show_format", "-of", "json", file,
  ], { encoding: "utf8", timeout: 30_000 })) as { streams: ProbedStream[]; format: { format_name: string; duration?: string } };
  return { streams: info.streams, formatName: info.format.format_name, duration: Number(info.format.duration ?? 0) };
}

const AUDIO_RATE = 48_000;

/** The `audioIndex`th audio stream as mono float samples, with its container start time. */
export function decodeAudio(file: string, audioIndex: number): { samples: Float32Array; start: number } {
  const { streams } = probeStreams(file);
  const stream = streams.filter(({ codec_type }) => codec_type === "audio")[audioIndex];
  if (!stream) throw new Error(`${file} has no audio stream ${audioIndex}`);
  const raw = execFileSync("ffmpeg", [
    "-v", "error", "-i", file, "-map", `0:a:${audioIndex}`, "-ac", "1", "-ar", String(AUDIO_RATE), "-f", "f32le", "-",
  ], { maxBuffer: MAX_BUFFER, timeout: 60_000 });
  const samples = new Float32Array(raw.buffer, raw.byteOffset, Math.floor(raw.byteLength / 4));
  return { samples: Float32Array.from(samples), start: Number(stream.start_time ?? 0) };
}

/**
 * Luma of each frame's brightest cell in a 16x9 grid, at its container
 * presentation time. The brightest cell, not the frame mean, so a source that
 * covers only part of the canvas still registers.
 */
export function lumaTimeline(file: string): LumaFrame[] {
  const times = execFileSync("ffprobe", [
    "-v", "error", "-select_streams", "v:0", "-show_entries", "frame=best_effort_timestamp_time", "-of", "csv=p=0", file,
  ], { encoding: "utf8", maxBuffer: MAX_BUFFER, timeout: 60_000 }).trim().split("\n").map(Number);
  const width = 16;
  const height = 9;
  const pixels = execFileSync("ffmpeg", [
    "-v", "error", "-i", file, "-map", "0:v:0", "-fps_mode", "passthrough",
    "-vf", `scale=${width}:${height},format=gray`, "-f", "rawvideo", "-",
  ], { maxBuffer: MAX_BUFFER, timeout: 60_000 });
  const frameSize = width * height;
  const count = Math.min(times.length, Math.floor(pixels.byteLength / frameSize));
  return Array.from({ length: count }, (_, frame) => {
    let brightest = 0;
    for (let offset = frame * frameSize; offset < (frame + 1) * frameSize; offset += 1) brightest = Math.max(brightest, pixels[offset]!);
    return { time: times[frame]!, luma: brightest };
  });
}

export interface TrackReport {
  /** Decibels per scenario frequency. */
  levels: Record<number, number>;
  start: number;
}

export interface RecordingReport {
  formatName: string;
  duration: number;
  videoCodec: string | undefined;
  tracks: TrackReport[];
  /** Sync between the flash and beeps carried on the given audio track. */
  sync: SyncMeasurement | undefined;
  flashes: number;
  beeps: number;
}

/** Everything the multi-track scenario asserts, read from the file alone. */
export function inspectRecording(file: string, syncTrackIndex = 0): RecordingReport {
  const { streams, formatName, duration } = probeStreams(file);
  const audioCount = streams.filter(({ codec_type }) => codec_type === "audio").length;
  const tracks: TrackReport[] = [];
  let beepTimes: number[] = [];
  for (let index = 0; index < audioCount; index += 1) {
    const { samples, start } = decodeAudio(file, index);
    // Skip the first second: media sources need a moment to start.
    const settled = samples.subarray(Math.min(samples.length, AUDIO_RATE));
    tracks.push({ levels: toneLevels(settled, AUDIO_RATE, SCENARIO_FREQUENCIES), start });
    if (index === syncTrackIndex) beepTimes = audioOnsets(samples, AUDIO_RATE).map((time) => start + time);
  }
  const flashes = flashTimes(lumaTimeline(file));
  return {
    formatName,
    duration,
    videoCodec: streams.find(({ codec_type }) => codec_type === "video")?.codec_name,
    tracks,
    sync: measureSync(flashes, beepTimes),
    flashes: flashes.length,
    beeps: beepTimes.length,
  };
}
