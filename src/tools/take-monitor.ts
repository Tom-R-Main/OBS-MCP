/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { EventEmitter } from "node:events";
import { writeFile } from "node:fs/promises";
import { EventSubscription, type OBSWebSocketClient } from "../client.js";
import { logger } from "../logger.js";
import { decodePpm, isBlank, StillTracker } from "./frame-sample.js";
import { recordedTracks } from "./preflight.js";

type JsonObject = Record<string, unknown>;

export type TakeWarningKind = "audio" | "blank" | "frames" | "output" | "connection";
export type TakeWarning = { atSeconds: number; kind: TakeWarningKind; message: string };
export type TakeEvent = { atSeconds: number; type: string; detail?: string };
export type TakeSpan = { startSeconds: number; endSeconds: number };
export type InputLevel = {
  inputName: string;
  /** Loudest level after volume and mute, in dBFS. */
  peakDb: number;
  /** Loudest level before volume and mute, in dBFS. */
  inputPeakDb: number;
  /** Whether the input feeds a track the recording writes. */
  recorded: boolean;
};

export type TakeSummary = {
  running: boolean;
  startedAt: string;
  durationSeconds: number;
  expectSilent: boolean;
  silenceThresholdDb: number;
  warnings: TakeWarning[];
  events: TakeEvent[];
  chapters: { atSeconds: number; name: string }[];
  audio: InputLevel[];
  frames: { renderSkipped: number; renderTotal: number; outputSkipped: number; outputTotal: number };
  /** Stretches where the program output did not change, for trimming dead time. */
  stills: TakeSpan[];
  /** Stretches where the program output was black. */
  blanks: TakeSpan[];
  framesSampled: number;
  pictureChecks: "on" | "off";
};

export type TakeMonitorOptions = {
  expectSilent?: boolean;
  silenceThresholdDb?: number;
  sampleIntervalMs?: number;
  statsIntervalMs?: number;
  /** Shortest unchanged stretch reported in stills. */
  stillAfterSeconds?: number;
};

/** Wide enough to see streaming text in a 1080p screen capture; about 15 KB per sample. */
const SAMPLE_WIDTH = 96;
const MAX_EVENTS = 500;
const MAX_WARNINGS_PER_KIND = 20;
/** Two black samples in a row, so a fade or cut through black is not flagged. */
const BLANK_SAMPLES_TO_WARN = 2;

/** Events worth a line in the take log. */
const LOGGED_EVENTS: Record<string, (data: JsonObject) => string | undefined> = {
  RecordStateChanged: (data) => String(data.outputState ?? ""),
  RecordFileChanged: (data) => String(data.newOutputPath ?? ""),
  StreamStateChanged: (data) => String(data.outputState ?? ""),
  CurrentProgramSceneChanged: (data) => String(data.sceneName ?? ""),
  SceneItemEnableStateChanged: (data) => `${String(data.sceneName)} #${String(data.sceneItemId)} ${data.sceneItemEnabled ? "shown" : "hidden"}`,
  InputMuteStateChanged: (data) => `${String(data.inputName)} ${data.inputMuted ? "muted" : "unmuted"}`,
  InputAudioTracksChanged: (data) => String(data.inputName ?? ""),
};

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function toDb(multiplier: number): number {
  return multiplier > 0 ? Math.round(20 * Math.log10(multiplier) * 10) / 10 : -Infinity;
}

function round(seconds: number): number {
  return Math.round(seconds * 100) / 100;
}

/** Tells the obs://take/current resource that the running take changed. */
export const takeUpdates = new EventEmitter().setMaxListeners(0); // one listener per MCP session
const activeTakes = new WeakMap<OBSWebSocketClient, TakeMonitor>();

export function activeTake(client: OBSWebSocketClient): TakeMonitor | undefined {
  return activeTakes.get(client);
}

/**
 * Watches OBS while a take records: audio levels on recorded tracks from
 * InputVolumeMeters, skipped frames from GetStats, and a 96px sample of the
 * program output for black or unchanging picture. Start it once the
 * recording is confirmed, so its clock matches the file's.
 */
export class TakeMonitor {
  private readonly client: OBSWebSocketClient;
  private readonly options: Required<TakeMonitorOptions>;
  private startedAtMs = 0;
  private stoppedAtMs: number | null = null;
  private stopExpected = false;
  private readonly warnings: TakeWarning[] = [];
  private readonly events: TakeEvent[] = [];
  private readonly chapters: { atSeconds: number; name: string }[] = [];
  private readonly levels = new Map<string, InputLevel>();
  private readonly audibleNow = new Set<string>();
  private readonly inputTracks = new Map<string, JsonObject>();
  private recordTracks: number[] = [1];
  private readonly frames = { renderSkipped: 0, renderTotal: 0, outputSkipped: 0, outputTotal: 0 };
  private lastStats: JsonObject | null = null;
  private lastFrameWarningMs = new Map<string, number>();
  private readonly stills: TakeSpan[] = [];
  private readonly blanks: TakeSpan[] = [];
  private stillTracker: StillTracker;
  private blankSince: number | null = null;
  private blankRun = 0;
  private framesSampled = 0;
  private pictureChecks: "on" | "off" = "off";
  private programScene: string | null = null;
  private recordingPath: string | null = null;
  private readonly timers: NodeJS.Timeout[] = [];
  private readonly busy = new Set<string>();
  private releaseMeters: (() => void) | null = null;
  private readonly listeners: [string, (...args: unknown[]) => void][] = [];

  constructor(client: OBSWebSocketClient, options: TakeMonitorOptions = {}) {
    this.client = client;
    this.options = {
      expectSilent: options.expectSilent ?? false,
      silenceThresholdDb: options.silenceThresholdDb ?? -60,
      sampleIntervalMs: options.sampleIntervalMs ?? 1_000,
      statsIntervalMs: options.statsIntervalMs ?? 2_000,
      stillAfterSeconds: options.stillAfterSeconds ?? 3,
    };
    this.stillTracker = this.newStillTracker();
  }

  /** Seconds since the monitor started. */
  now(): number {
    return round(((this.stoppedAtMs ?? Date.now()) - this.startedAtMs) / 1000);
  }

  async start(): Promise<void> {
    this.startedAtMs = Date.now();
    activeTakes.set(this.client, this);

    this.listen("InputVolumeMeters", (data) => this.onMeters(data));
    this.listen("InputAudioTracksChanged", (data) => {
      if (typeof data.inputName === "string" && isObject(data.inputAudioTracks)) {
        this.inputTracks.set(data.inputName, data.inputAudioTracks);
      }
    });
    this.listen("CurrentProgramSceneChanged", (data) => {
      if (typeof data.sceneName === "string") this.programScene = data.sceneName;
      this.closeStill(this.now());
    });
    this.listen("RecordStateChanged", (data) => {
      if (typeof data.outputPath === "string" && data.outputPath) this.recordingPath = data.outputPath;
      if (data.outputActive === false && !this.stopExpected && data.outputState === "OBS_WEBSOCKET_OUTPUT_STOPPED") {
        this.warn("output", "The recording stopped before it was asked to");
      }
    });
    for (const [event, describe] of Object.entries(LOGGED_EVENTS)) {
      this.listen(event, (data) => this.log(event, describe(data)));
    }
    const onDisconnect = () => this.warn("connection", "Lost the connection to OBS; checks paused until it reconnects");
    this.client.on("disconnected", onDisconnect);
    this.listeners.push(["disconnected", onDisconnect]);

    await Promise.all([this.loadTracks(), this.loadProgramScene(), this.sampleStats().catch(() => undefined)]);
    this.releaseMeters = this.client.subscribeHighVolume(EventSubscription.InputVolumeMeters);

    const formats = this.client.getConnectionStatus().versionInfo?.supportedImageFormats;
    this.pictureChecks = Array.isArray(formats) && formats.includes("ppm") ? "on" : "off";
    if (this.pictureChecks === "on") this.every(this.options.sampleIntervalMs, "picture", () => this.samplePicture());
    else this.log("picture-checks-off", "OBS cannot return PPM screenshots");
    this.every(this.options.statsIntervalMs, "stats", () => this.sampleStats());
    this.changed();
  }

  mark(name: string): void {
    this.chapters.push({ atSeconds: this.now(), name });
    this.changed();
  }

  /** The file OBS last reported for this take, from RecordStateChanged. */
  outputPath(): string | null {
    return this.recordingPath;
  }

  /** Call before stopping the recording, so the stop is not reported as a problem. */
  expectStop(): void {
    this.stopExpected = true;
  }

  async stop(): Promise<TakeSummary> {
    if (this.stoppedAtMs === null) {
      this.stopExpected = true;
      for (const timer of this.timers) clearInterval(timer);
      this.timers.length = 0;
      this.releaseMeters?.();
      this.releaseMeters = null;
      for (const [event, listener] of this.listeners) this.client.off(event, listener);
      this.listeners.length = 0;
      await this.sampleStats().catch(() => undefined);
      this.stoppedAtMs = Date.now();
      const end = this.now();
      this.closeStill(end);
      this.closeBlank(end);
      if (activeTakes.get(this.client) === this) activeTakes.delete(this.client);
      this.changed();
    }
    return this.summary();
  }

  summary(): TakeSummary {
    return {
      running: this.stoppedAtMs === null,
      startedAt: new Date(this.startedAtMs).toISOString(),
      durationSeconds: this.now(),
      expectSilent: this.options.expectSilent,
      silenceThresholdDb: this.options.silenceThresholdDb,
      warnings: [...this.warnings],
      events: [...this.events],
      chapters: [...this.chapters],
      audio: [...this.levels.values()].sort((a, b) => b.peakDb - a.peakDb),
      frames: { ...this.frames },
      stills: [
        ...this.stills,
        ...this.stillTracker.spans.map(({ start, end }) => ({ startSeconds: round(start), endSeconds: round(end) })),
        ...this.openSpan(this.stillTracker.stillSince()),
      ],
      blanks: [...this.blanks, ...this.openSpan(this.blankSince)],
      framesSampled: this.framesSampled,
      pictureChecks: this.pictureChecks,
    };
  }

  private openSpan(since: number | null): TakeSpan[] {
    return since === null || this.stoppedAtMs !== null ? [] : [{ startSeconds: since, endSeconds: this.now() }];
  }

  private listen(event: string, handler: (data: JsonObject) => void): void {
    const listener = (data: unknown) => handler(isObject(data) ? data : {});
    this.client.on(event, listener);
    this.listeners.push([event, listener as (...args: unknown[]) => void]);
  }

  private every(intervalMs: number, name: string, task: () => Promise<void>): void {
    const timer = setInterval(() => {
      if (this.busy.has(name)) return;
      this.busy.add(name);
      task().catch((error: unknown) => {
        logger.debug(`Take monitor ${name} check failed: ${error instanceof Error ? error.message : String(error)}`);
      }).finally(() => this.busy.delete(name));
    }, intervalMs);
    timer.unref();
    this.timers.push(timer);
  }

  private changed(): void {
    takeUpdates.emit("update");
  }

  private log(type: string, detail?: string): void {
    if (this.events.length >= MAX_EVENTS) return;
    this.events.push({ atSeconds: this.now(), type, ...(detail ? { detail } : {}) });
  }

  private warn(kind: TakeWarningKind, message: string): void {
    if (this.warnings.filter((warning) => warning.kind === kind).length >= MAX_WARNINGS_PER_KIND) return;
    this.warnings.push({ atSeconds: this.now(), kind, message });
    this.changed();
  }

  private async loadTracks(): Promise<void> {
    try {
      this.recordTracks = await recordedTracks(this.client);
      const { inputs } = await this.client.sendRequest("GetInputList") as { inputs?: unknown[] };
      const names = (inputs ?? []).filter(isObject).map(({ inputName }) => inputName).filter((name): name is string => typeof name === "string");
      const results = await this.client.sendBatch(
        names.map((inputName) => ({ requestType: "GetInputAudioTracks", requestData: { inputName } })),
      );
      names.forEach((name, index) => {
        const result = results[index];
        if (result?.ok && isObject(result.responseData) && isObject(result.responseData.inputAudioTracks)) {
          this.inputTracks.set(name, result.responseData.inputAudioTracks);
        }
      });
    } catch (error) {
      // Unknown tracks count as recorded, which errs toward warning.
      this.log("tracks-unknown", error instanceof Error ? error.message : String(error));
    }
  }

  private async loadProgramScene(): Promise<void> {
    const program = await this.client.sendRequest("GetCurrentProgramScene").catch(() => ({})) as JsonObject;
    const name = program.currentProgramSceneName ?? program.sceneName;
    this.programScene = typeof name === "string" ? name : null;
  }

  private isRecorded(inputName: string): boolean {
    const tracks = this.inputTracks.get(inputName);
    return !tracks || this.recordTracks.some((track) => tracks[String(track)] === true);
  }

  private onMeters(data: JsonObject): void {
    if (this.stoppedAtMs !== null || !Array.isArray(data.inputs)) return;
    for (const input of data.inputs) {
      if (!isObject(input) || typeof input.inputName !== "string" || !Array.isArray(input.inputLevelsMul)) continue;
      // Each channel is [magnitude, peak, input peak]; peak is after volume and mute.
      let peak = 0;
      let inputPeak = 0;
      for (const channel of input.inputLevelsMul) {
        if (!Array.isArray(channel)) continue;
        peak = Math.max(peak, Number(channel[1]) || 0);
        inputPeak = Math.max(inputPeak, Number(channel[2]) || 0);
      }
      const name = input.inputName;
      const recorded = this.isRecorded(name);
      const peakDb = toDb(peak);
      const level = this.levels.get(name);
      this.levels.set(name, {
        inputName: name,
        peakDb: Math.max(level?.peakDb ?? -Infinity, peakDb),
        inputPeakDb: Math.max(level?.inputPeakDb ?? -Infinity, toDb(inputPeak)),
        recorded,
      });

      const audible = recorded && peakDb > this.options.silenceThresholdDb;
      if (audible && !this.audibleNow.has(name)) {
        this.audibleNow.add(name);
        this.log("audio-start", `${name} at ${peakDb} dB`);
        if (this.options.expectSilent) {
          this.warn("audio", `${name} is audible in the recording at ${peakDb} dB (expected silence). `
            + "Mute it or clear its recorded tracks");
        }
      } else if (!audible && this.audibleNow.has(name)) {
        this.audibleNow.delete(name);
        this.log("audio-end", name);
      }
    }
  }

  private async sampleStats(): Promise<void> {
    const stats = await this.client.sendRequest("GetStats") as JsonObject;
    const previous = this.lastStats;
    this.lastStats = stats;
    if (!previous) return;

    const delta = (field: string) => Math.max(0, Number(stats[field] ?? 0) - Number(previous[field] ?? 0));
    const renderSkipped = delta("renderSkippedFrames");
    const outputSkipped = delta("outputSkippedFrames");
    this.frames.renderSkipped += renderSkipped;
    this.frames.renderTotal += delta("renderTotalFrames");
    this.frames.outputSkipped += outputSkipped;
    this.frames.outputTotal += delta("outputTotalFrames");

    const nowMs = Date.now();
    const report = (key: string, skipped: number, message: string) => {
      if (skipped === 0 || nowMs - (this.lastFrameWarningMs.get(key) ?? -Infinity) < 10_000) return;
      this.lastFrameWarningMs.set(key, nowMs);
      this.warn("frames", message);
    };
    report("render", renderSkipped, `OBS missed ${renderSkipped} frame(s) while rendering; the GPU or CPU is overloaded`);
    report("output", outputSkipped, `The encoder skipped ${outputSkipped} frame(s); lower the encoder preset or resolution`);
  }

  private async samplePicture(): Promise<void> {
    if (!this.programScene) return;
    const response = await this.client.sendRequest("GetSourceScreenshot", {
      sourceName: this.programScene,
      imageFormat: "ppm",
      imageWidth: SAMPLE_WIDTH,
    }) as JsonObject;
    if (typeof response.imageData !== "string" || this.stoppedAtMs !== null) return;
    const sample = decodePpm(response.imageData);
    const at = this.now();
    this.framesSampled += 1;

    if (isBlank(sample)) {
      this.blankRun += 1;
      if (this.blankRun === BLANK_SAMPLES_TO_WARN) {
        this.blankSince = round(at - this.options.sampleIntervalMs / 1000);
        this.warn("blank", `The program output (${this.programScene}) is black. A capture may have lost its window `
          + "or screen recording permission");
      }
    } else {
      this.blankRun = 0;
      this.closeBlank(at);
    }

    this.stillTracker.push(at, sample.luma);
  }

  /** The same rule as obs-trim-take, so a spinner or shimmer still counts as still. */
  private newStillTracker(): StillTracker {
    return new StillTracker(this.options.stillAfterSeconds);
  }

  /** Ends the current still stretch, e.g. on a scene switch, and starts tracking afresh. */
  private closeStill(at: number): void {
    for (const { start, end } of this.stillTracker.finish(at)) this.stills.push({ startSeconds: round(start), endSeconds: round(end) });
    this.stillTracker = this.newStillTracker();
  }

  private closeBlank(at: number): void {
    if (this.blankSince !== null) this.blanks.push({ startSeconds: this.blankSince, endSeconds: at });
    this.blankSince = null;
  }
}

/** "take.mp4" -> "take.take.json", next to the recording. */
export function takeLogPath(recordingPath: string): string {
  const dot = recordingPath.lastIndexOf(".");
  const slash = Math.max(recordingPath.lastIndexOf("/"), recordingPath.lastIndexOf("\\"));
  return `${dot > slash ? recordingPath.slice(0, dot) : recordingPath}.take.json`;
}

/** Writes the take log next to a local recording; returns its path, or null if it could not. */
export async function writeTakeLog(recordingPath: string, summary: TakeSummary & JsonObject): Promise<string | null> {
  const path = takeLogPath(recordingPath);
  try {
    await writeFile(path, `${JSON.stringify({ recording: recordingPath, ...summary }, (_key, value: unknown) => (
      value === -Infinity ? null : value
    ), 2)}\n`);
    return path;
  } catch (error) {
    logger.error(`Could not write the take log ${path}: ${error instanceof Error ? error.message : String(error)}`);
    return null;
  }
}

/** One-line summary of a finished take for tool results. */
export function describeTake(summary: TakeSummary): string[] {
  const lines: string[] = [];
  const loudest = summary.audio.filter(({ recorded }) => recorded)[0];
  lines.push(loudest && loudest.peakDb > summary.silenceThresholdDb
    ? `Monitor: loudest recorded input ${loudest.inputName} at ${loudest.peakDb} dB`
    : "Monitor: no recorded input rose above "
      + `${summary.silenceThresholdDb} dB`);
  if (summary.frames.renderSkipped + summary.frames.outputSkipped > 0) {
    lines.push(`Monitor: ${summary.frames.renderSkipped} frame(s) missed rendering, ${summary.frames.outputSkipped} skipped encoding`);
  }
  if (summary.pictureChecks === "on") {
    const still = summary.stills.reduce((total, span) => total + span.endSeconds - span.startSeconds, 0);
    lines.push(`Monitor: ${summary.framesSampled} picture sample(s), ${summary.blanks.length} black stretch(es), `
      + `${round(still)}s unchanged`);
  }
  for (const warning of summary.warnings) lines.push(`Warning at ${warning.atSeconds}s: ${warning.message}`);
  return lines;
}
