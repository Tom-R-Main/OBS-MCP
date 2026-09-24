/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { RECORD_OUTPUT, startOutputAndConfirm } from "./output-start.js";
import { runPreflight } from "./preflight.js";
import { READ_ONLY_TOOL } from "./request-tool.js";
import { callerName, claim, release } from "./lease.js";
import { describeTake, TakeMonitor, writeTakeLog, type TakeSummary } from "./take-monitor.js";

type JsonObject = Record<string, unknown>;

const execFileAsync = promisify(execFile);

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

/** JSON has no -Infinity; silence reports as null. */
const finite = (value: number): number | null => (Number.isFinite(value) ? value : null);

export type ClipCheck = {
  durationSeconds?: number;
  audioStreams?: number;
  maxVolumeDb?: number;
  skipped?: string;
};

/**
 * Inspects a finished recording with ffprobe (and ffmpeg for loudness) when
 * they are on PATH and OBS writes to this machine. Never throws.
 */
export async function inspectRecording(outputPath: string, measureLoudness: boolean): Promise<ClipCheck> {
  let probe: JsonObject;
  try {
    const { stdout } = await execFileAsync("ffprobe", [
      "-v", "error", "-print_format", "json", "-show_format", "-show_streams", outputPath,
    ], { timeout: 15_000 });
    probe = JSON.parse(stdout) as JsonObject;
  } catch (error) {
    const code = isObject(error) ? error.code : undefined;
    return { skipped: code === "ENOENT" ? "ffprobe is not installed" : `ffprobe failed: ${errorMessage(error)}` };
  }

  const streams = Array.isArray(probe.streams) ? probe.streams.filter(isObject) : [];
  const format = isObject(probe.format) ? probe.format : {};
  const check: ClipCheck = {
    durationSeconds: Number(format.duration),
    audioStreams: streams.filter((stream) => stream.codec_type === "audio").length,
  };
  if (!measureLoudness || check.audioStreams === 0) return check;

  try {
    const { stderr } = await execFileAsync("ffmpeg", [
      "-hide_banner", "-nostats", "-i", outputPath, "-map", "0:a:0", "-af", "volumedetect", "-f", "null", "-",
    ], { timeout: 60_000 });
    const match = /max_volume:\s*(-?[\d.]+|-inf) dB/.exec(stderr);
    if (match?.[1]) check.maxVolumeDb = match[1] === "-inf" ? -Infinity : Number(match[1]);
  } catch (error) {
    check.skipped = `ffmpeg loudness check failed: ${errorMessage(error)}`;
  }
  return check;
}

/** A recording started through this server and watched until it stops. */
export type Take = {
  id: string;
  monitor: TakeMonitor;
  expectSilent: boolean;
  /** OBS writes to this machine, so the file can be inspected. */
  local: boolean;
  /** Refuse changes that break a running recording. */
  locked: boolean;
  chapterNotes: string[];
  /** The client that holds control of OBS because it started this take. */
  leaseHolder?: string;
};

const takes = new WeakMap<OBSWebSocketClient, Take>();

export function currentTake(client: OBSWebSocketClient): Take | undefined {
  return takes.get(client);
}

export type StartTakeOptions = {
  expectSilent: boolean;
  minFreeDiskMb: number;
  lock: boolean;
  chapter?: string;
};

/** Preflights, starts recording, waits for OBS to confirm, and starts the monitor. */
export async function startTake(
  client: OBSWebSocketClient,
  options: StartTakeOptions,
): Promise<{ ok: true; take: Take } | { ok: false; result: CallToolResult }> {
  const refuse = (result: CallToolResult) => ({ ok: false as const, result });
  const running = currentTake(client);
  if (running) return refuse(errorResult(`Take ${running.id} is already recording; stop it with obs-take-stop first`));

  const preflight = await runPreflight(client, { minFreeDiskMb: options.minFreeDiskMb, expectSilent: options.expectSilent });
  if (!preflight.ready) {
    const failures = preflight.checks.filter(({ status }) => status === "fail");
    return refuse(errorResult(
      `Not recording; preflight failed:\n${failures.map(({ id, message }) => `- ${id}: ${message}`).join("\n")}`,
    ));
  }

  const started = await startOutputAndConfirm(client, RECORD_OUTPUT);
  if (started.isError) return refuse(started);

  const take: Take = {
    id: crypto.randomUUID().slice(0, 8),
    monitor: new TakeMonitor(client, { expectSilent: options.expectSilent }),
    expectSilent: options.expectSilent,
    local: preflight.checks.some(({ id, status }) => id === "record-directory" && status === "pass"),
    locked: options.lock,
    chapterNotes: [],
  };
  takes.set(client, take);
  await take.monitor.start();
  if (options.chapter) await markTake(client, take, options.chapter);
  return { ok: true, take };
}

/** Adds a chapter to the file and the take log. A rejected chapter does not stop the take. */
export async function markTake(client: OBSWebSocketClient, take: Take, name: string): Promise<{ atSeconds: number; note?: string }> {
  const atSeconds = take.monitor.now();
  take.monitor.mark(name);
  try {
    await client.sendRequest("CreateRecordChapter", { chapterName: name });
    return { atSeconds };
  } catch (error) {
    // Chapters need the Hybrid MP4 or Hybrid MOV format.
    const note = `Chapter "${name}" not added: ${errorMessage(error)}`;
    take.chapterNotes.push(note);
    return { atSeconds, note };
  }
}

/**
 * Stops the recording, stops the monitor, checks the file, and writes the
 * take log. Never throws, so callers can stop from a finally block.
 */
export async function stopTake(
  client: OBSWebSocketClient,
  take: Take,
  options: { requestedSeconds?: number } = {},
): Promise<CallToolResult> {
  take.monitor.expectStop();
  let outputPath: string | undefined;
  let stopError: string | undefined;
  try {
    const stopped: unknown = await client.sendRequest("StopRecord");
    if (isObject(stopped) && typeof stopped.outputPath === "string") outputPath = stopped.outputPath;
  } catch (error) {
    // The recording may have stopped already; OBS reported the file when it did.
    stopError = errorMessage(error);
  }
  const summary = await take.monitor.stop();
  if (takes.get(client) === take) takes.delete(client);
  outputPath ??= take.monitor.outputPath() ?? undefined;

  if (!outputPath) {
    return errorResult(
      `Take ${take.id} ran ${summary.durationSeconds}s, but OBS did not confirm the stop or report the file`
        + `${stopError ? ` (${stopError})` : ""}. Check obs-get-record-status`,
    );
  }
  return takeResult(take, summary, outputPath, options.requestedSeconds);
}

async function takeResult(
  take: Take,
  summary: TakeSummary,
  outputPath: string,
  requestedSeconds: number | undefined,
): Promise<CallToolResult> {
  const inspection = take.local
    ? await inspectRecording(outputPath, take.expectSilent)
    : { skipped: "OBS is remote, so the file was not inspected" };
  const takeLog = take.local ? await writeTakeLog(outputPath, summary) : null;

  const problems: string[] = [];
  for (const warning of summary.warnings) {
    if (warning.kind === "blank" || warning.kind === "output" || (warning.kind === "audio" && take.expectSilent)) {
      problems.push(`At ${warning.atSeconds}s: ${warning.message}`);
    }
  }
  const expectedSeconds = requestedSeconds ?? summary.durationSeconds;
  if (inspection.durationSeconds !== undefined && inspection.durationSeconds < expectedSeconds - 2) {
    problems.push(requestedSeconds !== undefined
      ? `The file is ${inspection.durationSeconds.toFixed(1)}s, shorter than the requested ${requestedSeconds}s`
      : `The file is ${inspection.durationSeconds.toFixed(1)}s, but the take ran ${summary.durationSeconds}s`);
  }
  if (take.expectSilent && inspection.maxVolumeDb !== undefined && inspection.maxVolumeDb > -60) {
    problems.push(`Expected silence, but the audio peaks at ${inspection.maxVolumeDb} dB`);
  }

  const lines = [
    `Recorded ${requestedSeconds ?? summary.durationSeconds}s to ${outputPath}`,
    inspection.skipped
      ? `Not verified: ${inspection.skipped}`
      : `Verified: ${inspection.durationSeconds?.toFixed(1)}s, ${inspection.audioStreams} audio stream(s)`
        + (inspection.maxVolumeDb !== undefined ? `, peak ${inspection.maxVolumeDb} dB` : ""),
    ...describeTake(summary).filter((line) => !line.startsWith("Warning")),
    ...(summary.chapters.length > 0
      ? [`Chapters: ${summary.chapters.map(({ atSeconds, name }) => `${atSeconds}s ${name}`).join(", ")}`]
      : []),
    ...(takeLog ? [`Take log: ${takeLog}`] : []),
    ...take.chapterNotes,
    ...problems.map((problem) => `Problem: ${problem}`),
  ];
  return {
    content: [{ type: "text", text: lines.join("\n") }],
    structuredContent: {
      takeId: take.id,
      outputPath,
      ...inspection,
      ...(inspection.maxVolumeDb === -Infinity ? { maxVolumeDb: null } : {}),
      chapterNotes: take.chapterNotes,
      problems,
      takeLog,
      take: {
        ...summary,
        audio: summary.audio.map((level) => ({ ...level, peakDb: finite(level.peakDb), inputPeakDb: finite(level.inputPeakDb) })),
      },
    },
    ...(problems.length > 0 ? { isError: true } : {}),
  };
}

/** Tools that change what a running recording writes, or where. Stopping is never locked. */
export const LOCKED_TOOLS: Readonly<Record<string, string>> = {
  "obs-set-current-profile": "switch profiles",
  "obs-create-profile": "create and switch to a profile",
  "obs-remove-profile": "remove a profile",
  "obs-set-current-scene-collection": "switch scene collections",
  "obs-create-scene-collection": "create and switch to a scene collection",
  "obs-set-profile-parameter": "change profile settings",
  "obs-set-record-directory": "change the recording directory",
  "obs-set-video-settings": "change the canvas and frame rate",
  "obs-set-stream-service-settings": "change the stream service",
};

const LOCKED_REQUESTS = new Set([
  "SetCurrentProfile",
  "CreateProfile",
  "RemoveProfile",
  "SetCurrentSceneCollection",
  "CreateSceneCollection",
  "SetProfileParameter",
  "SetRecordDirectory",
  "SetVideoSettings",
  "SetStreamServiceSettings",
]);

function lockedAction(name: string, args: unknown): string | undefined {
  const fixed = LOCKED_TOOLS[name];
  if (fixed) return fixed;
  if (!isObject(args)) return undefined;
  const requestTypes = name === "obs-call-request"
    ? [args.requestType]
    : name === "obs-batch" && Array.isArray(args.requests)
      ? args.requests.map((request) => (isObject(request) ? request.requestType : undefined))
      : [];
  const locked = requestTypes.filter((type): type is string => typeof type === "string" && LOCKED_REQUESTS.has(type));
  return locked.length > 0 ? `send ${locked.join(", ")}` : undefined;
}

/**
 * Wraps tool registration so that, while a locked take records, tools that
 * would change the profile, scene collection, or output settings refuse to
 * run. Other McpServer methods stay bound to the real server.
 */
export function withTakeLock(server: McpServer, client: OBSWebSocketClient): McpServer {
  return new Proxy(server, {
    get(target, property) {
      if (property === "registerTool") {
        const registerTool = Reflect.get(target, property, target) as (...args: unknown[]) => unknown;
        return (name: string, config: unknown, callback: (...args: unknown[]) => unknown) => {
          const guarded = async (...callbackArgs: unknown[]) => {
            const take = currentTake(client);
            // Tools without an input schema receive only the context.
            const action = take?.locked ? lockedAction(name, callbackArgs.length > 1 ? callbackArgs[0] : undefined) : undefined;
            if (take && action) {
              return errorResult(
                `Take ${take.id} is recording, and changing this now would ${action} under it. `
                  + "Stop the take with obs-take-stop first, or start takes with lock: false",
              );
            }
            return callback(...callbackArgs);
          };
          return Reflect.apply(registerTool, target, [name, config, guarded]);
        };
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

function noTake(): CallToolResult {
  return errorResult("No take is recording; start one with obs-take-start");
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-take-start",
    {
      title: "Start Take",
      description: "Start a watched recording of any length: runs obs-preflight, starts recording and waits for "
        + "OBS to confirm, then watches audio, frames, and picture until obs-take-stop. Mark steps with "
        + "obs-take-mark and check progress with obs-take-status. By default, profile, scene collection, and "
        + "output setting changes are refused until the take stops",
      inputSchema: z.object({
        expectSilent: z.boolean().default(false)
          .describe("Refuse to start with audible inputs, and treat audio during the take as a problem"),
        minFreeDiskMb: z.number().int().nonnegative().default(1024).describe("Minimum free disk space in MB"),
        lock: z.boolean().default(true).describe("Refuse profile, scene collection, and output setting changes until the take stops"),
        chapter: z.string().min(1).optional().describe("Name of a chapter to add at the start"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async (args, ctx) => {
      try {
        const started = await startTake(client, args);
        if (!started.ok) return started.result;
        const { take } = started;
        // Other clients sharing this server cannot change OBS until the take stops.
        const holder = callerName(server, ctx);
        if (claim(client, holder, 240, `recording take ${take.id}`).ok) take.leaseHolder = holder;
        const status = take.monitor.summary();
        return {
          content: [{
            type: "text",
            text: `Take ${take.id} is recording${take.locked ? " (profile and output changes locked)" : ""}. `
              + `Watching audio${status.pictureChecks === "on" ? ", frames, and picture" : " and frames"}; `
              + "stop it with obs-take-stop",
          }],
          structuredContent: { takeId: take.id, locked: take.locked, pictureChecks: status.pictureChecks, chapterNotes: take.chapterNotes },
        };
      } catch (error) {
        return errorResult(`Start Take failed: ${errorMessage(error)}`);
      }
    },
  );

  server.registerTool(
    "obs-take-mark",
    {
      title: "Mark Take",
      description: "Add a named chapter to the running take, in the file (Hybrid MP4/MOV) and in the take log",
      inputSchema: z.object({ name: z.string().min(1).describe("Chapter name") }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async ({ name }) => {
      const take = currentTake(client);
      if (!take) return noTake();
      const { atSeconds, note } = await markTake(client, take, name);
      return {
        content: [{ type: "text", text: note ?? `Marked "${name}" at ${atSeconds}s` }],
        structuredContent: { takeId: take.id, atSeconds, name, ...(note ? { note } : {}) },
      };
    },
  );

  server.registerTool(
    "obs-take-status",
    {
      title: "Take Status",
      description: "Report the running take: length, file size, loudest recorded input, skipped frames, black or "
        + "unchanging picture, chapters, and warnings so far",
      inputSchema: z.object({}),
      annotations: READ_ONLY_TOOL,
    },
    async () => {
      const take = currentTake(client);
      if (!take) return noTake();
      const summary = take.monitor.summary();
      const record = await client.sendRequest("GetRecordStatus").catch(() => ({})) as JsonObject;
      const lines = [
        `Take ${take.id}: ${summary.durationSeconds}s`
          + (typeof record.outputBytes === "number" ? `, ${(record.outputBytes / 1_048_576).toFixed(1)} MB` : "")
          + (record.outputPaused === true ? ", paused" : ""),
        ...describeTake(summary),
      ];
      return {
        content: [{ type: "text", text: lines.join("\n") }],
        structuredContent: {
          takeId: take.id,
          record,
          take: {
            ...summary,
            audio: summary.audio.map((level) => ({ ...level, peakDb: finite(level.peakDb), inputPeakDb: finite(level.inputPeakDb) })),
          },
        },
      };
    },
  );

  server.registerTool(
    "obs-take-stop",
    {
      title: "Stop Take",
      description: "Stop the running take: stops recording, then checks the file's length and audio with "
        + "ffprobe/ffmpeg when available, reports what the monitor saw, and writes <name>.take.json next to a "
        + "local recording",
      inputSchema: z.object({}),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
    },
    async () => {
      const take = currentTake(client);
      if (!take) return noTake();
      const result = await stopTake(client, take);
      if (take.leaseHolder) release(client, take.leaseHolder);
      return result;
    },
  );
}
