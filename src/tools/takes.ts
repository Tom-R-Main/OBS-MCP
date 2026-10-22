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
import { DEMO_REQUIRED_CHECKS, runPreflight } from "./preflight.js";
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
  videoStreams?: number;
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
  const duration = Number(format.duration);
  if (!Number.isFinite(duration)) return { skipped: "ffprobe could not read the file's length; it may be incomplete" };
  const check: ClipCheck = {
    durationSeconds: duration,
    videoStreams: streams.filter((stream) => stream.codec_type === "video").length,
    audioStreams: streams.filter((stream) => stream.codec_type === "audio").length,
  };
  if (!measureLoudness || check.audioStreams === 0) return check;

  // Every audio track, since a recording can write several: the loudest one counts.
  try {
    for (let track = 0; track < (check.audioStreams ?? 0); track += 1) {
      const { stderr } = await execFileAsync("ffmpeg", [
        "-hide_banner", "-nostats", "-i", outputPath, "-map", `0:a:${track}`, "-af", "volumedetect", "-f", "null", "-",
      ], { timeout: 60_000 });
      const match = /max_volume:\s*(-?[\d.]+|-inf) dB/.exec(stderr);
      if (!match?.[1]) {
        check.skipped = `ffmpeg did not report loudness for audio track ${track}`;
        continue;
      }
      const level = match[1] === "-inf" ? -Infinity : Number(match[1]);
      check.maxVolumeDb = Math.max(check.maxVolumeDb ?? -Infinity, level);
    }
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
  deadline?: NodeJS.Timeout;
  maxDurationSeconds?: number;
  ownershipLost?: string;
  stoppedObserved?: boolean;
  outputPath?: string;
  disposeOwnership?: () => void;
  stopPromise?: Promise<CallToolResult>;
  stopResult?: CallToolResult;
};

const takes = new WeakMap<OBSWebSocketClient, Take>();
const completedTakes = new WeakMap<OBSWebSocketClient, CallToolResult>();
/** Clients with a take being started: preflight and the start take time, and a second start must not slip in. */
const starting = new WeakSet<OBSWebSocketClient>();

export function currentTake(client: OBSWebSocketClient): Take | undefined {
  return takes.get(client);
}

export type StartTakeOptions = {
  expectSilent: boolean;
  minFreeDiskMb: number;
  lock: boolean;
  chapter?: string;
  maxDurationSeconds?: number;
  requireVerifiedPreflight?: boolean;
};

/** Preflights, starts recording, waits for OBS to confirm, and starts the monitor. */
export async function startTake(
  client: OBSWebSocketClient,
  options: StartTakeOptions,
): Promise<{ ok: true; take: Take } | { ok: false; result: CallToolResult }> {
  const refuse = (result: CallToolResult) => ({ ok: false as const, result });
  const running = currentTake(client);
  if (running) return refuse(errorResult(`Take ${running.id} is already recording; stop it with obs-take-stop first`));
  if (starting.has(client)) return refuse(errorResult("Another take is being started; wait for it, then stop it with obs-take-stop"));
  starting.add(client);
  completedTakes.delete(client);
  try {
    return await startTakeReserved(client, options);
  } finally {
    starting.delete(client);
  }
}

async function startTakeReserved(
  client: OBSWebSocketClient,
  options: StartTakeOptions,
): Promise<{ ok: true; take: Take } | { ok: false; result: CallToolResult }> {
  const refuse = (result: CallToolResult) => ({ ok: false as const, result });

  const preflight = await runPreflight(client, { minFreeDiskMb: options.minFreeDiskMb, expectSilent: options.expectSilent, requireVerified: options.requireVerifiedPreflight });
  if (options.requireVerifiedPreflight) {
    const missing = DEMO_REQUIRED_CHECKS.filter((id) => !preflight.checks.some((check) => check.id === id && check.status === "pass"));
    if (missing.length > 0) return refuse(errorResult(`Not recording; required preflight checks are not verified: ${missing.join(", ")}`));
  }
  if (!preflight.ready) {
    const failures = preflight.checks.filter(({ status }) => status === "fail");
    return refuse(errorResult(
      `Not recording; preflight failed:\n${failures.map(({ id, message }) => `- ${id}: ${message}`).join("\n")}`,
    ));
  }

  // Observe continuity before StartRecord, including during monitor initialization.
  let startedObserved = false;
  let stoppedObserved = false;
  let ownershipLost: string | undefined;
  let outputPath: string | undefined;
  let take: Take | undefined;
  const onRecord = (data: unknown) => {
    if (!isObject(data)) return;
    if (data.outputState === "OBS_WEBSOCKET_OUTPUT_STARTED") {
      if (startedObserved || stoppedObserved) ownershipLost = "A different recording may have started";
      startedObserved = true;
    }
    if (data.outputState === "OBS_WEBSOCKET_OUTPUT_STOPPED") stoppedObserved = true;
    if (typeof data.outputPath === "string" && data.outputPath && !ownershipLost) outputPath = data.outputPath;
    if (take) Object.assign(take, { stoppedObserved, ownershipLost, outputPath });
  };
  const onDisconnect = () => {
    ownershipLost = "OBS disconnected; recording ownership cannot be established after reconnect";
    if (take) take.ownershipLost = ownershipLost;
  };
  const disposeOwnership = () => {
    client.off("RecordStateChanged", onRecord);
    client.off("disconnected", onDisconnect);
  };
  client.on("RecordStateChanged", onRecord);
  client.on("disconnected", onDisconnect);
  const started = await startOutputAndConfirm(client, RECORD_OUTPUT);
  if (started.isError) { disposeOwnership(); return refuse(started); }
  if (!startedObserved) ownershipLost = "Start was confirmed only by status; recording identity is uncertain";
  take = {
    id: crypto.randomUUID().slice(0, 8),
    monitor: new TakeMonitor(client, { expectSilent: options.expectSilent }),
    expectSilent: options.expectSilent,
    local: preflight.checks.some(({ id, status }) => id === "record-directory" && status === "pass"),
    locked: options.lock,
    chapterNotes: [],
    maxDurationSeconds: options.maxDurationSeconds,
    ownershipLost, stoppedObserved, outputPath, disposeOwnership,
  };
  const ownedTake = take;
  takes.set(client, take);
  if (options.maxDurationSeconds !== undefined) {
    take.deadline = setTimeout(() => { void stopTake(client, ownedTake); }, options.maxDurationSeconds * 1000);
    take.deadline.unref();
  }
  try {
    await take.monitor.start();
    if (take.stopPromise || take.stopResult) {
      const stopped = await stopTake(client, take);
      return refuse({ ...stopped, isError: true, content: [{ type: "text", text: "Take deadline elapsed during monitor initialization; inspect the stop result", }, ...stopped.content] });
    }
    if (options.chapter) await markTake(client, take, options.chapter);
  } catch (error) {
    const cleanup = await stopTake(client, take);
    return refuse({ ...cleanup, isError: true, content: [{ type: "text", text: `Take monitor initialization failed: ${errorMessage(error)}\n${cleanup.content.filter((item) => item.type === "text").map((item) => item.text).join("\n")}` }] });
  }
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

const STOP_TIMEOUT_MS = 10_000;

/** Resolves with OBS's RecordStateChanged STOPPED event, or null after the timeout or cancel. */
function recordStopped(client: OBSWebSocketClient, timeoutMs: number): { done: Promise<{ outputPath?: string } | null>; cancel(): void } {
  let finish: (value: { outputPath?: string } | null) => void = () => undefined;
  const done = new Promise<{ outputPath?: string } | null>((resolve) => { finish = resolve; });
  const listener = (data: unknown) => {
    if (!isObject(data) || data.outputState !== "OBS_WEBSOCKET_OUTPUT_STOPPED") return;
    end(typeof data.outputPath === "string" ? { outputPath: data.outputPath } : {});
  };
  const timer = setTimeout(() => end(null), timeoutMs);
  const end = (value: { outputPath?: string } | null) => {
    clearTimeout(timer);
    client.off("RecordStateChanged", listener);
    finish(value);
  };
  client.on("RecordStateChanged", listener);
  return { done, cancel: () => end(null) };
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
  if (take.stopResult) return take.stopResult;
  if (take.stopPromise) return take.stopPromise;
  take.stopPromise = stopTakeOnce(client, take, options).catch((error) =>
    errorResult(`Take ${take.id}: stop outcome unavailable: ${errorMessage(error)}; check OBS before retrying`));
  try {
    const result = await take.stopPromise;
    if (takes.get(client) !== take) {
      take.stopResult = result;
      completedTakes.set(client, result);
    }
    return result;
  } finally { take.stopPromise = undefined; }
}

async function stopTakeOnce(client: OBSWebSocketClient, take: Take, options: { requestedSeconds?: number }): Promise<CallToolResult> {
  clearTimeout(take.deadline);
  const uncertain = (reason: string) => ({
    ...errorResult(`Take ${take.id}: ${reason}. Recording stop is unconfirmed; check OBS. No automatic retry was made`),
    structuredContent: { takeId: take.id, stopConfirmed: false, ownership: take.ownershipLost ? "unavailable" : "continuous", reason },
  });
  let status: unknown;
  try { status = await client.sendRequest("GetRecordStatus"); }
  catch { return uncertain("Recording status is unavailable"); }
  if (!isObject(status) || typeof status.outputActive !== "boolean") return uncertain("Recording status is unavailable");
  if (!status.outputActive && take.ownershipLost) {
    await take.monitor.stop();
    take.disposeOwnership?.();
    if (takes.get(client) === take) takes.delete(client);
    if (take.leaseHolder) release(client, take.leaseHolder);
    return uncertain(`${take.ownershipLost}; OBS is now inactive, but the owned file cannot be established`);
  }
  if (!status.outputActive && !take.stoppedObserved) return uncertain("Inactive status has no matching stopped event or completed file");
  if (status.outputActive && (take.ownershipLost || take.stoppedObserved)) {
    await take.monitor.stop();
    return uncertain(take.ownershipLost ?? "The owned recording stopped; the current recording may belong to another operator");
  }
  take.monitor.expectStop();
  let outputPath = take.outputPath;
  if (status.outputActive) {
    const finished = recordStopped(client, STOP_TIMEOUT_MS);
    try { await client.sendRequest("StopRecord"); }
    catch (error) {
      finished.cancel();
      return uncertain(`Stop request outcome is uncertain (${errorMessage(error)})`);
    }
    const stopped = await finished.done;
    outputPath = stopped?.outputPath ?? outputPath;
    let after: unknown;
    try { after = await client.sendRequest("GetRecordStatus"); } catch { /* unavailable */ }
    if (!isObject(after) || after.outputActive !== false || !stopped) return uncertain("OBS did not confirm a completed stop and inactive status");
  }
  const summary = await take.monitor.stop();
  take.disposeOwnership?.();
  if (take.leaseHolder) release(client, take.leaseHolder);
  outputPath ??= take.monitor.outputPath() ?? undefined;
  if (!outputPath) {
    if (takes.get(client) === take) takes.delete(client);
    return errorResult(`Take ${take.id} stopped, but OBS did not report its file`);
  }
  const result = await takeResult(take, summary, outputPath, options.requestedSeconds);
  if (takes.get(client) === take) takes.delete(client);
  return { ...result, structuredContent: { ...(isObject(result.structuredContent) ? result.structuredContent : {}), stopConfirmed: true } };
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

  if (inspection.videoStreams === 0) problems.push("The recording has no video stream");
  const verification = {
    status: problems.length ? "fail" : inspection.skipped ? "unavailable" : "pass",
    scope: "File duration, video presence, expected silence, and observed monitor warnings; capture identity and framing require separate review",
    ...(inspection.skipped ? { reason: inspection.skipped } : {}),
  };
  const lines = [
    `Recorded ${requestedSeconds ?? summary.durationSeconds}s to ${outputPath}`,
    inspection.skipped
      ? `Not verified: ${inspection.skipped}`
      : `File checks ${verification.status}: ${inspection.durationSeconds?.toFixed(1)}s, ${inspection.audioStreams} audio stream(s)`
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
      verification,
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
  "obs-set-output-settings": "change an output's settings",
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
  "SetOutputSettings",
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
        maxDurationSeconds: z.number().int().min(1).max(3600).optional()
          .describe("Server wall-clock stop deadline. Requires this process and OBS connection to remain alive; uncertain ownership prevents automatic stop"),
        requireVerifiedPreflight: z.boolean().optional()
          .describe("Require every software-demo preflight check to exist and pass before starting"),
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
        const claimed = claim(client, holder, 240, `recording take ${take.id}`);
        if (claimed.ok) take.leaseHolder = holder;
        const status = take.monitor.summary();
        return {
          content: [{
            type: "text",
            text: `Take ${take.id} is recording${take.locked ? " (profile and output changes locked)" : ""}. `
              + `Watching audio${status.pictureChecks === "on" ? ", frames, and picture" : " and frames"}; `
              + "stop it with obs-take-stop"
              + (take.maxDurationSeconds === undefined ? "" : `. Server stop deadline: ${take.maxDurationSeconds}s (requires this process and continuous OBS connection)`)
              + (take.ownershipLost ? `. Ownership unavailable: ${take.ownershipLost}; automatic stop is disabled` : "")
              + (claimed.ok ? "" : claimed.lease
                ? `. ${claimed.lease.holder} already has control of OBS`
                : ". Other clients sharing this server can still change OBS: this client did not identify itself "
                  + "(over HTTP, add ?client=<name> to the URL)"),
          }],
          structuredContent: { takeId: take.id, maxDurationSeconds: take.maxDurationSeconds, ownership: take.ownershipLost ? "unavailable" : "continuous", locked: take.locked, pictureChecks: status.pictureChecks, chapterNotes: take.chapterNotes },
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
          maxDurationSeconds: take.maxDurationSeconds,
          ownership: take.ownershipLost ? "unavailable" : "continuous",
          ownershipReason: take.ownershipLost,
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
      if (!take) return completedTakes.get(client) ?? noTake();
      const result = await stopTake(client, take);
      return result;
    },
  );
}
