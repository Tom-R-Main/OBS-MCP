/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { appendInputScreenshot, inputStateResult, readInputState } from "./after-change.js";
import { RECORD_OUTPUT, startOutputAndConfirm } from "./output-start.js";
import { runPreflight } from "./preflight.js";

type JsonObject = Record<string, unknown>;

const execFileAsync = promisify(execFile);

/** Many MCP clients time a tool call out at 60 seconds. */
export const MAX_CLIP_SECONDS = 50;
const SCK_WINDOW_CAPTURE = 1;
const SCK_APPLICATION_CAPTURE = 2;
const ALL_TRACKS_OFF = { 1: false, 2: false, 3: false, 4: false, 5: false, 6: false };

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

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

async function recordClip(
  client: OBSWebSocketClient,
  args: { durationSeconds: number; chapters: { atSeconds: number; name: string }[]; expectSilent: boolean },
): Promise<CallToolResult> {
  const preflight = await runPreflight(client, { minFreeDiskMb: 1024, expectSilent: args.expectSilent });
  if (!preflight.ready) {
    const failures = preflight.checks.filter(({ status }) => status === "fail");
    return errorResult(
      `Not recording; preflight failed:\n${failures.map(({ id, message }) => `- ${id}: ${message}`).join("\n")}`,
    );
  }

  const started = await startOutputAndConfirm(client, RECORD_OUTPUT);
  if (started.isError) return started;

  const startedAt = Date.now();
  const chapterNotes: string[] = [];
  let outputPath: string | undefined;
  try {
    const chapters = [...args.chapters].sort((a, b) => a.atSeconds - b.atSeconds);
    for (const chapter of chapters) {
      await sleep(Math.max(0, startedAt + chapter.atSeconds * 1000 - Date.now()));
      try {
        await client.sendRequest("CreateRecordChapter", { chapterName: chapter.name });
      } catch (error) {
        // Chapters need Hybrid MP4/MOV; a missing chapter must not abort the take.
        chapterNotes.push(`Chapter "${chapter.name}" not added: ${errorMessage(error)}`);
      }
    }
    await sleep(Math.max(0, startedAt + args.durationSeconds * 1000 - Date.now()));
  } finally {
    const stopped: unknown = await client.sendRequest("StopRecord").catch(() => undefined);
    if (isObject(stopped) && typeof stopped.outputPath === "string") outputPath = stopped.outputPath;
  }

  if (!outputPath) {
    return errorResult("Recording started, but OBS did not confirm the stop or report the file. Check obs-get-record-status");
  }

  const local = preflight.checks.some(({ id, status }) => id === "record-directory" && status === "pass");
  const inspection = local
    ? await inspectRecording(outputPath, args.expectSilent)
    : { skipped: "OBS is remote, so the file was not inspected" };

  const problems: string[] = [];
  if (inspection.durationSeconds !== undefined && inspection.durationSeconds < args.durationSeconds - 2) {
    problems.push(`The file is ${inspection.durationSeconds.toFixed(1)}s, shorter than the requested ${args.durationSeconds}s`);
  }
  if (args.expectSilent && inspection.maxVolumeDb !== undefined && inspection.maxVolumeDb > -60) {
    problems.push(`Expected silence, but the audio peaks at ${inspection.maxVolumeDb} dB`);
  }

  const lines = [
    `Recorded ${args.durationSeconds}s to ${outputPath}`,
    inspection.skipped
      ? `Not verified: ${inspection.skipped}`
      : `Verified: ${inspection.durationSeconds?.toFixed(1)}s, ${inspection.audioStreams} audio stream(s)`
        + (inspection.maxVolumeDb !== undefined ? `, peak ${inspection.maxVolumeDb} dB` : ""),
    ...chapterNotes,
    ...problems.map((problem) => `Problem: ${problem}`),
  ];
  return {
    content: [{ type: "text", text: lines.join("\n") }],
    structuredContent: {
      outputPath,
      ...inspection,
      ...(inspection.maxVolumeDb === -Infinity ? { maxVolumeDb: null } : {}),
      chapterNotes,
      problems,
    },
    ...(problems.length > 0 ? { isError: true } : {}),
  };
}

type ListItem = { itemName: string; itemValue: unknown; itemEnabled?: boolean };

async function listItems(client: OBSWebSocketClient, inputName: string, propertyName: string): Promise<ListItem[]> {
  const response: unknown = await client.sendRequest("GetInputPropertiesListPropertyItems", { inputName, propertyName });
  const items = isObject(response) && Array.isArray(response.propertyItems) ? response.propertyItems : [];
  return items.filter((item): item is ListItem => (
    isObject(item) && typeof item.itemName === "string" && item.itemEnabled !== false && item.itemName.trim() !== ""
  ));
}

async function captureWindow(
  client: OBSWebSocketClient,
  args: {
    sceneName: string;
    inputName: string;
    window?: string;
    application?: string;
    displayUuid?: string;
    silent: boolean;
    fit: boolean;
    includeScreenshot: boolean;
  },
): Promise<CallToolResult> {
  if (!args.window === !args.application) return errorResult("Give exactly one of window or application");
  await client.connect();
  if (client.getConnectionStatus().versionInfo?.platform !== "macos") {
    return errorResult("obs-capture-window drives the macOS screen_capture source; OBS is not on macOS");
  }

  const { inputs } = await client.sendRequest("GetInputList") as { inputs?: unknown[] };
  const existing = (inputs ?? []).filter(isObject).find(({ inputName }) => inputName === args.inputName);
  if (existing && existing.inputKind !== "screen_capture") {
    return errorResult(`Input ${args.inputName} exists but is a ${String(existing.inputKind)}, not a screen_capture`);
  }
  const type = args.window ? SCK_WINDOW_CAPTURE : SCK_APPLICATION_CAPTURE;
  if (!existing) {
    await client.sendRequest("CreateInput", {
      sceneName: args.sceneName,
      inputName: args.inputName,
      inputKind: "screen_capture",
      inputSettings: { type, show_cursor: true },
    });
  }

  // Select a display before listing applications: listing them without one
  // crashed OBS 32.2.2, and application capture renders nothing without it.
  let displayUuid = args.displayUuid;
  if (!displayUuid) {
    const displays = await listItems(client, args.inputName, "display_uuid");
    const first = displays.find(({ itemValue }) => typeof itemValue === "string" && itemValue);
    if (!first) return errorResult("OBS lists no displays; grant OBS screen recording permission");
    displayUuid = first.itemValue as string;
  }
  await client.sendRequest("SetInputSettings", {
    inputName: args.inputName,
    inputSettings: { type, display_uuid: displayUuid },
  });

  const property = args.window ? "window" : "application";
  const wanted = (args.window ?? args.application ?? "").toLowerCase();
  const items = await listItems(client, args.inputName, property);
  const matches = items.filter(({ itemName, itemValue }) => (
    itemName.toLowerCase().includes(wanted) || String(itemValue).toLowerCase() === wanted
  ));
  if (matches.length !== 1) {
    const names = (matches.length > 1 ? matches : items).slice(0, 15).map(({ itemName }) => `- ${itemName}`);
    return errorResult(
      `${matches.length === 0 ? "No" : "More than one"} ${property} matches "${args.window ?? args.application}". `
        + `${matches.length > 1 ? "Matches" : "Available"}:\n${names.join("\n")}`,
    );
  }
  const match = matches[0] as ListItem;
  await client.sendRequest("SetInputSettings", {
    inputName: args.inputName,
    inputSettings: { [property]: match.itemValue },
  });

  const notes: string[] = [`Capturing ${property} ${match.itemName}`];
  if (args.silent) {
    // screen_capture carries the application's audio on macOS 13+.
    await client.sendRequest("SetInputMute", { inputName: args.inputName, inputMuted: true });
    await client.sendRequest("SetInputAudioTracks", { inputName: args.inputName, inputAudioTracks: ALL_TRACKS_OFF });
    notes.push("Muted the capture and removed it from every audio track");
  }

  if (args.fit) {
    const video = await client.sendRequest("GetVideoSettings") as JsonObject;
    const { sceneItemId } = await client.sendRequest("GetSceneItemId", {
      sceneName: args.sceneName,
      sourceName: args.inputName,
    }) as { sceneItemId: number };
    await client.sendRequest("SetSceneItemTransform", {
      sceneName: args.sceneName,
      sceneItemId,
      sceneItemTransform: {
        boundsType: "OBS_BOUNDS_SCALE_INNER",
        boundsWidth: video.baseWidth,
        boundsHeight: video.baseHeight,
        boundsAlignment: 0,
        alignment: 5,
        positionX: 0,
        positionY: 0,
      },
    });
    notes.push(`Fitted to the ${String(video.baseWidth)}×${String(video.baseHeight)} canvas`);
  }

  const result = inputStateResult(notes.join(". "), await readInputState(client, args.inputName));
  return args.includeScreenshot ? appendInputScreenshot(client, args.inputName, result) : result;
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-record-clip",
    {
      title: "Record Clip",
      description: `Record a clip of up to ${MAX_CLIP_SECONDS}s in one call: runs obs-preflight, starts recording `
        + "and waits for OBS to confirm, adds chapter markers, stops, then checks the file's length and audio "
        + "with ffprobe/ffmpeg when available. For longer takes use obs-start-record and obs-stop-record",
      inputSchema: z.object({
        durationSeconds: z.number().positive().max(MAX_CLIP_SECONDS).describe("Clip length in seconds"),
        chapters: z.array(z.object({
          atSeconds: z.number().nonnegative().describe("Seconds after the start"),
          name: z.string().min(1).describe("Chapter name"),
        })).default([]).describe("Chapter markers; need the Hybrid MP4 or Hybrid MOV recording format"),
        expectSilent: z.boolean().default(false)
          .describe("Refuse to record with audible inputs, and check the file's audio is silent"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async (args) => {
      try {
        return await recordClip(client, args);
      } catch (error) {
        return errorResult(`Record Clip failed: ${errorMessage(error)}`);
      }
    },
  );

  server.registerTool(
    "obs-capture-window",
    {
      title: "Capture Window (macOS)",
      description: "Point a macOS screen_capture input at a window or application in one call: creates the "
        + "input if needed, selects a display (which application capture needs), matches the window or app by "
        + "name, optionally mutes it and clears its audio tracks, fits it to the canvas, and returns its size. "
        + "Window capture excludes the menu bar; application capture includes it",
      inputSchema: z.object({
        sceneName: z.string().describe("Scene to add the input to if it does not exist"),
        inputName: z.string().describe("Name of the screen_capture input to create or reuse"),
        window: z.string().optional().describe("Case-insensitive part of \"[App] Title\", or the window ID"),
        application: z.string().optional().describe("Case-insensitive part of the app name, or its bundle ID"),
        displayUuid: z.string().optional().describe("Display UUID; defaults to the first display OBS lists"),
        silent: z.boolean().default(true).describe("Mute the capture and remove it from every audio track"),
        fit: z.boolean().default(true).describe("Scale the capture to fit the canvas, centered"),
        includeScreenshot: z.boolean().default(false).describe("Also return a screenshot of the capture"),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (args) => {
      try {
        return await captureWindow(client, args);
      } catch (error) {
        return errorResult(`Capture Window failed: ${errorMessage(error)}`);
      }
    },
  );
}
