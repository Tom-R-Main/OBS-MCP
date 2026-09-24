/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { accessSync, constants, statSync } from "node:fs";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { READ_ONLY_TOOL } from "./request-tool.js";

export type PreflightStatus = "pass" | "warn" | "fail" | "unknown";

export type PreflightCheck = {
  id: string;
  status: PreflightStatus;
  message: string;
};

export type PreflightOptions = {
  minFreeDiskMb: number;
  expectSilent: boolean;
};

type JsonObject = Record<string, unknown>;

const LOCAL_HOSTS = new Set(["localhost", "127.0.0.1", "::1", "[::1]"]);

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function request(client: OBSWebSocketClient, requestType: string, data?: JsonObject): Promise<JsonObject> {
  const response = await client.sendRequest(requestType, data);
  return isObject(response) ? response : {};
}

async function profileParameter(
  client: OBSWebSocketClient,
  parameterCategory: string,
  parameterName: string,
): Promise<string | null> {
  const response = await request(client, "GetProfileParameter", { parameterCategory, parameterName });
  const value = response.parameterValue ?? response.defaultParameterValue;
  return typeof value === "string" ? value : null;
}

/** Runs one check, turning an unexpected request failure into an "unknown" result. */
async function runCheck(id: string, check: () => Promise<PreflightCheck | PreflightCheck[]>): Promise<PreflightCheck[]> {
  try {
    const result = await check();
    return Array.isArray(result) ? result : [result];
  } catch (error) {
    return [{ id, status: "unknown", message: `Could not check: ${errorMessage(error)}` }];
  }
}

async function checkOutputsIdle(client: OBSWebSocketClient): Promise<PreflightCheck> {
  const record = await request(client, "GetRecordStatus");
  return record.outputActive === true
    ? { id: "record-idle", status: "fail", message: "Recording is already active" }
    : { id: "record-idle", status: "pass", message: "Recording is not active" };
}

function isLocalObs(client: OBSWebSocketClient): boolean {
  try {
    return LOCAL_HOSTS.has(new URL(client.getConnectionStatus().url).hostname);
  } catch {
    return false;
  }
}

async function checkRecordDirectory(client: OBSWebSocketClient): Promise<PreflightCheck> {
  const { recordDirectory } = await request(client, "GetRecordDirectory");
  if (typeof recordDirectory !== "string" || !recordDirectory) {
    return { id: "record-directory", status: "fail", message: "OBS has no recording directory set" };
  }
  if (!isLocalObs(client)) {
    return {
      id: "record-directory",
      status: "unknown",
      message: `OBS records to ${recordDirectory}; OBS is remote, so the path was not checked`,
    };
  }
  try {
    if (!statSync(recordDirectory).isDirectory()) throw new Error("not a directory");
    accessSync(recordDirectory, constants.W_OK);
  } catch (error) {
    // OBS shows a blocking "invalid path" dialog instead of starting.
    return {
      id: "record-directory",
      status: "fail",
      message: `Recording directory ${recordDirectory} is not a writable directory (${errorMessage(error)})`,
    };
  }
  return { id: "record-directory", status: "pass", message: `Recording to ${recordDirectory}` };
}

async function checkDiskSpace(client: OBSWebSocketClient, minFreeDiskMb: number): Promise<PreflightCheck> {
  const { availableDiskSpace } = await request(client, "GetStats");
  if (typeof availableDiskSpace !== "number") {
    return { id: "disk-space", status: "unknown", message: "OBS did not report free disk space" };
  }
  const free = Math.round(availableDiskSpace);
  return free < minFreeDiskMb
    ? { id: "disk-space", status: "fail", message: `${free} MB free, below the ${minFreeDiskMb} MB minimum` }
    : { id: "disk-space", status: "pass", message: `${free} MB free` };
}

const NON_MAC_ENCODER = /nvenc|qsv|amd|amf|jim_/i;
const MAC_ONLY_ENCODER = /apple|videotoolbox/i;

async function recordEncoder(client: OBSWebSocketClient): Promise<{ mode: string; encoder: string | null }> {
  const mode = await profileParameter(client, "Output", "Mode") ?? "Simple";
  if (mode === "Advanced") {
    const encoder = await profileParameter(client, "AdvOut", "RecEncoder");
    // "none" means the recording shares the streaming encoder.
    return {
      mode,
      encoder: !encoder || encoder === "none" ? await profileParameter(client, "AdvOut", "Encoder") : encoder,
    };
  }
  const quality = await profileParameter(client, "SimpleOutput", "RecQuality");
  const encoder = quality === "Stream"
    ? await profileParameter(client, "SimpleOutput", "StreamEncoder")
    : await profileParameter(client, "SimpleOutput", "RecEncoder");
  return { mode, encoder };
}

async function checkEncoder(client: OBSWebSocketClient): Promise<PreflightCheck[]> {
  const checks: PreflightCheck[] = [];
  const { mode, encoder } = await recordEncoder(client);
  const platform = client.getConnectionStatus().versionInfo?.platform;

  if (!encoder) {
    checks.push({ id: "encoder", status: "unknown", message: `${mode} output mode has no recording encoder set` });
  } else if (platform === "macos" && NON_MAC_ENCODER.test(encoder)) {
    checks.push({ id: "encoder", status: "fail", message: `Recording encoder ${encoder} is not available on macOS` });
  } else if (platform && platform !== "macos" && MAC_ONLY_ENCODER.test(encoder)) {
    checks.push({ id: "encoder", status: "fail", message: `Recording encoder ${encoder} is only available on macOS` });
  } else {
    checks.push({ id: "encoder", status: "pass", message: `${mode} output mode records with ${encoder}` });
  }

  if (client.hasPendingOutputSettings()) {
    checks.push({
      id: "output-settings",
      status: "fail",
      message: "Output settings were changed through this server since OBS last started; "
        + "OBS will ignore them (or fail to start) until it rebuilds its outputs. Restart OBS first",
    });
  }
  return checks;
}

async function recordedTracks(client: OBSWebSocketClient): Promise<number[]> {
  if (await profileParameter(client, "Output", "Mode") !== "Advanced") return [1];
  const mask = Number(await profileParameter(client, "AdvOut", "RecTracks") ?? "1");
  const tracks = [1, 2, 3, 4, 5, 6].filter((track) => (mask & (1 << (track - 1))) !== 0);
  return tracks.length > 0 ? tracks : [1];
}

async function checkAudio(client: OBSWebSocketClient, expectSilent: boolean): Promise<PreflightCheck> {
  const [{ inputs }, tracks] = await Promise.all([
    request(client, "GetInputList"),
    recordedTracks(client),
  ]);
  const names = (Array.isArray(inputs) ? inputs : [])
    .filter(isObject)
    .map(({ inputName }) => inputName)
    .filter((name): name is string => typeof name === "string");
  // One round trip for every input's mute state and tracks. Video-only inputs
  // fail both requests, which the batch reports without failing the rest.
  const results = await client.sendBatch(names.flatMap((inputName) => [
    { requestType: "GetInputMute", requestData: { inputName } },
    { requestType: "GetInputAudioTracks", requestData: { inputName } },
  ]));
  const audible: string[] = [];

  names.forEach((inputName, index) => {
    const mute = results[index * 2];
    const trackResult = results[index * 2 + 1];
    if (!mute?.ok || !isObject(mute.responseData) || mute.responseData.inputMuted === true) return;
    const inputAudioTracks = trackResult?.ok && isObject(trackResult.responseData)
      ? trackResult.responseData.inputAudioTracks
      : undefined;
    const enabled = isObject(inputAudioTracks)
      ? tracks.filter((track) => inputAudioTracks[String(track)] === true)
      : tracks;
    if (enabled.length > 0) audible.push(`${inputName} (track ${enabled.join(", ")})`);
  });

  if (audible.length === 0) {
    return { id: "audio", status: "pass", message: "No unmuted input feeds a recorded audio track" };
  }
  return {
    id: "audio",
    status: expectSilent ? "fail" : "warn",
    message: `Recorded audio comes from: ${audible.join("; ")}. macOS screen_capture inputs carry `
      + "application audio; mute them or clear their tracks for a silent recording",
  };
}

async function checkSceneItems(client: OBSWebSocketClient): Promise<PreflightCheck> {
  const program = await request(client, "GetCurrentProgramScene");
  const sceneName = program.currentProgramSceneName ?? program.sceneName;
  if (typeof sceneName !== "string") {
    return { id: "scene-items", status: "unknown", message: "OBS did not report the program scene" };
  }
  const { sceneItems } = await request(client, "GetSceneItemList", { sceneName });
  const items = (Array.isArray(sceneItems) ? sceneItems : []).filter(isObject);
  const visible = items.filter((item) => item.sceneItemEnabled !== false);
  // GetSceneItemList includes transforms; look up any it left out in one batch.
  const missing = visible.filter((item) => !isObject(item.sceneItemTransform) && typeof item.sceneItemId === "number");
  const looked = await client.sendBatch(
    missing.map(({ sceneItemId }) => ({ requestType: "GetSceneItemTransform", requestData: { sceneName, sceneItemId } })),
  );
  const transforms = new Map(missing.map((item, index) => {
    const result = looked[index];
    return [item, result?.ok && isObject(result.responseData) ? result.responseData.sceneItemTransform : undefined];
  }));
  const blank = visible
    .filter((item) => {
      const transform = isObject(item.sceneItemTransform) ? item.sceneItemTransform : transforms.get(item);
      return isObject(transform) && (transform.sourceWidth === 0 || transform.sourceHeight === 0);
    })
    .map((item) => String(item.sourceName ?? item.sceneItemId));

  if (items.length === 0) {
    return { id: "scene-items", status: "warn", message: `Program scene ${sceneName} is empty` };
  }
  return blank.length > 0
    ? {
      id: "scene-items",
      status: "warn",
      message: `Visible items in ${sceneName} render at 0×0: ${blank.join(", ")}. `
        + "A screen capture usually needs a display or window selected (and screen recording permission)",
    }
    : { id: "scene-items", status: "pass", message: `${items.length} item(s) in ${sceneName} have a size` };
}

const SETTINGS_DIALOG_NOTE: PreflightCheck = {
  id: "settings-dialog",
  status: "unknown",
  message: "OBS cannot report open Settings or error dialogs over WebSocket. If a start is accepted but "
    + "never becomes active, close OBS Settings or dismiss its error dialog",
};

export async function runPreflight(
  client: OBSWebSocketClient,
  options: PreflightOptions,
): Promise<{ ready: boolean; checks: PreflightCheck[] }> {
  try {
    await client.connect();
  } catch (error) {
    return {
      ready: false,
      checks: [{ id: "connection", status: "fail", message: `Cannot reach OBS: ${errorMessage(error)}` }],
    };
  }

  const groups = await Promise.all([
    runCheck("record-idle", () => checkOutputsIdle(client)),
    runCheck("record-directory", () => checkRecordDirectory(client)),
    runCheck("disk-space", () => checkDiskSpace(client, options.minFreeDiskMb)),
    runCheck("encoder", () => checkEncoder(client)),
    runCheck("audio", () => checkAudio(client, options.expectSilent)),
    runCheck("scene-items", () => checkSceneItems(client)),
  ]);
  const checks = [
    { id: "connection", status: "pass" as const, message: "Connected to OBS" },
    ...groups.flat(),
    SETTINGS_DIALOG_NOTE,
  ];
  return { ready: !checks.some(({ status }) => status === "fail"), checks };
}

const STATUS_MARK: Record<PreflightStatus, string> = {
  pass: "ok  ",
  warn: "warn",
  fail: "FAIL",
  unknown: "?   ",
};

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-preflight",
    {
      title: "Recording Preflight",
      description: "Check for the conditions that make OBS silently refuse or botch a recording: an active "
        + "recording, an invalid output path, low disk space, an unusable encoder, unapplied output settings, "
        + "audible inputs, and blank (0×0) sources. Read-only; run it before obs-start-record",
      inputSchema: z.object({
        minFreeDiskMb: z.number().int().nonnegative().default(1024)
          .describe("Minimum free disk space in MB (default 1024)"),
        expectSilent: z.boolean().default(false)
          .describe("Treat any audible input as a failure instead of a warning"),
      }),
      annotations: READ_ONLY_TOOL,
    },
    async ({ minFreeDiskMb, expectSilent }): Promise<CallToolResult> => {
      const result = await runPreflight(client, { minFreeDiskMb, expectSilent });
      const lines = result.checks.map(({ status, id, message }) => `${STATUS_MARK[status]} ${id}: ${message}`);
      return {
        content: [{
          type: "text",
          text: `${result.ready ? "Ready to record" : "Not ready to record"}\n${lines.join("\n")}`,
        }],
        structuredContent: result,
      };
    },
  );
}
