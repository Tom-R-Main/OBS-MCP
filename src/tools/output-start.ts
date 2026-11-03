/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult } from "@modelcontextprotocol/server";
import type { OBSWebSocketClient } from "../client.js";

export type OutputStartDefinition = {
  label: string;
  requestType: string;
  statusRequest: string;
  stateEvent: string;
  timeoutMs: number;
};

export const RECORD_OUTPUT: OutputStartDefinition = {
  label: "Recording",
  requestType: "StartRecord",
  statusRequest: "GetRecordStatus",
  stateEvent: "RecordStateChanged",
  timeoutMs: 5_000,
};

export const STREAM_OUTPUT: OutputStartDefinition = {
  label: "Streaming",
  requestType: "StartStream",
  statusRequest: "GetStreamStatus",
  stateEvent: "StreamStateChanged",
  timeoutMs: 15_000,
};

export const VIRTUAL_CAM_OUTPUT: OutputStartDefinition = {
  label: "Virtual camera",
  requestType: "StartVirtualCam",
  statusRequest: "GetVirtualCamStatus",
  stateEvent: "VirtualcamStateChanged",
  timeoutMs: 5_000,
};

export const REPLAY_BUFFER_OUTPUT: OutputStartDefinition = {
  label: "Replay buffer",
  requestType: "StartReplayBuffer",
  statusRequest: "GetReplayBufferStatus",
  stateEvent: "ReplayBufferStateChanged",
  timeoutMs: 5_000,
};

const OUTPUT_STARTED = "OBS_WEBSOCKET_OUTPUT_STARTED";
const OUTPUT_STOPPED = "OBS_WEBSOCKET_OUTPUT_STOPPED";

type StartOutcome =
  | { kind: "started"; outputPath?: string }
  | { kind: "stopped" }
  | { kind: "timeout" };

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

/**
 * OBS acknowledges Start* requests before the output actually starts, and a
 * failed start (bad encoder, blocking dialog, stale output settings) produces
 * no request error. Confirm the start through the state event, falling back
 * to the status request when no event arrives in time.
 */
export async function startOutputAndConfirm(
  client: OBSWebSocketClient,
  definition: OutputStartDefinition,
  onAccepted?: () => void,
): Promise<CallToolResult> {
  const { label, requestType, statusRequest, stateEvent, timeoutMs } = definition;

  let onEvent: (eventData: unknown) => void = () => undefined;
  let timer: NodeJS.Timeout | undefined;
  const outcome = new Promise<StartOutcome>((resolve) => {
    onEvent = (eventData) => {
      if (!isObject(eventData)) return;
      if (eventData.outputState === OUTPUT_STARTED) {
        resolve({
          kind: "started",
          ...(typeof eventData.outputPath === "string" ? { outputPath: eventData.outputPath } : {}),
        });
      } else if (eventData.outputState === OUTPUT_STOPPED) {
        resolve({ kind: "stopped" });
      }
    };
    timer = setTimeout(() => resolve({ kind: "timeout" }), timeoutMs);
  });

  // Subscribe before sending: OBS may emit the state change before the response.
  client.on(stateEvent, onEvent);
  try {
    try {
      await client.sendRequest(requestType);
      onAccepted?.();
    } catch (error) {
      return errorResult(
        `${label} failed to start: ${error instanceof Error ? error.message : String(error)}`,
      );
    }

    const result = await outcome;
    if (result.kind === "started") {
      return {
        content: [{
          type: "text",
          text: result.outputPath
            ? `${label} started, writing to: ${result.outputPath}`
            : `${label} started`,
        }],
      };
    }

    let active = false;
    try {
      const status = await client.sendRequest(statusRequest);
      active = isObject(status) && status.outputActive === true;
    } catch {
      // Report the unconfirmed start below.
    }
    if (active) return { content: [{ type: "text", text: `${label} started` }] };

    const reason = result.kind === "stopped"
      ? "OBS stopped the output immediately after starting it"
      : `the output did not become active within ${timeoutMs / 1000}s`;
    return errorResult(
      `OBS accepted ${requestType}, but ${reason}. OBS may be showing an error dialog that `
      + "blocks further output requests until it is dismissed. If output settings were changed "
      + "with obs-set-profile-parameter, restart OBS so it rebuilds its outputs.",
    );
  } finally {
    clearTimeout(timer);
    client.off(stateEvent, onEvent);
  }
}

export type OutputStopDefinition = {
  requestType: string;
  statusRequest: string;
  statusTool: string;
  /** How the confirmation message names the output. */
  noun: string;
  /** Stream delay keeps a stream live for the configured delay after a stop. */
  delayNote: boolean;
};

export const STREAM_STOP: OutputStopDefinition = {
  requestType: "StopStream", statusRequest: "GetStreamStatus", statusTool: "obs-get-stream-status", noun: "streaming", delayNote: true,
};
export const VIRTUAL_CAM_STOP: OutputStopDefinition = {
  requestType: "StopVirtualCam", statusRequest: "GetVirtualCamStatus", statusTool: "obs-get-virtual-cam-status", noun: "the virtual camera", delayNote: false,
};
export const REPLAY_BUFFER_STOP: OutputStopDefinition = {
  requestType: "StopReplayBuffer", statusRequest: "GetReplayBufferStatus", statusTool: "obs-get-replay-buffer-status", noun: "the replay buffer", delayNote: false,
};

/** Matches the native safety-stop budget: the whole call, from admission. */
export const STOP_CONFIRMATION_MS = 10_000;
const STOP_POLL_MS = 100;
/** Matches native: a request that outlives its deadline after dispatch costs the connection there, so no status read starts with less than this left. */
const STATUS_READ_MIN_MS = 500;

/**
 * OBS acknowledges Stop* before the output stops: about 1.1 s for a stream,
 * plus any stream delay, and 0.25 s for the replay buffer on OBS 32.2.2.
 * A stop counts as complete only when a status read sent after the stop, on
 * the same connection, reports the output inactive. State events are not
 * used, so an event from an earlier stop can never confirm this one.
 * Returns undefined when confirmed, otherwise the error text.
 */
export async function confirmOutputStopped(
  client: OBSWebSocketClient,
  definition: OutputStopDefinition,
  startedAt: number,
  budgetMs = STOP_CONFIRMATION_MS,
): Promise<string | undefined> {
  const generation = client.getConnectionStatus().generation;
  const deadline = startedAt + budgetMs;
  for (;;) {
    const remaining = deadline - Date.now();
    if (remaining < Math.min(STATUS_READ_MIN_MS, budgetMs / 2)) return stopUnconfirmed(definition, budgetMs);
    let status: unknown;
    try {
      status = await client.sendRequestForGeneration(generation, definition.statusRequest, undefined, remaining);
    } catch {
      return Date.now() >= deadline ? stopUnconfirmed(definition, budgetMs) : stopUnknown(definition);
    }
    if (isObject(status) && status.outputActive === false) return undefined;
    const wait = Math.min(STOP_POLL_MS, deadline - Date.now());
    if (wait > 0) await new Promise((resolve) => setTimeout(resolve, wait));
  }
}

function stopUnconfirmed({ requestType, statusTool, noun, delayNote }: OutputStopDefinition, budgetMs: number): string {
  return `OBS accepted ${requestType}, but the service did not observe ${noun} stop within ${budgetMs / 1000}s.`
    + (delayNote ? " A stream delay keeps the stream live for the configured delay after a stop." : "")
    + ` This is not evidence that it is still running. Check ${statusTool} before changing related settings; do not resend the stop automatically.`;
}

function stopUnknown({ requestType, statusTool, noun }: OutputStopDefinition): string {
  return `OBS accepted ${requestType}, but the output status could not be read afterwards on the same connection, so whether ${noun} stopped is unknown.`
    + ` Check ${statusTool}: after a reconnect it shows current state, not this request's outcome. Do not resend the stop automatically.`;
}
