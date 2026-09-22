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
