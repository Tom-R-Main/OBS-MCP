/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";
import { registerObsRequestTool } from "./request-tool.js";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetMediaInputStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-media-input-status",
    title: "Get Media Input Status",
    description: "Gets the status of a media input",
    requestType: "GetMediaInputStatus",
    inputSchema: z.object({
      inputName: z.string().describe("Name of the media input")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetMediaInputCursor tool
  registerObsRequestTool(server, client, {
    name: "obs-set-media-input-cursor",
    title: "Set Media Input Cursor",
    description: "Sets the cursor position of a media input",
    requestType: "SetMediaInputCursor",
    inputSchema: z.object({
      inputName: z.string().describe("Name of the media input"),
      mediaCursor: z.number().min(0).describe("New cursor position to set (in milliseconds)")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ inputName, mediaCursor }) => `Successfully set media cursor position to ${mediaCursor}ms for input: ${inputName}`,
  });

  // OffsetMediaInputCursor tool
  registerObsRequestTool(server, client, {
    name: "obs-offset-media-input-cursor",
    title: "Offset Media Input Cursor",
    description: "Offsets the current cursor position of a media input",
    requestType: "OffsetMediaInputCursor",
    inputSchema: z.object({
      inputName: z.string().describe("Name of the media input"),
      mediaCursorOffset: z.number().describe("Value to offset the current cursor position by (in milliseconds)")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: ({ inputName, mediaCursorOffset }) => `Successfully offset media cursor position by ${mediaCursorOffset}ms for input: ${inputName}`,
  });

  // TriggerMediaInputAction tool
  registerObsRequestTool(server, client, {
    name: "obs-trigger-media-input-action",
    title: "Trigger Media Input Action",
    description: "Triggers an action on a media input",
    requestType: "TriggerMediaInputAction",
    inputSchema: z.object({
      inputName: z.string().describe("Name of the media input"),
      mediaAction: z.enum([
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PLAY",
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PAUSE",
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_STOP",
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_RESTART",
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_NEXT",
      "OBS_WEBSOCKET_MEDIA_INPUT_ACTION_PREVIOUS"
      ]).describe("Action to trigger (PLAY, PAUSE, STOP, RESTART, NEXT, PREVIOUS)")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: ({ inputName, mediaAction }) => `Successfully triggered media action '${mediaAction}' for input: ${inputName}`,
  });
}
