/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";
import { registerObsRequestTool } from "./request-tool.js";
import { startOutputAndConfirm, STREAM_OUTPUT } from "./output-start.js";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetStreamStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-stream-status",
    title: "Get Stream Status",
    description: "Get the current streaming status",
    requestType: "GetStreamStatus",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // StartStream tool
  server.registerTool(
    "obs-start-stream",
    {
      title: "Start Stream",
      description: "Start streaming in OBS and wait until OBS confirms the stream is active",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
    },
    async () => startOutputAndConfirm(client, STREAM_OUTPUT)
  );

  // StopStream tool
  registerObsRequestTool(server, client, {
    name: "obs-stop-stream",
    title: "Stop Stream",
    description: "Stop streaming in OBS",
    requestType: "StopStream",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: true },
    successMessage: () => "Successfully stopped streaming",
  });

  // ToggleStream tool
  server.registerTool(
    "obs-toggle-stream",
    {
      title: "Toggle Stream",
      description: "Toggle the streaming state in OBS",
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    },
    async () => {
      try {
        const response = await client.sendRequest("ToggleStream");
        return {
          content: [
            {
              type: "text",
              text: `Successfully toggled streaming state. Stream is now ${response.outputActive ? 'active' : 'inactive'}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error toggling stream: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SendStreamCaption tool
  registerObsRequestTool(server, client, {
    name: "obs-send-stream-caption",
    title: "Send Stream Caption",
    description: "Sends CEA-608 caption text over the stream output",
    requestType: "SendStreamCaption",
    inputSchema: z.object({
      captionText: z.string().describe("Caption text to send")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
    successMessage: () => "Successfully sent stream caption",
  });
}
