/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";
import { startOutputAndConfirm, STREAM_OUTPUT } from "./output-start.js";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetStreamStatus tool
  server.registerTool(
    "obs-get-stream-status",
    {
      title: "Get Stream Status",
      description: "Get the current streaming status",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const status = await client.sendRequest("GetStreamStatus");
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(status, null, 2)
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error getting stream status: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

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
  server.registerTool(
    "obs-stop-stream",
    {
      title: "Stop Stream",
      description: "Stop streaming in OBS",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: true },
    },
    async () => {
      try {
        await client.sendRequest("StopStream");
        return {
          content: [
            {
              type: "text",
              text: "Successfully stopped streaming"
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error stopping stream: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // ToggleStream tool
  server.registerTool(
    "obs-toggle-stream",
    {
      title: "Toggle Stream",
      description: "Toggle the streaming state in OBS",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
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
  server.registerTool(
    "obs-send-stream-caption",
    {
      title: "Send Stream Caption",
      description: "Sends CEA-608 caption text over the stream output",
      inputSchema: z.object({
              captionText: z.string().describe("Caption text to send")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
    },
    async ({ captionText }) => {
      try {
        await client.sendRequest("SendStreamCaption", { captionText });
        return {
          content: [
            {
              type: "text",
              text: "Successfully sent stream caption"
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error sending stream caption: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );
}
