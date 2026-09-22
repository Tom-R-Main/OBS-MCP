/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";
import { registerObsRequestTool } from "./request-tool.js";
import { RECORD_OUTPUT, startOutputAndConfirm } from "./output-start.js";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetRecordStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-record-status",
    title: "Get Record Status",
    description: "Gets the status of the record output",
    requestType: "GetRecordStatus",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // ToggleRecord tool
  server.registerTool(
    "obs-toggle-record",
    {
      title: "Toggle Recording",
      description: "Toggles the status of the record output",
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
    },
    async () => {
      try {
        const response = await client.sendRequest("ToggleRecord");
        return {
          content: [
            {
              type: "text",
              text: `Recording toggled, now ${response.outputActive ? "active" : "inactive"}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error toggling recording: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // StartRecord tool
  server.registerTool(
    "obs-start-record",
    {
      title: "Start Recording",
      description: "Starts the record output and waits until OBS confirms it is active",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async () => startOutputAndConfirm(client, RECORD_OUTPUT)
  );

  // StopRecord tool
  server.registerTool(
    "obs-stop-record",
    {
      title: "Stop Recording",
      description: "Stops the record output",
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const response = await client.sendRequest("StopRecord");
        return {
          content: [
            {
              type: "text",
              text: `Recording stopped, saved to: ${response.outputPath}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error stopping recording: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // ToggleRecordPause tool
  registerObsRequestTool(server, client, {
    name: "obs-toggle-record-pause",
    title: "Toggle Record Pause",
    description: "Toggles pause on the record output",
    requestType: "ToggleRecordPause",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: () => "Recording pause toggled",
  });

  // PauseRecord tool
  registerObsRequestTool(server, client, {
    name: "obs-pause-record",
    title: "Pause Recording",
    description: "Pauses the record output",
    requestType: "PauseRecord",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: () => "Recording paused",
  });

  // ResumeRecord tool
  registerObsRequestTool(server, client, {
    name: "obs-resume-record",
    title: "Resume Recording",
    description: "Resumes the record output",
    requestType: "ResumeRecord",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: () => "Recording resumed",
  });

  // SplitRecordFile tool
  registerObsRequestTool(server, client, {
    name: "obs-split-record-file",
    title: "Split Record File",
    description: "Splits the current file being recorded into a new file",
    requestType: "SplitRecordFile",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: () => "Recording file split",
  });

  // CreateRecordChapter tool
  server.registerTool(
    "obs-create-record-chapter",
    {
      title: "Create Record Chapter",
      description: "Adds a new chapter marker to the file currently being recorded",
      inputSchema: z.object({
              chapterName: z.string().optional().describe("Name of the new chapter")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async ({ chapterName }) => {
      try {
        const requestParams: Record<string, unknown> = {};
        if (chapterName !== undefined) {
          requestParams.chapterName = chapterName;
        }

        await client.sendRequest("CreateRecordChapter", requestParams);
        return {
          content: [
            {
              type: "text",
              text: `Record chapter${chapterName ? ` "${chapterName}"` : ""} created`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error creating record chapter: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );
}
