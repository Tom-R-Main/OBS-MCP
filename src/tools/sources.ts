/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { screenshotResult } from "./screenshot.js";
import { z } from "zod";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetSourceActive tool
  server.registerTool(
    "obs-get-source-active",
    {
      title: "Get Source Active State",
      description: "Gets the active and show state of a source",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().optional().describe("Name of the source to get the active state of"),
              sourceUuid: z.string().optional().describe("UUID of the source to get the active state of")
            }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, sourceUuid }) => {
      try {
        const response = await client.sendRequest("GetSourceActive", {
          canvasUuid,
          sourceName,
          sourceUuid
        });

        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(response, null, 2)
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error getting source active state: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSourceScreenshot tool
  server.registerTool(
    "obs-get-source-screenshot",
    {
      title: "Get Source Screenshot",
      description: "Gets a source screenshot as MCP image content",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().optional().describe("Name of the source to take a screenshot of"),
              sourceUuid: z.string().optional().describe("UUID of the source to take a screenshot of"),
              imageFormat: z.string().min(1).max(16).describe("Image compression format to use"),
              imageWidth: z.number().int().min(8).max(4096).optional().describe("Width to scale the screenshot to"),
              imageHeight: z.number().int().min(8).max(4096).optional().describe("Height to scale the screenshot to"),
              imageCompressionQuality: z.number().int().min(-1).max(100).optional().describe("Compression quality to use (0-100, -1 for default)")
            }),
      outputSchema: z.object({
        mimeType: z.string(),
        sizeBytes: z.number().int().nonnegative(),
      }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, sourceUuid, imageFormat, imageWidth, imageHeight, imageCompressionQuality }) => {
      try {
        const response = await client.sendRequest("GetSourceScreenshot", {
          canvasUuid,
          sourceName,
          sourceUuid,
          imageFormat,
          imageWidth,
          imageHeight,
          imageCompressionQuality
        });

        return screenshotResult(response);
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error getting source screenshot: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SaveSourceScreenshot tool
  server.registerTool(
    "obs-save-source-screenshot",
    {
      title: "Save Source Screenshot",
      description: "Saves a screenshot of a source to the filesystem",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().optional().describe("Name of the source to take a screenshot of"),
              sourceUuid: z.string().optional().describe("UUID of the source to take a screenshot of"),
              imageFormat: z.string().describe("Image compression format to use"),
              imageFilePath: z.string().describe("Path to save the screenshot file to"),
              imageWidth: z.number().optional().describe("Width to scale the screenshot to"),
              imageHeight: z.number().optional().describe("Height to scale the screenshot to"),
              imageCompressionQuality: z.number().optional().describe("Compression quality to use (0-100, -1 for default)")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, sourceUuid, imageFormat, imageFilePath, imageWidth, imageHeight, imageCompressionQuality }) => {
      try {
        await client.sendRequest("SaveSourceScreenshot", {
          canvasUuid,
          sourceName,
          sourceUuid,
          imageFormat,
          imageFilePath,
          imageWidth,
          imageHeight,
          imageCompressionQuality
        });

        return {
          content: [
            {
              type: "text",
              text: `Successfully saved screenshot to: ${imageFilePath}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error saving source screenshot: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );
}
