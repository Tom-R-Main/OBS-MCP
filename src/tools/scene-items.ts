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
  // GetSceneItemList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-scene-items",
    title: "Get Scene Items",
    description: "Get a list of all scene items in a scene",
    requestType: "GetSceneItemList",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
      sceneName: z.string().optional().describe("Name of the scene to get items from"),
      sceneUuid: z.string().optional().describe("UUID of the scene to get items from")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // CreateSceneItem tool
  server.registerTool(
    "obs-create-scene-item",
    {
      title: "Create Scene Item",
      description: "Create a scene item for a source in a scene",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
              sceneName: z.string().optional().describe("Name of the scene to add the source to"),
              sceneUuid: z.string().optional().describe("UUID of the scene to add the source to"),
              sourceName: z.string().optional().describe("Name of the source to add"),
              sourceUuid: z.string().optional().describe("UUID of the source to add"),
              enabled: z.boolean().optional().describe("Whether the scene item is enabled/visible (default: true)")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async ({ canvasUuid, sceneName, sceneUuid, sourceName, sourceUuid, enabled = true }) => {
      try {
        const response = await client.sendRequest("CreateSceneItem", {
          canvasUuid,
          sceneName,
          sceneUuid,
          sourceName,
          sourceUuid,
          sceneItemEnabled: enabled
        });

        return {
          content: [
            {
              type: "text",
              text: `Successfully added ${sourceName} to ${sceneName} with ID: ${response.sceneItemId}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error creating scene item: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // RemoveSceneItem tool
  registerObsRequestTool(server, client, {
    name: "obs-remove-scene-item",
    title: "Remove Scene Item",
    description: "Remove a scene item from a scene",
    requestType: "RemoveSceneItem",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
      sceneName: z.string().optional().describe("Name of the scene to remove the item from"),
      sceneUuid: z.string().optional().describe("UUID of the scene to remove the item from"),
      sceneItemId: z.number().int().nonnegative().describe("The ID of the scene item to remove")
    }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    successMessage: ({ sceneItemId, sceneName }) => `Successfully removed item with ID ${sceneItemId} from ${sceneName}`,
  });

  // SetSceneItemEnabled tool
  server.registerTool(
    "obs-set-scene-item-enabled",
    {
      title: "Set Scene Item Visibility",
      description: "Show or hide a scene item",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
              sceneName: z.string().optional().describe("Name of the scene containing the item"),
              sceneUuid: z.string().optional().describe("UUID of the scene containing the item"),
              sceneItemId: z.number().int().nonnegative().describe("The ID of the scene item"),
              enabled: z.boolean().describe("Whether to show (true) or hide (false) the item")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sceneName, sceneUuid, sceneItemId, enabled }) => {
      try {
        await client.sendRequest("SetSceneItemEnabled", {
          canvasUuid,
          sceneName,
          sceneUuid,
          sceneItemId,
          sceneItemEnabled: enabled
        });

        return {
          content: [
            {
              type: "text",
              text: `Successfully ${enabled ? "showed" : "hid"} item with ID ${sceneItemId} in ${sceneName}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting scene item visibility: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSceneItemTransform tool
  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-transform",
    title: "Get Scene Item Transform",
    description: "Get the position, rotation, scale, or crop of a scene item",
    requestType: "GetSceneItemTransform",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
      sceneName: z.string().optional().describe("Name of the scene containing the item"),
      sceneUuid: z.string().optional().describe("UUID of the scene containing the item"),
      sceneItemId: z.number().int().nonnegative().describe("The ID of the scene item")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetSceneItemTransform tool
  server.registerTool(
    "obs-set-scene-item-transform",
    {
      title: "Set Scene Item Transform",
      description: "Set the position, rotation, scale, or crop of a scene item",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
              sceneName: z.string().optional().describe("Name of the scene containing the item"),
              sceneUuid: z.string().optional().describe("UUID of the scene containing the item"),
              sceneItemId: z.number().int().nonnegative().describe("The ID of the scene item"),
              sceneItemTransform: z.record(z.string(), z.unknown()).optional()
                .describe("Complete or partial OBS scene item transform object"),
              positionX: z.number().optional().describe("The x position"),
              positionY: z.number().optional().describe("The y position"),
              rotation: z.number().optional().describe("The rotation in degrees"),
              scaleX: z.number().optional().describe("The x scale factor"),
              scaleY: z.number().optional().describe("The y scale factor"),
              cropTop: z.number().optional().describe("The number of pixels cropped off the top"),
              cropBottom: z.number().optional().describe("The number of pixels cropped off the bottom"),
              cropLeft: z.number().optional().describe("The number of pixels cropped off the left"),
              cropRight: z.number().optional().describe("The number of pixels cropped off the right")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (params) => {
      try {
        const { canvasUuid, sceneName, sceneUuid, sceneItemId, sceneItemTransform: suppliedTransform, ...transformParams } = params;

        // Build the transform object
        const sceneItemTransform: Record<string, unknown> = { ...suppliedTransform };

        if (transformParams.positionX !== undefined || transformParams.positionY !== undefined) {
          sceneItemTransform.positionX = transformParams.positionX;
          sceneItemTransform.positionY = transformParams.positionY;
        }

        if (transformParams.rotation !== undefined) {
          sceneItemTransform.rotation = transformParams.rotation;
        }

        if (transformParams.scaleX !== undefined || transformParams.scaleY !== undefined) {
          sceneItemTransform.scaleX = transformParams.scaleX;
          sceneItemTransform.scaleY = transformParams.scaleY;
        }

        if (transformParams.cropTop !== undefined || transformParams.cropBottom !== undefined ||
          transformParams.cropLeft !== undefined || transformParams.cropRight !== undefined) {
          if (transformParams.cropTop !== undefined) sceneItemTransform.cropTop = transformParams.cropTop;
          if (transformParams.cropBottom !== undefined) sceneItemTransform.cropBottom = transformParams.cropBottom;
          if (transformParams.cropLeft !== undefined) sceneItemTransform.cropLeft = transformParams.cropLeft;
          if (transformParams.cropRight !== undefined) sceneItemTransform.cropRight = transformParams.cropRight;
        }

        await client.sendRequest("SetSceneItemTransform", {
          canvasUuid,
          sceneName,
          sceneUuid,
          sceneItemId,
          sceneItemTransform
        });

        return {
          content: [
            {
              type: "text",
              text: `Successfully updated transform for item with ID ${sceneItemId} in ${sceneName}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting scene item transform: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSceneItemIdByName tool
  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-id",
    title: "Get Scene Item ID",
    description: "Get the ID of a scene item by its source name",
    requestType: "GetSceneItemId",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
      sceneName: z.string().optional().describe("Name of the scene to search"),
      sceneUuid: z.string().optional().describe("UUID of the scene to search"),
      sourceName: z.string().optional().describe("Name of the source to find"),
      sourceUuid: z.string().optional().describe("UUID of the source to find"),
      searchOffset: z.number().int().nonnegative().optional().describe("Match offset when the source appears multiple times")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });
}
