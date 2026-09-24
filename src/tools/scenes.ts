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
  // GetSceneList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-scene-list",
    title: "Get Scene List",
    description: "Get a list of scenes in OBS",
    requestType: "GetSceneList",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas to list scenes from")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetCurrentProgramScene tool
  registerObsRequestTool(server, client, {
    name: "obs-get-current-scene",
    title: "Get Current Scene",
    description: "Get the current active scene in OBS",
    requestType: "GetCurrentProgramScene",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetCurrentProgramScene tool
  registerObsRequestTool(server, client, {
    name: "obs-set-current-scene",
    title: "Set Current Scene",
    description: "Set the current active scene in OBS",
    requestType: "SetCurrentProgramScene",
    inputSchema: z.object({
      sceneName: z.string().describe("The name of the scene to set as current")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ sceneName }) => `Successfully switched to scene: ${sceneName}`,
  });

  // GetCurrentPreviewScene tool (Studio Mode)
  registerObsRequestTool(server, client, {
    name: "obs-get-preview-scene",
    title: "Get Preview Scene",
    description: "Get the current preview scene in OBS Studio Mode",
    requestType: "GetCurrentPreviewScene",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetCurrentPreviewScene tool (Studio Mode)
  registerObsRequestTool(server, client, {
    name: "obs-set-preview-scene",
    title: "Set Preview Scene",
    description: "Set the current preview scene in OBS Studio Mode",
    requestType: "SetCurrentPreviewScene",
    inputSchema: z.object({
      sceneName: z.string().describe("The name of the scene to set as preview")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ sceneName }) => `Successfully set preview scene to: ${sceneName}`,
  });

  // CreateScene tool
  registerObsRequestTool(server, client, {
    name: "obs-create-scene",
    title: "Create Scene",
    description: "Create a new scene in OBS",
    requestType: "CreateScene",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas to create the scene in"),
      sceneName: z.string().describe("The name for the new scene")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: ({ sceneName }) => `Successfully created scene: ${sceneName}`,
  });

  // RemoveScene tool
  registerObsRequestTool(server, client, {
    name: "obs-remove-scene",
    title: "Remove Scene",
    description: "Remove a scene from OBS",
    requestType: "RemoveScene",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
      sceneName: z.string().describe("The name of the scene to remove")
    }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    successMessage: ({ sceneName }) => `Successfully removed scene: ${sceneName}`,
  });

  // TriggerStudioModeTransition tool
  registerObsRequestTool(server, client, {
    name: "obs-trigger-studio-transition",
    title: "Trigger Studio Transition",
    description: "Trigger a transition from preview to program scene in Studio Mode",
    requestType: "TriggerStudioModeTransition",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: () => "Successfully triggered studio mode transition",
  });
}
