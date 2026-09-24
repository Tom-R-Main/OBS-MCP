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
  // GetSceneTransitionList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-transition-list",
    title: "Get Transition List",
    description: "Get a list of available transitions in OBS",
    requestType: "GetSceneTransitionList",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetCurrentTransition tool
  registerObsRequestTool(server, client, {
    name: "obs-get-current-transition",
    title: "Get Current Transition",
    description: "Get the name of the currently active transition",
    requestType: "GetCurrentSceneTransition",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetCurrentTransition tool
  registerObsRequestTool(server, client, {
    name: "obs-set-current-transition",
    title: "Set Current Transition",
    description: "Set the current transition in OBS",
    requestType: "SetCurrentSceneTransition",
    inputSchema: z.object({
      transitionName: z.string().describe("The name of the transition to set as current")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ transitionName }) => `Successfully set current transition to: ${transitionName}`,
  });

  // GetTransitionDuration tool
  registerObsRequestTool(server, client, {
    name: "obs-get-transition-duration",
    title: "Get Transition Duration",
    description: "Get the duration of the current transition in milliseconds",
    requestType: "GetCurrentSceneTransition",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetTransitionDuration tool
  server.registerTool(
    "obs-set-transition-duration",
    {
      title: "Set Transition Duration",
      description: "Set the duration of the current transition in milliseconds",
      inputSchema: z.object({
              duration: z.number().min(0).describe("The duration to set in milliseconds")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ duration }) => {
      try {
        await client.sendRequest("SetCurrentSceneTransitionDuration", { transitionDuration: duration });
        return {
          content: [
            {
              type: "text",
              text: `Successfully set transition duration to: ${duration}ms`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting transition duration: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetTransitionKind tool
  registerObsRequestTool(server, client, {
    name: "obs-get-transition-kind",
    title: "Get Transition Kind",
    description: "Get the kind/type of the current transition",
    requestType: "GetCurrentSceneTransition",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetTransitionSettings tool
  registerObsRequestTool(server, client, {
    name: "obs-set-transition-settings",
    title: "Set Transition Settings",
    description: "Set the settings of the current transition",
    requestType: "SetCurrentSceneTransitionSettings",
    inputSchema: z.object({
      transitionSettings: z.record(z.string(), z.unknown()).describe("The settings to apply to the transition")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: () => "Successfully updated current transition settings",
  });

  // GetTransitionSettings tool
  registerObsRequestTool(server, client, {
    name: "obs-get-transition-settings",
    title: "Get Transition Settings",
    description: "Get the settings of the current transition",
    requestType: "GetCurrentSceneTransition",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // TriggerStudioModeTransition tool
  registerObsRequestTool(server, client, {
    name: "obs-trigger-transition",
    title: "Trigger Transition",
    description: "Trigger a scene transition in OBS (Studio Mode must be enabled)",
    requestType: "TriggerStudioModeTransition",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: () => "Successfully triggered studio mode transition",
  });
}
