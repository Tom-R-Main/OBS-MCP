/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";
import { registerObsRequestTool } from "./request-tool.js";
import {
  REPLAY_BUFFER_OUTPUT,
  startOutputAndConfirm,
  VIRTUAL_CAM_OUTPUT,
} from "./output-start.js";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetVirtualCamStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-virtual-cam-status",
    title: "Get Virtual Camera Status",
    description: "Gets the status of the virtualcam output",
    requestType: "GetVirtualCamStatus",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // ToggleVirtualCam tool
  server.registerTool(
    "obs-toggle-virtual-cam",
    {
      title: "Toggle Virtual Camera",
      description: "Toggles the state of the virtualcam output",
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
    },
    async () => {
      try {
        const response = await client.sendRequest("ToggleVirtualCam");
        return {
          content: [
            {
              type: "text",
              text: `Virtual camera toggled, now ${response.outputActive ? "active" : "inactive"}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error toggling virtual camera: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // StartVirtualCam tool
  server.registerTool(
    "obs-start-virtual-cam",
    {
      title: "Start Virtual Camera",
      description: "Starts the virtualcam output and waits until OBS confirms it is active",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async () => startOutputAndConfirm(client, VIRTUAL_CAM_OUTPUT)
  );

  // StopVirtualCam tool
  registerObsRequestTool(server, client, {
    name: "obs-stop-virtual-cam",
    title: "Stop Virtual Camera",
    description: "Stops the virtualcam output",
    requestType: "StopVirtualCam",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    successMessage: () => "Virtual camera stopped",
  });

  // GetReplayBufferStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-replay-buffer-status",
    title: "Get Replay Buffer Status",
    description: "Gets the status of the replay buffer output",
    requestType: "GetReplayBufferStatus",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // ToggleReplayBuffer tool
  server.registerTool(
    "obs-toggle-replay-buffer",
    {
      title: "Toggle Replay Buffer",
      description: "Toggles the state of the replay buffer output",
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false },
    },
    async () => {
      try {
        const response = await client.sendRequest("ToggleReplayBuffer");
        return {
          content: [
            {
              type: "text",
              text: `Replay buffer toggled, now ${response.outputActive ? "active" : "inactive"}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error toggling replay buffer: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // StartReplayBuffer tool
  server.registerTool(
    "obs-start-replay-buffer",
    {
      title: "Start Replay Buffer",
      description: "Starts the replay buffer output and waits until OBS confirms it is active",
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async () => startOutputAndConfirm(client, REPLAY_BUFFER_OUTPUT)
  );

  // StopReplayBuffer tool
  registerObsRequestTool(server, client, {
    name: "obs-stop-replay-buffer",
    title: "Stop Replay Buffer",
    description: "Stops the replay buffer output",
    requestType: "StopReplayBuffer",
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    successMessage: () => "Replay buffer stopped",
  });

  // SaveReplayBuffer tool
  registerObsRequestTool(server, client, {
    name: "obs-save-replay-buffer",
    title: "Save Replay Buffer",
    description: "Saves the contents of the replay buffer output",
    requestType: "SaveReplayBuffer",
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    successMessage: () => "Replay buffer saved",
  });

  // GetLastReplayBufferReplay tool
  registerObsRequestTool(server, client, {
    name: "obs-get-last-replay-buffer-replay",
    title: "Get Last Replay Buffer File",
    description: "Gets the filename of the last replay buffer save file",
    requestType: "GetLastReplayBufferReplay",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetOutputList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-output-list",
    title: "Get Output List",
    description: "Gets the list of available outputs",
    requestType: "GetOutputList",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetOutputStatus tool
  registerObsRequestTool(server, client, {
    name: "obs-get-output-status",
    title: "Get Output Status",
    description: "Gets the status of an output",
    requestType: "GetOutputStatus",
    inputSchema: z.object({
      outputName: z.string().describe("Output name")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // ToggleOutput tool
  server.registerTool(
    "obs-toggle-output",
    {
      title: "Toggle Output",
      description: "Toggles the status of an output",
      inputSchema: z.object({
              outputName: z.string().describe("Output name")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    },
    async ({ outputName }) => {
      try {
        const response = await client.sendRequest("ToggleOutput", { outputName });
        return {
          content: [
            {
              type: "text",
              text: `Output '${outputName}' toggled, now ${response.outputActive ? "active" : "inactive"}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error toggling output: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // StartOutput tool
  registerObsRequestTool(server, client, {
    name: "obs-start-output",
    title: "Start Output",
    description: "Starts an output",
    requestType: "StartOutput",
    inputSchema: z.object({
      outputName: z.string().describe("Output name")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
    successMessage: ({ outputName }) => `Output '${outputName}' started`,
  });

  // StopOutput tool
  registerObsRequestTool(server, client, {
    name: "obs-stop-output",
    title: "Stop Output",
    description: "Stops an output",
    requestType: "StopOutput",
    inputSchema: z.object({
      outputName: z.string().describe("Output name")
    }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: true },
    successMessage: ({ outputName }) => `Output '${outputName}' stopped`,
  });

  // GetOutputSettings tool
  registerObsRequestTool(server, client, {
    name: "obs-get-output-settings",
    title: "Get Output Settings",
    description: "Gets the settings of an output",
    requestType: "GetOutputSettings",
    inputSchema: z.object({
      outputName: z.string().describe("Output name")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetOutputSettings tool
  registerObsRequestTool(server, client, {
    name: "obs-set-output-settings",
    title: "Set Output Settings",
    description: "Sets the settings of an output",
    requestType: "SetOutputSettings",
    inputSchema: z.object({
      outputName: z.string().describe("Output name"),
      outputSettings: z.record(z.string(), z.unknown()).describe("Output settings")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ outputName }) => `Settings updated for output '${outputName}'`,
  });
}
