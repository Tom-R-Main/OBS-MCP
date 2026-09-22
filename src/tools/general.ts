/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { PACKAGE_VERSION } from "../version.js";
import { z } from "zod";

const MAX_SLEEP_MILLIS = 50_000;

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // Get server status
  server.registerTool(
    "obs-get-status",
    {
      title: "OBS Server Status",
      description: "Get the current status of the OBS MCP server and OBS connection",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      const status = client.getConnectionStatus();
      const obsConnected = client.isConnected();

      const statusInfo = {
        server: {
          name: "obs-mcp",
          version: PACKAGE_VERSION,
          status: "running"
        },
        obs: {
          connected: obsConnected,
          url: status.url,
          hasPassword: status.hasPassword,
          identified: status.identified
        },
        timestamp: new Date().toISOString()
      };

      return {
        content: [
          {
            type: "text",
            text: JSON.stringify(statusInfo, null, 2)
          }
        ]
      };
    }
  );

  // Get OBS version info
  server.registerTool(
    "obs-get-version",
    {
      title: "OBS Version Info",
      description: "Get OBS Studio version information",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const version = await client.sendRequest("GetVersion");
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(version, null, 2)
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Failed to get OBS version: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // Test OBS connection
  server.registerTool(
    "obs-test-connection",
    {
      title: "Test OBS Connection",
      description: "Test the connection to OBS WebSocket",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        // Try a simple request to test the connection
        await client.sendRequest("GetVersion");
        return {
          content: [
            {
              type: "text",
              text: "Connection test successful - OBS is responding"
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Connection test failed: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetStats tool
  server.registerTool(
    "obs-get-stats",
    {
      title: "OBS Statistics",
      description: "Gets statistics about OBS, obs-websocket, and the current session",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const stats = await client.sendRequest("GetStats");
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(stats, null, 2)
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error getting stats: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // BroadcastCustomEvent tool
  server.registerTool(
    "obs-broadcast-custom-event",
    {
      title: "Broadcast Custom Event",
      description: "Broadcasts a CustomEvent to all WebSocket clients",
      inputSchema: z.object({
              eventData: z.record(z.string(), z.unknown()).describe("Data payload to emit to all receivers")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async ({ eventData }) => {
      try {
        await client.sendRequest("BroadcastCustomEvent", { eventData });
        return {
          content: [
            {
              type: "text",
              text: "Custom event broadcast successfully"
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error broadcasting custom event: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // CallVendorRequest tool
  server.registerTool(
    "obs-call-vendor-request",
    {
      title: "Call Vendor Request",
      description: "Call a request registered to a vendor",
      inputSchema: z.object({
              vendorName: z.string().describe("Name of the vendor to use"),
              requestType: z.string().describe("The request type to call"),
              requestData: z.record(z.string(), z.unknown()).optional().describe("Object containing appropriate request data")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    },
    async ({ vendorName, requestType, requestData }) => {
      try {
        const params: Record<string, unknown> = {
          vendorName,
          requestType
        };

        if (requestData !== undefined) {
          params.requestData = requestData;
        }

        const response = await client.sendRequest("CallVendorRequest", params);
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
              text: `Error calling vendor request: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetHotkeyList tool
  server.registerTool(
    "obs-get-hotkey-list",
    {
      title: "Get Hotkey List",
      description: "Gets an array of all hotkey names in OBS",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const hotkeyList = await client.sendRequest("GetHotkeyList");
        return {
          content: [
            {
              type: "text",
              text: JSON.stringify(hotkeyList, null, 2)
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error getting hotkey list: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // TriggerHotkeyByName tool
  server.registerTool(
    "obs-trigger-hotkey-by-name",
    {
      title: "Trigger Hotkey by Name",
      description: "Triggers a hotkey using its name",
      inputSchema: z.object({
              hotkeyName: z.string().describe("Name of the hotkey to trigger"),
              contextName: z.string().optional().describe("Name of context of the hotkey to trigger")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    },
    async ({ hotkeyName, contextName }) => {
      try {
        const params: Record<string, unknown> = { hotkeyName };

        if (contextName !== undefined) {
          params.contextName = contextName;
        }

        await client.sendRequest("TriggerHotkeyByName", params);
        return {
          content: [
            {
              type: "text",
              text: `Successfully triggered hotkey: ${hotkeyName}`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error triggering hotkey: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // TriggerHotkeyByKeySequence tool
  server.registerTool(
    "obs-trigger-hotkey-by-key-sequence",
    {
      title: "Trigger Hotkey by Key Sequence",
      description: "Triggers a hotkey using a sequence of keys",
      inputSchema: z.object({
              keyId: z.string().optional().describe("The OBS key ID to use"),
              keyModifiers: z.object({
                shift: z.boolean().optional().describe("Press Shift"),
                control: z.boolean().optional().describe("Press CTRL"),
                alt: z.boolean().optional().describe("Press ALT"),
                command: z.boolean().optional().describe("Press CMD (Mac)")
              }).optional().describe("Object containing key modifiers to apply")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: true },
    },
    async ({ keyId, keyModifiers }) => {
      try {
        const params: Record<string, unknown> = {};

        if (keyId !== undefined) {
          params.keyId = keyId;
        }

        if (keyModifiers !== undefined) {
          params.keyModifiers = keyModifiers;
        }

        await client.sendRequest("TriggerHotkeyByKeySequence", params);
        return {
          content: [
            {
              type: "text",
              text: "Hotkey triggered by key sequence successfully"
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error triggering hotkey by key sequence: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // Sleep tool. OBS only honors its Sleep request inside a request batch, so a
  // standalone call always failed; wait in the server to pace tool sequences.
  server.registerTool(
    "obs-sleep",
    {
      title: "OBS Sleep",
      description: "Waits for a time duration or a number of OBS video frames before returning. Use it to pace sequences such as holding a recorded shot on screen.",
      inputSchema: z.object({
              sleepMillis: z.number().int().min(0).max(MAX_SLEEP_MILLIS).optional().describe("Number of milliseconds to sleep for"),
              sleepFrames: z.number().int().min(0).max(10_000).optional().describe("Number of video frames to sleep for, at the current OBS frame rate")
            }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ sleepMillis, sleepFrames }) => {
      if ((sleepMillis === undefined) === (sleepFrames === undefined)) {
        return {
          content: [{ type: "text", text: "Provide exactly one of sleepMillis or sleepFrames" }],
          isError: true
        };
      }

      try {
        let millis = sleepMillis ?? 0;
        if (sleepFrames !== undefined) {
          const video = await client.sendRequest("GetVideoSettings");
          const fps = video.fpsNumerator / video.fpsDenominator;
          if (!Number.isFinite(fps) || fps <= 0) {
            throw new Error("OBS reported an invalid frame rate");
          }
          millis = Math.round((sleepFrames / fps) * 1000);
        }
        if (millis > MAX_SLEEP_MILLIS) {
          throw new Error(`Sleep cannot exceed ${MAX_SLEEP_MILLIS}ms`);
        }

        await new Promise((resolve) => setTimeout(resolve, millis));
        return {
          content: [
            {
              type: "text",
              text: `Slept for ${millis}ms`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error during sleep operation: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );
}
