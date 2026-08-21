/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";
import { z } from "zod";

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  // GetSourceFilterKindList tool
  server.registerTool(
    "obs-get-filter-kind-list",
    {
      title: "Get Filter Kind List",
      description: "Gets an array of all available source filter kinds",
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async () => {
      try {
        const response = await client.sendRequest("GetSourceFilterKindList");
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
              text: `Error getting filter kind list: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSourceFilterList tool
  server.registerTool(
    "obs-get-source-filter-list",
    {
      title: "Get Source Filter List",
      description: "Gets an array of all of a source's filters",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source")
            }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName }) => {
      try {
        const response = await client.sendRequest("GetSourceFilterList", { canvasUuid, sourceName });
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
              text: `Error getting source filter list: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSourceFilterDefaultSettings tool
  server.registerTool(
    "obs-get-filter-default-settings",
    {
      title: "Get Filter Default Settings",
      description: "Gets the default settings for a filter kind",
      inputSchema: z.object({
              filterKind: z.string().describe("Filter kind to get the default settings for")
            }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ filterKind }) => {
      try {
        const response = await client.sendRequest("GetSourceFilterDefaultSettings", { filterKind });
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
              text: `Error getting filter default settings: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // CreateSourceFilter tool
  server.registerTool(
    "obs-create-source-filter",
    {
      title: "Create Source Filter",
      description: "Creates a new filter, adding it to the specified source",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source to add the filter to"),
              filterName: z.string().describe("Name of the new filter to be created"),
              filterKind: z.string().describe("The kind of filter to be created"),
              filterSettings: z.record(z.string(), z.unknown()).optional().describe("Settings object to initialize the filter with")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName, filterKind, filterSettings }) => {
      try {
        const requestParams: Record<string, unknown> = { canvasUuid, sourceName, filterName, filterKind };
        if (filterSettings !== undefined) {
          requestParams.filterSettings = filterSettings;
        }

        await client.sendRequest("CreateSourceFilter", requestParams);
        return {
          content: [
            {
              type: "text",
              text: `Successfully created filter '${filterName}' of kind '${filterKind}' on source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error creating source filter: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // RemoveSourceFilter tool
  server.registerTool(
    "obs-remove-source-filter",
    {
      title: "Remove Source Filter",
      description: "Removes a filter from a source",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source the filter is on"),
              filterName: z.string().describe("Name of the filter to remove")
            }),
      annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName }) => {
      try {
        await client.sendRequest("RemoveSourceFilter", { canvasUuid, sourceName, filterName });
        return {
          content: [
            {
              type: "text",
              text: `Successfully removed filter '${filterName}' from source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error removing source filter: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SetSourceFilterName tool
  server.registerTool(
    "obs-set-source-filter-name",
    {
      title: "Rename Source Filter",
      description: "Sets the name of a source filter (rename)",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source the filter is on"),
              filterName: z.string().describe("Current name of the filter"),
              newFilterName: z.string().describe("New name for the filter")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName, newFilterName }) => {
      try {
        await client.sendRequest("SetSourceFilterName", { canvasUuid, sourceName, filterName, newFilterName });
        return {
          content: [
            {
              type: "text",
              text: `Successfully renamed filter '${filterName}' to '${newFilterName}' on source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error renaming source filter: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // GetSourceFilter tool
  server.registerTool(
    "obs-get-source-filter",
    {
      title: "Get Source Filter",
      description: "Gets the info for a specific source filter",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source"),
              filterName: z.string().describe("Name of the filter")
            }),
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName }) => {
      try {
        const response = await client.sendRequest("GetSourceFilter", { canvasUuid, sourceName, filterName });
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
              text: `Error getting source filter info: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SetSourceFilterIndex tool
  server.registerTool(
    "obs-set-source-filter-index",
    {
      title: "Set Source Filter Index",
      description: "Sets the index position of a filter on a source",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source the filter is on"),
              filterName: z.string().describe("Name of the filter"),
              filterIndex: z.number().min(0).describe("New index position of the filter")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName, filterIndex }) => {
      try {
        await client.sendRequest("SetSourceFilterIndex", { canvasUuid, sourceName, filterName, filterIndex });
        return {
          content: [
            {
              type: "text",
              text: `Successfully set filter '${filterName}' to index ${filterIndex} on source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting source filter index: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SetSourceFilterSettings tool
  server.registerTool(
    "obs-set-source-filter-settings",
    {
      title: "Set Source Filter Settings",
      description: "Sets the settings of a source filter",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source the filter is on"),
              filterName: z.string().describe("Name of the filter to set the settings of"),
              filterSettings: z.record(z.string(), z.unknown()).describe("Object of settings to apply"),
              overlay: z.boolean().optional().describe("True to apply settings on top of existing ones, False to reset to defaults first")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName, filterSettings, overlay }) => {
      try {
        const requestParams: Record<string, unknown> = { canvasUuid, sourceName, filterName, filterSettings };
        if (overlay !== undefined) {
          requestParams.overlay = overlay;
        }

        await client.sendRequest("SetSourceFilterSettings", requestParams);
        return {
          content: [
            {
              type: "text",
              text: `Successfully updated settings for filter '${filterName}' on source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting source filter settings: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );

  // SetSourceFilterEnabled tool
  server.registerTool(
    "obs-set-source-filter-enabled",
    {
      title: "Set Source Filter Enabled",
      description: "Sets the enable state of a source filter",
      inputSchema: z.object({
              canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
              sourceName: z.string().describe("Name of the source the filter is on"),
              filterName: z.string().describe("Name of the filter"),
              filterEnabled: z.boolean().describe("New enable state of the filter")
            }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ canvasUuid, sourceName, filterName, filterEnabled }) => {
      try {
        await client.sendRequest("SetSourceFilterEnabled", { canvasUuid, sourceName, filterName, filterEnabled });
        return {
          content: [
            {
              type: "text",
              text: `Successfully ${filterEnabled ? "enabled" : "disabled"} filter '${filterName}' on source '${sourceName}'`
            }
          ]
        };
      } catch (error) {
        return {
          content: [
            {
              type: "text",
              text: `Error setting source filter enabled state: ${error instanceof Error ? error.message : String(error)}`
            }
          ],
          isError: true
        };
      }
    }
  );
}
