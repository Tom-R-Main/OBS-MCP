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
  // GetSourceFilterKindList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-filter-kind-list",
    title: "Get Filter Kind List",
    description: "Gets an array of all available source filter kinds",
    requestType: "GetSourceFilterKindList",
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetSourceFilterList tool
  registerObsRequestTool(server, client, {
    name: "obs-get-source-filter-list",
    title: "Get Source Filter List",
    description: "Gets an array of all of a source's filters",
    requestType: "GetSourceFilterList",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // GetSourceFilterDefaultSettings tool
  registerObsRequestTool(server, client, {
    name: "obs-get-filter-default-settings",
    title: "Get Filter Default Settings",
    description: "Gets the default settings for a filter kind",
    requestType: "GetSourceFilterDefaultSettings",
    inputSchema: z.object({
      filterKind: z.string().describe("Filter kind to get the default settings for")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

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
  registerObsRequestTool(server, client, {
    name: "obs-remove-source-filter",
    title: "Remove Source Filter",
    description: "Removes a filter from a source",
    requestType: "RemoveSourceFilter",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source the filter is on"),
      filterName: z.string().describe("Name of the filter to remove")
    }),
    annotations: { readOnlyHint: false, destructiveHint: true, idempotentHint: true, openWorldHint: false },
    successMessage: ({ filterName, sourceName }) => `Successfully removed filter '${filterName}' from source '${sourceName}'`,
  });

  // SetSourceFilterName tool
  registerObsRequestTool(server, client, {
    name: "obs-set-source-filter-name",
    title: "Rename Source Filter",
    description: "Sets the name of a source filter (rename)",
    requestType: "SetSourceFilterName",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source the filter is on"),
      filterName: z.string().describe("Current name of the filter"),
      newFilterName: z.string().describe("New name for the filter")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ filterName, newFilterName, sourceName }) => `Successfully renamed filter '${filterName}' to '${newFilterName}' on source '${sourceName}'`,
  });

  // GetSourceFilter tool
  registerObsRequestTool(server, client, {
    name: "obs-get-source-filter",
    title: "Get Source Filter",
    description: "Gets the info for a specific source filter",
    requestType: "GetSourceFilter",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source"),
      filterName: z.string().describe("Name of the filter")
    }),
    annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
  });

  // SetSourceFilterIndex tool
  registerObsRequestTool(server, client, {
    name: "obs-set-source-filter-index",
    title: "Set Source Filter Index",
    description: "Sets the index position of a filter on a source",
    requestType: "SetSourceFilterIndex",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source the filter is on"),
      filterName: z.string().describe("Name of the filter"),
      filterIndex: z.number().min(0).describe("New index position of the filter")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ filterIndex, filterName, sourceName }) => `Successfully set filter '${filterName}' to index ${filterIndex} on source '${sourceName}'`,
  });

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
  registerObsRequestTool(server, client, {
    name: "obs-set-source-filter-enabled",
    title: "Set Source Filter Enabled",
    description: "Sets the enable state of a source filter",
    requestType: "SetSourceFilterEnabled",
    inputSchema: z.object({
      canvasUuid: z.string().optional().describe("UUID of the canvas containing the source"),
      sourceName: z.string().describe("Name of the source the filter is on"),
      filterName: z.string().describe("Name of the filter"),
      filterEnabled: z.boolean().describe("New enable state of the filter")
    }),
    annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    successMessage: ({ filterEnabled, filterName, sourceName }) => `Successfully ${filterEnabled ? "enabled" : "disabled"} filter '${filterName}' on source '${sourceName}'`,
  });
}
