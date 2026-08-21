import type { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { getObsProtocolRequest, OBS_PROTOCOL_REQUESTS } from "../obs-protocol.js";
import { READ_ONLY_TOOL } from "./request-tool.js";

const UNRESTRICTED_REQUEST_TOOL = {
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: false,
  openWorldHint: true,
} as const;

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-describe-request",
    {
      title: "Describe OBS Request",
      description: "Inspect the exact fields and constraints of requests in the pinned OBS WebSocket protocol",
      inputSchema: z.object({
              requestType: z.string().optional().describe("Exact OBS WebSocket request type"),
              category: z.string().optional().describe("Optional category filter when listing requests"),
            }),
      annotations: READ_ONLY_TOOL,
    },
    async ({ requestType, category }) => {
      if (requestType) {
        const request = getObsProtocolRequest(requestType);
        if (!request) {
          return {
            content: [{ type: "text", text: `Unknown OBS WebSocket request: ${requestType}` }],
            isError: true,
          };
        }

        return {
          content: [{ type: "text", text: JSON.stringify(request, null, 2) }],
        };
      }

      const normalizedCategory = category?.toLowerCase();
      const requests = OBS_PROTOCOL_REQUESTS
        .filter((request) => !normalizedCategory || request.category.toLowerCase() === normalizedCategory)
        .map(({ requestType: name, category: requestCategory, description, deprecated, initialVersion }) => ({
          requestType: name,
          category: requestCategory,
          description,
          deprecated,
          initialVersion,
        }));

      return {
        content: [{ type: "text", text: JSON.stringify(requests, null, 2) }],
      };
    },
  );

  server.registerTool(
    "obs-call-request",
    {
      title: "Call OBS Request",
      description: "Call any request in the pinned OBS WebSocket protocol; inspect it with obs-describe-request first",
      inputSchema: z.object({
              requestType: z.string().describe("Exact OBS WebSocket request type"),
              requestData: z.record(z.string(), z.unknown()).optional().describe("Request fields defined by obs-describe-request"),
            }),
      annotations: UNRESTRICTED_REQUEST_TOOL,
    },
    async ({ requestType, requestData }) => {
      if (!getObsProtocolRequest(requestType)) {
        return {
          content: [{ type: "text", text: `Unknown OBS WebSocket request: ${requestType}` }],
          isError: true,
        };
      }

      try {
        const response = await client.sendRequest(requestType, requestData);
        return {
          content: [{ type: "text", text: JSON.stringify(response, null, 2) }],
        };
      } catch (error) {
        return {
          content: [{
            type: "text",
            text: `OBS request ${requestType} failed: ${error instanceof Error ? error.message : String(error)}`,
          }],
          isError: true,
        };
      }
    },
  );
}
