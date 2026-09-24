/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { RequestBatchExecutionType, type OBSWebSocketClient } from "../client.js";
import { getObsProtocolRequest, OBS_PROTOCOL_REQUESTS } from "../obs-protocol.js";
import { READ_ONLY_TOOL } from "./request-tool.js";

const UNRESTRICTED_REQUEST_TOOL = {
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: false,
  openWorldHint: true,
} as const;

const MAX_BATCH_REQUESTS = 100;

const EXECUTION_TYPES = {
  "serial-realtime": RequestBatchExecutionType.SerialRealtime,
  "serial-frame": RequestBatchExecutionType.SerialFrame,
  parallel: RequestBatchExecutionType.Parallel,
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

  server.registerTool(
    "obs-batch",
    {
      title: "Run OBS Requests in a Batch",
      description: "Send several requests from the pinned OBS WebSocket protocol in one message. "
        + "serial-realtime runs them in order as fast as possible; serial-frame runs one per rendered frame, "
        + "so changes such as hiding one source and showing another land on the same frame; parallel runs "
        + "them together. Sleep ({sleepMillis} or {sleepFrames}) pauses serial batches. Each request "
        + "succeeds or fails on its own unless haltOnFailure is set",
      inputSchema: z.object({
        requests: z.array(z.object({
          requestType: z.string().describe("Exact OBS WebSocket request type"),
          requestData: z.record(z.string(), z.unknown()).optional().describe("Request fields defined by obs-describe-request"),
        })).min(1).max(MAX_BATCH_REQUESTS).describe("Requests in the order OBS should run them"),
        executionType: z.enum(["serial-realtime", "serial-frame", "parallel"]).default("serial-realtime")
          .describe("How OBS runs the requests"),
        haltOnFailure: z.boolean().default(false).describe("Skip the remaining requests after one fails"),
      }),
      annotations: UNRESTRICTED_REQUEST_TOOL,
    },
    async ({ requests, executionType, haltOnFailure }) => {
      const unknown = requests.filter(({ requestType }) => !getObsProtocolRequest(requestType));
      if (unknown.length > 0) {
        return {
          content: [{ type: "text", text: `Unknown OBS WebSocket request: ${unknown.map(({ requestType }) => requestType).join(", ")}` }],
          isError: true,
        };
      }
      if (executionType === "parallel" && requests.some(({ requestType }) => requestType === "Sleep")) {
        return {
          content: [{ type: "text", text: "Sleep only works in serial-realtime and serial-frame batches" }],
          isError: true,
        };
      }

      try {
        const batch = await client.sendBatch(requests, { executionType: EXECUTION_TYPES[executionType], haltOnFailure });
        const results = batch.map((result, index) => ({ index, ...result }));
        const failed = results.filter(({ ok }) => !ok);
        const structuredContent = {
          results,
          succeeded: results.length - failed.length,
          failed: failed.length,
          skipped: requests.length - results.length,
        };
        const summary = [
          `${structuredContent.succeeded} of ${requests.length} request(s) succeeded`,
          ...failed.map(({ index, requestType, code, comment }) => (
            `#${index} ${requestType} failed with code ${code}${comment ? `: ${comment}` : ""}`
          )),
          ...(structuredContent.skipped > 0 ? [`${structuredContent.skipped} request(s) skipped after the failure`] : []),
        ];
        return {
          content: [{ type: "text", text: `${summary.join("\n")}\n${JSON.stringify(results, null, 2)}` }],
          structuredContent,
          ...(failed.length > 0 ? { isError: true } : {}),
        };
      } catch (error) {
        return {
          content: [{
            type: "text",
            text: `OBS request batch failed: ${error instanceof Error ? error.message : String(error)}`,
          }],
          isError: true,
        };
      }
    },
  );
}
