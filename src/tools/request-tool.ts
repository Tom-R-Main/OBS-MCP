/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer, ToolAnnotations } from "@modelcontextprotocol/server";
import type { ZodObject } from "zod";
import type { OBSWebSocketClient } from "../client.js";

export const READ_ONLY_TOOL = {
  readOnlyHint: true,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: false,
} satisfies ToolAnnotations;

export const IDEMPOTENT_WRITE_TOOL = {
  readOnlyHint: false,
  destructiveHint: false,
  idempotentHint: true,
  openWorldHint: false,
} satisfies ToolAnnotations;

export const NON_IDEMPOTENT_WRITE_TOOL = {
  readOnlyHint: false,
  destructiveHint: false,
  idempotentHint: false,
  openWorldHint: false,
} satisfies ToolAnnotations;

export const DESTRUCTIVE_WRITE_TOOL = {
  readOnlyHint: false,
  destructiveHint: true,
  idempotentHint: false,
  openWorldHint: false,
} satisfies ToolAnnotations;

type RequestToolDefinition = {
  name: string;
  title: string;
  description: string;
  requestType: string;
  inputSchema?: ZodObject;
  annotations: ToolAnnotations;
  responseMode?: "json" | "success";
  /** Replaces the JSON response with a confirmation built from the arguments. */
  successMessage?: (args: Record<string, any>) => string;
  /** Returns a refusal message when the request must not reach OBS. */
  guard?: (client: OBSWebSocketClient, args: Record<string, unknown>) => Promise<string | undefined>;
};

function formatResponse(
  definition: RequestToolDefinition,
  response: unknown,
  requestData: Record<string, unknown>,
): string {
  if (definition.successMessage) return definition.successMessage(requestData);
  if (definition.responseMode === "success") {
    return `${definition.title} completed successfully`;
  }

  return JSON.stringify(response, null, 2);
}

async function executeRequest(
  client: OBSWebSocketClient,
  definition: RequestToolDefinition,
  requestData?: Record<string, unknown>,
) {
  try {
    const refusal = await definition.guard?.(client, requestData ?? {});
    if (refusal) {
      return {
        content: [{ type: "text" as const, text: `${definition.title} refused: ${refusal}` }],
        isError: true,
      };
    }
    const response = await client.sendRequest(definition.requestType, requestData);
    return {
      content: [{ type: "text" as const, text: formatResponse(definition, response, requestData ?? {}) }],
    };
  } catch (error) {
    return {
      content: [{
        type: "text" as const,
        text: `${definition.title} failed: ${error instanceof Error ? error.message : String(error)}`,
      }],
      isError: true,
    };
  }
}

export function registerObsRequestTool(
  server: McpServer,
  client: OBSWebSocketClient,
  definition: RequestToolDefinition,
): void {
  const { name, title, description, inputSchema, annotations } = definition;

  if (inputSchema) {
    server.registerTool(
      name,
      { title, description, inputSchema, annotations },
      async (args) => executeRequest(
        client,
        definition,
        args as Record<string, unknown>,
      ),
    );
    return;
  }

  server.registerTool(
    name,
    { title, description, annotations },
    async () => executeRequest(client, definition),
  );
}
