/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer, ToolAnnotations } from "@modelcontextprotocol/server";
import type { ZodObject } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { MESSAGE_OUTPUT_SCHEMA, responseOutputSchema } from "./output-schema.js";

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

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function returnsMessage(definition: RequestToolDefinition): boolean {
  return definition.successMessage !== undefined || definition.responseMode === "success";
}

function structuredResponse(
  definition: RequestToolDefinition,
  response: unknown,
  requestData: Record<string, unknown>,
): Record<string, unknown> {
  if (!returnsMessage(definition)) return isObject(response) ? response : {};
  return {
    message: definition.successMessage?.(requestData) ?? `${definition.title} completed successfully`,
  };
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
    const structuredContent = structuredResponse(definition, response, requestData ?? {});
    return {
      content: [{
        type: "text" as const,
        text: returnsMessage(definition)
          ? String(structuredContent.message)
          : JSON.stringify(structuredContent, null, 2),
      }],
      structuredContent,
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
  const outputSchema = returnsMessage(definition)
    ? MESSAGE_OUTPUT_SCHEMA
    : responseOutputSchema(definition.requestType);
  const outputConfig = outputSchema ? { outputSchema } : {};

  if (inputSchema) {
    server.registerTool(
      name,
      { title, description, inputSchema, annotations, ...outputConfig },
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
    { title, description, annotations, ...outputConfig },
    async () => executeRequest(client, definition),
  );
}
