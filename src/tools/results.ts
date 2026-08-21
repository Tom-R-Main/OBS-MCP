/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";

type JsonObject = Record<string, unknown>;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function structuredFromText(text: string): JsonObject {
  try {
    const parsed: unknown = JSON.parse(text);
    return isObject(parsed) ? parsed : { result: parsed };
  } catch {
    return { message: text };
  }
}

export function ensureStructuredContent(result: CallToolResult): CallToolResult {
  if (result.isError || result.structuredContent !== undefined) return result;

  const text = result.content
    .filter((block): block is Extract<typeof block, { type: "text" }> => block.type === "text")
    .map((block) => block.text)
    .join("\n");

  return {
    ...result,
    structuredContent: text ? structuredFromText(text) : {},
  };
}

/**
 * Wraps only tool registration. All other McpServer methods remain bound to
 * the real server instance, so SDK private state is never redirected to the proxy.
 */
export function withStructuredToolResults(server: McpServer): McpServer {
  return new Proxy(server, {
    get(target, property) {
      if (property === "registerTool") {
        const registerTool: unknown = Reflect.get(target, property, target);
        return (...registrationArgs: unknown[]) => {
          const callback = registrationArgs[2];
          if (typeof registerTool !== "function" || typeof callback !== "function") {
            throw new TypeError("MCP tool registration requires a callable handler");
          }

          const wrappedCallback = async (...callbackArgs: unknown[]) => {
            const result: unknown = await Reflect.apply(callback, undefined, callbackArgs);
            if (!isObject(result) || !Array.isArray(result.content)) return result;
            return ensureStructuredContent(result as CallToolResult);
          };

          return Reflect.apply(registerTool, target, [
            registrationArgs[0],
            registrationArgs[1],
            wrappedCallback,
          ]);
        };
      }

      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
