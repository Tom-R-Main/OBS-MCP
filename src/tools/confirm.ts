/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import {
  CLIENT_CAPABILITIES_META_KEY,
  acceptedContent,
  inputRequired,
  inputResponse,
  type CallToolResult,
  type McpServer,
} from "@modelcontextprotocol/server";
import { z, type ZodObject } from "zod";

/** Tools that end a live broadcast or a take, with the question to ask. */
export const LIVE_ACTIONS: Readonly<Record<string, string>> = {
  "obs-stop-stream": "Stop the live stream?",
  "obs-toggle-stream": "Toggle the live stream? If it is live, this ends the broadcast.",
  "obs-stop-record": "Stop the recording?",
  "obs-toggle-record": "Toggle recording? If it is recording, this ends the take.",
  "obs-take-stop": "Stop the recording?",
};

/** OBS requests that end a live broadcast or a take, with the question to ask. */
const LIVE_REQUESTS: Readonly<Record<string, (data: Record<string, unknown>) => string>> = {
  StopStream: () => "Stop the live stream?",
  ToggleStream: () => "Toggle the live stream? If it is live, this ends the broadcast.",
  StopRecord: () => "Stop the recording?",
  ToggleRecord: () => "Toggle recording? If it is recording, this ends the take.",
  // The generic output requests reach the stream and recording outputs too (e.g. simple_stream, adv_file_output).
  StopOutput: ({ outputName }) => `Stop the output ${String(outputName)}? If it is the stream or the recording, this ends it.`,
  ToggleOutput: ({ outputName }) => `Toggle the output ${String(outputName)}? If it is the stream or the recording, this can end it.`,
};

type RequestLike = { requestType?: unknown; requestData?: unknown };

function questionForRequests(requests: RequestLike[]): string | undefined {
  const questions = [...new Set(requests.map(({ requestType, requestData }) => {
    const ask = LIVE_REQUESTS[String(requestType)];
    return ask?.(typeof requestData === "object" && requestData !== null ? requestData as Record<string, unknown> : {});
  }).filter((question): question is string => Boolean(question)))];
  return questions.length > 0 ? questions.join(" ") : undefined;
}

/**
 * Generic tools that are live actions only for some arguments: the question
 * to ask, or undefined when these arguments need no confirmation.
 */
export const CONDITIONAL_LIVE_ACTIONS: Readonly<Record<string, (args: Record<string, unknown>) => string | undefined>> = {
  "obs-call-request": ({ requestType, requestData }) => questionForRequests([{ requestType, requestData }]),
  "obs-batch": ({ requests }) => questionForRequests(Array.isArray(requests) ? requests as RequestLike[] : []),
  "obs-stop-output": (args) => questionForRequests([{ requestType: "StopOutput", requestData: args }]),
  "obs-toggle-output": (args) => questionForRequests([{ requestType: "ToggleOutput", requestData: args }]),
};

const CONFIRM_KEY = "confirm";
const CONFIRM_SCHEMA = z.object({ confirm: z.boolean().describe("Confirm the action") });

export function liveConfirmationEnabled(env: NodeJS.ProcessEnv = process.env): boolean {
  return ["1", "true", "yes"].includes((env.OBS_MCP_CONFIRM_LIVE ?? "").trim().toLowerCase());
}

type RequestContext = {
  mcpReq?: {
    inputResponses?: Record<string, unknown>;
    envelope?: Record<string, unknown>;
  };
};

function clientCanElicit(server: McpServer, ctx: RequestContext): boolean {
  // 2026-07-28 requests carry client capabilities in each request envelope;
  // 2025-era sessions declared them once at initialize.
  const envelopeCapabilities = ctx.mcpReq?.envelope?.[CLIENT_CAPABILITIES_META_KEY] as
    | { elicitation?: unknown }
    | undefined;
  if (envelopeCapabilities) return envelopeCapabilities.elicitation !== undefined;
  return server.server.getClientCapabilities()?.elicitation !== undefined;
}

function textResult(text: string, isError = false): CallToolResult {
  return { content: [{ type: "text", text }], ...(isError ? { isError: true } : {}) };
}

/**
 * Decides whether a live action may run: yes when the caller passed
 * confirm: true or the user accepted the elicitation; otherwise the result
 * to return instead (an elicitation, a cancellation, or instructions).
 */
function confirmation(
  server: McpServer,
  question: string,
  confirmArg: boolean | undefined,
  ctx: RequestContext,
): "proceed" | ReturnType<typeof inputRequired> | CallToolResult {
  const responses = ctx.mcpReq?.inputResponses;
  const answered = acceptedContent(responses, CONFIRM_KEY, CONFIRM_SCHEMA);
  if (answered) return answered.confirm ? "proceed" : textResult("Cancelled; nothing was changed");
  if (inputResponse(responses, CONFIRM_KEY).kind !== "missing") {
    return textResult("Cancelled; nothing was changed");
  }

  if (clientCanElicit(server, ctx)) {
    return inputRequired({
      inputRequests: {
        [CONFIRM_KEY]: inputRequired.elicit({
          message: question,
          requestedSchema: {
            type: "object",
            properties: { confirm: { type: "boolean", title: "Confirm", description: question } },
            required: ["confirm"],
          },
        }),
      },
    });
  }
  // Only a client that cannot ask the user relies on the model passing confirm: true;
  // one that can always asks, so the model cannot approve on the user's behalf.
  if (confirmArg === true) return "proceed";
  return textResult(
    `${question} This server requires confirmation for live actions and this client cannot ask. `
      + "Ask the user, then call again with confirm: true.",
    true,
  );
}

/**
 * Wraps tool registration so the tools in LIVE_ACTIONS, and the generic
 * request tools when they carry a live request, ask before running.
 * Adds an optional `confirm` argument to their input schema; the original
 * handler never sees it.
 */
export function withLiveConfirmation(server: McpServer): McpServer {
  return new Proxy(server, {
    get(target, property) {
      if (property === "registerTool") {
        const registerTool = Reflect.get(target, property, target) as (...args: unknown[]) => unknown;
        return (name: string, config: { inputSchema?: ZodObject }, callback: (...args: unknown[]) => unknown) => {
          const fixedQuestion = LIVE_ACTIONS[name];
          const questionFor = CONDITIONAL_LIVE_ACTIONS[name];
          if (!fixedQuestion && !questionFor) return Reflect.apply(registerTool, target, [name, config, callback]);

          const hadSchema = config.inputSchema !== undefined;
          const inputSchema = (config.inputSchema ?? z.object({})).extend({
            confirm: z.boolean().optional()
              .describe("For clients that cannot show the user a question: pass true only after the user has agreed. Ignored when the server can ask the user itself"),
          });
          const confirmedCallback = async (args: Record<string, unknown>, ctx: RequestContext) => {
            const { confirm, ...rest } = args;
            const question = fixedQuestion ?? questionFor?.(rest);
            if (!question) return hadSchema ? callback(rest, ctx) : callback(ctx);
            const decision = confirmation(target, question, confirm as boolean | undefined, ctx);
            if (decision !== "proceed") return decision;
            return hadSchema ? callback(rest, ctx) : callback(ctx);
          };
          return Reflect.apply(registerTool, target, [name, { ...config, inputSchema }, confirmedCallback]);
        };
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
