/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { CLIENT_INFO_META_KEY, type CallToolResult, type McpServer, type ToolAnnotations } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { READ_ONLY_TOOL } from "./request-tool.js";

/** Which client may change OBS, and until when. */
export type Lease = { holder: string; reason: string; since: number; expiresAt: number };

const leases = new WeakMap<OBSWebSocketClient, Lease>();

/** Always allowed: managing control itself, and stopping, which must never be blocked. */
const ALWAYS_ALLOWED = new Set([
  "obs-claim-control",
  "obs-release-control",
  "obs-stop-record",
  "obs-stop-stream",
  "obs-take-stop",
  "obs-stop-virtual-cam",
  "obs-stop-replay-buffer",
  "obs-stop-output",
]);

const UNIDENTIFIED = "an unidentified client";

type RequestContext = { mcpReq?: { envelope?: Record<string, unknown> } };

function errorResult(text: string): CallToolResult {
  return { content: [{ type: "text", text }], isError: true };
}

/**
 * The calling client's name: from the request envelope on 2026-07-28, or
 * from initialize on 2025-era sessions. Clients with the same name share a lease.
 */
export function callerName(server: McpServer, ctx: unknown): string {
  const envelope = (ctx as RequestContext | undefined)?.mcpReq?.envelope;
  const info = envelope?.[CLIENT_INFO_META_KEY] as { name?: unknown } | undefined;
  if (typeof info?.name === "string" && info.name) return info.name;
  const legacy = server.server.getClientVersion()?.name;
  return legacy || UNIDENTIFIED;
}

export function activeLease(client: OBSWebSocketClient, now = Date.now()): Lease | undefined {
  const lease = leases.get(client);
  if (lease && lease.expiresAt <= now) {
    leases.delete(client);
    return undefined;
  }
  return lease;
}

/** Takes control for `holder`, or returns the lease that stands in the way. */
export function claim(client: OBSWebSocketClient, holder: string, minutes: number, reason: string): { ok: true; lease: Lease } | { ok: false; lease: Lease } {
  const current = activeLease(client);
  if (current && current.holder !== holder) return { ok: false, lease: current };
  const now = Date.now();
  const lease = { holder, reason, since: current?.since ?? now, expiresAt: now + minutes * 60_000 };
  leases.set(client, lease);
  return { ok: true, lease };
}

export function release(client: OBSWebSocketClient, holder: string): boolean {
  const current = activeLease(client);
  if (!current || current.holder !== holder) return false;
  leases.delete(client);
  return true;
}

function until(lease: Lease): string {
  return new Date(lease.expiresAt).toISOString().slice(11, 16) + " UTC";
}

function describe(lease: Lease): string {
  return `${lease.holder} has control${lease.reason ? ` (${lease.reason})` : ""} until ${until(lease)}`;
}

/**
 * Wraps tool registration so that, while one client holds control, tools
 * that change OBS refuse calls from other clients. Read-only tools and
 * stopping are always allowed. Other McpServer methods stay bound to the
 * real server.
 */
export function withControlLease(server: McpServer, client: OBSWebSocketClient): McpServer {
  return new Proxy(server, {
    get(target, property) {
      if (property === "registerTool") {
        const registerTool = Reflect.get(target, property, target) as (...args: unknown[]) => unknown;
        return (name: string, config: { annotations?: ToolAnnotations }, callback: (...args: unknown[]) => unknown) => {
          if (config.annotations?.readOnlyHint === true || ALWAYS_ALLOWED.has(name)) {
            return Reflect.apply(registerTool, target, [name, config, callback]);
          }
          const guarded = async (...callbackArgs: unknown[]) => {
            const lease = activeLease(client);
            if (lease) {
              const caller = callerName(target, callbackArgs.at(-1));
              if (caller !== lease.holder) {
                return errorResult(`Not changed: ${describe(lease)}. Ask them to call obs-release-control, `
                  + "or wait. Reading OBS and stopping outputs still work");
              }
            }
            return callback(...callbackArgs);
          };
          return Reflect.apply(registerTool, target, [name, config, guarded]);
        };
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool(
    "obs-claim-control",
    {
      title: "Claim Control of OBS",
      description: "When several agents share this server, claim OBS for this client: until it is released or "
        + "expires, other clients cannot change OBS (reading and stopping outputs still work). Claiming again "
        + "extends it. obs-take-start claims control for the length of a take",
      inputSchema: z.object({
        minutes: z.number().int().min(1).max(240).default(30).describe("How long to hold control"),
        reason: z.string().max(200).optional().describe("Shown to other clients, e.g. \"recording the demo\""),
      }),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ minutes, reason }, ctx): Promise<CallToolResult> => {
      const holder = callerName(server, ctx);
      const result = claim(client, holder, minutes, reason ?? "");
      if (!result.ok) return errorResult(`Not claimed: ${describe(result.lease)}`);
      return {
        content: [{ type: "text", text: `${describe(result.lease)}. Release it with obs-release-control` }],
        structuredContent: { ...result.lease },
      };
    },
  );

  server.registerTool(
    "obs-release-control",
    {
      title: "Release Control of OBS",
      description: "Let other clients change OBS again after obs-claim-control",
      inputSchema: z.object({}),
      annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async (ctx): Promise<CallToolResult> => {
      const holder = callerName(server, ctx);
      const current = activeLease(client);
      if (release(client, holder)) return { content: [{ type: "text", text: "Released control of OBS" }] };
      return { content: [{ type: "text", text: current ? `Not released: ${describe(current)}` : "No client has control" }], ...(current ? { isError: true } : {}) };
    },
  );

  server.registerTool(
    "obs-control-status",
    {
      title: "Control Status",
      description: "Report which client, if any, holds control of OBS and until when",
      inputSchema: z.object({}),
      annotations: READ_ONLY_TOOL,
    },
    async (ctx): Promise<CallToolResult> => {
      const lease = activeLease(client);
      const caller = callerName(server, ctx);
      return {
        content: [{ type: "text", text: lease ? `${describe(lease)}${lease.holder === caller ? " (this client)" : ""}` : "No client has control; any client may change OBS" }],
        structuredContent: { caller, lease: lease ?? null },
      };
    },
  );
}
