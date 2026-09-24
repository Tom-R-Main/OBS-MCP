/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { Readable } from "node:stream";
import { pipeline } from "node:stream/promises";
import {
  createMcpHandler,
  hostHeaderValidationResponse,
  localhostAllowedHostnames,
  localhostAllowedOrigins,
  originValidationResponse,
  type McpServer,
  type ServerNotifier,
} from "@modelcontextprotocol/server";
import { logger } from "./logger.js";

export const MCP_PATH = "/mcp";
const LOOPBACK = "127.0.0.1";

export type HttpServerHandle = {
  url: string;
  notify: ServerNotifier;
  close(): Promise<void>;
};

export type HttpOptions = {
  port: number;
  /** When set, every request must carry `Authorization: Bearer <token>`. */
  token?: string;
};

function tokenMatches(header: string | null, token: string): boolean {
  const presented = Buffer.from(header?.startsWith("Bearer ") ? header.slice(7) : "");
  const expected = Buffer.from(token);
  return presented.length === expected.length && crypto.timingSafeEqual(presented, expected);
}

function toRequest(req: http.IncomingMessage): Request {
  const headers = new Headers();
  for (const [name, value] of Object.entries(req.headers)) {
    if (value === undefined) continue;
    for (const item of Array.isArray(value) ? value : [value]) headers.append(name, item);
  }
  const hasBody = req.method !== "GET" && req.method !== "HEAD";
  return new Request(`http://${req.headers.host ?? LOOPBACK}${req.url ?? "/"}`, {
    method: req.method ?? "GET",
    headers,
    ...(hasBody ? { body: Readable.toWeb(req) as ReadableStream, duplex: "half" } : {}),
  } as RequestInit);
}

async function send(res: http.ServerResponse, response: Response): Promise<void> {
  const headers: Record<string, string | string[]> = {};
  response.headers.forEach((value, name) => {
    const existing = headers[name];
    headers[name] = existing === undefined ? value : [...(Array.isArray(existing) ? existing : [existing]), value];
  });
  res.writeHead(response.status, headers);
  if (!response.body) {
    res.end();
    return;
  }
  // pipeline destroys both sides on error or client disconnect instead of throwing an unhandled 'error'.
  await pipeline(Readable.fromWeb(response.body as import("node:stream/web").ReadableStream), res).catch((error: unknown) => {
    logger.debug(`MCP HTTP response stream ended early: ${error instanceof Error ? error.message : String(error)}`);
  });
}

/**
 * The name a client declares with `?client=<name>` in the URL it connects to.
 * 2025-era clients cannot otherwise be told apart over stateless HTTP, and
 * the control lease needs a name.
 */
function declaredClient(url: URL): string | undefined {
  const name = url.searchParams.get("client")?.trim();
  return name && /^[\w.@:-]{1,64}$/.test(name) ? name : undefined;
}

/**
 * Serves MCP over Streamable HTTP on 127.0.0.1 so several agents can share
 * one server and one OBS connection. Host and Origin headers must name
 * localhost (DNS rebinding protection), and a bearer token can be required.
 */
export async function serveHttp(factory: () => McpServer, options: HttpOptions): Promise<HttpServerHandle> {
  const handler = createMcpHandler(factory, {
    onerror: (error) => logger.debug(`MCP HTTP: ${error.message}`),
  });
  const allowedHosts = localhostAllowedHostnames();
  const allowedOrigins = localhostAllowedOrigins();

  const server = http.createServer((req, res) => {
    void (async () => {
      try {
        const url = new URL(req.url ?? "/", `http://${LOOPBACK}`);
        if (url.pathname !== MCP_PATH) {
          res.writeHead(404, { "content-type": "text/plain" }).end(`Not found; the MCP endpoint is ${MCP_PATH}`);
          return;
        }
        const request = toRequest(req);
        const rejected = hostHeaderValidationResponse(request, allowedHosts) ?? originValidationResponse(request, allowedOrigins);
        if (rejected) {
          await send(res, rejected);
          return;
        }
        if (options.token && !tokenMatches(request.headers.get("authorization"), options.token)) {
          res.writeHead(401, { "content-type": "text/plain", "www-authenticate": "Bearer" }).end("Missing or wrong bearer token");
          return;
        }
        const clientId = declaredClient(url);
        await send(res, await handler.fetch(request, clientId ? { authInfo: { token: options.token ?? "", clientId, scopes: [] } } : undefined));
      } catch (error) {
        logger.error(`MCP HTTP request failed: ${error instanceof Error ? error.message : String(error)}`);
        if (!res.headersSent) res.writeHead(500, { "content-type": "text/plain" });
        res.end();
      }
    })();
  });

  await new Promise<void>((resolve, reject) => {
    server.once("error", reject);
    server.listen(options.port, LOOPBACK, () => {
      server.off("error", reject);
      resolve();
    });
  });
  const { port } = server.address() as AddressInfo;
  return {
    url: `http://${LOOPBACK}:${port}${MCP_PATH}`,
    notify: handler.notify,
    close: async () => {
      await handler.close();
      server.closeAllConnections();
      await new Promise<void>((resolve) => server.close(() => resolve()));
    },
  };
}
