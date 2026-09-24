/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { serveStdio, type StdioServerHandle } from "@modelcontextprotocol/server/stdio";
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "./client.js";
import * as tools from "./tools/index.js";
import { PACKAGE_VERSION } from "./version.js";
import { ALL_TOOLS, assertKnownTools, parseToolFilter, type ToolFilter } from "./tools/toolsets.js";
import { logger } from "./logger.js";
import { resolveObsPassword } from "./obs-config.js";
import { serveHttp, type HttpServerHandle } from "./http.js";
import { forwardResourceEvents } from "./tools/resources.js";

// Create the OBS WebSocket client
const obsClient = new OBSWebSocketClient(
  process.env.OBS_WEBSOCKET_URL || "ws://localhost:4455",
  resolveObsPassword(),
);

export let serverConnected = false;
export let obsConnected = false;
let stdioServer: StdioServerHandle | null = null;
let httpServer: HttpServerHandle | null = null;
let stopResourceEvents: (() => void) | null = null;
let reconnectTimer: NodeJS.Timeout | null = null;
let connectionAttempt: Promise<void> | null = null;
let reconnectAttempts = 0;
let shuttingDown = false;
let shutdownPromise: Promise<void> | null = null;
const INITIAL_RECONNECT_DELAY_MS = 1000;
const MAX_RECONNECT_DELAY_MS = 30000;
const SHUTDOWN_DEADLINE_MS = 1500;


function getReconnectDelay(): number {
  return Math.min(
    INITIAL_RECONNECT_DELAY_MS * 2 ** Math.max(0, reconnectAttempts - 1),
    MAX_RECONNECT_DELAY_MS,
  );
}

function scheduleReconnect(): void {
  if (shuttingDown || obsClient.isConnected() || reconnectTimer) return;

  const delay = getReconnectDelay();
  logger.debug(`Will retry the OBS connection in ${delay / 1000} seconds...`);
  reconnectTimer = setTimeout(() => {
    reconnectTimer = null;
    void attemptOBSConnection();
  }, delay);
  reconnectTimer.unref();
}

async function attemptOBSConnection(): Promise<void> {
  if (obsClient.isConnected()) {
    obsConnected = true;
    return;
  }

  if (connectionAttempt) return connectionAttempt;

  connectionAttempt = (async () => {
    try {
      logger.log("Attempting to connect to OBS WebSocket...");
      await obsClient.connect();
      obsConnected = true;
      reconnectAttempts = 0;
      logger.log("Connected and identified with OBS WebSocket server");
    } catch (obsError) {
      obsConnected = false;
      reconnectAttempts += 1;
      const errorMessage = obsError instanceof Error ? obsError.message : String(obsError);
      logger.error(`Failed to connect to OBS WebSocket: ${errorMessage}`);

      if (reconnectAttempts === 1) {
        logger.error("The MCP server will remain available while OBS is offline.");
        logger.error("Verify OBS is running and OBS_WEBSOCKET_URL/OBS_WEBSOCKET_PASSWORD are correct (or set OBS_MCP_READ_OBS_CONFIG=true to use the password OBS saved).");
      }

      scheduleReconnect();
    } finally {
      connectionAttempt = null;
    }
  })();

  return connectionAttempt;
}

obsClient.on("disconnected", () => {
  obsConnected = false;
  if (!shuttingDown) {
    logger.log("OBS WebSocket disconnected; scheduling a reconnect.");
    scheduleReconnect();
  }
});

// Parsed once at startup so a bad OBS_MCP_TOOLSETS/OBS_MCP_TOOLS value fails fast.
let toolFilter: ToolFilter | null = null;

export function createServer(): McpServer {
  const server = new McpServer({
    name: "obs-mcp",
    version: PACKAGE_VERSION,
  });

  tools.initialize(server, obsClient, { filter: toolFilter ?? ALL_TOOLS, sessionNotifications: !httpMode() });
  return server;
}

/** OBS_MCP_HTTP_PORT selects Streamable HTTP on 127.0.0.1 instead of stdio. */
function httpPort(): number | null {
  const value = process.env.OBS_MCP_HTTP_PORT?.trim();
  if (!value) return null;
  const port = Number(value);
  if (!Number.isInteger(port) || port < 0 || port > 65535) throw new Error(`OBS_MCP_HTTP_PORT must be a port number, not "${value}"`);
  return port;
}

function httpMode(): boolean {
  return Boolean(process.env.OBS_MCP_HTTP_PORT?.trim());
}

function loadToolFilter(): ToolFilter {
  const filter = parseToolFilter();
  const registry = tools.initialize(
    new McpServer({ name: "obs-mcp-validation", version: PACKAGE_VERSION }),
    obsClient,
    { filter, resourcesAndPrompts: false },
  );
  assertKnownTools(filter, registry);
  const count = registry.filter(({ registered }) => registered).length;
  if (count === 0) throw new Error("The tool filter excludes every tool");
  if (filter.dynamic) {
    const enabled = registry.filter(({ handle }) => handle?.enabled).length;
    logger.log(`Dynamic tool groups: ${enabled} of ${count} tools enabled at the start${filter.readOnly ? " (read-only)" : ""}`);
  } else if (filter.groups !== null || filter.readOnly) {
    logger.log(`Registering ${count} of ${registry.length} tools${filter.readOnly ? " (read-only)" : ""}`);
  }
  return filter;
}

// Set up server startup logic
export async function startServer(): Promise<void> {
  try {
    toolFilter = loadToolFilter();
    const port = httpPort();
    if (port !== null) {
      if (toolFilter.dynamic) {
        // Each HTTP request gets a fresh server, so a session cannot keep groups it turned on.
        logger.error("OBS_MCP_DYNAMIC_TOOLSETS is ignored over HTTP; registering the OBS_MCP_TOOLSETS groups instead");
        toolFilter = { ...toolFilter, dynamic: false };
      }
      httpServer = await serveHttp(createServer, { port, ...(process.env.OBS_MCP_HTTP_TOKEN ? { token: process.env.OBS_MCP_HTTP_TOKEN } : {}) });
      const notify = httpServer.notify;
      stopResourceEvents = forwardResourceEvents(obsClient, (uri) => notify.resourceUpdated(uri), () => notify.resourcesChanged());
      logger.log(`Serving MCP over HTTP at ${httpServer.url}${process.env.OBS_MCP_HTTP_TOKEN ? " (bearer token required)" : ""}`);
    } else {
      stdioServer = serveStdio(createServer, {
        onerror: (error) => logger.error(`MCP stdio error: ${error.message}`),
      });
      logger.log("Initialized MCP tools and started dual-era stdio server");
    }

    serverConnected = true;

    // Connect in the background so MCP discovery remains available while OBS is offline.
    void attemptOBSConnection();

    // stdin EOF is the only portable graceful-shutdown signal for stdio MCP
    // servers. Signals remain useful for terminals and process supervisors.
    if (stdioServer) {
      process.stdin.once("end", () => void handleShutdown("stdin end"));
      process.stdin.once("close", () => void handleShutdown("stdin close"));
    }
    process.once("SIGINT", () => void handleShutdown("SIGINT"));
    process.once("SIGTERM", () => void handleShutdown("SIGTERM"));
    
    logger.log("Server startup complete");
    
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    logger.error(`Error starting server: ${errorMessage}`);
    if (error instanceof Error && error.stack) {
      logger.error(`Stack trace: ${error.stack}`);
    }
    process.exit(1);
  }
}

// Handle graceful shutdown
function handleShutdown(reason: string): Promise<void> {
  if (shutdownPromise) return shutdownPromise;

  shuttingDown = true;
  shutdownPromise = (async () => {
    logger.log(`Shutting down after ${reason}...`);

    if (reconnectTimer) {
      clearTimeout(reconnectTimer);
      reconnectTimer = null;
    }

    const deadline = new Promise<void>((resolve) => {
      const timer = setTimeout(resolve, SHUTDOWN_DEADLINE_MS);
      timer.unref();
    });

    await Promise.race([
      Promise.allSettled([
        obsClient.disconnect(),
        stdioServer?.close() ?? Promise.resolve(),
        httpServer?.close() ?? Promise.resolve(),
      ]).then(() => undefined),
      deadline,
    ]);

    stopResourceEvents?.();
    stdioServer = null;
    httpServer = null;
    process.exit(0);
  })();

  return shutdownPromise;
}

export { obsClient };
