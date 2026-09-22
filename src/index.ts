#!/usr/bin/env node

/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */

import { startServer } from "./server.js";
import { logger } from "./logger.js";


// Set up better error handling
process.on("uncaughtException", (error) => {
  logger.error(`Uncaught exception: ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});

process.on("unhandledRejection", (reason, promise) => {
  logger.error(`Unhandled rejection at: ${promise}, reason: ${reason}`);
  process.exit(1);
});

// Start the server
startServer().catch((error) => {
  logger.error(`Fatal error in main(): ${error instanceof Error ? error.message : String(error)}`);
  process.exit(1);
});
