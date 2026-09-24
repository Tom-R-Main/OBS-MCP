/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */

// stdout carries the MCP stdio protocol, so every diagnostic goes to stderr.
// MCP logging notifications are deprecated as of the 2026-07-28 spec.

const LEVELS = { debug: 10, info: 20, error: 30, silent: 100 } as const;
type LogLevel = keyof typeof LEVELS;

function configuredLevel(value = process.env.OBS_MCP_LOG_LEVEL): number {
  const normalized = value?.trim().toLowerCase();
  return normalized && normalized in LEVELS ? LEVELS[normalized as LogLevel] : LEVELS.info;
}

function write(level: Exclude<LogLevel, "silent">, message: string): void {
  if (LEVELS[level] < configuredLevel()) return;
  console.error(`[obs-mcp] ${level === "info" ? "" : `${level}: `}${message}`);
}

export const logger = {
  debug: (message: string) => write("debug", message),
  log: (message: string) => write("info", message),
  error: (message: string) => write("error", message),
};
