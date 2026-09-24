/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach } from "vitest";

// Each unit test gets its own state directory, so saved snapshots never reach
// the user's real one or leak between tests.
let directory: string | undefined;

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "obs-mcp-state-"));
  process.env.OBS_MCP_STATE_DIR = directory;
});

afterEach(() => {
  if (directory) rmSync(directory, { recursive: true, force: true });
  directory = undefined;
  delete process.env.OBS_MCP_STATE_DIR;
});
