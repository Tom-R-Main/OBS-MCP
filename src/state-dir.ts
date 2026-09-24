/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { homedir } from "node:os";
import { join } from "node:path";

/**
 * Where the server keeps state that should outlive one process, such as
 * snapshots: OBS_MCP_STATE_DIR, or the platform's per-user app data folder.
 */
export function stateDirectory(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
  home = homedir(),
): string {
  if (env.OBS_MCP_STATE_DIR) return env.OBS_MCP_STATE_DIR;
  if (platform === "darwin") return join(home, "Library", "Application Support", "obs-mcp");
  if (platform === "win32") return join(env.LOCALAPPDATA ?? join(home, "AppData", "Local"), "obs-mcp");
  return join(env.XDG_STATE_HOME || join(home, ".local", "state"), "obs-mcp");
}
