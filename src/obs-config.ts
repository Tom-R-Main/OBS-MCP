/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { readFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { logger } from "./logger.js";

const CONFIG_SUBPATH = ["obs-studio", "plugin_config", "obs-websocket", "config.json"];

function isEnabled(value: string | undefined): boolean {
  return ["1", "true", "yes"].includes((value ?? "").trim().toLowerCase());
}

/**
 * Where obs-websocket keeps config.json, most likely first. OBS stores it
 * under its user config directory (os_get_config_path), and the Linux
 * Flatpak build keeps that inside its sandbox.
 */
export function obsWebSocketConfigPaths(
  env: NodeJS.ProcessEnv = process.env,
  platform: NodeJS.Platform = process.platform,
  home = homedir(),
): string[] {
  if (platform === "darwin") return [join(home, "Library", "Application Support", ...CONFIG_SUBPATH)];
  if (platform === "win32") {
    return [join(env.APPDATA ?? join(home, "AppData", "Roaming"), ...CONFIG_SUBPATH)];
  }
  return [
    join(env.XDG_CONFIG_HOME || join(home, ".config"), ...CONFIG_SUBPATH),
    join(home, ".var", "app", "com.obsproject.Studio", "config", ...CONFIG_SUBPATH),
  ];
}

export type ObsConfigPassword =
  | { kind: "password"; password: string; path: string }
  | { kind: "no-auth"; path: string }
  | { kind: "not-found"; searched: string[] };

type ReadFile = (path: string) => string;

/**
 * Reads the password OBS itself uses from obs-websocket's config.json.
 * Unreadable or malformed files count as not found; the error text never
 * includes file contents.
 */
export function readObsConfigPassword(
  paths: readonly string[] = obsWebSocketConfigPaths(),
  readFile: ReadFile = (path) => readFileSync(path, "utf8"),
): ObsConfigPassword {
  for (const path of paths) {
    let config: unknown;
    try {
      config = JSON.parse(readFile(path));
    } catch {
      continue;
    }
    if (typeof config !== "object" || config === null) continue;
    const { auth_required: authRequired, server_password: password } = config as Record<string, unknown>;
    if (authRequired === false) return { kind: "no-auth", path };
    if (typeof password === "string" && password.length > 0) return { kind: "password", password, path };
  }
  return { kind: "not-found", searched: [...paths] };
}

/**
 * OBS_WEBSOCKET_PASSWORD wins. Otherwise, with OBS_MCP_READ_OBS_CONFIG
 * enabled, falls back to the password in OBS's own config so a local agent
 * needs no copy of it. Logs where the password came from, never the value.
 */
export function resolveObsPassword(
  env: NodeJS.ProcessEnv = process.env,
  read: () => ObsConfigPassword = () => readObsConfigPassword(obsWebSocketConfigPaths(env)),
): string | null {
  if (env.OBS_WEBSOCKET_PASSWORD) return env.OBS_WEBSOCKET_PASSWORD;
  if (!isEnabled(env.OBS_MCP_READ_OBS_CONFIG)) return null;

  const result = read();
  switch (result.kind) {
    case "password":
      logger.log(`Using the OBS WebSocket password from ${result.path}`);
      return result.password;
    case "no-auth":
      logger.log(`OBS WebSocket authentication is off according to ${result.path}`);
      return null;
    case "not-found":
      logger.error(`OBS_MCP_READ_OBS_CONFIG is set, but no OBS WebSocket password was found in: ${result.searched.join(", ")}`);
      return null;
  }
}
