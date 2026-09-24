/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { obsWebSocketConfigPaths, readObsConfigPassword, resolveObsPassword } from "./obs-config.js";

const SECRET = "s3cret-from-obs";
let directory: string;
let stderr: ReturnType<typeof vi.spyOn>;

function configFile(contents: string): string {
  const path = join(directory, `config-${Math.random()}.json`);
  writeFileSync(path, contents);
  return path;
}

function logged(): string {
  return stderr.mock.calls.map((call: unknown[]) => String(call[0])).join("\n");
}

beforeEach(() => {
  directory = mkdtempSync(join(tmpdir(), "obs-mcp-config-"));
  stderr = vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(() => {
  rmSync(directory, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("obsWebSocketConfigPaths", () => {
  it("follows each platform's OBS config directory", () => {
    expect(obsWebSocketConfigPaths({}, "darwin", "/Users/me")).toEqual([
      "/Users/me/Library/Application Support/obs-studio/plugin_config/obs-websocket/config.json",
    ]);
    expect(obsWebSocketConfigPaths({ APPDATA: "C:/Users/me/AppData/Roaming" }, "win32", "C:/Users/me")[0])
      .toContain(join("C:/Users/me/AppData/Roaming", "obs-studio"));
    expect(obsWebSocketConfigPaths({ XDG_CONFIG_HOME: "/xdg" }, "linux", "/home/me")).toEqual([
      "/xdg/obs-studio/plugin_config/obs-websocket/config.json",
      "/home/me/.var/app/com.obsproject.Studio/config/obs-studio/plugin_config/obs-websocket/config.json",
    ]);
  });
});

describe("readObsConfigPassword", () => {
  it("returns the first readable password", () => {
    const path = configFile(JSON.stringify({ auth_required: true, server_password: SECRET }));

    expect(readObsConfigPassword([join(directory, "missing.json"), path]))
      .toEqual({ kind: "password", password: SECRET, path });
  });

  it("reports that authentication is off", () => {
    const path = configFile(JSON.stringify({ auth_required: false, server_password: SECRET }));

    expect(readObsConfigPassword([path])).toEqual({ kind: "no-auth", path });
  });

  it("skips malformed files and empty passwords", () => {
    const paths = [configFile("{not json"), configFile(JSON.stringify({ server_password: "" }))];

    expect(readObsConfigPassword(paths)).toEqual({ kind: "not-found", searched: paths });
  });
});

describe("resolveObsPassword", () => {
  const found = () => ({ kind: "password" as const, password: SECRET, path: "/obs/config.json" });

  it("prefers OBS_WEBSOCKET_PASSWORD and ignores the config file", () => {
    const read = vi.fn(found);

    expect(resolveObsPassword({ OBS_WEBSOCKET_PASSWORD: "env", OBS_MCP_READ_OBS_CONFIG: "1" }, read)).toBe("env");
    expect(read).not.toHaveBeenCalled();
  });

  it("does not read OBS's config unless asked to", () => {
    const read = vi.fn(found);

    expect(resolveObsPassword({}, read)).toBeNull();
    expect(read).not.toHaveBeenCalled();
  });

  it("uses OBS's password when enabled, logging only where it came from", () => {
    expect(resolveObsPassword({ OBS_MCP_READ_OBS_CONFIG: "true" }, found)).toBe(SECRET);
    expect(logged()).toContain("/obs/config.json");
    expect(logged()).not.toContain(SECRET);
  });

  it("explains where it looked when nothing was found", () => {
    const result = resolveObsPassword(
      { OBS_MCP_READ_OBS_CONFIG: "yes" },
      () => ({ kind: "not-found", searched: ["/a.json", "/b.json"] }),
    );

    expect(result).toBeNull();
    expect(logged()).toContain("/a.json, /b.json");
  });
});
