/* SPDX-License-Identifier: GPL-2.0-only */
import { nativeDefaultFixture } from "../support/native-default-fixture.js";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it } from "vitest";
import { obsWebSocketConfigPaths } from "../../src/obs-config.js";
import { FakeOBSServer } from "../support/fake-obs-server.js";
const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer; directory: string }[] = [];
function children(pid: number) {
  return execFileSync("ps", ["-axo", "pid=,ppid="], { encoding: "utf8" }).trim().split("\n").map(line => line.trim().split(/\s+/).map(Number)).filter(([, parent]) => parent === pid).map(([id]) => id);
}
afterEach(async () => {
  for (const session of sessions.splice(0)) { try { await session.client.close(); } finally { try { await session.transport.close(); } finally { await session.fake.close(); rmSync(session.directory, { recursive: true, force: true }); } } }
});
describe.each([false, true])("native default authentication configuration (legacy=%s)", legacy => {
  it.each(["saved", "explicit", "disabled", "no-auth", "oversized", "malformed"])("preserves %s configuration and keeps secret values out of logs", async mode => {
    const saved = "TEST_ONLY_SAVED_PASSWORD"; const explicit = "TEST_ONLY_EXPLICIT_PASSWORD";
    const fake = await FakeOBSServer.start({ password: mode === "saved" || mode === "oversized" ? saved : mode === "explicit" ? explicit : undefined });
    const nativeEnv = nativeDefaultFixture(root);
    const directory = mkdtempSync(join(nativeEnv.OBS_MCP_STATE_DIR!, "config-native-"));
    const state = join(directory, "state"); mkdirSync(state, { mode: 0o700 });
    const env = { ...getDefaultEnvironment(), ...nativeEnv, HOME: directory, USERPROFILE: directory, APPDATA: join(directory, "Roaming"), XDG_CONFIG_HOME: join(directory, "config"), OBS_MCP_STATE_DIR: state,
      OBS_WEBSOCKET_URL: fake.url,
      OBS_MCP_READ_OBS_CONFIG: mode === "disabled" ? "false" : "true", OBS_WEBSOCKET_PASSWORD: mode === "explicit" ? explicit : "",
      OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "", OBS_MCP_READ_ONLY: "false", OBS_MCP_CONFIRM_LIVE: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false" };
    const path = obsWebSocketConfigPaths(env, process.platform, directory)[0]!; mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, mode === "malformed" ? "{incomplete" : JSON.stringify({ auth_required: mode !== "no-auth", server_password: saved, ...(mode === "oversized" ? { padding: "x".repeat(256 * 1024) } : {}) }));
    const client = new Client({ name: "native-config-parity", version: "1" }, { versionNegotiation: { mode: legacy ? "legacy" : { pin: "2026-07-28" } } });
    const transport = new StdioClientTransport({ command: binary, env, cwd: root, stderr: "pipe" });
    let stderr = ""; transport.stderr?.on("data", chunk => { stderr += String(chunk); });
    sessions.push({ client, transport, fake, directory });
    if (mode === "oversized" || mode === "malformed") {
      // Startup refusal is era-independent; capture the actual process rather
      // than a disposable SDK negotiation probe's disconnected stderr pipe.
      const refusal = spawnSync(binary, [], { cwd: root, env, encoding: "utf8", timeout: 5000 });
      expect(refusal.error).toBeUndefined(); expect(refusal.status).toBe(1); expect(refusal.stdout).toBe("");
      expect(refusal.stderr).toContain("Native authentication configuration is unavailable");
      expect(refusal.stderr).not.toContain(saved); expect(refusal.stderr).not.toContain(explicit);
      expect(fake.history()).toEqual([]); expect(fake.connectionCount).toBe(0);
      return;
    }
    await client.connect(transport);
    const result = await client.callTool({ name: "obs-get-version", arguments: {} }); expect(result.isError).not.toBe(true);
    expect(result.structuredContent).toMatchObject({ obsVersion: "32.2.2" });
    expect(children(transport.pid!)).toHaveLength(0);
    expect(stderr).not.toContain(saved); expect(stderr).not.toContain(explicit);
    expect(fake.history().filter(({ frame }) => frame.op === 6).every(({ frame }) => frame.d.requestType === "GetVersion")).toBe(true);
  });
});
