/* SPDX-License-Identifier: MIT */
import { nativeDefaultFixture } from "../support/native-default-fixture.js";
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { createServer, type AddressInfo } from "node:net";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { describe, expect, it } from "vitest";

const root = fileURLToPath(new URL("../../", import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, "native/target/release/obs-mcp-native");

async function bounded<T>(work: Promise<T>, message: string, milliseconds = 4_000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_resolve, reject) => {
      timer = setTimeout(() => reject(new Error(message)), milliseconds);
    })]);
  } finally { clearTimeout(timer); }
}

function watchExit(child: ChildProcessWithoutNullStreams) {
  return new Promise<void>(resolve => {
    child.once("close", () => resolve());
    child.once("error", () => resolve());
  });
}

// Feature-off negative admission only. Native TLS certificate/hostname and
// secure-WebSocket qualification belong to the platform TLS suite.
describe("unsupported native encrypted OBS endpoint admission", () => {
  it.each([
    ["ordinary", (port: number) => `wss://127.0.0.1:${port}/obs`],
    ["uppercase scheme", (port: number) => `WSS://127.0.0.1:${port}/obs`],
    ["surrounding whitespace", (port: number) => ` \tWSS://127.0.0.1:${port}/obs \n`],
  ] as const)("refuses %s before any native connection, plaintext downgrade or JS fallback", async (_label, endpoint) => {
    let connectionAttempts = 0;
    const listener = createServer(socket => { connectionAttempts++; socket.destroy(); });
    listener.listen({ host: "127.0.0.1", port: 0 }); await once(listener, "listening");
    const url = endpoint((listener.address() as AddressInfo).port);
    expect(new URL(url).protocol).toBe("wss:");
    const child = spawn(binary, [], { cwd: root, env: {
      ...getDefaultEnvironment(), ...nativeDefaultFixture(root), OBS_WEBSOCKET_URL: url,
      OBS_MCP_TOOLSETS: "all", OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false",
    }, stdio: ["pipe", "pipe", "pipe"] });
    const exited = watchExit(child); let stdout = "", stderr = "";
    child.stdout.on("data", bytes => { stdout += String(bytes); });
    child.stderr.on("data", bytes => { stderr += String(bytes); });
    child.stdin.on("error", () => undefined);
    try {
      // No MCP input, OBS authentication, or method bytes may cross the endpoint.
      await bounded(exited, "Unsupported native encrypted endpoint did not refuse startup");
      expect(child.exitCode).toBe(1); expect(stdout).toBe("");
      expect(stderr.trim()).toBe("OBS MCP native: Native configuration requires a supported WS/WSS endpoint; unavailable transports cannot activate");
      expect(stderr).not.toContain(url); expect(stderr).not.toContain("missing-sdk.js");
      expect(connectionAttempts).toBe(0);
    } finally {
      if (child.exitCode === null && child.signalCode === null) {
        child.stdin.end(); child.kill("SIGTERM");
        await bounded(exited, "Owned native child exit unconfirmed");
      }
      await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
    }
  });
});
