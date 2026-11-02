/* SPDX-License-Identifier: GPL-2.0-only */
import { spawn, type ChildProcessWithoutNullStreams } from "node:child_process";
import { once } from "node:events";
import { writeFileSync } from "node:fs";
import { createServer, type AddressInfo } from "node:net";
import { join } from "node:path";
import { createInterface } from "node:readline";
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

function running(pid: number): boolean {
  try { process.kill(pid, 0); return true; }
  catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ESRCH") return false;
    throw error;
  }
}

describe("encrypted OBS endpoint admission", () => {
  it.each([
    ["ordinary", (port: number) => `wss://127.0.0.1:${port}/obs`],
    ["uppercase scheme", (port: number) => `WSS://127.0.0.1:${port}/obs`],
    ["surrounding whitespace", (port: number) => ` \tWSS://127.0.0.1:${port}/obs \n`],
  ] as const)("delegates %s before a native connection and preserves the configured endpoint", async (_label, endpoint) => {
    // ws@8.22 parses strings with new URL(address) before testing `wss:`.
    // Its predecessor accepts these spellings; retain their original bytes
    // through delegation instead of normalizing or downgrading the URL.
    let connectionAttempts = 0;
    const listener = createServer(socket => {
      connectionAttempts++;
      socket.destroy();
    });
    listener.listen({ host: "127.0.0.1", port: 0 });
    await once(listener, "listening");
    const url = endpoint((listener.address() as AddressInfo).port);
    expect(new URL(url).protocol).toBe("wss:");
    const fixture = join(process.env.OBS_MCP_STATE_DIR!, "encrypted-compat.mjs");
    writeFileSync(fixture, `
import { createInterface } from "node:readline";
const configured = process.env.OBS_WEBSOCKET_URL;
const observation = {
  encrypted: new URL(configured).protocol === "wss:",
  endpointUnchanged: configured === process.env.ENCRYPTED_TEST_EXPECTED_ENDPOINT,
};
// Report only predicates and an owned process ID; never URL or credential values.
process.stdout.write(JSON.stringify({kind:"compat-started",pid:process.pid,...observation})+"\\n");
const input=createInterface({input:process.stdin});
input.on("line",line=>{
  const request=JSON.parse(line);
  process.stdout.write(JSON.stringify({jsonrpc:"2.0",id:request.id,result:{
    compatibility:true,requestForwarded:request.method==="tools/call"&&request.params?.name==="obs-get-version",...observation
  }})+"\\n");
});
input.on("close",()=>process.exit(0));
`, { flag: "wx" });
    const child = spawn(binary, [], {
      cwd: root,
      env: {
        ...getDefaultEnvironment(),
        OBS_WEBSOCKET_URL: url,
        ENCRYPTED_TEST_EXPECTED_ENDPOINT: url,
        OBS_MCP_COMPAT_ENTRY: fixture,
        OBS_MCP_COMPAT_NODE: process.execPath,
        OBS_MCP_STATE_DIR: process.env.OBS_MCP_STATE_DIR!,
        OBS_MCP_READ_OBS_CONFIG: "false",
        OBS_MCP_TOOLSETS: "all",
        OBS_MCP_READ_ONLY: "false",
        OBS_MCP_DYNAMIC_TOOLSETS: "false",
        OBS_MCP_CONFIRM_LIVE: "false",
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    const exited = watchExit(child);
    child.stderr.resume();
    const lines = createInterface({ input: child.stdout });
    const iterator = lines[Symbol.asyncIterator]();
    let compatibilityPid: number | undefined;
    try {
      // No MCP input has been supplied. A startup marker proves selection of
      // the compatibility owner, rather than fallback after a failed read.
      const startup = await bounded(iterator.next(), "Encrypted endpoint did not select compatibility at startup");
      expect(startup.done).not.toBe(true);
      const marker = JSON.parse(startup.value!) as Record<string, unknown>;
      expect(marker).toMatchObject({ kind: "compat-started", encrypted: true, endpointUnchanged: true });
      expect(typeof marker.pid).toBe("number");
      compatibilityPid = marker.pid as number;
      expect(running(compatibilityPid)).toBe(true);
      expect(connectionAttempts).toBe(0);

      child.stdin.write(JSON.stringify({ jsonrpc: "2.0", id: 7, method: "tools/call", params: { name: "obs-get-version", arguments: {} } }) + "\n");
      const response = await bounded(iterator.next(), "Compatibility owner did not receive the request");
      expect(response.done).not.toBe(true);
      expect(JSON.parse(response.value!)).toEqual({ jsonrpc: "2.0", id: 7, result: {
        compatibility: true, requestForwarded: true, encrypted: true, endpointUnchanged: true,
      } });

      child.stdin.end();
      await bounded(exited, "Encrypted endpoint supervisor did not exit", 6_000);
      expect(child.exitCode).toBe(0);
      expect(running(compatibilityPid)).toBe(false);
      expect(connectionAttempts).toBe(0);
    } finally {
      lines.close();
      if (child.exitCode === null && child.signalCode === null) {
        child.stdin.end();
        child.kill("SIGTERM");
        try { await bounded(exited, "Owned supervisor ignored termination", 6_000); }
        catch {
          child.kill("SIGKILL");
          await bounded(exited, "Owned supervisor exit unconfirmed");
        }
      }
      if (compatibilityPid !== undefined && running(compatibilityPid)) process.kill(compatibilityPid, "SIGTERM");
      await new Promise<void>((resolve, reject) => listener.close(error => error ? reject(error) : resolve()));
    }
  });
});
