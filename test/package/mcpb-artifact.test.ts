/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { constants } from "node:fs";
import { access, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { unpackExtension, validateManifest } from "@anthropic-ai/mcpb";
import { afterEach, describe, expect, it, vi } from "vitest";
import { listTools } from "../../scripts/list-tools.mjs";
import { FakeOBSServer } from "../support/fake-obs-server.js";
import { TOOL_COUNT } from "../support/tool-count.js";

type ArtifactManifest = {
  version: string;
  tools: Array<{ name: string; description?: string }>;
};

type PackageMetadata = { version: string };

const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const artifactPath = path.join(projectRoot, "dist/obs-studio.mcpb");
const temporaryDirectories: string[] = [];
const clients: Client[] = [];
const fakeServers: FakeOBSServer[] = [];

async function waitForObsConnected(client: Client): Promise<void> {
  await vi.waitFor(async () => {
    const status = await client.callTool({ name: "obs-get-status", arguments: {} });
    const text = status.content
      .filter((item): item is { type: "text"; text: string } => item.type === "text")
      .map(({ text }) => text)
      .join("\n");
    expect(text).toContain('"connected": true');
  }, { timeout: 3_000 });
}

afterEach(async () => {
  await Promise.allSettled(clients.splice(0).map((client) => client.close()));
  await Promise.allSettled(fakeServers.splice(0).map((server) => server.close()));
  await Promise.allSettled(
    temporaryDirectories.splice(0).map((directory) => rm(directory, { recursive: true, force: true })),
  );
});

describe("MCPB artifact", () => {
  it("contains the published source and runs from the extracted package", async () => {
    await access(artifactPath, constants.R_OK);
    const extractionRoot = await mkdtemp(path.join(tmpdir(), "obs-mcp-artifact-"));
    temporaryDirectories.push(extractionRoot);
    const extracted = path.join(extractionRoot, "extension");

    await expect(unpackExtension({
      mcpbPath: artifactPath,
      outputDir: extracted,
      silent: true,
    })).resolves.toBe(true);
    expect(validateManifest(path.join(extracted, "manifest.json"))).toBe(true);

    const manifest = JSON.parse(
      await readFile(path.join(extracted, "manifest.json"), "utf8"),
    ) as ArtifactManifest;
    const packageMetadata = JSON.parse(
      await readFile(path.join(projectRoot, "package.json"), "utf8"),
    ) as PackageMetadata;
    const sourceTools = await listTools(projectRoot);
    const artifactToolNames = manifest.tools.map(({ name }) => name);

    expect(manifest.version).toBe(packageMetadata.version);
    expect(artifactToolNames).toHaveLength(TOOL_COUNT);
    expect(new Set(artifactToolNames).size).toBe(TOOL_COUNT);
    expect(artifactToolNames).toEqual(sourceTools.map(({ name }) => name));

    for (const relativePath of [
      "LICENSE",
      "NOTICE.md",
      "package-lock.json",
      "src",
      "scripts",
      "tsconfig.json",
      "docs/protocol.json",
      "server/index.js",
      "node_modules/ws/package.json",
      "node_modules/@modelcontextprotocol/server/package.json",
    ]) {
      await expect(access(path.join(extracted, relativePath), constants.R_OK)).resolves.toBeUndefined();
    }
    await expect(access(path.join(extracted, "server/index.js"), constants.X_OK)).resolves.toBeUndefined();

    const fake = await FakeOBSServer.start();
    fakeServers.push(fake);
    const client = new Client(
      { name: "obs-mcp-artifact-test", version: "1.0.0" },
      { versionNegotiation: { mode: { pin: "2026-07-28" } } },
    );
    const transport = new StdioClientTransport({
      command: process.execPath,
      args: [path.join(extracted, "server/index.js")],
      cwd: extracted,
      env: {
        ...getDefaultEnvironment(),
        OBS_WEBSOCKET_URL: fake.url,
      },
      stderr: "pipe",
    });
    transport.stderr?.on("data", () => undefined);
    await client.connect(transport);
    clients.push(client);

    expect((await client.listTools()).tools).toHaveLength(TOOL_COUNT);
    await waitForObsConnected(client);
    const version = await client.callTool({ name: "obs-get-version", arguments: {} });
    expect(version.isError).not.toBe(true);

    await client.close();
    clients.splice(clients.indexOf(client), 1);
    await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0));
  });
});
