/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import { parseToolFilter } from "./toolsets.js";

let harness: McpHarness | undefined;
let listChanges = 0;

async function start(env: Record<string, string>): Promise<McpHarness> {
  harness = await startMcpHarness({ filter: parseToolFilter(env) });
  harness.mcpClient.setNotificationHandler("notifications/tools/list_changed", async () => {
    listChanges += 1;
  });
  harness.fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: false }));
  return harness;
}

async function toolNames(): Promise<string[]> {
  return (await harness!.mcpClient.listTools()).tools.map(({ name }) => name);
}

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  listChanges = 0;
});

afterEach(async () => {
  await harness?.close();
  harness = undefined;
  vi.restoreAllMocks();
});

describe("dynamic tool groups", () => {
  it("parses OBS_MCP_DYNAMIC_TOOLSETS, starting with general unless groups are named", () => {
    expect(parseToolFilter({ OBS_MCP_DYNAMIC_TOOLSETS: "true" })).toMatchObject({ dynamic: true, groups: new Set(["general"]) });
    expect(parseToolFilter({ OBS_MCP_DYNAMIC_TOOLSETS: "1", OBS_MCP_TOOLSETS: "record" }).groups).toEqual(new Set(["record"]));
    expect(parseToolFilter({}).dynamic).toBe(false);
  });

  it("starts small, then enables and disables groups with a list-changed notification each time", async () => {
    await start({ OBS_MCP_DYNAMIC_TOOLSETS: "true" });
    const initial = await toolNames();
    expect(initial).toEqual(expect.arrayContaining(["obs-get-status", "obs-list-toolsets", "obs-enable-toolset", "obs-disable-toolset"]));
    expect(initial).not.toContain("obs-get-record-status");
    expect(initial.length).toBeLessThan(30);

    const list = await harness!.call("obs-list-toolsets");
    expect(resultText(list)).toMatch(/^on  general \(\d+\/\d+\)/m);
    expect(resultText(list)).toMatch(/^off record \(0\/\d+\)/m);

    const enabled = await harness!.call("obs-enable-toolset", { groups: ["record"] });
    expect(resultText(enabled)).toMatch(/Enabled \d+ tool\(s\) in record/);
    await vi.waitFor(() => expect(listChanges).toBeGreaterThan(0));
    expect(await toolNames()).toContain("obs-get-record-status");
    expect((await harness!.call("obs-get-record-status")).isError).toBeFalsy();

    await harness!.call("obs-disable-toolset", { groups: ["record"] });
    expect(await toolNames()).not.toContain("obs-get-record-status");
    await expect(harness!.call("obs-get-record-status")).rejects.toThrow("disabled");
    expect(resultText(await harness!.call("obs-disable-toolset", { groups: ["record"] }))).toContain("already off");
  });

  it("enables only read-only tools in read-only mode", async () => {
    await start({ OBS_MCP_DYNAMIC_TOOLSETS: "true", OBS_MCP_READ_ONLY: "true" });

    await harness!.call("obs-enable-toolset", { groups: ["record"] });

    const names = await toolNames();
    expect(names).toContain("obs-get-record-status");
    expect(names).not.toContain("obs-start-record");
  });

  it("adds no group tools without OBS_MCP_DYNAMIC_TOOLSETS", async () => {
    await start({});

    expect(await toolNames()).not.toContain("obs-enable-toolset");
  });
});
