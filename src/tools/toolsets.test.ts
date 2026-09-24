/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { describe, expect, it } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { TOOL_COUNT } from "../../test/support/tool-count.js";
import { initialize } from "./index.js";
import { CORE_GROUPS, TOOL_GROUPS, assertKnownTools, parseToolFilter, type ToolFilter } from "./toolsets.js";

function registry(filter?: ToolFilter) {
  return initialize(
    new McpServer({ name: "obs-mcp-test", version: "0.0.0" }),
    new OBSWebSocketClient("ws://127.0.0.1:1"),
    { filter, resourcesAndPrompts: false },
  );
}

function registeredNames(filter: ToolFilter): string[] {
  return registry(filter).filter(({ registered }) => registered).map(({ name }) => name);
}

describe("parseToolFilter", () => {
  it("registers every tool when nothing is configured", () => {
    expect(parseToolFilter({})).toEqual({ groups: null, tools: new Set(), readOnly: false, dynamic: false });
    expect(parseToolFilter({ OBS_MCP_TOOLSETS: "all,scenes" }).groups).toBeNull();
  });

  it("expands core and accepts individual tools", () => {
    const filter = parseToolFilter({ OBS_MCP_TOOLSETS: " core , stream ", OBS_MCP_TOOLS: "obs-describe-request" });

    expect([...filter.groups ?? []].sort()).toEqual([...CORE_GROUPS, "stream"].sort());
    expect([...filter.tools]).toEqual(["obs-describe-request"]);
  });

  it("rejects unknown groups", () => {
    expect(() => parseToolFilter({ OBS_MCP_TOOLSETS: "scenez" })).toThrow(/Unknown OBS_MCP_TOOLSETS entry "scenez"/);
  });

  it("reads OBS_MCP_READ_ONLY", () => {
    expect(parseToolFilter({ OBS_MCP_READ_ONLY: "true" }).readOnly).toBe(true);
    expect(parseToolFilter({ OBS_MCP_READ_ONLY: "0" }).readOnly).toBe(false);
  });
});

describe("tool registry", () => {
  it("assigns every tool to a group and registers all of them by default", () => {
    const tools = registry();

    expect(tools).toHaveLength(TOOL_COUNT);
    expect(tools.every(({ registered }) => registered)).toBe(true);
    expect(new Set(tools.map(({ group }) => group))).toEqual(new Set(TOOL_GROUPS));
  });

  it("registers only the chosen groups plus named tools", () => {
    const names = registeredNames(parseToolFilter({ OBS_MCP_TOOLSETS: "record", OBS_MCP_TOOLS: "obs-get-scene-list" }));

    expect(names).toEqual(expect.arrayContaining([
      "obs-start-record",
      "obs-stop-record",
      "obs-preflight",
      "obs-get-scene-list",
    ]));
    expect(names).not.toContain("obs-start-stream");
    expect(names).not.toContain("obs-set-current-scene");
  });

  it("keeps the core set small", () => {
    const names = registeredNames(parseToolFilter({ OBS_MCP_TOOLSETS: "core" }));

    expect(names.length).toBeLessThan(TOOL_COUNT * 0.6);
    expect(names).toEqual(expect.arrayContaining(["obs-get-status", "obs-preflight", "obs-get-source-screenshot"]));
  });

  it("registers only read-only tools in read-only mode, even when named explicitly", () => {
    const tools = registry(parseToolFilter({ OBS_MCP_READ_ONLY: "1", OBS_MCP_TOOLSETS: "record", OBS_MCP_TOOLS: "obs-start-record" }));
    const registered = tools.filter((tool) => tool.registered);

    expect(registered.length).toBeGreaterThan(0);
    expect(registered.every(({ annotations }) => annotations?.readOnlyHint === true)).toBe(true);
    expect(registered.map(({ name }) => name)).not.toContain("obs-start-record");
    expect(registered.map(({ name }) => name)).toContain("obs-get-record-status");
  });

  it("rejects unknown tool names", () => {
    const filter = parseToolFilter({ OBS_MCP_TOOLS: "obs-start-recording" });

    expect(() => assertKnownTools(filter, registry(filter))).toThrow(/obs-start-recording/);
  });
});
