/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { READ_ONLY_TOOL } from "./request-tool.js";
import { TOOL_GROUPS, type RegisteredTool, type ToolGroup } from "./toolsets.js";

const GROUP_DESCRIPTIONS: Record<ToolGroup, string> = {
  general: "status, version, statistics, hotkeys, vendor requests, custom events, sleep",
  scenes: "scenes, canvases, program and preview scene, obs-apply-scene",
  "scene-items": "scene items, groups, transforms, ordering, locking, blend modes, snapshots and restore",
  sources: "source screenshots and active state",
  inputs: "inputs, input settings and properties, audio, deinterlacing, obs-capture-window",
  media: "media input playback",
  filters: "source filters",
  transitions: "transitions, overrides, the T-Bar",
  record: "recording, chapters, preflight, clips, watched takes, trimming, contact sheets",
  stream: "streaming and captions",
  outputs: "virtual camera, replay buffer, and generic outputs",
  config: "profiles, scene collections, video settings, persistent data",
  ui: "studio mode, dialogs, projectors",
  protocol: "describing and calling any OBS request, request batches",
};

const GroupList = z.array(z.enum(TOOL_GROUPS)).min(1);

function summary(registry: readonly RegisteredTool[]) {
  return TOOL_GROUPS.map((group) => {
    const tools = registry.filter((tool) => tool.group === group && tool.handle);
    const enabled = tools.filter(({ handle }) => handle!.enabled).length;
    return { group, enabled, total: tools.length, covers: GROUP_DESCRIPTIONS[group] };
  }).filter(({ total }) => total > 0);
}

function setGroups(registry: readonly RegisteredTool[], groups: readonly ToolGroup[], enabled: boolean): string[] {
  const changed: string[] = [];
  for (const tool of registry) {
    if (!tool.handle || !groups.includes(tool.group) || tool.handle.enabled === enabled) continue;
    if (enabled) tool.handle.enable();
    else tool.handle.disable();
    changed.push(tool.name);
  }
  return changed;
}

/**
 * With OBS_MCP_DYNAMIC_TOOLSETS, the session starts with a few groups and
 * these tools turn the others on and off. Each change sends
 * notifications/tools/list_changed, so the client reloads the tool list.
 */
export function initialize(server: McpServer, registry: readonly RegisteredTool[]): void {
  server.registerTool(
    "obs-list-toolsets",
    {
      title: "List Tool Groups",
      description: "List the groups of OBS tools, what each covers, and how many of its tools are enabled. "
        + "Enable a group with obs-enable-toolset before using its tools",
      inputSchema: z.object({}),
      annotations: READ_ONLY_TOOL,
    },
    async (): Promise<CallToolResult> => {
      const groups = summary(registry);
      return {
        content: [{
          type: "text",
          text: groups.map(({ group, enabled, total, covers }) => (
            `${enabled === total ? "on " : enabled === 0 ? "off" : "part"} ${group} (${enabled}/${total}): ${covers}`
          )).join("\n"),
        }],
        structuredContent: { groups },
      };
    },
  );

  const register = (enabled: boolean) => server.registerTool(
    enabled ? "obs-enable-toolset" : "obs-disable-toolset",
    {
      title: enabled ? "Enable Tool Groups" : "Disable Tool Groups",
      description: enabled
        ? "Turn on groups of OBS tools for this session; the client reloads its tool list. See obs-list-toolsets"
        : "Turn off groups of OBS tools for this session to keep the tool list short",
      inputSchema: z.object({ groups: GroupList.describe("Groups from obs-list-toolsets") }),
      // Read-only toward OBS: they only change which tools this session lists.
      annotations: { readOnlyHint: true, destructiveHint: false, idempotentHint: true, openWorldHint: false },
    },
    async ({ groups }): Promise<CallToolResult> => {
      const changed = setGroups(registry, groups, enabled);
      const verb = enabled ? "Enabled" : "Disabled";
      return {
        content: [{
          type: "text",
          text: changed.length > 0
            ? `${verb} ${changed.length} tool(s) in ${groups.join(", ")}: ${changed.join(", ")}`
            : `Nothing to change: ${groups.join(", ")} ${enabled ? "already on" : "already off"}`,
        }],
        structuredContent: { groups, changed },
      };
    },
  );
  register(true);
  register(false);
}
