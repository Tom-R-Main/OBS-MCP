/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { McpServer, ToolAnnotations } from "@modelcontextprotocol/server";

export const TOOL_GROUPS = [
  "general",
  "scenes",
  "scene-items",
  "sources",
  "inputs",
  "media",
  "filters",
  "transitions",
  "record",
  "stream",
  "outputs",
  "config",
  "ui",
  "protocol",
] as const;

export type ToolGroup = typeof TOOL_GROUPS[number];

/** The small set recommended for recording and scene work: `OBS_MCP_TOOLSETS=core`. */
export const CORE_GROUPS: readonly ToolGroup[] = [
  "general",
  "scenes",
  "scene-items",
  "sources",
  "inputs",
  "record",
];

/** Groups enabled at the start with OBS_MCP_DYNAMIC_TOOLSETS and no OBS_MCP_TOOLSETS. */
export const DYNAMIC_START_GROUPS: readonly ToolGroup[] = ["general"];

export type ToolFilter = {
  /** null registers every group. With dynamic, the groups enabled at the start. */
  groups: ReadonlySet<ToolGroup> | null;
  tools: ReadonlySet<string>;
  readOnly: boolean;
  /** Register every group but enable only `groups` and `tools`; the model enables more at runtime. */
  dynamic?: boolean;
};

export const ALL_TOOLS: ToolFilter = { groups: null, tools: new Set(), readOnly: false };

/** Enables or disables a registered tool; the SDK tells connected clients the list changed. */
export type ToolHandle = { enabled: boolean; enable(): void; disable(): void };

export type RegisteredTool = {
  name: string;
  group: ToolGroup;
  annotations: ToolAnnotations | undefined;
  registered: boolean;
  /** Present for registered tools; with dynamic toolsets, disabled ones can be enabled later. */
  handle?: ToolHandle;
};

function splitList(value: string | undefined): string[] {
  return (value ?? "").split(",").map((entry) => entry.trim()).filter(Boolean);
}

function isToolGroup(value: string): value is ToolGroup {
  return (TOOL_GROUPS as readonly string[]).includes(value);
}

/**
 * Reads OBS_MCP_TOOLSETS, OBS_MCP_TOOLS, and OBS_MCP_READ_ONLY. With neither
 * list set every tool is registered, matching earlier releases. Unknown group
 * names throw so a typo cannot silently hide tools.
 */
export function parseToolFilter(env: NodeJS.ProcessEnv = process.env): ToolFilter {
  const groupNames = splitList(env.OBS_MCP_TOOLSETS);
  const tools = new Set(splitList(env.OBS_MCP_TOOLS));
  const readOnly = ["1", "true", "yes"].includes((env.OBS_MCP_READ_ONLY ?? "").trim().toLowerCase());
  const dynamic = ["1", "true", "yes"].includes((env.OBS_MCP_DYNAMIC_TOOLSETS ?? "").trim().toLowerCase());

  if (dynamic && groupNames.length === 0) {
    return { groups: new Set(DYNAMIC_START_GROUPS), tools, readOnly, dynamic };
  }
  if (groupNames.length === 0 && tools.size === 0) return { groups: null, tools, readOnly, dynamic };
  if (groupNames.includes("all")) return { groups: null, tools, readOnly, dynamic };

  const groups = new Set<ToolGroup>();
  for (const name of groupNames) {
    if (name === "core") {
      CORE_GROUPS.forEach((group) => groups.add(group));
    } else if (isToolGroup(name)) {
      groups.add(name);
    } else {
      throw new Error(
        `Unknown OBS_MCP_TOOLSETS entry "${name}". Use all, core, or: ${TOOL_GROUPS.join(", ")}`,
      );
    }
  }
  return { groups, tools, readOnly, dynamic };
}

/** Whether a tool is registered: with dynamic toolsets, every tool read-only mode allows. */
export function registersTool(filter: ToolFilter, tool: Pick<RegisteredTool, "name" | "group" | "annotations">): boolean {
  if (filter.readOnly && tool.annotations?.readOnlyHint !== true) return false;
  return filter.dynamic ? true : includesTool(filter, tool);
}

/** Whether a tool is registered, or with dynamic toolsets, enabled at the start. */
export function includesTool(filter: ToolFilter, tool: Pick<RegisteredTool, "name" | "group" | "annotations">): boolean {
  // Read-only mode wins over explicit groups and tool names.
  if (filter.readOnly && tool.annotations?.readOnlyHint !== true) return false;
  if (filter.groups === null) return true;
  return filter.groups.has(tool.group) || filter.tools.has(tool.name);
}

/** Throws when OBS_MCP_TOOLS names a tool that does not exist. */
export function assertKnownTools(filter: ToolFilter, tools: readonly RegisteredTool[]): void {
  const known = new Set(tools.map(({ name }) => name));
  const unknown = [...filter.tools].filter((name) => !known.has(name));
  if (unknown.length > 0) {
    throw new Error(`Unknown OBS_MCP_TOOLS entries: ${unknown.join(", ")}`);
  }
}

/**
 * Tools whose module does not match their group. protocol-extensions holds
 * requests added after the original modules, across several areas.
 */
const GROUP_OVERRIDES: Record<string, ToolGroup> = {
  "obs-get-canvas-list": "scenes",
  "obs-set-scene-name": "scenes",
  "obs-get-input-audio-tracks": "inputs",
  "obs-set-input-audio-tracks": "inputs",
  "obs-get-input-deinterlace-field-order": "inputs",
  "obs-set-input-deinterlace-field-order": "inputs",
  "obs-get-input-deinterlace-mode": "inputs",
  "obs-set-input-deinterlace-mode": "inputs",
  "obs-get-input-property-list-items": "inputs",
  "obs-press-input-property-button": "inputs",
  "obs-duplicate-scene-item": "scene-items",
  "obs-get-group-scene-items": "scene-items",
  "obs-get-group-list": "scene-items",
  "obs-get-scene-item-enabled": "scene-items",
  "obs-get-scene-item-index": "scene-items",
  "obs-set-scene-item-index": "scene-items",
  "obs-get-scene-item-locked": "scene-items",
  "obs-set-scene-item-locked": "scene-items",
  "obs-get-scene-item-blend-mode": "scene-items",
  "obs-set-scene-item-blend-mode": "scene-items",
  "obs-get-scene-item-source": "scene-items",
  "obs-get-scene-transition-override": "transitions",
  "obs-set-scene-transition-override": "transitions",
  "obs-get-transition-kind-list": "transitions",
  "obs-get-transition-cursor": "transitions",
  "obs-set-tbar-position": "transitions",
  "obs-capture-window": "inputs",
};

/**
 * Wraps tool registration for one module: records every tool with its group
 * and registers only those the filter includes. Other McpServer methods stay
 * bound to the real server.
 */
export function scopedServer(
  server: McpServer,
  group: ToolGroup,
  filter: ToolFilter,
  registry: RegisteredTool[],
): McpServer {
  return new Proxy(server, {
    get(target, property) {
      if (property === "registerTool") {
        return (...registrationArgs: unknown[]) => {
          const [name, config] = registrationArgs;
          if (typeof name !== "string") throw new TypeError("MCP tool registration requires a name");
          const annotations = (config as { annotations?: ToolAnnotations } | undefined)?.annotations;
          const tool = { name, group: GROUP_OVERRIDES[name] ?? group, annotations };
          const registered = registersTool(filter, tool);
          if (!registered) {
            registry.push({ ...tool, registered });
            return undefined;
          }
          const handle = Reflect.apply(Reflect.get(target, property, target) as Function, target, registrationArgs) as ToolHandle;
          // Disabled before the session connects, so no list-changed notification is sent.
          if (filter.dynamic && !includesTool(filter, tool)) handle.disable();
          registry.push({ ...tool, registered, handle });
          return handle;
        };
      }
      const value: unknown = Reflect.get(target, property, target);
      return typeof value === "function" ? value.bind(target) : value;
    },
  });
}
