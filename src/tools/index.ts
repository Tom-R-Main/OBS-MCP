/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { McpServer } from "@modelcontextprotocol/server";
import { OBSWebSocketClient } from "../client.js";

// Import specific tool modules
import * as general from "./general.js";
import * as scenes from "./scenes.js";
import * as sources from "./sources.js";
import * as sceneItems from "./scene-items.js";
import * as streaming from "./streaming.js";
import * as transitions from "./transitions.js";
import * as config from "./config.js";
import * as filters from "./filters.js";
import * as inputs from "./inputs.js";
import * as mediaInputs from "./media-inputs.js";
import * as outputs from "./outputs.js";
import * as record from "./record.js";
import * as ui from "./ui.js";
import * as protocolExtensions from "./protocol-extensions.js";
import * as protocol from "./protocol.js";
import * as preflight from "./preflight.js";
import * as workflows from "./workflows.js";
import * as takes from "./takes.js";
import * as trim from "./trim.js";
import * as snapshots from "./snapshots.js";
import * as resources from "./resources.js";
import * as prompts from "./prompts.js";
import { withStructuredToolResults } from "./results.js";
import { liveConfirmationEnabled, withLiveConfirmation } from "./confirm.js";
import { ALL_TOOLS, scopedServer, type RegisteredTool, type ToolFilter, type ToolGroup } from "./toolsets.js";

const MODULES: ReadonlyArray<[ToolGroup, { initialize(server: McpServer, client: OBSWebSocketClient): void }]> = [
  ["general", general],
  ["scenes", scenes],
  ["sources", sources],
  ["scene-items", sceneItems],
  ["stream", streaming],
  ["transitions", transitions],
  ["config", config],
  ["filters", filters],
  ["inputs", inputs],
  ["media", mediaInputs],
  ["outputs", outputs],
  ["record", record],
  ["ui", ui],
  ["protocol", protocolExtensions], // Regrouped per tool in toolsets.ts.
  ["protocol", protocol],
  ["record", preflight],
  ["record", workflows],
  ["record", takes],
  ["record", trim],
  ["scene-items", snapshots],
];

/**
 * Registers the tools the filter includes and returns every tool with its
 * group and whether it was registered.
 */
export type InitializeOptions = {
  filter?: ToolFilter;
  /** Defaults to OBS_MCP_CONFIRM_LIVE. */
  confirmLive?: boolean;
  /** Resources and prompts; off for servers built only to inspect the tool list. */
  resourcesAndPrompts?: boolean;
};

export function initialize(
  server: McpServer,
  client: OBSWebSocketClient,
  { filter = ALL_TOOLS, confirmLive = liveConfirmationEnabled(), resourcesAndPrompts = true }: InitializeOptions = {},
): RegisteredTool[] {
  // Registration passes through: group filter -> live confirmation -> take lock -> structured results -> server.
  // At call time the take lock runs first, so a locked call never asks the user.
  const structuredServer = withStructuredToolResults(server);
  const lockingServer = takes.withTakeLock(structuredServer, client);
  const confirmingServer = confirmLive ? withLiveConfirmation(lockingServer) : lockingServer;
  const registry: RegisteredTool[] = [];
  for (const [group, module] of MODULES) {
    module.initialize(scopedServer(confirmingServer, group, filter, registry), client);
  }
  if (resourcesAndPrompts) {
    resources.initialize(server, client);
    prompts.initialize(server);
  }
  return registry;
}

// Export tool modules
export { 
  general, 
  scenes, 
  sources, 
  sceneItems, 
  streaming, 
  transitions,
  config,
  filters,
  inputs,
  mediaInputs,
  outputs,
  record,
  ui,
  protocolExtensions,
  protocol,
  preflight,
  workflows,
  takes,
  trim,
  snapshots
};
