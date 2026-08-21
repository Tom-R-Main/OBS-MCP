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
import { withStructuredToolResults } from "./results.js";

// Export the initialization function for all tools
export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  const structuredServer = withStructuredToolResults(server);
  general.initialize(structuredServer, client);
  scenes.initialize(structuredServer, client);
  sources.initialize(structuredServer, client);
  sceneItems.initialize(structuredServer, client);
  streaming.initialize(structuredServer, client);
  transitions.initialize(structuredServer, client);
  config.initialize(structuredServer, client);
  filters.initialize(structuredServer, client);
  inputs.initialize(structuredServer, client);
  mediaInputs.initialize(structuredServer, client);
  outputs.initialize(structuredServer, client);
  record.initialize(structuredServer, client);
  ui.initialize(structuredServer, client);
  protocolExtensions.initialize(structuredServer, client);
  protocol.initialize(structuredServer, client);
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
  protocol
};
