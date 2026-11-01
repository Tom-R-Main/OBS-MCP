#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// Generate the native surface from the actual SDK registration, never a second handwritten list.
import { Client } from '@modelcontextprotocol/client';
import { McpServer, InMemoryTransport } from '@modelcontextprotocol/server';
import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { initialize } from '../build/tools/index.js';
import { nativeRequestDefinitions } from '../build/tools/request-tool.js';
import { TOOL_GROUPS, CORE_GROUPS } from '../build/tools/toolsets.js';
const server = new McpServer({ name: 'obs-mcp', version: '1.1.1' });
const client = new Client({ name: 'native-contract-export', version: '1.0.0' });
const fake = { isConnected: () => false, getConnectionStatus: () => ({}), on() {}, off() {} };
const registry = initialize(server, fake, { resourcesAndPrompts: false, confirmLive: false });
const [a,b] = InMemoryTransport.createLinkedPair();
try {
  await Promise.all([server.connect(a), client.connect(b)]);
  const { tools } = await client.listTools();
  const requests = {};
  const simple = (s) => {
    const allowed = new Set(['$schema','type','properties','required','default','description','title','minimum','maximum','minLength','maxLength','enum','additionalProperties']);
    if (!s || typeof s !== 'object' || Object.keys(s).some(k => !allowed.has(k))) return false;
    if (!['object','string','number','integer','boolean'].includes(s.type)) return false;
    if (s.additionalProperties === true || typeof s.additionalProperties === 'object') return false;
    if (s.type === 'object') return Object.values(s.properties ?? {}).every(simple);
    return !s.properties && !s.required;
  };
  for (const tool of tools) {
    const definition = nativeRequestDefinitions.get(tool.name);
    if (definition?.eligible && simple(tool.inputSchema)) requests[tool.name] = {requestType: definition.requestType, inputSchema: tool.inputSchema};
  }
  const toolsets = {
    groups: [...TOOL_GROUPS],
    coreGroups: [...CORE_GROUPS],
    toolGroups: Object.fromEntries(registry.map(({ name, group }) => [name, group])),
  };
  if (registry.length !== tools.length || Object.keys(toolsets.toolGroups).length !== tools.length || tools.some(({ name }) => !Object.hasOwn(toolsets.toolGroups, name))) {
    throw new Error('Native toolset metadata must describe the actual SDK registry exactly');
  }
  const describe = tools.find(({ name }) => name === 'obs-describe-request');
  if (!describe?.inputSchema) throw new Error('Missing actual describe-request tool schema');
  const locals = { 'obs-describe-request': { handler: 'describe-request', inputSchema: describe.inputSchema } };
  const plan = tools.find(({ name }) => name === 'obs-plan-scene-template');
  if (!plan?.inputSchema || plan.annotations?.readOnlyHint !== true) throw new Error('Missing actual read-only scene-template plan schema');
  const audit = tools.find(({ name }) => name === 'obs-audit-scene');
  if (!audit?.inputSchema || audit.annotations?.readOnlyHint !== true) throw new Error('Missing actual read-only scene audit schema');
  const workflows = { 'obs-plan-scene-template': { handler: 'scene-template-plan', inputSchema: plan.inputSchema }, 'obs-audit-scene': { handler: 'scene-audit', inputSchema: audit.inputSchema } };
  const serviceHandlers = {
    'obs-record-clip':'record-clip',
    'obs-restore':'restore',
    'obs-stop-record':'manual-record-stop',
    'obs-toggle-stream':'literal-setter', 'obs-toggle-virtual-cam':'literal-setter', 'obs-toggle-replay-buffer':'literal-setter',
    'obs-start-stream':'confirmed-output-start', 'obs-start-virtual-cam':'confirmed-output-start', 'obs-start-replay-buffer':'confirmed-output-start',
    'obs-stop-stream':'literal-setter', 'obs-stop-virtual-cam':'literal-setter', 'obs-stop-replay-buffer':'literal-setter',
    'obs-set-studio-mode':'literal-setter', 'obs-open-input-properties':'literal-setter', 'obs-open-input-filters':'literal-setter', 'obs-open-input-interact':'literal-setter', 'obs-open-video-mix-projector':'literal-setter', 'obs-open-source-projector':'literal-setter',
    'obs-pause-record':'literal-setter', 'obs-resume-record':'literal-setter', 'obs-toggle-record-pause':'literal-setter', 'obs-split-record-file':'literal-setter', 'obs-create-record-chapter':'literal-setter',
    'obs-set-scene-item-transform':'literal-setter',
    'obs-broadcast-custom-event':'literal-setter', 'obs-set-transition-settings':'literal-setter', 'obs-create-source-filter':'literal-setter', 'obs-set-source-filter-settings':'literal-setter', 'obs-press-input-property-button':'literal-setter', 'obs-save-source-screenshot':'literal-setter',
    'obs-create-input':'literal-setter', 'obs-set-input-settings':'literal-setter',
    'obs-set-persistent-data':'literal-setter', 'obs-set-current-scene-collection':'literal-setter', 'obs-create-scene-collection':'literal-setter', 'obs-set-current-profile':'literal-setter', 'obs-create-profile':'literal-setter', 'obs-remove-profile':'literal-setter', 'obs-set-stream-service-settings':'literal-setter', 'obs-set-record-directory':'literal-setter', 'obs-set-output-settings':'literal-setter', 'obs-set-video-settings':'literal-setter', 'obs-set-profile-parameter':'literal-setter',
    'obs-set-media-input-cursor':'literal-setter', 'obs-offset-media-input-cursor':'literal-setter', 'obs-trigger-media-input-action':'literal-setter', 'obs-set-input-audio-tracks':'literal-setter', 'obs-toggle-input-mute':'literal-setter', 'obs-set-input-volume':'literal-setter', 'obs-send-stream-caption':'literal-setter', 'obs-save-replay-buffer':'literal-setter',
    'obs-create-scene':'literal-setter', 'obs-remove-scene':'literal-setter', 'obs-remove-scene-item':'literal-setter', 'obs-remove-source-filter':'literal-setter', 'obs-set-source-filter-name':'literal-setter', 'obs-set-source-filter-index':'literal-setter', 'obs-remove-input':'literal-setter', 'obs-set-input-name':'literal-setter', 'obs-duplicate-scene-item':'literal-setter', 'obs-set-scene-name':'literal-setter', 'obs-create-scene-item':'literal-setter', 'obs-set-scene-item-enabled':'literal-setter', 'obs-set-current-scene':'literal-setter', 'obs-set-preview-scene':'literal-setter', 'obs-trigger-studio-transition':'literal-setter', 'obs-set-current-transition':'literal-setter', 'obs-trigger-transition':'literal-setter', 'obs-set-scene-transition-override':'literal-setter', 'obs-set-tbar-position':'literal-setter', 'obs-set-transition-duration':'literal-setter',
    'obs-set-input-mute':'literal-setter', 'obs-set-input-audio-balance':'literal-setter', 'obs-set-input-audio-sync-offset':'literal-setter', 'obs-set-input-audio-monitor-type':'literal-setter', 'obs-set-input-deinterlace-field-order':'literal-setter', 'obs-set-input-deinterlace-mode':'literal-setter', 'obs-set-scene-item-index':'literal-setter', 'obs-set-scene-item-locked':'literal-setter', 'obs-set-scene-item-blend-mode':'literal-setter', 'obs-set-source-filter-enabled':'literal-setter',
    'obs-get-status':'get-status', 'obs-test-connection':'test-connection', 'obs-sleep':'sleep', 'obs-get-input-list':'input-list', 'obs-get-input-kind-list':'input-kind-list', 'obs-get-source-screenshot':'source-screenshot', 'obs-preflight':'preflight',
    'obs-observe':'observe-production', 'obs-stage-scene':'stage-scene', 'obs-take-scene':'take-scene',
    'obs-take-start':'take-start', 'obs-take-stop':'take-stop', 'obs-take-mark':'take-mark', 'obs-take-status':'take-status',
    'obs-plan-scene-template':'scene-template-plan', 'obs-audit-scene':'scene-audit', 'obs-contact-sheet':'contact-sheet', 'obs-trim-take':'trim-take',
    'obs-snapshot':'snapshot', 'obs-get-input-property-list-items':'property-list',
    'obs-prepare-demo':'prepare-demo', 'obs-recover-demo':'recover-demo',
    'obs-claim-control':'claim-control', 'obs-release-control':'release-control', 'obs-control-status':'control-status',
  };
  const service = { activation: 'OBS_MCP_NATIVE_STATEFUL', handlers: Object.fromEntries(Object.entries(serviceHandlers).map(([name,handler])=>{
    const tool=tools.find(tool=>tool.name===name);
    if(!tool?.inputSchema) throw new Error(`Missing actual persistent native service schema: ${name}`);
    return [name,{handler,inputSchema:tool.inputSchema}];
  })) };
  const manifest = { formatVersion: 1, tools, requests, toolsets, locals, workflows, service };
  const path = fileURLToPath(new URL('../native/assets/tools.json', import.meta.url));
  const bytes = JSON.stringify(manifest, null, 2)+'\n';
  if (!existsSync(path) || !readFileSync(path).equals(Buffer.from(bytes))) writeFileSync(path, bytes);
  console.log(JSON.stringify({ tools: tools.length, nativeReadOnly: Object.keys(requests).length, nativeLocal: Object.keys(locals).length, nativeReadOnlyWorkflow: Object.keys(workflows).length, compatibility: tools.length-Object.keys(requests).length-Object.keys(locals).length-Object.keys(workflows).length }));
} finally { await client.close(); await server.close(); }
