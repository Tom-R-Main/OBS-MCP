/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { readFileSync, statSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { expect, it } from "vitest";
import { NATIVE_STATEFUL_DECLARED_TOOL_COUNT, NATIVE_STATEFUL_TOOL_COUNT } from "../support/tool-count.js";

it("preserves embedded asset bytes and modification times across unchanged actual SDK exports", () => {
  const root=fileURLToPath(new URL("../../",import.meta.url));
  for(const [script,asset] of [["scripts/export-native-contract.mjs","native/assets/tools.json"],["scripts/export-native-prompts.mjs","native/assets/prompts.json"]] as const) {
    const path=fileURLToPath(new URL(`../../${asset}`,import.meta.url));
    const before=readFileSync(path);const stamp=statSync(path,{bigint:true}).mtimeNs;
    const first=execFileSync(process.execPath,[script],{cwd:root,encoding:"utf8"});
    const second=execFileSync(process.execPath,[script],{cwd:root,encoding:"utf8"});
    expect(first).toBe(second);expect(readFileSync(path)).toEqual(before);expect(statSync(path,{bigint:true}).mtimeNs).toBe(stamp);
  }
});

it("binds actual Source dynamic controllers, covers and registration order separately from the unchanged static catalog", async () => {
  const {Client}=await import('@modelcontextprotocol/client');
  const {McpServer,InMemoryTransport}=await import('@modelcontextprotocol/server');
  const {initialize}=await import(new URL('../../build/tools/index.js',import.meta.url).href) as typeof import('../../src/tools/index.js');
  const {parseToolFilter}=await import(new URL('../../build/tools/toolsets.js',import.meta.url).href) as typeof import('../../src/tools/toolsets.js');
  const server=new McpServer({name:'dynamic-contract-independent-source',version:'1'});
  const client=new Client({name:'dynamic-contract-independent-source',version:'1'},{versionNegotiation:{mode:'legacy'}});
  let obsCalls=0,obsConnections=0;
  const fake={isConnected:()=>false,getConnectionStatus:()=>({}),on(){},off(){},connect(){obsConnections++;throw Error('No OBS connection permitted');},sendRequest(){obsCalls++;throw Error('No OBS request permitted');},sendBatch(){obsCalls++;throw Error('No OBS batch permitted');}};
  try {
    const registry=initialize(server,fake as unknown as Parameters<typeof initialize>[1],{filter:parseToolFilter({OBS_MCP_DYNAMIC_TOOLSETS:'true',OBS_MCP_TOOLSETS:'all'}),confirmLive:false,resourcesAndPrompts:false});
    const[a,b]=InMemoryTransport.createLinkedPair();await Promise.all([server.connect(a),client.connect(b)]);
    const listed=(await client.listTools()).tools;const controlNames=new Set(['obs-list-toolsets','obs-enable-toolset','obs-disable-toolset']);
    const groups=((await client.callTool({name:'obs-list-toolsets',arguments:{}})).structuredContent as {groups:{group:string;covers:string}[]}).groups.map(({group,covers})=>({group,covers}));
    const expected={groups,tools:listed.filter(tool=>controlNames.has(tool.name)),registryOrder:registry.map(tool=>tool.name)};
    const assetPath=fileURLToPath(new URL('../../native/assets/tools.json',import.meta.url));const catalog=JSON.parse(readFileSync(assetPath,'utf8'));
    expect(catalog.toolsets.dynamic).toEqual(expected);
    expect(catalog.tools).toHaveLength(178);expect(catalog.tools).toEqual(listed.filter(tool=>!controlNames.has(tool.name)));
    const union=[...new Set([...Object.keys(catalog.requests),...Object.keys(catalog.locals),...Object.keys(catalog.workflows),...Object.keys(catalog.service.handlers)])];expect(union).toHaveLength(NATIVE_STATEFUL_DECLARED_TOOL_COUNT);expect(union.filter(name=>!['obs-start-stream','obs-stop-stream','obs-toggle-stream'].includes(name))).toHaveLength(NATIVE_STATEFUL_TOOL_COUNT);
    expect(catalog.tools.some((tool:{name:string})=>controlNames.has(tool.name))).toBe(false);for(const name of controlNames)expect(catalog.service.handlers[name]).toBeUndefined();
    if(process.env.NATIVE_DYNAMIC_CONTRACT_ORACLE){const prior=JSON.parse(readFileSync(process.env.NATIVE_DYNAMIC_CONTRACT_ORACLE,'utf8')).contract;expect(expected).toEqual({groups:prior.groups.map(({group,covers}:{group:string;covers:string})=>({group,covers})),tools:prior.tools,registryOrder:prior.registryOrder});}
    if(process.env.NATIVE_CONTRACT_BASELINE){const prior=readFileSync(process.env.NATIVE_CONTRACT_BASELINE,'utf8');delete catalog.toolsets.dynamic;expect(JSON.stringify(catalog,null,2)+'\n').toBe(prior);}
    expect(obsCalls).toBe(0);expect(obsConnections).toBe(0);
  }finally{await client.close();await server.close();}
});

it("fails closed on malformed Source dynamic metadata without rewriting the bound asset", async () => {
  const path=fileURLToPath(new URL('../../native/assets/tools.json',import.meta.url));const before=readFileSync(path);const stamp=statSync(path,{bigint:true}).mtimeNs;const catalog=JSON.parse(before.toString('utf8'));
  const modulePath=fileURLToPath(new URL('../../scripts/export-native-contract.mjs',import.meta.url));const exporter=await import(modulePath) as {validateDynamicContract?:(value:unknown,context:unknown)=>unknown};expect(exporter.validateDynamicContract).toBeTypeOf('function');
  const valid=catalog.toolsets.dynamic;expect(valid).toBeDefined();const context={tools:catalog.tools,groups:catalog.toolsets.groups,registryOrder:valid.registryOrder};
  const invalid:((value:Record<string,unknown>)=>void)[]=[
    value=>{value.groups=[];},value=>{(value.groups as {covers:string}[])[0]!.covers='';},value=>{(value.groups as {group:string}[])[0]!.group='unknown';},
    value=>{value.tools=[];},value=>{const tools=value.tools as unknown[];tools.push(tools[0]);},value=>{(value.tools as {name:string}[])[0]!.name='obs-get-status';},
    value=>{(value.tools as {annotations:{readOnlyHint:boolean}}[])[0]!.annotations.readOnlyHint=false;},value=>{(value.tools as {description:string}[])[0]!.description='';},
    value=>{(value.tools as {inputSchema:{properties:{groups:{minItems:number}}}}[])[1]!.inputSchema.properties.groups.minItems=0;},
    value=>{(value.tools as {inputSchema:{properties:{groups:{items:{enum:string[]}}}}}[])[1]!.inputSchema.properties.groups.items.enum=['unknown'];},
    value=>{value.registryOrder=[];},value=>{const order=value.registryOrder as string[];order[1]=order[0]!;},value=>{const order=value.registryOrder as string[];[order[0],order[1]]=[order[1]!,order[0]!];},
  ];
  for(const change of invalid){const value=structuredClone(valid);change(value);expect(()=>exporter.validateDynamicContract!(value,context)).toThrow(/dynamic|controller|registry|group/i);}
  expect(()=>exporter.validateDynamicContract!(null,context)).toThrow();expect(()=>exporter.validateDynamicContract!({...valid,extra:true},context)).toThrow();
  expect(readFileSync(path)).toEqual(before);expect(statSync(path,{bigint:true}).mtimeNs).toBe(stamp);
});


it("refuses an invalid actual Source descriptor before publishing any asset bytes", () => {
  const root=fileURLToPath(new URL("../../",import.meta.url));const asset=join(root,'native/assets/tools.json');const before=readFileSync(asset);const stamp=statSync(asset,{bigint:true}).mtimeNs;
  const fixture=mkdtempSync(join(root,'native/export-contract-fixture-'));const preload=join(fixture,'invalid-source.mjs');
  try {
    // Corrupt only the child process's captured SDK discovery return. No Source
    // module, dependency or registration code on disk is modified.
    writeFileSync(preload,`import {Client} from '@modelcontextprotocol/client';
const original=Client.prototype.listTools;
Client.prototype.listTools=async function(...args){const result=await original.apply(this,args);for(const tool of result.tools)if(['obs-enable-toolset','obs-disable-toolset'].includes(tool.name))tool.inputSchema.properties.groups.minItems=0;return result;};
`);
    let failure:unknown;try{execFileSync(process.execPath,['--import',preload,'scripts/export-native-contract.mjs'],{cwd:root,encoding:'utf8',stdio:['ignore','pipe','pipe']});}catch(error){failure=error;}
    expect(failure).toBeDefined();expect((failure as {status:number}).status).not.toBe(0);expect(String((failure as {stderr:unknown}).stderr)).toContain('Invalid Source dynamic controller contract: group schema vocabulary or minimum');
    expect(readFileSync(asset)).toEqual(before);expect(statSync(asset,{bigint:true}).mtimeNs).toBe(stamp);
  } finally {rmSync(fixture,{recursive:true,force:true});}
});
