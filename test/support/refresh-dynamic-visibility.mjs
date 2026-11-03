// SPDX-License-Identifier: GPL-2.0-only
// Development fixture refresh from actual compiled Source controllers/SDK.
// Requires npm run build. Never connects to OBS or computes native projections.
import assert from 'node:assert/strict';
import {readFileSync,writeFileSync,mkdirSync,mkdtempSync,rmSync} from 'node:fs';
import {spawnSync} from 'node:child_process';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Client} from '@modelcontextprotocol/client';
import {McpServer,InMemoryTransport} from '@modelcontextprotocol/server';
import {initialize} from '../../build/tools/index.js';
import {parseToolFilter,assertKnownTools} from '../../build/tools/toolsets.js';
const path=fileURLToPath(new URL('../../native/tests/fixtures/dynamic_visibility_source.json',import.meta.url));
const prior=JSON.parse(readFileSync(path,'utf8')),controllers=new Set(['obs-list-toolsets','obs-enable-toolset','obs-disable-toolset']);
async function collect(env,commands,invalid=[]){
 const server=new McpServer({name:'source-dynamic-visibility-oracle',version:'1'}),client=new Client({name:'source-dynamic-visibility-oracle',version:'1'},{versionNegotiation:{mode:'legacy'}});
 let obsRequests=0;const denied=()=>{obsRequests++;throw Error('Dynamic visibility fixture must not connect to or request OBS');};
 const obs={isConnected:()=>false,getConnectionStatus:()=>({}),on(){},off(){},connect:denied,sendRequest:denied,sendBatch:denied};
 try{
  const filter=parseToolFilter(env),registry=initialize(server,obs,{filter,confirmLive:false,resourcesAndPrompts:false,sessionNotifications:false});assertKnownTools(filter,registry);
  const[a,b]=InMemoryTransport.createLinkedPair();await Promise.all([server.connect(a),client.connect(b)]);
  const snapshot=async()=>{
   const listed=(await client.listTools()).tools;
   const registered=registry.filter(tool=>tool.registered).map(tool=>tool.name),enabled=registry.filter(tool=>tool.handle?.enabled).map(tool=>tool.name);
   assert.deepEqual(listed.filter(tool=>!controllers.has(tool.name)).map(tool=>tool.name),enabled,'SDK discovery must match actual Source handle state/order');
   const list=listed.some(tool=>tool.name==='obs-list-toolsets')?await client.callTool({name:'obs-list-toolsets',arguments:{}}):null;
   return{registered,enabled,list};
  };
  const initial=await snapshot(),steps=[];
  for(const command of commands){const result=await client.callTool({name:command.enable?'obs-enable-toolset':'obs-disable-toolset',arguments:{groups:command.groups}});steps.push({command,result,after:await snapshot()});}
  const errors=[];for(const entry of invalid){const result=await client.callTool({name:'obs-enable-toolset',arguments:entry.args});assert.equal(result.isError,true);errors.push({args:entry.args,result});}
  assert.equal(obsRequests,0);
  const listed=(await client.listTools()).tools,contract={groups:initial.list?.structuredContent.groups,tools:listed.filter(tool=>controllers.has(tool.name)),registryOrder:registry.map(tool=>tool.name)};
  return{initial,steps,...(invalid.length?{invalid:errors}:{}),obsRequests,contract};
 }catch(error){assert.equal(obsRequests,0);return{error:String(error),obsRequests};}
 finally{await client.close();await server.close();}
}
function startup(env){
 const root=fileURLToPath(new URL('../../',import.meta.url)),parent=join(root,'native/target');mkdirSync(parent,{recursive:true});const state=mkdtempSync(join(parent,'dynamic-source-startup-'));
 // Exercise actual Source startup validation, including its empty-registry
 // refusal. Overrides live only in this child; every OBS operation is denied.
 const script=`import{logger}from'./build/logger.js';import{OBSWebSocketClient}from'./build/client.js';let obsRequests=0;const denied=()=>{obsRequests++;throw Error('OBS forbidden');};for(const name of['connect','sendRequest','sendBatch'])OBSWebSocketClient.prototype[name]=denied;const messages=[];logger.error=message=>messages.push(String(message));const stop=new Error('controlled exit');let exit;process.exit=code=>{exit=code;throw stop;};const{startServer}=await import('./build/server.js');try{await startServer();}catch(error){if(error!==stop)throw error;}if(exit!==1||obsRequests!==0)throw Error('Source startup did not refuse before OBS');const issue=messages.find(message=>message.startsWith('Error starting server: '));if(!issue)throw Error('Source startup error missing');console.log(JSON.stringify({error:'Error: '+issue.slice('Error starting server: '.length),obsRequests}));`;
 try{const variables={...process.env,OBS_MCP_DYNAMIC_TOOLSETS:'',OBS_MCP_TOOLSETS:'',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'',...env,OBS_MCP_STATE_DIR:state,OBS_MCP_READ_OBS_CONFIG:'false',OBS_WEBSOCKET_PASSWORD:'',OBS_MCP_HTTP_PORT:'',OBS_MCP_LOG_LEVEL:'silent'};const child=spawnSync(process.execPath,['--input-type=module','-e',script],{cwd:root,env:variables,encoding:'utf8',timeout:5000,killSignal:'SIGKILL',maxBuffer:64*1024});if(child.error)throw child.error;assert.equal(child.status,0,child.stderr);return JSON.parse(child.stdout);}
 finally{rmSync(state,{recursive:true,force:true});}
}
const all=await collect({OBS_MCP_DYNAMIC_TOOLSETS:'true',OBS_MCP_TOOLSETS:'all'},[]);assert.ok(!all.error);assert.ok(Array.isArray(all.contract.groups));assert.equal(all.contract.tools.length,3);
const cases=[];for(const old of prior.cases){const actual=old.error?startup(old.env):await collect(old.env,old.commands,old.invalid??[]);if(old.error)assert.equal(actual.error,old.error,old.label);else assert.ok(!actual.error,`${old.label}: ${actual.error}`);const{contract,...results}=actual;cases.push({label:old.label,env:old.env,commands:old.commands,...results});}
const next={scope:'Actual current tranche175 compiled Source + pinned SDK registry/controllers (179 ordinary tools); no OBS. Original 14-case matrix and 5 invalid SDK inputs retained; responses regenerated through public MCP.',contract:all.contract,cases};
assert.deepEqual(next.cases.map(c=>({label:c.label,env:c.env,commands:c.commands,invalidArgs:c.invalid?.map(v=>v.args)})),prior.cases.map(c=>({label:c.label,env:c.env,commands:c.commands,invalidArgs:c.invalid?.map(v=>v.args)})),'Case inputs must remain unchanged');
const bytes=JSON.stringify(next,null,2)+'\n';if(readFileSync(path,'utf8')!==bytes)writeFileSync(path,bytes);
console.log(JSON.stringify({ordinaryTools:next.contract.registryOrder.length,cases:cases.length,invalidInputs:cases.reduce((n,c)=>n+(c.invalid?.length??0),0),obsRequests:cases.reduce((n,c)=>n+c.obsRequests,0)}));
