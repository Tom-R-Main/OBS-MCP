/* SPDX-License-Identifier: GPL-2.0-only */
import { expectNativeInventory } from '../support/native-inventory.js';
import { execFileSync, spawnSync, spawn } from "node:child_process";
import { mkdirSync, rmSync, readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterEach, describe, expect, it, vi } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../support/fake-obs-server.js";
import { serveProduction } from "../support/production-fixture.js";
const root=fileURLToPath(new URL("../../",import.meta.url));
const binary=process.env.NATIVE_TEST_BINARY??join(root,"native/target/debug/obs-mcp-native");
const compat=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,"build/index.js");
const sessions:Session[]=[];let sequence=0;
type Session={endpoint:string;client:Client;transport:StdioClientTransport;fake:FakeOBSServer;state:ReturnType<typeof serveProduction>;directory:string;recording:()=>boolean};
async function start(native:boolean,legacy:boolean,recordingRoot=false,extra:Record<string,string>={},configure?:(session:Session)=>void):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:"macos",supportedImageFormats:["png"]});const state=serveProduction(fake);state.preview="Main";
 const directory=join(root,"native/target",`public-service-${process.pid}-${sequence++}`);mkdirSync(directory);const recordingDir=join(directory,"recordings");mkdirSync(recordingDir);
 let recording=false;const path=join(recordingDir,"clip.mkv");
 fake.respondWith("GetProfileParameter",data=>({parameterValue:({"Output/Mode":"Simple","SimpleOutput/RecQuality":"Small","SimpleOutput/RecEncoder":"apple_h264","SimpleOutput/RecTracks":"1","SimpleOutput/RecFormat2":"mkv"} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));
 // Deliberate missing local directory evidence exercises source's unknown path
 // without creating media or calling an installed decoder on a nonexistent file.
 fake.respondWith("GetRecordDirectory",()=>{throw new FakeOBSRequestError(604,"fixture directory unavailable");});
 fake.respondWith("GetStats",()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));
 fake.respondWith("GetRecordStatus",()=>({outputActive:recording,outputPaused:false,outputBytes:1048576}));
 fake.respondWith("StartRecord",()=>{recording=true;fake.sendEvent("RecordStateChanged",{outputState:"OBS_WEBSOCKET_OUTPUT_STARTED",outputActive:true,outputPath:path});return {};});
 fake.respondWith("StopRecord",()=>{recording=false;fake.sendEvent("RecordStateChanged",{outputState:"OBS_WEBSOCKET_OUTPUT_STOPPED",outputActive:false,outputPath:path});return {outputPath:path};});
 fake.respondWith("CreateRecordChapter",()=>({}));
 const client=new Client({name:"persistent-parity",version:"1"},{versionNegotiation:{mode:legacy?"legacy":{pin:"2026-07-28"}}});
 const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[compat],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?"1":"0",OBS_MCP_NATIVE_RECORDING_ROOT:recordingRoot?recordingDir:"",OBS_MCP_COMPAT_NODE:join(root,"native/target/nonexistent-service-node"),OBS_MCP_COMPAT_ENTRY:join(root,"native/target/nonexistent-service-sdk.js"),OBS_MCP_TOOLSETS:"all",OBS_MCP_TOOLS:"",OBS_MCP_READ_ONLY:"false",OBS_MCP_DYNAMIC_TOOLSETS:"false",OBS_MCP_CONFIRM_LIVE:"false",OBS_MCP_READ_OBS_CONFIG:"false",...extra},stderr:"pipe"});transport.stderr?.on("data",()=>undefined);
 const session={endpoint:extra.OBS_WEBSOCKET_URL??fake.url,client,transport,fake,state,directory,recording:()=>recording};sessions.push(session);configure?.(session);await client.connect(transport);return session;
}
function children(pid:number):number[]{return execFileSync("ps",["-axo","pid=,ppid="],{encoding:"utf8"}).trim().split("\n").map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id!);}
function count(session:Session,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).length;}
const uuid=/[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
function normal(value:unknown,session:Session):unknown{
 if(Array.isArray(value))return value.map(value=>normal(value,session));
 if(value&&typeof value==="object"){
  const input=value as Record<string,unknown>;const output:Record<string,unknown>={};
  for(const[key,item]of Object.entries(input)){
   if(["timestamp","observedAt","completedAt","startedAt","createdAt","updatedAt","expiresAt"].includes(key)&&typeof item==="string"){expect(item).toMatch(/^\d{4}-\d\d-\d\dT/);output[key]="TIME";}
   else if(["durationMs","durationSeconds","atSeconds"].includes(key)&&typeof item==="number"){expect(Number.isFinite(item)).toBe(true);output[key]=0;}
   else if(key==="since"&&typeof item==="number"){output[key]=0;}
   else if(key==="expiresAt"&&typeof item==="number"&&typeof input.since==="number"){output[key]=item-input.since;}
   else if(key==="snapshotId"&&typeof item==="string"&&/^[0-9a-f]{8}$/.test(item)){output[key]="SNAPSHOT_ID";}
   else if((key==="takeId"||key==="id")&&typeof item==="string"&&/^[0-9a-f]{8}$/.test(item)){output[key]="TAKE_ID";}
   else if(key==="url"&&typeof item==="string"){expect(item).toBe(session.endpoint);output[key]="FIXTURE_URL";}
   else if(key==="generation"&&typeof item==="number"){expect(Number.isSafeInteger(item)).toBe(true);output[key]=1;}
   else output[key]=normal(item,session);
  }return output;
 }
 if(typeof value==="string"){
  try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==="object")return {jsonValue:normal(parsed,session)};}catch{/* ordinary text */}
  return value.replaceAll(session.directory,"FIXTURE_ROOT").replace(uuid,"UUID").replace(/[0-9a-f]{8}(?= at | \"|:)/g,"SNAPSHOT_ID").replace(/(Take |take )[0-9a-f]{8}\b/g,"$1TAKE_ID").replace(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z/g,"TIME").replace(/until \d\d:\d\d UTC/g,"until TIME UTC").replace(/ at \d+(?:\.\d+)?s/g," at 0s").replace(/^Recorded \d+(?:\.\d+)?s /,"Recorded 0s ").replace(/Chapters: \d+(?:\.\d+)?s/g,"Chapters: 0s").replace(/Take UUID: \d+(?:\.\d+)?s/g,"Take UUID: 0s");
 }return value;
}
async function same(oracle:Session,native:Session,name:string,args:Record<string,unknown>={}){
 const expected=await oracle.client.callTool({name,arguments:args});const actual=await native.client.callTool({name,arguments:args});expect(normal(actual,native)).toEqual(normal(expected,oracle));return actual;
}
async function raw(session:Session,id:number,method:string,params:Record<string,unknown>){
 const prior=session.transport.onmessage;let response:Record<string,unknown>|undefined;
 session.transport.onmessage=(message)=>{const value=message as unknown as Record<string,unknown>;if(value.id===id)response=value;prior?.(message);};
 try{await session.transport.send({jsonrpc:"2.0",id,method,params} as Parameters<StdioClientTransport["send"]>[0]);await vi.waitFor(()=>expect(response).toBeDefined(),{timeout:3000});return response!;}finally{session.transport.onmessage=prior;}
}
function meta(name:string){return {"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{name,version:"1"},"io.modelcontextprotocol/clientCapabilities":{}};}
async function close(session:Session){await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0),{timeout:4000});}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await close(session);}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});

for(const legacy of[false,true])describe(`persistent public native lane (legacy=${legacy})`,()=>{
 it("limits inventory to actual routes and serves read/observe/status childlessly on one socket",async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  const list=await native.client.listTools();const names=list.tools.map(tool=>tool.name);
  expect(names).toContain("obs-stage-scene");expect(names).toContain("obs-observe");expect(names).toContain("obs-get-record-status");expect(names).not.toContain("obs-take-start");expect(names).not.toContain("obs-call-request");expect(names).toContain("obs-plan-scene-template");expect(names).toContain("obs-audit-scene");expect(names).not.toContain("obs-contact-sheet");
  const sdk=(await oracle.client.listTools()).tools;for(const tool of list.tools)expect(tool).toEqual(sdk.find(candidate=>candidate.name===tool.name));
  expect(native.fake.connectionCount).toBe(0);
  await same(oracle,native,"obs-get-record-status");await same(oracle,native,"obs-observe");
  const resource=await native.client.readResource({uri:"obs://status"});const expected=await oracle.client.readResource({uri:"obs://status"});expect(normal(resource,native)).toEqual(normal(expected,oracle));
  expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);expect(native.fake.eventSubscriptions()).toBe(4095|(1<<16)|(1<<19));
 });
 it("keeps caller leases through first OBS connection and projects explicit OBS errors",async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  await same(oracle,native,"obs-control-status");const claim=await same(oracle,native,"obs-claim-control",{minutes:2,reason:"qualification"});expect(claim.structuredContent).toMatchObject({holder:"persistent-parity",reason:"qualification"});expect(native.fake.connectionCount).toBe(0);
  await same(oracle,native,"obs-control-status");await same(oracle,native,"obs-observe");await same(oracle,native,"obs-release-control");
  for(const session of[oracle,native])session.fake.respondWith("GetRecordStatus",()=>{throw new FakeOBSRequestError(500,"owned fixture rejection");});
  await same(oracle,native,"obs-get-record-status");expect(children(native.transport.pid!)).toEqual([]);
 });
 it("retains a stage token after an unknown method and completes the same native take-scene",async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  const args={sceneCollection:native.state.collection,sceneName:native.state.sceneName};
  const expected=await oracle.client.callTool({name:"obs-stage-scene",arguments:args});const actual=await native.client.callTool({name:"obs-stage-scene",arguments:args});expect(normal(actual,native)).toEqual(normal(expected,oracle));
  const stage=(actual.structuredContent as {stageId:string}).stageId;expect(stage).toMatch(uuid);
  const unsupported=await raw(native,9000,"qualification/unowned",legacy?{}:{_meta:meta("persistent-parity")});expect(unsupported.error).toMatchObject({code:-32601});
  await expect(native.client.callTool({name:"obs-call-request",arguments:{requestType:"SetCurrentProgramScene",requestData:{sceneName:"Main"}}})).rejects.toThrow(/native-only qualification/);
  const expectedTake=await oracle.client.callTool({name:"obs-take-scene",arguments:{stageId:(expected.structuredContent as {stageId:string}).stageId}});const actualTake=await native.client.callTool({name:"obs-take-scene",arguments:{stageId:stage}});expect(normal(actualTake,native)).toEqual(normal(expectedTake,oracle));
  expect(native.state.program).toBe(native.state.sceneName);expect(count(native,"TriggerStudioModeTransition")).toBe(1);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it("lists snapshots offline and captures exact source batch fields without a second writer",async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  await same(oracle,native,"obs-snapshot",{list:true,ignored:"strip"});expect(native.fake.connectionCount).toBe(0);
  for(const session of[oracle,native]){
   session.fake.respondWith("GetSceneItemList",()=>({sceneItems:[{sceneItemId:7,sceneItemIndex:2,sourceName:"Window",sceneItemEnabled:true,sceneItemLocked:false,sceneItemTransform:{positionX:17,positionY:4,scaleX:1,scaleY:1,rotation:0,sourceWidth:300,sourceHeight:200}}]}));
   session.fake.respondWith("GetInputSettings",()=>({inputKind:"screen_capture",inputSettings:{window:77,type:1,future:{retained:true}}}));
   session.fake.respondWith("GetInputMute",()=>({inputMuted:true}));session.fake.respondWith("GetInputVolume",()=>({inputVolumeMul:0.5}));session.fake.respondWith("GetInputAudioTracks",()=>({inputAudioTracks:{1:true,2:false,3:false,4:false,5:false,6:false}}));
  }
  const saved=await same(oracle,native,"obs-snapshot",{scenes:["Proof"],inputs:["Window"],label:'A "quoted" snapshot',ignored:1});expect(saved.isError).not.toBe(true);
  await same(oracle,native,"obs-snapshot",{list:true});
  const history=JSON.parse(readFileSync(join(native.directory,"snapshots.json"),"utf8"));expect(history).toHaveLength(1);expect(history[0].inputs[0].inputSettings).toEqual({window:77,type:1,future:{retained:true}});expect(history[0].items[0]).toMatchObject({sceneName:"Proof",sourceName:"Window",sceneItemId:7});
  const trace=(session:Session)=>session.fake.history().filter(item=>[6,8].includes(item.frame.op)).map(item=>item.frame.op===8?{op:8,requests:(item.frame.d.requests as Record<string,unknown>[]).map(({requestId,...rest})=>rest),haltOnFailure:item.frame.d.haltOnFailure,executionType:item.frame.d.executionType}:{op:6,requestType:item.frame.d.requestType,requestData:item.frame.d.requestData??null});
  expect(trace(native)).toEqual(trace(oracle));expect(trace(native).filter(frame=>frame.op===8)).toHaveLength(2);expect(count(native,"GetVersion")).toBe(1);expect(count(oracle,"GetVersion")).toBe(1);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
  for(const args of[{list:null},{scenes:null},{inputs:[1]},{label:3},{list:"true"}])await same(oracle,native,"obs-snapshot",args);
 });
 it("preserves exact property guard bodies, metadata and ordered safe reads",async()=>{
  const configure=(session:Session)=>{session.fake.respondWith("GetInputSettings",()=>({inputKind:"screen_capture",inputSettings:{display_uuid:"configured"}}));session.fake.respondWith("GetInputPropertiesListPropertyItems",()=>({propertyItems:[{itemName:"A",itemValue:77,itemEnabled:true,extra:"retained"}],extra:{opaque:true}}));};
  const oracle=await start(false,legacy,false,{},configure);const native=await start(true,legacy,false,{},configure);
  await same(oracle,native,"obs-get-input-property-list-items",{inputName:"Window",propertyName:"display_uuid"});expect(count(native,"GetInputPropertiesListPropertyItems")).toBe(0);expect(count(native,"GetVersion")).toBe(1);expect(count(oracle,"GetVersion")).toBe(1);
  await same(oracle,native,"obs-get-input-property-list-items",{inputName:"",inputUuid:"uuid",propertyName:"window",ignored:"strip"});
  for(const session of[oracle,native])session.fake.respondWith("GetInputSettings",()=>({inputKind:"xcomposite_input",inputSettings:{capture_window:"   "}}));
  await same(oracle,native,"obs-get-input-property-list-items",{inputName:"X",propertyName:"capture_window"});
  for(const session of[oracle,native])session.fake.respondWith("GetInputPropertiesListPropertyItems",()=>{throw new FakeOBSRequestError(500,"PRIVATE PROPERTY DETAIL");});
  await same(oracle,native,"obs-get-input-property-list-items",{propertyName:"window"});
  for(const args of[{propertyName:null},{propertyName:"window",inputName:null},{propertyName:3},{}])await same(oracle,native,"obs-get-input-property-list-items",args);
  for(const response of[{propertyItems:3,extra:"retained"},{propertyItems:{}},{propertyItems:[null,7,[],false,"wrong"]}]){for(const session of[oracle,native])session.fake.respondWith("GetInputPropertiesListPropertyItems",()=>response);await same(oracle,native,"obs-get-input-property-list-items",{propertyName:"window"});}
  const trace=(session:Session)=>session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType!=="GetVersion").map(item=>({request:item.frame.d.requestType,data:item.frame.d.requestData??null}));expect(trace(native)).toEqual(trace(oracle));expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it("refuses corrupt snapshot history without offline OBS reads",async()=>{
  const native=await start(true,legacy);writeFileSync(join(native.directory,"snapshots.json"),"{broken",{mode:0o600});
  const result=await native.client.callTool({name:"obs-snapshot",arguments:{list:true}});expect(result.isError).toBe(true);expect(result.content).toEqual([{type:"text",text:"Snapshot failed: Snapshot history is malformed; preserve it for explicit inspection"}]);expect(readFileSync(join(native.directory,"snapshots.json"),"utf8")).toBe("{broken");expect(native.fake.connectionCount).toBe(0);
 });
 it("runs actual take ownership, marker/status/stop and completed cache without a JS child",async()=>{
  const oracle=await start(false,legacy,true);const native=await start(true,legacy,true);
  expect((await native.client.listTools()).tools.some(tool=>tool.name==="obs-take-start")).toBe(true);
  await same(oracle,native,"obs-take-status");
  const started=await same(oracle,native,"obs-take-start",{expectSilent:true});expect(started.isError).not.toBe(true);
  const during=await same(oracle,native,"obs-snapshot",{scenes:[],inputs:[],label:"recording"});expect(during.isError).not.toBe(true);
  await same(oracle,native,"obs-take-mark",{name:"checkpoint"});
  const status=await native.client.callTool({name:"obs-take-status",arguments:{}});expect(status.isError).not.toBe(true);
  const stopped=await same(oracle,native,"obs-take-stop");expect((stopped.structuredContent as {stopConfirmed:boolean}).stopConfirmed).toBe(true);
  const repeated=await native.client.callTool({name:"obs-take-stop",arguments:{}});expect(repeated).toEqual(stopped);
  expect(count(native,"StartRecord")).toBe(1);expect(count(native,"StopRecord")).toBe(1);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it("keeps the independent recording deadline running after frontend cancellation and joins EOF cleanup",async()=>{
  const native=await start(true,legacy,true);
  const started=await native.client.callTool({name:"obs-take-start",arguments:{expectSilent:true,maxDurationSeconds:1}});expect(started.isError).not.toBe(true);
  const before=native.fake.history().at(-1)?.cursor??0;
  native.fake.queueSuccess("GetRecordStatus",{outputActive:true,outputPaused:false,outputBytes:1048576},150);
  const controller=new AbortController();const cancelled=native.client.callTool({name:"obs-get-record-status",arguments:{}},{signal:controller.signal}).then(()=>({ok:true}),reason=>({error:String(reason)}));
  await native.fake.waitForRequest("GetRecordStatus",{after:before});controller.abort();expect(await cancelled).toHaveProperty("error");
  await vi.waitFor(()=>expect(count(native,"StopRecord")).toBe(1),{timeout:4000});expect(native.recording()).toBe(false);expect(count(native,"StartRecord")).toBe(1);
  await close(native);expect(children(native.transport.pid??-1)).toEqual([]);
 });
});

it("handles a second modern caller while stage inspection is held and rechecks authority before mutation",async()=>{
 const native=await start(true,false);const filters={filters:[]};native.fake.queueSuccess("GetSourceFilterList",filters,350);
 const stage=native.client.callTool({name:"obs-stage-scene",arguments:{sceneCollection:native.state.collection,sceneName:native.state.sceneName}});
 await native.fake.waitForRequest("GetSourceFilterList");
 const claimed=await raw(native,9001,"tools/call",{name:"obs-claim-control",arguments:{minutes:2,reason:"another client"},_meta:meta("other-caller")});
 expect(claimed.result).toMatchObject({structuredContent:{holder:"other-caller"}});
 const result=await stage;expect(result.isError).toBe(true);expect(count(native,"SetCurrentPreviewScene")).toBe(0);
 const status=await raw(native,9002,"tools/call",{name:"obs-control-status",arguments:{},_meta:meta("other-caller")});expect(status.result).toMatchObject({structuredContent:{caller:"other-caller",lease:{holder:"other-caller"}}});
 expect(children(native.transport.pid!)).toEqual([]);expect(native.fake.connectionCount).toBe(1);
});

it("retains and joins a real active take on transport EOF without starting a compatibility child",async()=>{
 const native=await start(true,false,true);const started=await native.client.callTool({name:"obs-take-start",arguments:{expectSilent:true,maxDurationSeconds:10}});expect(started.isError).not.toBe(true);expect(native.recording()).toBe(true);
 expect(children(native.transport.pid!)).toEqual([]);await close(native);expect(count(native,"StartRecord")).toBe(1);expect(count(native,"StopRecord")).toBe(1);expect(native.recording()).toBe(false);
});

it("refuses unowned task metadata before OBS activation while keeping the qualified lane native",async()=>{
 const native=await start(true,false);
 const rich=meta("rich-client");rich["io.modelcontextprotocol/clientCapabilities"]={tasks:{}} as unknown as typeof rich["io.modelcontextprotocol/clientCapabilities"];
 const result=await raw(native,9003,"server/discover",{_meta:rich});expect(result.error).toMatchObject({code:-32601});expect(native.fake.connectionCount).toBe(0);
 await native.client.callTool({name:"obs-get-record-status",arguments:{}});expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
});

it("fails CLI and HTTP configuration before activation instead of executing the SDK",()=>{
 for(const [args,extra] of [[['--help'],{}],[[],{OBS_MCP_HTTP_PORT:'3456'}],[[],{OBS_MCP_DYNAMIC_TOOLSETS:'true'}]] as const){
  const result=spawnSync(binary,[...args],{cwd:root,encoding:'utf8',env:{PATH:process.env.PATH,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(root,'native/target/nonexistent-service-node'),OBS_MCP_COMPAT_ENTRY:join(root,'native/target/nonexistent-service-sdk.js'),...extra}});
  expect(result.status).toBe(1);expect(result.stdout).toBe('');expect(result.stderr).toContain('Native stateful qualification supports static stdio only');
 }
});

function demoFixture(session:Session){
 session.state.program=session.state.sceneName;
 const item=session.state.items[0]!;item.sceneItemTransform.rotation=10;item.sceneItemTransform.positionX=25;
 let muted=false;let tracks:Record<string,boolean>={1:true,2:false,3:false,4:false,5:false,6:false};
 session.fake.respondWith('GetSceneItemList',data=>({sceneItems:data.sceneName===session.state.sceneName||data.sceneUuid===session.state.sceneUuid?structuredClone(session.state.items):[]}));
 session.fake.respondWith('GetInputMute',()=>({inputMuted:muted}));session.fake.respondWith('GetInputAudioTracks',()=>({inputAudioTracks:tracks}));
 session.fake.respondWith('SetInputMute',data=>{muted=data.inputMuted===true;return {};});
 session.fake.respondWith('SetInputAudioTracks',data=>{tracks=structuredClone(data.inputAudioTracks) as Record<string,boolean>;return {};});
 session.fake.respondWith('SetSceneItemTransform',data=>{Object.assign(item.sceneItemTransform,data.sceneItemTransform);return {};});
 return {args:{sceneCollection:session.state.collection,sceneName:session.state.sceneName,inputName:'Application'},muted:()=>muted,tracks:()=>tracks,setMuted:(value:boolean)=>{muted=value;}};
}
async function startDemo(native:boolean,legacy:boolean){let demo:ReturnType<typeof demoFixture>|undefined;const session=await start(native,legacy,false,{},session=>{demo=demoFixture(session);});return {session,demo:demo!};}
async function recoverPair(oracle:Session,native:Session,expectedId:string,actualId:string,apply:boolean){
 const expected=await oracle.client.callTool({name:'obs-recover-demo',arguments:{operationId:expectedId,apply}});const actual=await native.client.callTool({name:'obs-recover-demo',arguments:{operationId:actualId,apply}});expect(normal(actual,native)).toEqual(normal(expected,oracle));return actual;
}
for(const legacy of[false,true])describe(`persistent demo owner (legacy=${legacy})`,()=>{
 it('matches full SDK preparation/recovery reports while writing native V2 and retaining one socket',async()=>{
  const {session:oracle,demo:source}=await startDemo(false,legacy);const {session:native,demo:target}=await startDemo(true,legacy);
  await same(oracle,native,'obs-prepare-demo',target.args);expect(count(native,'SetInputMute')).toBe(0);
  const expected=await oracle.client.callTool({name:'obs-prepare-demo',arguments:{...source.args,apply:true}});const actual=await native.client.callTool({name:'obs-prepare-demo',arguments:{...target.args,apply:true}});expect(normal(actual,native)).toEqual(normal(expected,oracle));expect(actual.structuredContent).toMatchObject({status:'prepared',applied:true});
  const operation=(actual.structuredContent as {operationId:string}).operationId;const sourceId=(expected.structuredContent as {operationId:string}).operationId;
  const journal=JSON.parse(readFileSync(join(native.directory,'demo-recovery',`${operation}.json`),'utf8'));expect(journal.version).toBe(2);expect(journal.provenance.fields.every((field:{preparation:{steps:string[]}})=>field.preparation.steps.at(-1)==='accepted')).toBe(true);
  expect(target.muted()).toBe(true);expect(Object.values(target.tracks()).every(value=>value===false)).toBe(true);
  await recoverPair(oracle,native,sourceId,operation,false);await recoverPair(oracle,native,sourceId,operation,true);await recoverPair(oracle,native,sourceId,operation,true);
  expect(target.muted()).toBe(false);expect(target.tracks()['1']).toBe(true);expect(native.state.items[0]!.sceneItemTransform.rotation).toBe(10);
  const final=JSON.parse(readFileSync(join(native.directory,'demo-recovery',`${operation}.json`),'utf8'));expect(final.recovered).toBe(true);expect(final.provenance.completed).toBe(true);
  const observed=await native.client.callTool({name:'obs-observe',arguments:{}});expect(observed.structuredContent).toMatchObject({recovery:{status:'observed',operations:[]}});expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('refuses an operator conflict with the exact SDK report and no restoration writes',async()=>{
  const {session:oracle,demo:source}=await startDemo(false,legacy);const {session:native,demo:target}=await startDemo(true,legacy);
  const expected=await oracle.client.callTool({name:'obs-prepare-demo',arguments:{...source.args,apply:true}});const actual=await native.client.callTool({name:'obs-prepare-demo',arguments:{...target.args,apply:true}});expect((actual.structuredContent as {status:string}).status).toBe('prepared');
  oracle.state.items[0]!.sceneItemTransform.rotation=123;native.state.items[0]!.sceneItemTransform.rotation=123;
  const before=native.fake.history().filter(record=>record.frame.op===6&&String(record.frame.d.requestType).startsWith('Set')).length;
  const result=await recoverPair(oracle,native,(expected.structuredContent as {operationId:string}).operationId,(actual.structuredContent as {operationId:string}).operationId,true);expect(result.isError).toBe(true);expect(result.structuredContent).toMatchObject({status:'conflict',applied:false});
  expect(native.fake.history().filter(record=>record.frame.op===6&&String(record.frame.d.requestType).startsWith('Set'))).toHaveLength(before);
 });
});

async function reopen(session:Session,legacy:boolean,native=true){
 await close(session);
 const client=new Client({name:'persistent-parity',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
 const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[compat],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:session.fake.url,OBS_MCP_STATE_DIR:session.directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:'',OBS_MCP_COMPAT_NODE:join(root,'native/target/nonexistent-service-node'),OBS_MCP_COMPAT_ENTRY:join(root,'native/target/nonexistent-service-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});
 transport.stderr?.on('data',()=>undefined);session.client=client;session.transport=transport;await client.connect(transport);
}
function journalPath(session:Session){const files=readdirSync(join(session.directory,'demo-recovery')).filter(file=>file.endsWith('.json'));expect(files).toHaveLength(1);return join(session.directory,'demo-recovery',files[0]!);}

it('retains a cancelled frontend mutation and permits fresh observation while the sole demo loan settles',async()=>{
 const {session:native,demo}=await startDemo(true,false);
 native.fake.queueSuccess('SetInputMute',{},350);const controller=new AbortController();
 const frontend=native.client.callTool({name:'obs-prepare-demo',arguments:{...demo.args,apply:true}},{signal:controller.signal}).then(value=>({value}),reason=>({error:String(reason)}));
 await native.fake.waitForRequest('SetInputMute');demo.setMuted(true);const path=journalPath(native);
 expect(JSON.parse(readFileSync(path,'utf8')).provenance.fields[0].preparation.steps.at(-1)).toBe('possible-dispatch');
 controller.abort();expect(await frontend).toHaveProperty('error');
 const rival=await native.client.callTool({name:'obs-stage-scene',arguments:{sceneCollection:native.state.collection,sceneName:native.state.sceneName}});expect(rival.isError).toBe(true);expect(count(native,'SetCurrentPreviewScene')).toBe(0);
 const observation=native.client.callTool({name:'obs-observe',arguments:{}});
 await vi.waitFor(()=>expect(JSON.parse(readFileSync(path,'utf8')).provenance.fields[0].preparation.steps.at(-1)).toBe('unknown'),{timeout:3000});
 await observation;
 const stable=await native.client.callTool({name:'obs-observe',arguments:{}});expect(stable.structuredContent).toMatchObject({recovery:{operations:[{provenance:{sourceUncertain:true}}]}});
 const operation=JSON.parse(readFileSync(path,'utf8')).operationId;const before=native.fake.history().length;
 const refused=await native.client.callTool({name:'obs-recover-demo',arguments:{operationId:operation,apply:true}});expect(refused.structuredContent).toMatchObject({status:'unknown',applied:false});expect(native.fake.history()).toHaveLength(before);
 expect(count(native,'SetInputMute')).toBe(1);expect(count(native,'SetInputAudioTracks')).toBe(0);expect(children(native.transport.pid!)).toEqual([]);
});

it('preserves possible-dispatch across actual process death and refuses recovery before any restart OBS read',async()=>{
 const {session:native,demo}=await startDemo(true,false);
 native.fake.queueSuccess('SetInputMute',{},1200);
 const frontend=native.client.callTool({name:'obs-prepare-demo',arguments:{...demo.args,apply:true}}).then(value=>({value}),reason=>({error:String(reason)}));
 await native.fake.waitForRequest('SetInputMute');demo.setMuted(true);const path=journalPath(native);const bytes=readFileSync(path,'utf8');const journal=JSON.parse(bytes);
 expect(journal.version).toBe(2);expect(journal.provenance.fields[0].preparation.steps.at(-1)).toBe('possible-dispatch');
 process.kill(native.transport.pid!,'SIGKILL');expect(await frontend).toHaveProperty('error');await vi.waitFor(()=>expect(native.fake.openConnectionCount).toBe(0),{timeout:3000});
 const connections=native.fake.connectionCount;const requests=native.fake.history().length;
 await reopen(native,false);
 for(const apply of[false,true]){const refused=await native.client.callTool({name:'obs-recover-demo',arguments:{operationId:journal.operationId,apply}});expect(refused.structuredContent).toMatchObject({status:'unknown',applied:false,provenance:{sourceUncertain:true}});}
 expect(native.fake.connectionCount).toBe(connections);expect(native.fake.history()).toHaveLength(requests);expect(readFileSync(path,'utf8')).toBe(bytes);expect(children(native.transport.pid!)).toEqual([]);
 // The predecessor eagerly bootstraps capabilities, but its handler rejects
 // native V2 before operational reads/writes or reinterpretation of authority.
 await reopen(native,false,false);const old=await native.client.callTool({name:'obs-recover-demo',arguments:{operationId:journal.operationId,apply:true}});expect(old.structuredContent).toMatchObject({status:'failed',applied:false,issues:['Invalid recovery journal; manual investigation required']});
 expect(native.fake.history().slice(requests).filter(record=>record.frame.op===6).every(record=>record.frame.d.requestType==='GetVersion')).toBe(true);expect(count(native,'SetInputMute')).toBe(1);expect(count(native,'SetInputAudioTracks')).toBe(0);expect(readFileSync(path,'utf8')).toBe(bytes);
});

it('retains the first demo reply and matches the SDK conflict for an overlapping preparation',async()=>{
 const {session:oracle,demo:source}=await startDemo(false,false);const {session:native,demo:target}=await startDemo(true,false);
 const first:ReturnType<Client['callTool']>[]=[];const held:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>[]=[];
 let released=false;
 const release=()=>{for(const [index,session] of[oracle,native].entries()){const request=held[index];if(request&&session.fake.openConnectionCount>0)session.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetSceneCollectionList',requestStatus:{result:true,code:100},responseData:{currentSceneCollectionName:session.state.collection}}},request.connectionId);}released=true;};
 try{
  for(const [session,demo] of [[oracle,source],[native,target]] as const){
   // Explicit replies below, rather than a short timer, end the overlap.
   session.fake.queueSuccess('GetSceneCollectionList',{currentSceneCollectionName:session.state.collection},60_000);
   const request=session.fake.waitForRequest('GetSceneCollectionList',{timeoutMs:10_000});
   const pending=session.client.callTool({name:'obs-prepare-demo',arguments:demo.args});void pending.catch(()=>undefined);first.push(pending);
   held.push(await Promise.race([request,pending.then(()=>{throw new Error('Preparation completed before its held fixture read was observed');})]));
  }
  const conflictExpected=await oracle.client.callTool({name:'obs-prepare-demo',arguments:source.args});const conflictActual=await native.client.callTool({name:'obs-prepare-demo',arguments:target.args});expect(normal(conflictActual,native)).toEqual(normal(conflictExpected,oracle));expect(conflictActual.structuredContent).toMatchObject({status:'conflict',applied:false});
  release();
  const [expected,actual]=await Promise.all(first);expect(normal(actual,native)).toEqual(normal(expected,oracle));expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 }finally{
  if(!released)release();
  await Promise.allSettled(first);
 }
});

function planArgs(session:Session){return {sceneCollection:session.state.collection,sceneName:session.state.sceneName,template:{version:1,name:'Fixture template',canvas:{width:1920,height:1080},roles:[{role:'application',inputKinds:['screen_capture'],transform:{positionX:12},enabled:true,locked:true,index:0}]},bindings:[{role:'application',sourceName:'Application',sourceUuid:'application-source-uuid',containerUuid:session.state.sceneUuid,sceneItemId:7}]};}
for(const legacy of[false,true])describe(`persistent read workflows/catalogs (legacy=${legacy})`,()=>{
 it('uses the same physical owner for complete SDK planner/audit reports and narrow optional capabilities',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  const expectedPlan=await oracle.client.callTool({name:'obs-plan-scene-template',arguments:planArgs(oracle)});const actualPlan=await native.client.callTool({name:'obs-plan-scene-template',arguments:planArgs(native)});expect(normal(actualPlan,native)).toEqual(normal(expectedPlan,oracle));
  await same(oracle,native,'obs-audit-scene',{sceneCollection:native.state.collection,sceneName:native.state.sceneName,includeScreenshot:true,requiredItems:[{sourceName:'Application',sourceUuid:'application-source-uuid',containerUuid:native.state.sceneUuid,sceneItemId:7}]});
  await same(oracle,native,'obs-get-record-status');expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('returns the exact three prompts and two resource templates without activating OBS',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  const prompts=await native.client.listPrompts();expect(prompts).toEqual(await oracle.client.listPrompts());expect(prompts.prompts).toHaveLength(3);
  expect(await native.client.listResourceTemplates()).toEqual(await oracle.client.listResourceTemplates());
  for(const [name,args] of [['pre-stream-check',{}],['software-demo',{sceneCollection:'Fixture',sceneName:'Review',inputName:'Application',maxDurationSeconds:'8'}],['record-demo',{window:'Fixture app',scene:'Review',seconds:'8'}]] as const){expect(await native.client.getPrompt({name,arguments:args})).toEqual(await oracle.client.getPrompt({name,arguments:args}));}
  expect(native.fake.connectionCount).toBe(0);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('matches pinned Zod codepoint bounds rather than UTF16 unit counts',async()=>{
  for(const length of[256,257]){const oracle=await start(false,legacy);const native=await start(true,legacy);if(length===256)await same(oracle,native,'obs-get-version');const result=await same(oracle,native,'obs-observe',{canvasUuid:'💡'.repeat(length)});if(length===257){expect(result.isError).toBe(true);expect(native.fake.connectionCount).toBe(0);}else{expect(result.structuredContent).toMatchObject({status:'unknown',reason:'capability_unavailable'});}}
 });
});

it('retains a cancelled planner read on the existing owner and fences structural changes',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('GetVideoSettings',{baseWidth:1920,baseHeight:1080},250);
 const controller=new AbortController();const work=native.client.callTool({name:'obs-plan-scene-template',arguments:planArgs(native)},{signal:controller.signal}).then(value=>({value}),reason=>({error:String(reason)}));await native.fake.waitForRequest('GetVideoSettings');controller.abort();expect(await work).toHaveProperty('error');
 await native.client.callTool({name:'obs-get-record-status',arguments:{}});expect(count(native,'GetVideoSettings')).toBe(1);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
});

for(const legacy of[false,true])it(`reads both advertised resource templates on the same owner (legacy=${legacy})`,async()=>{
 const configure=(session:Session)=>{session.fake.respondWith('GetSceneItemList',data=>({sceneItems:[{sceneName:data.sceneName,sceneItemId:7,sourceName:'fixture'}]}));session.fake.respondWith('GetSourceScreenshot',()=>({imageData:'data:image/jpeg;base64,YWJjZA=='}));};
 const oracle=await start(false,legacy,false,{},configure);const native=await start(true,legacy,false,{},configure);
 for(const name of['Review','My scene','日本語 💡','one/two','100% +'])for(const suffix of['items','screenshot']){
  const uri=`obs://scene/${encodeURIComponent(name)}/${suffix}`;
  expect(normal(await native.client.readResource({uri}),native)).toEqual(normal(await oracle.client.readResource({uri}),oracle));
  const request=suffix==='items'?'GetSceneItemList':'GetSourceScreenshot';for(const session of[oracle,native]){const frame=session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).at(-1)!.frame;expect(frame.d.requestData).toEqual(suffix==='items'?{sceneName:name}:{sourceName:name,imageFormat:'jpeg',imageWidth:960});}
 }
 const uri='obs://scene/Missing/items';for(const session of[oracle,native])session.fake.respondWith('GetSceneItemList',()=>{throw new FakeOBSRequestError(600,'fixture missing');});
 const failure=async(session:Session,uri:string)=>{try{await session.client.readResource({uri});throw new Error('expected rejection');}catch(error){return {code:(error as {code?:number}).code,message:String(error)};}};
 expect(await failure(native,uri)).toEqual(await failure(oracle,uri));expect(await failure(native,'obs://unknown/resource')).toEqual(await failure(oracle,'obs://unknown/resource'));
 for(const imageData of['data:image/gif;base64,YWJjZA==','data:image/jpeg;base64,!!!!','data:image/jpeg;base64,'+'A'.repeat(4*Math.ceil((4*1024*1024+1)/3))]){for(const session of[oracle,native])session.fake.respondWith('GetSourceScreenshot',()=>({imageData}));expect(await failure(native,'obs://scene/Review/screenshot')).toEqual(await failure(oracle,'obs://scene/Review/screenshot'));}
 expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
});

function mediaFixture(session:Session){
 const directory=join(session.directory,'recordings');session.fake.respondWith('GetRecordDirectory',()=>({recordDirectory:directory}));
 execFileSync('ffmpeg',['-v','error','-f','lavfi','-i','color=c=red:s=160x90:r=10:d=4','-f','lavfi','-i','color=c=blue:s=160x90:r=10:d=4','-filter_complex','[0:v:0][1:v:0]concat=n=2:v=1:a=0[v]','-map','[v]','-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-t','8',join(directory,'source.mkv')],{stdio:'pipe'});
}
// Media evidence (including all durations and hashes) remains intact; only the
// independently approved fixture root and JSON property ordering differ.
function mediaNormal(value:unknown,session:Session):unknown{
 if(Array.isArray(value))return value.map(value=>mediaNormal(value,session));
 if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,value])=>[key,mediaNormal(value,session)]));
 if(typeof value==='string'){try{const data=JSON.parse(value);if(data&&typeof data==='object')return {jsonValue:mediaNormal(data,session)};}catch{}return value.replaceAll(session.directory,'FIXTURE_ROOT');}return value;
}
for(const legacy of[false,true])it(`qualifies global-registry media result wrappers and verified publication (legacy=${legacy})`,async()=>{
 const oracle=await start(false,legacy,true,{},mediaFixture);const native=await start(true,legacy,true,{},mediaFixture);
 expect((await native.client.listTools()).tools.map(tool=>tool.name)).toEqual(expect.arrayContaining(['obs-contact-sheet','obs-trim-take']));
 const sourceBefore=readFileSync(join(native.directory,'recordings/source.mkv'));
 const sheetArgs={path:'source.mkv',times:[0.5,4.5],columns:2,width:160};
 const expectedSheet=await oracle.client.callTool({name:'obs-contact-sheet',arguments:sheetArgs});const actualSheet=await native.client.callTool({name:'obs-contact-sheet',arguments:sheetArgs});
 expect(actualSheet.isError).not.toBe(true);expect(actualSheet.structuredContent).toHaveProperty('message');expect(mediaNormal(actualSheet,native)).toEqual(mediaNormal(expectedSheet,oracle));
 const args={path:'source.mkv',minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false};
 for(const apply of[false,true]){const expected=await oracle.client.callTool({name:'obs-trim-take',arguments:{...args,apply}});const actual=await native.client.callTool({name:'obs-trim-take',arguments:{...args,apply}});expect(actual.isError).not.toBe(true);expect(mediaNormal(actual,native)).toEqual(mediaNormal(expected,oracle));}
 const output=join(native.directory,'recordings/source.trimmed.mp4');execFileSync('ffmpeg',['-v','error','-xerror','-err_detect','explode','-i',output,'-map','0:v:0','-f','null','-'],{stdio:'pipe'});expect(readFileSync(join(native.directory,'recordings/source.mkv'))).toEqual(sourceBefore);
 const repeated=await native.client.callTool({name:'obs-trim-take',arguments:{...args,apply:true}});expect(repeated.isError).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 await close(native);expect([...readdirSync(native.directory),...readdirSync(join(native.directory,'recordings'))].some(name=>name.startsWith('.native-media-')||name.includes('.partial'))).toBe(false);
},45000);

it('refuses media publication after recording directory changes and joins cancelled read jobs on EOF',async()=>{
 const native=await start(true,false,true,{},mediaFixture);let directories=0;
 native.fake.respondWith('GetRecordDirectory',()=>({recordDirectory:++directories>1?native.directory:join(native.directory,'recordings')}));
 const before=readFileSync(join(native.directory,'recordings/source.mkv'));
 const result=await native.client.callTool({name:'obs-trim-take',arguments:{path:'source.mkv',apply:true,minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false}});
 expect(result.isError).toBe(true);expect(readdirSync(join(native.directory,'recordings')).some(name=>name==='source.trimmed.mp4'||name.includes('.partial'))).toBe(false);expect(readFileSync(join(native.directory,'recordings/source.mkv'))).toEqual(before);
 native.fake.queueSuccess('GetRecordDirectory',{recordDirectory:join(native.directory,'recordings')},250);const cursor=native.fake.history().at(-1)?.cursor??0;
 const abort=new AbortController();const pending=native.client.callTool({name:'obs-contact-sheet',arguments:{path:'source.mkv'}},{signal:abort.signal}).then(value=>({value}),reason=>({error:String(reason)}));await native.fake.waitForRequest('GetRecordDirectory',{after:cursor});abort.abort();expect(await pending).toHaveProperty('error');await close(native);expect(native.fake.openConnectionCount).toBe(0);expect(children(native.transport.pid??-1)).toEqual([]);expect([...readdirSync(native.directory),...readdirSync(join(native.directory,'recordings'))].some(name=>name.startsWith('.native-media-')||name.includes('.partial'))).toBe(false);
},45000);

it('bounds independent media admission and refuses recording until retained media jobs settle',async()=>{
 const native=await start(true,false,true,{},mediaFixture);const directory=join(native.directory,'recordings');
 native.fake.queueSuccess('GetRecordDirectory',{recordDirectory:directory},350);
 const controllers=[new AbortController(),new AbortController()];const jobs=controllers.map(controller=>native.client.callTool({name:'obs-contact-sheet',arguments:{path:'source.mkv'}},{signal:controller.signal}).then(value=>({value}),reason=>({error:String(reason)})));
 await native.fake.waitForRequest('GetRecordDirectory');
 const third=await native.client.callTool({name:'obs-contact-sheet',arguments:{path:'source.mkv'}});expect(third.isError).toBe(true);expect(JSON.stringify(third)).toContain('Capacity');
 const take=await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true}});expect(take.isError).toBe(true);expect(count(native,'StartRecord')).toBe(0);
 controllers.forEach(controller=>controller.abort());await Promise.all(jobs);await close(native);expect(native.fake.openConnectionCount).toBe(0);expect(readdirSync(native.directory).some(name=>name.startsWith('.native-media-'))).toBe(false);
},15000);

const customReadNames=['obs-get-status','obs-test-connection','obs-sleep','obs-get-input-list','obs-get-input-kind-list','obs-get-source-screenshot','obs-preflight'];
function directoryFixture(session:Session){session.fake.respondWith('GetRecordDirectory',()=>({recordDirectory:join(session.directory,'recordings')}));}
for(const legacy of[false,true])describe(`persistent custom read callbacks (legacy=${legacy})`,()=>{
 it('qualifies all seven exact SDK contracts, reports and read effects on one physical connection',async()=>{
  const oracle=await start(false,legacy,true,{},directoryFixture);const native=await start(true,legacy,true,{},directoryFixture);
  const list=await native.client.listTools();const expectedList=await oracle.client.listTools();const contract=JSON.parse(readFileSync(join(root,"native/assets/tools.json"),"utf8"));const declared=[...new Set([...Object.keys(contract.requests),...Object.keys(contract.locals),...Object.keys(contract.workflows),...Object.keys(contract.service.handlers)])].filter(name=>!['obs-start-stream','obs-stop-stream','obs-toggle-stream'].includes(name)).sort();expect(list.tools.map(tool=>tool.name).sort()).toEqual(declared);expectNativeInventory(list.tools,expectedList.tools);for(const name of customReadNames)expect(list.tools.find(tool=>tool.name===name)).toEqual(expectedList.tools.find(tool=>tool.name===name));
  await same(oracle,native,'obs-get-version');await same(oracle,native,'obs-get-status');
  for(const[name,args]of[['obs-test-connection',{}],['obs-get-input-list',{}],['obs-get-input-list',{inputKind:'screen_capture'}],['obs-get-input-kind-list',{}],['obs-get-input-kind-list',{unversioned:false}],['obs-get-source-screenshot',{sourceName:'Application',imageFormat:'jpeg',imageWidth:960}],['obs-sleep',{sleepMillis:0}],['obs-sleep',{sleepMillis:3}],['obs-sleep',{sleepFrames:0}],['obs-sleep',{sleepFrames:1}],['obs-sleep',{}],['obs-sleep',{sleepMillis:1,sleepFrames:1}]] as const)await same(oracle,native,name,args);
  expect(count(native,'GetVersion')).toBe(3);expect(count(native,'GetVideoSettings')).toBe(2);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
  const trace=(session:Session)=>session.fake.history().filter(item=>item.frame.op===6||item.frame.op===8).map(item=>({op:item.frame.op,type:item.frame.d.requestType,data:item.frame.d.requestData??{},requests:item.frame.d.requests,executionType:item.frame.d.executionType,haltOnFailure:item.frame.d.haltOnFailure}));
  const stripIds=(value:unknown):unknown=>Array.isArray(value)?value.map(stripIds):value&&typeof value==='object'?Object.fromEntries(Object.entries(value).filter(([key])=>key!=='requestId').map(([key,value])=>[key,stripIds(value)])):value;
  expect(stripIds(trace(native))).toEqual(stripIds(trace(oracle)));
  const previous=[trace(oracle).length,trace(native).length];await same(oracle,native,'obs-preflight',{expectSilent:true,minFreeDiskMb:0});
  // Source intentionally launches six independent checks with Promise.all.
  // Preserve every read and each serial batch's order, allowing their overall
  // wire interleaving to differ on the single native actor FIFO.
  const canonical=(value:unknown):string=>JSON.stringify(value,(_key,item)=>item&&typeof item==='object'&&!Array.isArray(item)?Object.fromEntries(Object.entries(item).sort(([a],[b])=>a.localeCompare(b))):item);
  expect((stripIds(trace(native).slice(previous[1])) as unknown[]).map(canonical).sort()).toEqual((stripIds(trace(oracle).slice(previous[0])) as unknown[]).map(canonical).sort());
 });
 it('serves disconnected status and millisecond sleep offline without OBS requests or SDK fallback',async()=>{
  const extra={OBS_WEBSOCKET_URL:'ws://127.0.0.1:1'};const oracle=await start(false,legacy,false,extra);const native=await start(true,legacy,false,extra);
  const status=await same(oracle,native,'obs-get-status');expect(status.structuredContent).toMatchObject({obs:{connected:false,identified:false,url:extra.OBS_WEBSOCKET_URL}});
  for(const args of[{sleepMillis:0},{sleepMillis:2},{},{sleepMillis:0,sleepFrames:0}])await same(oracle,native,'obs-sleep',args);
  expect(native.fake.history()).toHaveLength(0);expect(native.fake.connectionCount).toBe(0);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('preserves screenshot default, tiny, invalid and exact upper-limit configuration errors',async()=>{
  for(const limit of['','2',' 2','0','6291457']){const extra={OBS_MCP_MAX_SCREENSHOT_BYTES:limit};const oracle=await start(false,legacy,false,extra);const native=await start(true,legacy,false,extra);const result=await same(oracle,native,'obs-get-source-screenshot',{sourceName:'Application',imageFormat:'png'});expect(result.isError).toBe(limit?true:undefined);expect(count(native,'GetSourceScreenshot')).toBe(1);}
 });
 it('keeps detailed errors and preflight root refusal explicit',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  for(const session of[oracle,native]){session.fake.respondWith('GetInputList',()=>{throw new FakeOBSRequestError(500,'PRIVATE input diagnostic');});session.fake.respondWith('GetInputKindList',()=>{throw new FakeOBSRequestError(500,'PRIVATE kind diagnostic');});}
  await same(oracle,native,'obs-get-input-list');await same(oracle,native,'obs-get-input-kind-list');
  const result=await native.client.callTool({name:'obs-preflight',arguments:{}});expect(result.isError).toBe(true);expect(JSON.stringify(result)).toContain('explicitly approved recording root');expect(children(native.transport.pid!)).toEqual([]);
 });
});
it('keeps local pacing concurrent with reads and leases and cancels its timer promptly on EOF',async()=>{
 const native=await start(true,false);const long=native.client.callTool({name:'obs-sleep',arguments:{sleepMillis:50_000}}).then(value=>({value}),reason=>({error:String(reason)}));
 await same(await start(false,false),native,'obs-control-status');const read=await native.client.callTool({name:'obs-get-record-status',arguments:{}});expect(read.isError).not.toBe(true);const claim=await native.client.callTool({name:'obs-claim-control',arguments:{minutes:1,reason:'local timer control'}});expect(claim.isError).not.toBe(true);
 const begin=Date.now();await close(native);expect(Date.now()-begin).toBeLessThan(1500);expect(await long).toHaveProperty('error');expect(count(native,'GetVideoSettings')).toBe(0);expect(native.fake.connectionCount).toBe(1);
});

it('keeps disconnected status readable after failed bootstrap and refuses invalid stage image policy before OBS activation',async()=>{
 const native=await start(true,false,false,{OBS_WEBSOCKET_URL:'ws://127.0.0.1:1'});const failed=await native.client.callTool({name:'obs-test-connection',arguments:{}});expect(failed.isError).toBe(true);const status=await native.client.callTool({name:'obs-get-status',arguments:{}});expect(status.isError).not.toBe(true);expect(status.structuredContent).toMatchObject({obs:{connected:false,identified:false}});
 const invalid=await start(true,false,false,{OBS_MCP_MAX_SCREENSHOT_BYTES:'0'});const refused=await invalid.client.callTool({name:'obs-stage-scene',arguments:{sceneCollection:invalid.state.collection,sceneName:invalid.state.sceneName}});expect(refused.isError).toBe(true);expect(JSON.stringify(refused)).toContain('must be a positive integer');expect(invalid.fake.connectionCount).toBe(0);
});
it('roundtrips the exact six MiB decoded screenshot ceiling through the public MCP boundary',async()=>{
 const imageData='data:image/jpeg;base64,'+'A'.repeat(8*1024*1024);const configure=(session:Session)=>session.fake.respondWith('GetSourceScreenshot',()=>({imageData}));
 const extra={OBS_MCP_MAX_SCREENSHOT_BYTES:String(6*1024*1024)};const oracle=await start(false,false,false,extra,configure);const native=await start(true,false,false,extra,configure);
 const expected=await oracle.client.callTool({name:'obs-get-source-screenshot',arguments:{sourceName:'Application',imageFormat:'jpeg'}});const actual=await native.client.callTool({name:'obs-get-source-screenshot',arguments:{sourceName:'Application',imageFormat:'jpeg'}});expect(actual).toEqual(expected);expect(actual.structuredContent).toEqual({mimeType:'image/jpeg',sizeBytes:6*1024*1024});expect(native.fake.connectionCount).toBe(1);expect(count(native,'GetSourceScreenshot')).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
},15000);
for(const legacy of[false,true])it(`projects read callback validation errors before any native OBS activation (legacy=${legacy})`,async()=>{
 const oracle=await start(false,legacy);const native=await start(true,legacy);
 for(const[name,args]of[['obs-get-input-list',{inputKind:null}],['obs-get-input-kind-list',{unversioned:0}],['obs-sleep',{sleepMillis:1.5}],['obs-get-source-screenshot',{sourceName:'Application',imageWidth:7}],['obs-get-source-screenshot',{sourceName:'Application',imageFormat:'💡'.repeat(17)}],['obs-preflight',{minFreeDiskMb:-1}]] as const){const result=await same(oracle,native,name,args);expect(result.isError).toBe(true);}
 expect(native.fake.connectionCount).toBe(0);expect(children(native.transport.pid!)).toEqual([]);
});

it('exits cleanly after EOF with an admitted fifty-second local timer, without transport kill fallback',async()=>{
 const directory=join(root,'native/target',`public-local-timer-${process.pid}-${sequence++}`);mkdirSync(directory,{mode:0o700});
 const child=spawn(binary,[],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:'ws://127.0.0.1:1',OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_NATIVE_RECORDING_ROOT:'',OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_HTTP_PORT:'',OBS_MCP_HTTP_TOKEN:'',OBS_MCP_COMPAT_NODE:join(directory,'nonexistent-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'nonexistent-sdk.js')},stdio:['pipe','pipe','pipe']});
 let output='',exit:{code:number|null;signal:NodeJS.Signals|null}|undefined;child.stdout!.setEncoding('utf8');child.stdout!.on('data',chunk=>{output+=chunk;});child.stderr!.resume();const closed=new Promise<void>(resolve=>child.once('close',(code,signal)=>{exit={code,signal};resolve();}));
 try{for(const[id,name,args]of[[1,'obs-sleep',{sleepMillis:50_000}],[2,'obs-control-status',{}]] as const)child.stdin!.write(JSON.stringify({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,_meta:meta('timer-owner')}})+'\n');await vi.waitFor(()=>expect(output.split('\n').filter(Boolean).map(line=>JSON.parse(line)).some(reply=>reply.id===2)).toBe(true),{timeout:1500});expect(output.split('\n').filter(Boolean).map(line=>JSON.parse(line)).some(reply=>reply.id===1)).toBe(false);child.stdin!.end();await vi.waitFor(()=>expect(exit).toEqual({code:0,signal:null}),{timeout:1500});await closed;}
 finally{if(!exit){child.kill('SIGKILL');await closed;}rmSync(directory,{recursive:true,force:true});}
});

const setters=[
 ['obs-set-input-mute','SetInputMute',{inputName:'Application',inputMuted:true}],
 ['obs-set-input-audio-balance','SetInputAudioBalance',{inputName:'Application',inputAudioBalance:0.25}],
 ['obs-set-input-audio-sync-offset','SetInputAudioSyncOffset',{inputName:'Application',inputAudioSyncOffset:-12.5}],
 ['obs-set-input-audio-monitor-type','SetInputAudioMonitorType',{inputName:'Application',monitorType:'OBS_MONITORING_TYPE_NONE'}],
 ['obs-set-input-deinterlace-field-order','SetInputDeinterlaceFieldOrder',{inputUuid:'application-source-uuid',inputDeinterlaceFieldOrder:'OBS_DEINTERLACE_FIELD_ORDER_TOP'}],
 ['obs-set-input-deinterlace-mode','SetInputDeinterlaceMode',{inputName:'Application',inputDeinterlaceMode:'OBS_DEINTERLACE_MODE_DISABLE'}],
 ['obs-set-scene-item-index','SetSceneItemIndex',{sceneUuid:'review-scene-uuid',sceneItemId:7,sceneItemIndex:0}],
 ['obs-set-scene-item-locked','SetSceneItemLocked',{sceneName:'Review',sceneItemId:7,sceneItemLocked:true}],
 ['obs-set-scene-item-blend-mode','SetSceneItemBlendMode',{sceneName:'Review',sceneItemId:7,sceneItemBlendMode:'OBS_BLEND_NORMAL'}],
 ['obs-set-source-filter-enabled','SetSourceFilterEnabled',{sourceName:'Application',filterName:'fixture filter',filterEnabled:false}],
] as const;
function genericFixture(session:Session){for(const[,request]of setters)session.fake.respondWith(request,()=>({sourceExtra:'callback-success-must-project-the-actual-message'}));}
function receipts(session:Session):Record<string,unknown>[] {const directory=join(session.directory,'operation-receipts');try{return readdirSync(directory).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(directory,name),'utf8')) as Record<string,unknown>);}catch{return [];}}
for(const legacy of[false,true])describe(`closed ten setter receipts (legacy=${legacy})`,()=>{
 it('matches full SDK contracts, success/error messages, and exact single dispatch for every closed setter',async()=>{
  const oracle=await start(false,legacy,false,{},genericFixture);const native=await start(true,legacy,false,{},genericFixture);
  const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;
  for(const[name,request,args]of setters){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));const result=await same(oracle,native,name,{...args,discarded:'SDK strips unknown fields'});expect(result.isError).not.toBe(true);expect(count(native,request)).toBe(1);const sent=native.fake.history().find(item=>item.frame.op===6&&item.frame.d.requestType===request)!.frame;expect(sent.d.requestData).toEqual(args);}
  for(const[name,request,args]of setters){for(const session of[oracle,native])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'PRIVATE authorized setter diagnostic');});const result=await same(oracle,native,name,args);expect(result.isError).toBe(true);expect(count(native,request)).toBe(2);}
  const durable=receipts(native);expect(durable).toHaveLength(20);expect(durable.filter(receipt=>receipt.phase==='accepted')).toHaveLength(10);expect(durable.filter(receipt=>receipt.phase==='rejected')).toHaveLength(10);for(const receipt of durable){expect(receipt).toMatchObject({version:2,kind:'request',postconditionObserved:false,renderedComposition:'unverified'});expect(receipt).not.toHaveProperty('arguments');expect(receipt).not.toHaveProperty('responseData');}
  const observed=await native.client.callTool({name:'obs-observe',arguments:{}});expect((observed.structuredContent as {operationReceipts:{operations:unknown[]}}).operationReceipts.operations).toHaveLength(20);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('keeps Source-permitted edits and staging live during a watched locked take, without stealing its stop owner',async()=>{
  const configure=(session:Session)=>{genericFixture(session);session.fake.respondWith('SetProfileParameter',()=>({}));};
  const oracle=await start(false,legacy,true,{},configure);const native=await start(true,legacy,true,{},configure);
  await same(oracle,native,'obs-take-start',{expectSilent:true,maxDurationSeconds:8});
  for(const[name,,args]of setters)await same(oracle,native,name,args);
  const target={sceneCollection:native.state.collection,sceneName:native.state.sceneName};const expectedStage=await oracle.client.callTool({name:'obs-stage-scene',arguments:target});const stage=await native.client.callTool({name:'obs-stage-scene',arguments:target});expect(normal(stage,native)).toEqual(normal(expectedStage,oracle));expect(stage.isError).not.toBe(true);
  const expectedTake=await oracle.client.callTool({name:'obs-take-scene',arguments:{stageId:(expectedStage.structuredContent as {stageId:string}).stageId}});const take=await native.client.callTool({name:'obs-take-scene',arguments:{stageId:(stage.structuredContent as {stageId:string}).stageId}});expect(normal(take,native)).toEqual(normal(expectedTake,oracle));expect(take.isError).not.toBe(true);
  // The native profile route now owns this same Source take-lock refusal.
  // Its diagnostic result cannot authorize an OBS profile-setting request.
  const sourcePolicy=await oracle.client.callTool({name:'obs-set-profile-parameter',arguments:{parameterCategory:'SimpleOutput',parameterName:'RecQuality',parameterValue:'HQ'}});expect(sourcePolicy.isError).toBe(true);expect(JSON.stringify(sourcePolicy)).toContain('is recording');
  const policy=await native.client.callTool({name:'obs-set-profile-parameter',arguments:{parameterCategory:'SimpleOutput',parameterName:'RecQuality',parameterValue:'HQ'}});expect(policy.isError).toBe(true);expect(normal(policy,native)).toEqual(normal(sourcePolicy,oracle));expect(count(native,'SetProfileParameter')).toBe(0);
  await native.client.callTool({name:'obs-take-stop',arguments:{}});await oracle.client.callTool({name:'obs-take-stop',arguments:{}});expect(count(native,'StartRecord')).toBe(1);expect(count(native,'StopRecord')).toBe(1);expect(native.fake.connectionCount).toBe(1);expect(native.recording()).toBe(false);expect(children(native.transport.pid!)).toEqual([]);
 });
 it('projects invalid arguments exactly before native connection or receipt admission',async()=>{
  const oracle=await start(false,legacy,false,{},genericFixture);const native=await start(true,legacy,false,{},genericFixture);
  for(const[name,,args]of setters){const result=await same(oracle,native,name,Object.fromEntries(Object.keys(args).map(key=>[key,null])));expect(result.isError).toBe(true);}
  expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
 });
});
it('retains a frontend-cancelled accepted setter and the independent recording deadline',async()=>{
 const native=await start(true,false,true,{},genericFixture);await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:1}});
 native.fake.queueSuccess('SetInputMute',{},200);const abort=new AbortController();const work=native.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}},{signal:abort.signal}).catch(reason=>({error:String(reason)}));await native.fake.waitForRequest('SetInputMute');abort.abort();expect(await work).toHaveProperty('error');
 await vi.waitFor(()=>expect(receipts(native).some(receipt=>receipt.version===2&&receipt.phase==='accepted')).toBe(true));await vi.waitFor(()=>expect(count(native,'StopRecord')).toBe(1),{timeout:4000});expect(count(native,'SetInputMute')).toBe(1);expect(native.recording()).toBe(false);expect(native.fake.connectionCount).toBe(1);
});
it('rechecks queued setter authority at actual dispatch and records no-send without replay',async()=>{
 const native=await start(true,false,false,{},genericFixture);native.fake.queueSuccess('GetRecordStatus',{outputActive:false},350);
 const held=native.client.callTool({name:'obs-get-record-status',arguments:{}});await native.fake.waitForRequest('GetRecordStatus');
 const setter=native.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}});await vi.waitFor(()=>expect(receipts(native).some(receipt=>receipt.version===2&&receipt.phase==='dispatching')).toBe(true));
 const claim=await raw(native,9801,'tools/call',{name:'obs-claim-control',arguments:{minutes:1,reason:'queue-race'},_meta:meta('other-client')});expect(claim.result).toMatchObject({structuredContent:{holder:'other-client'}});
 await held;const refused=await setter;expect(refused.isError).toBe(true);expect(JSON.stringify(refused)).toContain('Not changed:');expect(count(native,'SetInputMute')).toBe(0);expect(receipts(native).find(receipt=>receipt.version===2)).toMatchObject({phase:'not-dispatched',requestDispatched:false,requestAccepted:null});
});
it('persists a lost setter reply as unknown and refuses another explicit command without replay',async()=>{
 const native=await start(true,false,false,{},genericFixture);native.fake.queueDisconnect('SetInputMute','lost setter reply');
 const first=await native.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}});expect(first.isError).toBe(true);expect(count(native,'SetInputMute')).toBe(1);expect(receipts(native)[0]).toMatchObject({version:2,phase:'unknown',postconditionObserved:false});
 const next=await native.client.callTool({name:'obs-set-input-audio-balance',arguments:{inputName:'Application',inputAudioBalance:0.5}});expect(next.isError).toBe(true);expect(count(native,'SetInputAudioBalance')).toBe(0);expect(count(native,'SetInputMute')).toBe(1);expect(native.fake.connectionCount).toBe(1);
});
async function restartNative(session:Session,legacy:boolean):Promise<Session>{
 await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));
 const client=new Client({name:'persistent-parity',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
 const transport=new StdioClientTransport({command:binary,args:[],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:session.endpoint,OBS_MCP_STATE_DIR:session.directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_NATIVE_RECORDING_ROOT:'',OBS_MCP_COMPAT_NODE:join(session.directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(session.directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
 const restarted={...session,client,transport};sessions[sessions.indexOf(session)]=restarted;await client.connect(transport);return restarted;
}
for(const legacy of[false,true])it(`preserves possible generic dispatch across real SIGKILL and restart without readback authority (legacy=${legacy})`,async()=>{
 const native=await start(true,legacy,false,{},genericFixture);native.fake.queueSuccess('SetInputMute',{},500);const pending=native.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}}).catch(reason=>({error:String(reason)}));await native.fake.waitForRequest('SetInputMute');expect(receipts(native)[0]).toMatchObject({version:2,phase:'dispatching',requestDispatched:null,requestAccepted:null});
 process.kill(native.transport.pid!,'SIGKILL');expect(await pending).toHaveProperty('error');await vi.waitFor(()=>expect(native.fake.openConnectionCount).toBe(0));
 const resumed=await restartNative(native,legacy);const refused=await resumed.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:false}});expect(refused.isError).toBe(true);expect(count(resumed,'SetInputMute')).toBe(1);
 const observed=await resumed.client.callTool({name:'obs-observe',arguments:{}});expect((observed.structuredContent as {operationReceipts:{operations:unknown[]}}).operationReceipts.operations).toEqual(expect.arrayContaining([expect.objectContaining({version:2,phase:'dispatching',status:'unknown',postconditionObserved:false})]));expect(receipts(resumed)[0]).toMatchObject({phase:'dispatching',requestAccepted:null,postconditionObserved:false});expect(resumed.fake.connectionCount).toBe(2);expect(children(resumed.transport.pid!)).toEqual([]);
});
for(const legacy of[false,true])it(`retains both explicit concurrent setter replies while a watched take deadline stays live (legacy=${legacy})`,async()=>{
 const oracle=await start(false,legacy,true,{},genericFixture);const native=await start(true,legacy,true,{},genericFixture);await same(oracle,native,'obs-take-start',{expectSilent:true,maxDurationSeconds:1});
 const pair=async(session:Session)=>{session.fake.queueSuccess('SetInputMute',{},200);const a=session.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}});await session.fake.waitForRequest('SetInputMute');const b=session.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:false}});return Promise.all([a,b]);};
 const expected=pair(oracle);const actual=pair(native);expect(normal(await actual,native)).toEqual(normal(await expected,oracle));expect(count(native,'SetInputMute')).toBe(2);
 const wire=native.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType==='SetInputMute').map(item=>item.frame.d.requestData);expect(wire).toEqual([{inputName:'Application',inputMuted:true},{inputName:'Application',inputMuted:false}]);
 await vi.waitFor(()=>expect(count(native,'StopRecord')).toBe(1),{timeout:4000});expect(native.recording()).toBe(false);expect(receipts(native).filter(receipt=>receipt.version===2&&receipt.phase==='accepted')).toHaveLength(2);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
});

it('blocks a queued setter on fresh unrelated uncertainty while continuous owned stop still runs',async()=>{
 const native=await start(true,false,true,{},genericFixture);await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:1}});
 native.fake.queueSuccess('GetRecordStatus',{outputActive:true,outputPaused:false,outputBytes:1048576},300);const cursor=native.fake.history().at(-1)?.cursor??0;const held=native.client.callTool({name:'obs-get-record-status',arguments:{}});await native.fake.waitForRequest('GetRecordStatus',{after:cursor});
 const setter=native.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Application',inputMuted:true}});await vi.waitFor(()=>expect(receipts(native).some(receipt=>receipt.version===2&&receipt.phase==='dispatching')).toBe(true));
 const own=receipts(native).find(receipt=>receipt.version===2)!;const operationId='00000000-0000-4000-8000-000000009999';writeFileSync(join(native.directory,'operation-receipts',`${operationId}.json`),JSON.stringify({...own,operationId,phase:'unknown'}),{flag:'wx',mode:0o600});
 await held;expect((await setter).isError).toBe(true);expect(count(native,'SetInputMute')).toBe(0);expect(receipts(native).filter(receipt=>receipt.version===2).map(receipt=>receipt.phase).sort()).toEqual(['not-dispatched','unknown']);
 await vi.waitFor(()=>expect(count(native,'StopRecord')).toBe(1),{timeout:4000});expect(native.recording()).toBe(false);expect(count(native,'StartRecord')).toBe(1);expect(native.fake.connectionCount).toBe(1);const blocked=await native.client.callTool({name:'obs-set-input-audio-balance',arguments:{inputName:'Application',inputAudioBalance:0.5}});expect(blocked.isError).toBe(true);expect(count(native,'SetInputAudioBalance')).toBe(0);
});
