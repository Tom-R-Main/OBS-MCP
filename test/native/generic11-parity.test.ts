/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');const sdk=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,'build/index.js');let seq=0;
const cases=[
 ['obs-set-persistent-data','SetPersistentData',{realm:'arbitrary Realm',slotName:'Slot',slotValue:{nested:[null,true,42],__ordinary:'kept'}}],
 ['obs-set-current-scene-collection','SetCurrentSceneCollection',{sceneCollectionName:'Fixture collection'}],
 ['obs-create-scene-collection','CreateSceneCollection',{sceneCollectionName:'日本語 collection'}],
 ['obs-set-current-profile','SetCurrentProfile',{profileName:'Fixture profile'}],
 ['obs-create-profile','CreateProfile',{profileName:'New profile'}],
 ['obs-remove-profile','RemoveProfile',{profileName:'Old profile'}],
 ['obs-set-stream-service-settings','SetStreamServiceSettings',{streamServiceType:'rtmp_custom',streamServiceSettings:{server:'fixture',key:'PRIVATE fixture',nested:{array:[null,true]}}}],
 ['obs-set-record-directory','SetRecordDirectory',{recordDirectory:'/remote/not-the-approved-root'}],
 ['obs-set-output-settings','SetOutputSettings',{outputName:'Fixture output',outputSettings:{value:42,nested:{preserved:true}}}],
 ['obs-set-video-settings','SetVideoSettings',{fpsNumerator:59.94,fpsDenominator:1,baseWidth:1920.5,baseHeight:1080,outputWidth:960,outputHeight:540}],
 ['obs-set-profile-parameter','SetProfileParameter',{parameterCategory:'Output',parameterName:'Mode',parameterValue:null}],
] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;recordingRoot:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,unadvertised=false,extra:Record<string,string>={}):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`generic11-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({sceneItemId:42,extension:{preserved:true,values:[null,1,false]}}));
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));let recordDirectory=recording?recordingRoot:undefined;fake.respondWith('SetRecordDirectory',data=>{recordDirectory=String(data.recordDirectory);return {};});fake.respondWith('GetRecordDirectory',()=>{if(recordDirectory)return {recordDirectory};throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 const client=new Client({name:'generic11',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false',...extra},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recordingRoot,recording:()=>active};sessions.push(session);await client.connect(transport);if(unadvertised&&!native){const version=await client.callTool({name:'obs-get-version',arguments:{}});expect(version.structuredContent).toMatchObject({availableRequests:['GetVersion']});}return session;
}
function normalized(value:unknown):unknown{if(Array.isArray(value))return value.map(normalized);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,value])=>[key,normalized(value)]));if(typeof value==='string'){try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==='object')return {jsonValue:normalized(parsed)};}catch{}}return value;}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(normalized(actual)).toEqual(normalized(expected));return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});
function report(value:unknown,session:Session):unknown{
 if(Array.isArray(value))return value.map(item=>report(item,session));
 if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,item])=>[key,report(item,session)]));
 if(typeof value==='string'){try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==='object')return {jsonValue:report(parsed,session)};}catch{}return value.replaceAll(session.directory,'FIXTURE_ROOT').replaceAll(session.fake.url,'FIXTURE_URL').replace(/\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z/g,'TIME');}return value;
}
for(const legacy of[false,true])describe(`generic11 actual public SDK parity (legacy=${legacy})`,()=>{
 it('preserves eleven complete contracts, Source callbacks and one-request traces',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;
  for(const[name,request,args]of cases){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));await same(oracle,native,name,{...args,discarded:'Source strips this'});expect(trace(native,request)).toEqual(trace(oracle,request));}
  expect(receipts(native)).toHaveLength(11);for(const receipt of receipts(native))expect(receipt).toMatchObject({version:2,kind:'request',phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
 it('preserves eleven full explicit rejection bodies and never marks rejection as acceptance',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,request,args]of cases){for(const session of[oracle,native])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'PRIVATE explicit callback detail');});expect((await same(oracle,native,name,args)).isError).toBe(true);}expect(receipts(native)).toHaveLength(11);expect(receipts(native).every(receipt=>receipt.phase==='rejected'&&receipt.postconditionObserved===false)).toBe(true);
 });
 it('preserves opaque records, unknown JSON, empty video and exact profile nullable/category semantics',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  for(const slotValue of[null,false,42,'literal',[null,{nested:true}],JSON.parse('{"__proto__":{"keep":true},"constructor":"keep","nested":{"__proto__":"keep"}}')])await same(oracle,native,'obs-set-persistent-data',{realm:'',slotName:'',slotValue});
  for(const settings of[{},JSON.parse('{"__proto__":"strip","constructor":"keep","nested":{"__proto__":"keep","values":[null,1,false]}}')]){await same(oracle,native,'obs-set-stream-service-settings',{streamServiceType:'rtmp_custom',streamServiceSettings:settings});await same(oracle,native,'obs-set-output-settings',{outputName:'Fixture',outputSettings:settings});}
  for(const args of[{}, {fpsNumerator:1.5},{baseWidth:1.5,baseHeight:4096.0},{outputWidth:4096,outputHeight:1}])await same(oracle,native,'obs-set-video-settings',args);
  for(const category of['Output','SimpleOutput','AdvOut','output','General',''])for(const parameterValue of[null,''])await same(oracle,native,'obs-set-profile-parameter',{parameterCategory:category,parameterName:'',parameterValue});
  for(const[,request]of cases)expect(trace(native,request)).toEqual(trace(oracle,request));expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);
 });
 it('projects actual Source unknown/record/nullable validation errors before any native socket or receipt',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  for(const[name,args]of[['obs-set-persistent-data',{}],['obs-set-persistent-data',{realm:'R',slotName:'N'}],['obs-set-persistent-data',{realm:null,slotName:42,slotValue:null}],['obs-set-stream-service-settings',{streamServiceType:'rtmp_custom'}],['obs-set-stream-service-settings',{streamServiceType:'rtmp_custom',streamServiceSettings:null}],['obs-set-stream-service-settings',{streamServiceType:'rtmp_custom',streamServiceSettings:[]}],['obs-set-output-settings',{outputName:'N',outputSettings:[]}],['obs-set-output-settings',{outputName:'N'}],['obs-set-video-settings',{fpsNumerator:0.5,baseWidth:0,outputHeight:4096.5}],['obs-set-video-settings',{baseWidth:null,fpsDenominator:'1'}],['obs-create-profile',{}],['obs-set-current-profile',{profileName:null}],['obs-set-record-directory',{recordDirectory:false}],['obs-set-profile-parameter',{parameterCategory:'Output',parameterName:'N',parameterValue:42}],['obs-set-profile-parameter',{parameterCategory:'Output',parameterName:'N'}]] as const)expect((await same(oracle,native,name,args)).isError).toBe(true);
  expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
 });
 it('keeps the Source output-pending readiness failure through profile switches and rejects new take before StartRecord',async()=>{
  const oracle=await start(false,legacy,true);const native=await start(true,legacy,true);
  await same(oracle,native,'obs-set-profile-parameter',{parameterCategory:'Output',parameterName:'Mode',parameterValue:'Advanced'});await same(oracle,native,'obs-set-current-profile',{profileName:'Another profile'});
  const expected=await oracle.client.callTool({name:'obs-preflight',arguments:{expectSilent:true}});const actual=await native.client.callTool({name:'obs-preflight',arguments:{expectSilent:true}});expect(report(actual,native)).toEqual(report(expected,oracle));expect(actual.structuredContent).toMatchObject({ready:false,checks:expect.arrayContaining([expect.objectContaining({id:'output-settings',status:'fail'})])});
  for(const session of[oracle,native]){expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:5}})).isError).toBe(true);expect(trace(session,'StartRecord')).toHaveLength(0);}expect(native.fake.connectionCount).toBe(1);
 });
 it('refuses exactly the Source ten locked configuration mutations while persistent data and owned Stop remain live',async()=>{
  const oracle=await start(false,legacy,true);const native=await start(true,legacy,true);for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:10}})).isError).not.toBe(true);
  await same(oracle,native,'obs-set-persistent-data',{realm:'R',slotName:'during-take',slotValue:{allowed:true}});
  for(const[name,request,args]of cases.slice(1)){const expected=await oracle.client.callTool({name,arguments:args});const actual=await native.client.callTool({name,arguments:args});expect(expected.isError).toBe(true);expect(actual.isError).toBe(true);const stripId=(value:unknown)=>JSON.stringify(value).replace(/Take [0-9a-f]{8}/g,'Take ID');expect(stripId(actual)).toBe(stripId(expected));expect(trace(native,request)).toHaveLength(0);}
  for(const session of[oracle,native])await session.client.callTool({name:'obs-take-stop',arguments:{}});expect(trace(native,'StopRecord')).toHaveLength(1);expect(native.recording()).toBe(false);expect(native.fake.connectionCount).toBe(1);
 });
 it('preserves advertised capability refusal without intent or an OBS mutation',async()=>{
  const oracle=await start(false,legacy,false,true);const native=await start(true,legacy,false,true);for(const[name,,args]of cases)expect((await same(oracle,native,name,args)).isError).toBe(true);expect(receipts(native)).toEqual([]);for(const[,request]of cases)expect(trace(native,request)).toHaveLength(0);expect(native.fake.connectionCount).toBe(1);
 });
});
it('accepts the remote recording setting without widening the constructor-approved local file authority',async()=>{
 const native=await start(true,false,true);const unapproved=join(native.directory,'unapproved');mkdirSync(unapproved);const sentinel=join(unapproved,'human.txt');writeFileSync(sentinel,'preserve human data');expect((await native.client.callTool({name:'obs-set-record-directory',arguments:{recordDirectory:unapproved}})).isError).not.toBe(true);const preflight=await native.client.callTool({name:'obs-preflight',arguments:{expectSilent:true}});expect(preflight.structuredContent).toMatchObject({ready:false});expect(JSON.stringify(preflight)).toMatch(/outside|approved|confined/i);expect(readFileSync(sentinel,'utf8')).toBe('preserve human data');expect(readdirSync(unapproved)).toEqual(['human.txt']);expect(native.fake.connectionCount).toBe(1);
});
it('does not replay lost collection creation and keeps its unknown durable custody barrier',async()=>{
 const native=await start(true,false);native.fake.queueDisconnect('CreateSceneCollection','lost create reply');expect((await native.client.callTool({name:'obs-create-scene-collection',arguments:{sceneCollectionName:'Maybe created'}})).isError).toBe(true);expect(receipts(native)[0]).toMatchObject({requestType:'CreateSceneCollection',phase:'unknown',requestAccepted:null,postconditionObserved:false});expect((await native.client.callTool({name:'obs-create-scene-collection',arguments:{sceneCollectionName:'Explicit later'}})).isError).toBe(true);expect(trace(native,'CreateSceneCollection')).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
});
it('exposes exactly142 possible qualified tools and all full Source contracts with approved adapters',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;expect(actual).toHaveLength(142);for(const tool of actual)expect(tool).toEqual(expected.find(entry=>entry.name===tool.name));expect(native.fake.connectionCount).toBe(0);expect(children(native)).toEqual([]);for(const name of['obs-pause-record','obs-start-stream','obs-stop-stream','obs-call-request'])expect(actual.map(tool=>tool.name)).not.toContain(name);
});
