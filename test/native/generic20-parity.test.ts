/* SPDX-License-Identifier: GPL-2.0-only */
import { expectNativeInventory } from '../support/native-inventory.js';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { WebSocketServer } from 'ws';
import { mkdirSync, readFileSync, readdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');const sdk=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,'build/index.js');let seq=0;
const cases=[
 ['obs-create-scene','CreateScene',{canvasUuid:'canvas',sceneName:'日本語 scene'}],['obs-remove-scene','RemoveScene',{sceneName:'Review'}],['obs-remove-scene-item','RemoveSceneItem',{sceneUuid:'review-scene-uuid',sceneItemId:7}],['obs-remove-source-filter','RemoveSourceFilter',{sourceName:'Application',filterName:'Filter'}],['obs-set-source-filter-name','SetSourceFilterName',{sourceName:'Application',filterName:'Filter',newFilterName:'Renamed'}],['obs-set-source-filter-index','SetSourceFilterIndex',{sourceName:'Application',filterName:'Filter',filterIndex:0.5}],['obs-remove-input','RemoveInput',{inputName:'Application'}],['obs-set-input-name','SetInputName',{inputName:'Application',newInputName:'New App'}],['obs-duplicate-scene-item','DuplicateSceneItem',{sceneUuid:'review-scene-uuid',sceneItemId:7,destinationSceneUuid:'main-scene-uuid'}],['obs-set-scene-name','SetSceneName',{sceneUuid:'review-scene-uuid',newSceneName:'Renamed'}],['obs-create-scene-item','CreateSceneItem',{sceneUuid:'review-scene-uuid',sourceUuid:'application-source-uuid',enabled:false}],['obs-set-scene-item-enabled','SetSceneItemEnabled',{sceneUuid:'review-scene-uuid',sceneItemId:7,enabled:false}],['obs-set-current-scene','SetCurrentProgramScene',{sceneName:'Review'}],['obs-set-preview-scene','SetCurrentPreviewScene',{sceneName:'Review'}],['obs-trigger-studio-transition','TriggerStudioModeTransition',{}],['obs-set-current-transition','SetCurrentSceneTransition',{transitionName:'Fade'}],['obs-trigger-transition','TriggerStudioModeTransition',{}],['obs-set-scene-transition-override','SetSceneSceneTransitionOverride',{sceneUuid:'review-scene-uuid',transitionName:null,transitionDuration:null}],['obs-set-tbar-position','SetTBarPosition',{position:0.25}],['obs-set-transition-duration','SetCurrentSceneTransitionDuration',{duration:125.5}],
] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,model=false,unadvertised=false):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`generic20-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({sceneItemId:42,extension:{preserved:true,values:[null,1,false]}}));
 if(model){fake.respondWith('SetCurrentPreviewScene',data=>{state.preview=data.sceneUuid===state.sceneUuid||data.sceneName===state.sceneName?state.sceneName:'Main';fake.sendEvent('CurrentPreviewSceneChanged',{sceneName:state.preview,sceneUuid:state.preview===state.sceneName?state.sceneUuid:'main-scene-uuid'});return {};});fake.respondWith('TriggerStudioModeTransition',()=>{[state.program,state.preview]=[state.preview,state.program];fake.sendEvent('CurrentProgramSceneChanged',{sceneName:state.program});return {};});fake.respondWith('GetInputList',()=>({inputs:state.items.length?[{inputName:state.items[0]!.sourceName,inputUuid:'application-source-uuid',inputKind:'screen_capture'}]:[]}));fake.respondWith('SetInputName',data=>{state.items[0]!.sourceName=String(data.newInputName);fake.sendEvent('InputNameChanged',{inputUuid:'application-source-uuid',inputName:data.newInputName});return {};});fake.respondWith('RemoveInput',()=>{state.items=[];fake.sendEvent('InputRemoved',{inputUuid:'application-source-uuid',inputName:'Application'});return {};});fake.respondWith('RemoveSceneItem',()=>{state.items=[];fake.sendEvent('SceneItemRemoved',{sceneUuid:state.sceneUuid,sceneItemId:7});return {};});fake.respondWith('SetSceneItemEnabled',data=>{state.items[0]!.sceneItemEnabled=Boolean(data.sceneItemEnabled);fake.sendEvent('SceneItemEnableStateChanged',{sceneUuid:state.sceneUuid,sceneItemId:7,sceneItemEnabled:data.sceneItemEnabled});return {};});fake.respondWith('SetSceneName',data=>{state.sceneName=String(data.newSceneName);fake.sendEvent('SceneNameChanged',{sceneUuid:state.sceneUuid,sceneName:data.newSceneName,oldSceneName:'Review'});return {};});}
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));fake.respondWith('GetRecordDirectory',()=>{throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 const client=new Client({name:'generic20',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recording:()=>active};sessions.push(session);await client.connect(transport);if(unadvertised&&!native){const version=await client.callTool({name:'obs-get-version',arguments:{}});expect(version.structuredContent).toMatchObject({availableRequests:['GetVersion']});}return session;
}
function normalized(value:unknown):unknown{if(Array.isArray(value))return value.map(normalized);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,value])=>[key,normalized(value)]));if(typeof value==='string'){try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==='object')return {jsonValue:normalized(parsed)};}catch{}}return value;}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(normalized(actual)).toEqual(normalized(expected));return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});
for(const legacy of[false,true])describe(`generic20 actual public SDK parity (legacy=${legacy})`,()=>{
 it('preserves twenty full contracts, annotations, callbacks and literal single-RPC traces',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;for(const[name,request,args]of cases){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));await same(oracle,native,name,{...args,discarded:'Source strips unknown fields'});expect(trace(native,request)).toEqual(trace(oracle,request));}
  expect(receipts(native)).toHaveLength(20);for(const receipt of receipts(native))expect(receipt).toMatchObject({version:2,kind:'request',phase:'accepted',requestAccepted:true,postconditionObserved:false,renderedComposition:'unverified'});expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
 it('preserves twenty complete explicit rejection bodies and permits subsequent explicit commands',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,request,args]of cases){for(const session of[oracle,native])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'PRIVATE explicit callback detail');});expect((await same(oracle,native,name,args)).isError).toBe(true);}expect(receipts(native).every(receipt=>receipt.phase==='rejected'&&receipt.requestAccepted===false&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);
 });
 it('keeps nullable/default/undefined formatting and JSON duplicate output validation source-exact',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);
  for(const args of[{},{enabled:true},{enabled:false},{sceneName:'Review',sourceName:'Application'}])await same(oracle,native,'obs-create-scene-item',args);
  for(const args of[{sceneItemId:7,sceneUuid:'review-scene-uuid',enabled:true},{sceneItemId:7,enabled:false}])await same(oracle,native,'obs-set-scene-item-enabled',args);
  for(const args of[{}, {transitionName:null},{transitionDuration:null},{transitionName:'Fade',transitionDuration:50},{transitionDuration:20000}])await same(oracle,native,'obs-set-scene-transition-override',args);
  for(const args of[{position:0},{position:0.5,release:false},{position:1,release:true}])await same(oracle,native,'obs-set-tbar-position',args);
  for(const response of[{sceneItemId:'wrong',opaque:'never quoted in diagnostics'},{sceneItemId:null,extension:{all:['retained',true]}},{}]){for(const session of[oracle,native])session.fake.respondWith('DuplicateSceneItem',()=>response);const result=await same(oracle,native,'obs-duplicate-scene-item',{sceneItemId:7});if(typeof response.sceneItemId==='string'){expect(result.isError).toBe(true);expect(JSON.stringify(result)).not.toContain('never quoted in diagnostics');}expect(receipts(native).at(-1)?.postconditionObserved).toBe(false);}
  expect(receipts(native).every(receipt=>receipt.phase==='accepted')).toBe(true);
 });
 it('projects invalid arguments without OBS admission, including nullable and default controls',async()=>{
  const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,,args]of cases){if(Object.keys(args).length)expect((await same(oracle,native,name,Object.fromEntries(Object.keys(args).map(key=>[key,{}])))).isError).toBe(true);}
  for(const[name,args]of[['obs-set-scene-transition-override',{transitionDuration:49}],['obs-set-scene-transition-override',{transitionDuration:50.5}],['obs-set-scene-transition-override',{transitionName:42}],['obs-set-tbar-position',{position:0.5,release:null}],['obs-create-scene-item',{enabled:null}],['obs-set-scene-name',{newSceneName:''}]] as const)expect((await same(oracle,native,name,args)).isError).toBe(true);expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
 });
 it('consumes stale staged evidence after actual rename/remove/visibility effects and source events',async()=>{
  for(const[name,args]of[['obs-set-input-name',{inputName:'Application',newInputName:'Renamed App'}],['obs-remove-scene-item',{sceneName:'Review',sceneItemId:7}],['obs-set-scene-item-enabled',{sceneName:'Review',sceneItemId:7,enabled:false}],['obs-set-scene-name',{sceneName:'Review',newSceneName:'Renamed Scene'}]] as const){const oracle=await start(false,legacy,false,true);const native=await start(true,legacy,false,true);const target={sceneCollection:native.state.collection,sceneName:'Review'};const old=await oracle.client.callTool({name:'obs-stage-scene',arguments:target});const staged=await native.client.callTool({name:'obs-stage-scene',arguments:target});expect(old.isError).not.toBe(true);expect(staged.isError).not.toBe(true);await same(oracle,native,name,args);
   const expected=await oracle.client.callTool({name:'obs-take-scene',arguments:{stageId:(old.structuredContent as {stageId:string}).stageId}});const actual=await native.client.callTool({name:'obs-take-scene',arguments:{stageId:(staged.structuredContent as {stageId:string}).stageId}});expect(actual.isError).toBe(true);expect(expected.isError).toBe(true);expect((actual.structuredContent as Record<string,unknown>).reason).toBe((expected.structuredContent as Record<string,unknown>).reason);expect(trace(native,'TriggerStudioModeTransition')).toHaveLength(0);expect(native.fake.connectionCount).toBe(1);}
 });
});
it('retains an accepted frontend-cancelled non-idempotent mutation while the owned take deadline stops once',async()=>{
 const native=await start(true,false,true);await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:1}});native.fake.queueSuccess('CreateScene',{sceneUuid:'created'},200);const abort=new AbortController();const work=native.client.callTool({name:'obs-create-scene',arguments:{sceneName:'Owned created'}},{signal:abort.signal}).catch(reason=>({error:String(reason)}));await native.fake.waitForRequest('CreateScene');abort.abort();expect(await work).toHaveProperty('error');await vi.waitFor(()=>expect(receipts(native).some(receipt=>receipt.requestType==='CreateScene'&&receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true));await vi.waitFor(()=>expect(trace(native,'StopRecord')).toHaveLength(1),{timeout:4000});expect(native.recording()).toBe(false);expect(trace(native,'CreateScene')).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);
});
it('never replays a lost non-idempotent mutation reply or treats present state as recovery authority',async()=>{
 const native=await start(true,false);native.fake.queueDisconnect('CreateScene','lost non-idempotent reply');expect((await native.client.callTool({name:'obs-create-scene',arguments:{sceneName:'Maybe created'}})).isError).toBe(true);expect(receipts(native)[0]).toMatchObject({requestType:'CreateScene',phase:'unknown',requestAccepted:null,postconditionObserved:false});expect((await native.client.callTool({name:'obs-create-scene',arguments:{sceneName:'Explicit later'}})).isError).toBe(true);expect(trace(native,'CreateScene')).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
});

function meta(name:string){return {'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{}};}
async function raw(session:Session,id:number,name:string,args:Record<string,unknown>,caller:string){const prior=session.transport.onmessage;let answer:Record<string,unknown>|undefined;session.transport.onmessage=message=>{const value=message as unknown as Record<string,unknown>;if(value.id===id)answer=value;prior?.(message);};try{await session.transport.send({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,_meta:meta(caller)}} as Parameters<StdioClientTransport['send']>[0]);await vi.waitFor(()=>expect(answer).toBeDefined(),{timeout:10_000});return answer!;}finally{session.transport.onmessage=prior;}}
for(const legacy of[false,true])it(`preserves actual advertised-capability refusal before a receipt or packet (legacy=${legacy})`,async()=>{
 const oracle=await start(false,legacy,false,false,true);const native=await start(true,legacy,false,false,true);for(const session of[oracle,native]){const claim=await session.client.callTool({name:'obs-claim-control',arguments:{minutes:1,reason:'own caller capability control'}});expect(claim.structuredContent).toMatchObject({holder:'generic20'});}for(const[name,args]of[['obs-create-scene',{sceneName:'No capability'}],['obs-create-scene-item',{}],['obs-set-scene-item-enabled',{sceneItemId:7,enabled:true}],['obs-set-transition-duration',{duration:25}],['obs-trigger-transition',{}]] as const){expect((await same(oracle,native,name,args)).isError).toBe(true);}expect(receipts(native)).toEqual([]);for(const[,request]of cases)expect(trace(native,request)).toHaveLength(0);expect(native.fake.connectionCount).toBe(1);
});
it('rechecks a queued non-idempotent caller lease at send and preserves the exact Source refusal',async()=>{
 const oracle=await start(false,false);const native=await start(true,false);
 // The deferred fixture response cannot auto-release inside this15s test.
 // Only the explicit reply below releases the actor after the lease ACKs.
 native.fake.queueSuccess('GetRecordStatus',{outputActive:false},60_000);
 const request=native.fake.waitForRequest('GetRecordStatus',{timeoutMs:10_000});
 let heldSettled=false;const held=native.client.callTool({name:'obs-get-record-status',arguments:{}}).finally(()=>{heldSettled=true;});void held.catch(()=>undefined);
 const heldRequest=await Promise.race([request,held.then(()=>{throw new Error('Held read completed before its fixture request was observed');})]);
 let released=false;let call:ReturnType<Client['callTool']>|undefined;let callSettled=false;
 const release=()=>{native.fake.sendRaw({op:7,d:{requestId:heldRequest.frame.d.requestId,requestType:'GetRecordStatus',requestStatus:{result:true,code:100},responseData:{outputActive:false}}},heldRequest.connectionId);released=true;};
 try{
  call=native.client.callTool({name:'obs-create-scene',arguments:{sceneName:'Refused'}}).finally(()=>{callSettled=true;});void call.catch(()=>undefined);
  await vi.waitFor(()=>expect(receipts(native).some(receipt=>receipt.requestType==='CreateScene'&&receipt.phase==='dispatching')).toBe(true),{timeout:10_000});
  expect(heldSettled).toBe(false);expect(callSettled).toBe(false);
  for(const session of[oracle,native]){const claim=await raw(session,8801,'obs-claim-control',{minutes:1,reason:'queue guard'},'other-client');expect(claim.result).toMatchObject({structuredContent:{holder:'other-client'}});}
  expect(heldSettled).toBe(false);expect(callSettled).toBe(false);expect(trace(native,'CreateScene')).toHaveLength(0);
  release();expect((await held).isError).not.toBe(true);
  const actual=await call;const expected=await oracle.client.callTool({name:'obs-create-scene',arguments:{sceneName:'Refused'}});const clockOnly=(value:unknown)=>JSON.stringify(value).replace(/until \d\d:\d\d UTC/g,'until TIME UTC');expect(clockOnly(actual)).toEqual(clockOnly(expected));expect(trace(native,'CreateScene')).toHaveLength(0);expect(receipts(native)[0]).toMatchObject({phase:'not-dispatched',requestDispatched:false,requestAccepted:null});
 }finally{
  if(!released&&native.fake.openConnectionCount>0)release();
  await Promise.allSettled(call?[held,call]:[held]);
 }
});
it('exposes precisely the declared native inventory with approved host adapters and exact Source contracts',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);const actual=(await native.client.listTools()).tools;const expected=(await oracle.client.listTools()).tools;const catalog=JSON.parse(readFileSync(join(root,'native/assets/tools.json'),'utf8')) as {requests:Record<string,unknown>;locals:Record<string,unknown>;service:{handlers:Record<string,unknown>}};const captureExcluded=new Set(['obs-start-stream','obs-stop-stream','obs-toggle-stream']);const declared=[...new Set([...Object.keys(catalog.requests),...Object.keys(catalog.locals),...Object.keys(catalog.service.handlers)])].filter(name=>!captureExcluded.has(name)).sort();expect(actual.map(tool=>tool.name).sort()).toEqual(declared);expectNativeInventory(actual,expected);expect(native.fake.connectionCount).toBe(0);expect(children(native)).toEqual([]);
});

// An owned raw public frontend writes both frames in one stdin operation; two
// SDK transport writes may reach the parser on different legitimate turns.
async function rawNativeCapability(){
 const fake=await FakeOBSServer.start({platform:'macos',autoGetVersion:false});
 fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 const directory=join(root,'native/target',`generic20-raw-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});
 const child=spawn(binary,[],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});
 const messages:Record<string,unknown>[]=[];let buffer='';child.stderr.on('data',()=>undefined);child.stdout.on('data',(chunk:Buffer)=>{buffer+=chunk.toString();let newline:number;while((newline=buffer.indexOf('\n'))>=0){messages.push(JSON.parse(buffer.slice(0,newline)) as Record<string,unknown>);buffer=buffer.slice(newline+1);}});
 const send=(...frames:Parameters<StdioClientTransport['send']>[0][])=>child.stdin.write(frames.map(frame=>JSON.stringify(frame)+'\n').join(''));
 const response=async(id:number)=>{await vi.waitFor(()=>expect(messages.some(message=>message.id===id)).toBe(true));return messages.find(message=>message.id===id)!;};
 return {fake,directory,messages,transport:{pid:child.pid},send,response,async close(){child.stdin.end();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(fake.openConnectionCount).toBe(0));await fake.close();rmSync(directory,{recursive:true,force:true});}};
}
it('checks a newly installed foreign lease before cached capability refusal after frontend admission',async()=>{
 const oracle=await start(false,false,false,false,true);const native=await rawNativeCapability();
 const frame=(id:number,name:string,arguments_:Record<string,unknown>,caller:string)=>({jsonrpc:'2.0' as const,id,method:'tools/call',params:{name,arguments:arguments_,_meta:meta(caller)}});
 try{
  native.send(frame(9900,'obs-get-version',{},'generic20'));await native.response(9900);
  const args={sceneName:'Unsupported queued command'};const claimArgs={minutes:1,reason:'queued policy precedence'};
  const claim=await raw(oracle,9910,'obs-claim-control',claimArgs,'other-client');expect(claim.result).toMatchObject({structuredContent:{holder:'other-client'}});
  const expected=(await raw(oracle,9913,'obs-create-scene',args,'generic20')).result;
  native.send(frame(9911,'obs-create-scene',args,'generic20'),frame(9912,'obs-claim-control',claimArgs,'other-client'));
  const actual=await native.response(9911);expect((await native.response(9912)).result).toMatchObject({structuredContent:{holder:'other-client'}});
  const clockOnly=(value:unknown):unknown=>JSON.parse(JSON.stringify(value).replace(/until \d\d:\d\d UTC/g,'until TIME UTC'));
  expect(clockOnly(actual.result)).toEqual(clockOnly(expected));
  expect(native.messages.filter(message=>message.id===9911||message.id===9912).map(message=>message.id)).toEqual([9912,9911]);expect(trace(native,'CreateScene')).toHaveLength(0);expect(receipts(native)).toEqual([]);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 }finally{await native.close();}
});

// Capture the actual OBS text frame: a Node-parsed trace would hide both
// unsafe-integer rounding and integral floating lexemes before comparison.
async function numericWireSession(native:boolean,legacy:boolean,versionDelayMs=0){
 const socket=new WebSocketServer({host:'127.0.0.1',port:0});await once(socket,'listening');
 const wire:string[]=[];const availableRequests=['GetVersion','SetSourceFilterIndex','SetCurrentSceneTransitionDuration','SetSceneItemIndex','SetInputAudioBalance'];
 socket.on('connection',peer=>{peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));peer.on('message',data=>{const text=data.toString();const frame=JSON.parse(text) as {op:number;d:{requestType?:string;requestId?:string}};if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));if(frame.op===6){wire.push(text);const reply=()=>{if(peer.readyState===1)peer.send(JSON.stringify({op:7,d:{requestType:frame.d.requestType,requestId:frame.d.requestId,requestStatus:{result:true,code:100},responseData:frame.d.requestType==='GetVersion'?{obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests,supportedImageFormats:['png']}:{}}}));};if(frame.d.requestType==='GetVersion'&&versionDelayMs>0)setTimeout(reply,versionDelayMs);else reply();}});});
 const directory=join(root,'native/target',`generic20-numbers-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const address=socket.address();if(!address||typeof address==='string')throw new Error('numeric fixture listener unavailable');
 const child=spawn(native?binary:process.execPath,native?[]:[sdk],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:`ws://127.0.0.1:${address.port}`,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});
 const replies:Record<string,unknown>[]=[];let buffer='';child.stderr.on('data',()=>undefined);child.stdout.on('data',(chunk:Buffer)=>{buffer+=chunk.toString();let newline:number;while((newline=buffer.indexOf('\n'))>=0){replies.push(JSON.parse(buffer.slice(0,newline)) as Record<string,unknown>);buffer=buffer.slice(newline+1);}});
 // Match the surrounding raw-MCP request budget rather than Vitest's implicit
 // one-second poll. Retain side, request identity and child state on failure.
 const response=async(id:number)=>{await vi.waitFor(()=>expect(replies.some(message=>message.id===id),JSON.stringify({surface:'numeric-wire',native,legacy,id,pid:child.pid,exitCode:child.exitCode,signalCode:child.signalCode,replyIds:replies.map(message=>message.id),pendingStdoutBytes:Buffer.byteLength(buffer),obsRequests:wire.map(text=>(JSON.parse(text) as {d:{requestType:string}}).d.requestType)})).toBe(true),{timeout:10_000});return replies.find(message=>message.id===id)!;};
 const close=async()=>{child.stdin.end();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(socket.clients.size).toBe(0));await new Promise<void>((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));rmSync(directory,{recursive:true,force:true});};
 try{
  if(legacy){child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'numeric-wire',version:'1'},capabilities:{}}})+'\n');await response(1);child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');}
  return {wire,directory,transport:{pid:child.pid},close,async call(id:number,name:string,args:string){const metadata=legacy?'':`,"_meta":${JSON.stringify(meta('numeric-wire'))}`;child.stdin.write(`{"jsonrpc":"2.0","id":${id},"method":"tools/call","params":{"name":${JSON.stringify(name)},"arguments":${args}${metadata}}}\n`);return (await response(id)).result;}};
 }catch(error){await close();throw error;}
}
for(const legacy of[false,true])it(`matches Source numeric OBS wire lexemes from literal raw MCP numbers (legacy=${legacy})`,async()=>{
 const oracle=await numericWireSession(false,legacy);const native=await numericWireSession(true,legacy);
 const cases=[
  ['obs-set-source-filter-index','SetSourceFilterIndex','{"sourceName":"App","filterName":"Filter","filterIndex":9007199254740993}',{filterIndex:'9007199254740992'}],
  ['obs-set-source-filter-index','SetSourceFilterIndex','{"sourceName":"App","filterName":"Filter","filterIndex":1000000000000000128}',{filterIndex:'1000000000000000100'}],
  ['obs-set-source-filter-index','SetSourceFilterIndex','{"sourceName":"App","filterName":"Filter","filterIndex":144713588892437.625}',{filterIndex:'144713588892437.62'}],
  ['obs-set-transition-duration','SetCurrentSceneTransitionDuration','{"duration":9007199254740993}',{transitionDuration:'9007199254740992'}],
  ['obs-set-transition-duration','SetCurrentSceneTransitionDuration','{"duration":-0}',{transitionDuration:'0'}],
  ['obs-set-scene-item-index','SetSceneItemIndex','{"sceneItemId":0.0,"sceneItemIndex":1.0}',{sceneItemId:'0',sceneItemIndex:'1'}],
  ['obs-set-input-audio-balance','SetInputAudioBalance','{"inputName":"App","inputAudioBalance":-0}',{inputAudioBalance:'0'}],
 ] as const;
 try{
  let id=10;
  for(const[name,request,args,expectedLexemes]of cases){
   const expected=await oracle.call(id,name,args);const actual=await native.call(id,name,args);expect(actual).toEqual(expected);
   const lastWire=(wire:string[])=>wire.slice().reverse().find(frame=>(JSON.parse(frame) as {d:{requestType:string}}).d.requestType===request)!;
   const sourceWire=lastWire(oracle.wire);const nativeWire=lastWire(native.wire);
   for(const[key,expectedLexeme]of Object.entries(expectedLexemes)){
    const extract=(frame:string)=>frame.match(new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)`))?.[1];
    expect(extract(sourceWire),`Source ${key} actual lexeme`).toBe(expectedLexeme);
    expect.soft(extract(nativeWire),`Native ${key} must use Source numeric value AND integer representation`).toBe(expectedLexeme);
   }
   expect.soft((JSON.parse(nativeWire) as {d:{requestData:unknown}}).d.requestData).toEqual((JSON.parse(sourceWire) as {d:{requestData:unknown}}).d.requestData);id++;
  }
  const beforeSource=oracle.wire.length;const beforeNative=native.wire.length;
  for(const[name,args]of[['obs-set-scene-item-index','{"sceneItemId":9007199254740993,"sceneItemIndex":0}'],['obs-set-tbar-position','{"position":1.0000000000000002}']] as const){
   const expected=await oracle.call(id,name,args);expect(expected).toMatchObject({isError:true});expect(await native.call(id,name,args)).toEqual(expected);id++;
  }
  expect(native.wire).toHaveLength(beforeNative);expect(oracle.wire).toHaveLength(beforeSource);expect(receipts(native)).toHaveLength(cases.length);
  for(const receipt of receipts(native))expect(receipt).toMatchObject({phase:'accepted',postconditionObserved:false});expect(children(native)).toEqual([]);
 }finally{await native.close();await oracle.close();}
});

// The request budget includes OBS bootstrap. A fixture reply wait must not use
// Vitest's shorter implicit one-second polling window.
for(const native of[false,true])it(`waits for bounded OBS bootstrap before numeric wire reply (native=${native})`,async()=>{
 const session=await numericWireSession(native,false,1150);
 try{
  const result=await session.call(80,'obs-set-source-filter-index','{"sourceName":"App","filterName":"Filter","filterIndex":9007199254740993}');
  expect(result).not.toHaveProperty('isError',true);
  const mutation=session.wire.filter(frame=>(JSON.parse(frame) as {d:{requestType:string}}).d.requestType==='SetSourceFilterIndex');
  expect(mutation).toHaveLength(1);
  expect(mutation[0]).toMatch(/"filterIndex":9007199254740992/);
 }finally{await session.close();}
});
