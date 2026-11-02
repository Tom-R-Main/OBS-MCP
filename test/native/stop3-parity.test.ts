/* SPDX-License-Identifier: GPL-2.0-only */
import { expectNativeInventory } from '../support/native-inventory.js';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
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
 ['obs-stop-stream','StopStream',{},'Successfully stopped streaming'],
 ['obs-stop-virtual-cam','StopVirtualCam',{},'Virtual camera stopped'],
 ['obs-stop-replay-buffer','StopReplayBuffer',{},'Replay buffer stopped'],
] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;recordingRoot:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean;externalRecording:(active:boolean,path:string)=>void};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,unadvertised=false,extra:Record<string,string>={},configure?:(fake:FakeOBSServer)=>void):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`stop3-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({ignored:'PRIVATE ACK BODY',outputActive:true}));fake.respondWith('SetInputMute',()=>({}));
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));let recordDirectory=recording?recordingRoot:undefined;fake.respondWith('SetRecordDirectory',data=>{recordDirectory=String(data.recordDirectory);return {};});fake.respondWith('GetRecordDirectory',()=>{if(recordDirectory)return {recordDirectory};throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 configure?.(fake);
 const client=new Client({name:'stop3',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false',...extra},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recordingRoot,recording:()=>active,externalRecording:(next:boolean,nextPath:string)=>{active=next;fake.sendEvent('RecordStateChanged',{outputActive:next,outputState:next?'OBS_WEBSOCKET_OUTPUT_STARTED':'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:nextPath});}};sessions.push(session);await client.connect(transport);if(unadvertised&&!native){const version=await client.callTool({name:'obs-get-version',arguments:{}});expect(version.structuredContent).toMatchObject({availableRequests:['GetVersion']});}return session;
}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(actual).toEqual(expected);return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
async function closeSession(session:Session){
 const index=sessions.indexOf(session);if(index>=0)sessions.splice(index,1);
 try{try{await session.client.close();}finally{await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0),{timeout:10_000});}}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}
}
afterEach(async()=>{for(const session of [...sessions])await closeSession(session);});
function saveProgress(name:string,value:unknown){const directory=process.env.OBS_PUBLIC_STOP_PROGRESS_EVIDENCE_DIR;if(!directory)return;mkdirSync(directory,{recursive:true});writeFileSync(join(directory,name+'.json'),JSON.stringify(value,null,2)+'\n');}




const names=cases.map(([name])=>name);
function meta(name:string){return {'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{}};}
async function raw(session:Session,id:number,name:string,args:Record<string,unknown>,caller:string){const prior=session.transport.onmessage;let answer:Record<string,unknown>|undefined;session.transport.onmessage=message=>{const value=message as unknown as Record<string,unknown>;if(value.id===id)answer=value;prior?.(message);};try{await session.transport.send({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,_meta:meta(caller)}} as Parameters<StdioClientTransport['send']>[0]);await vi.waitFor(()=>expect(answer).toBeDefined(),{timeout:10_000});return answer!;}finally{session.transport.onmessage=prior;}}
function requests(session:Session){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType!=='GetVersion');}
async function acknowledgedBody(session:Session,name:string,request:string,responseData:unknown){
 session.fake.queueSuccess(request,{},60_000);const after=session.fake.cursor();const result=session.client.callTool({name,arguments:{}}).then(value=>({value}),error=>({error:String(error)}));const held=await session.fake.waitForRequest(request,{after,timeoutMs:10_000});const d:Record<string,unknown>={requestType:request,requestId:held.frame.d.requestId,requestStatus:{result:true,code:100}};if(responseData!==undefined)d.responseData=responseData;session.fake.sendRaw({op:7,d},held.connectionId);return result;
}
for(const legacy of[false,true])describe(`three ACK-only Stops Source/native legacy=${legacy}`,()=>{
 it('retains complete Source contracts, fixed messages and omitted requestData without status or event confirmation',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);const expected=(await source.client.listTools()).tools,actual=(await native.client.listTools()).tools;
  for(const[name,request,,message]of cases){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));const result=await same(source,native,name,{discarded:{requestType:'StopRecord'},outputName:'arbitrary'});expect(result).toMatchObject({content:[{type:'text',text:message}],structuredContent:{message}});expect(trace(native,request)).toEqual([undefined]);}
  for(const session of[source,native])expect(requests(session).map(item=>item.frame.d.requestType)).toEqual(cases.map(([,request])=>request));for(const item of requests(native))expect(Object.hasOwn(item.frame.d,'requestData')).toBe(false);
  expect(receipts(native)).toHaveLength(3);for(const receipt of receipts(native))expect(receipt).toMatchObject({version:2,kind:'request',phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
 it('ignores missing/null/scalar/array/contradictory ACK bodies without inventing confirmed inactive state',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);for(const[name,request,,message]of cases)for(const body of[undefined,null,false,0,'ignored',[],{outputActive:true},{private:'ignored'}]){const expected=await acknowledgedBody(source,name,request,body),actual=await acknowledgedBody(native,name,request,body);expect(actual).toEqual(expected);expect(actual).toMatchObject({value:{content:[{type:'text',text:message}],structuredContent:{message}}});}
  expect(receipts(native)).toHaveLength(24);expect(receipts(native).every(r=>r.phase==='accepted'&&r.postconditionObserved===false)).toBe(true);expect(requests(native)).toHaveLength(24);
 });
 it('preserves exact Source errors and rejection comments without a success or observed receipt',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);for(const[name,request]of cases){for(const session of[source,native])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'PRIVATE Stop detail');});expect((await same(source,native,name)).isError).toBe(true);}
  expect(receipts(native)).toHaveLength(3);for(const receipt of receipts(native)){expect(receipt).toMatchObject({phase:'rejected',requestAccepted:false,postconditionObserved:false});expect(JSON.stringify(receipt)).not.toContain('PRIVATE Stop detail');}expect(requests(native)).toHaveLength(3);
 });
 it('uses established capability knowledge to refuse all three Stops before any effect or receipt',async()=>{
  const source=await start(false,legacy,false,true),native=await start(true,legacy,false,true);for(const[name]of cases)expect((await same(source,native,name)).isError).toBe(true);expect(requests(native)).toEqual([]);expect(receipts(native)).toEqual([]);expect(native.fake.connectionCount).toBe(1);
 });
 it('exempts Stops from lease/active-take policy without stopping the owned recording (foreign modern caller; initialized legacy holder)',async()=>{
  const source=await start(false,legacy,true),native=await start(true,legacy,true);for(const session of[source,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);
  try{for(const session of[source,native])expect((await session.client.callTool({name:'obs-control-status',arguments:{}})).structuredContent).toMatchObject({lease:{holder:'stop3'}});
   let requestId=9600;for(const[name]of cases.filter(([,request])=>request!=='StopStream')){if(legacy){expect((await same(source,native,name)).isError).not.toBe(true);}else{const expected=(await raw(source,requestId,name,{},'other-client')).result,actual=(await raw(native,requestId,name,{},'other-client')).result;expect(actual).toEqual(expected);expect(actual).not.toMatchObject({isError:true});expect(actual).toHaveProperty('structuredContent.message');requestId++;}}for(const session of[source,native]){expect(session.recording()).toBe(true);expect(trace(session,'StopRecord')).toHaveLength(0);expect((await session.client.callTool({name:'obs-take-status',arguments:{}})).structuredContent).toMatchObject({ownership:'continuous',record:{outputActive:true}});}
  }finally{for(const session of[source,native])await session.client.callTool({name:'obs-take-stop',arguments:{}});}for(const session of[source,native])expect(trace(session,'StopRecord')).toHaveLength(1);
 });
});
it('keeps foreign-lease exemption before known unsupported Stop errors with zero effects',async()=>{
 const source=await start(false,false,false,true),native=await start(true,false,false,true);for(const session of[source,native])expect((await raw(session,9600,'obs-claim-control',{minutes:1,reason:'foreign'},'other-client')).result).toMatchObject({structuredContent:{holder:'other-client'}});for(const[name]of cases){const result=await same(source,native,name);expect(JSON.stringify(result)).toContain('does not advertise support');expect(JSON.stringify(result)).not.toContain('other-client has control');}expect(requests(native)).toEqual([]);expect(receipts(native)).toEqual([]);
});
it('retains a canceled admitted Stop until its same ACK and accepts only a later explicit Stop as another command',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('StopStream',{},60_000);const after=native.fake.cursor(),abort=new AbortController();const job=native.client.callTool({name:'obs-stop-stream',arguments:{}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));const held=await native.fake.waitForRequest('StopStream',{after,timeoutMs:10_000});abort.abort();expect(await job).toHaveProperty('error');native.fake.sendRaw({op:7,d:{requestType:'StopStream',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{outputActive:true}}},held.connectionId);await vi.waitFor(()=>expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false}));expect((await native.client.callTool({name:'obs-stop-stream',arguments:{}})).isError).not.toBe(true);expect(trace(native,'StopStream')).toHaveLength(2);expect(receipts(native)).toHaveLength(2);
});
it('never replays any lost Stop ACK and keeps the uncertainty barrier for other explicit Generic Stops',async()=>{
 for(const[name,request]of cases){const native=await start(true,false);native.fake.queueDisconnect(request,'PRIVATE Stop disconnect');expect((await native.client.callTool({name,arguments:{}})).isError).toBe(true);expect(receipts(native)[0]).toMatchObject({phase:'unknown',requestAccepted:null,postconditionObserved:false});for(const[other]of cases)expect((await native.client.callTool({name:other,arguments:{}})).isError).toBe(true);expect(requests(native)).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);}
});
for(const legacy of[false,true])for(const[name,request,,message]of cases)it(`public ${request} reaches OBS before an unrelated held mutation ACK (legacy=${legacy})`,async()=>{
 const configure=(fake:FakeOBSServer)=>fake.queueSuccess('SetInputMute',{},60_000);
 const source=await start(false,legacy,false,false,{},configure),native=await start(true,legacy,false,false,{},configure);
 const held:{session:Session;request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;job:Promise<unknown>}[]=[];
 const stops:Promise<unknown>[]=[];
 let observed:({wire:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>}|{error:string})[]=[];
 let results:unknown[]=[];let durable:Record<string,unknown>[]=[];let wire:unknown[]=[];
 try{
  for(const session of[source,native]){
   const after=session.fake.cursor();
   const job=session.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Fixture',inputMuted:true}}).then(value=>({value}),error=>({error:String(error)}));
   held.push({session,request:await session.fake.waitForRequest('SetInputMute',{after,timeoutMs:10_000}),job});
  }
  for(const session of[source,native])stops.push(session.client.callTool({name,arguments:{}}).then(value=>({value}),error=>({error:String(error)})));
  observed=await Promise.all(held.map(({session,request:mutation})=>session.fake.waitForRequest(request,{after:mutation.cursor,timeoutMs:3000}).then(wire=>({wire}),error=>({error:String(error)}))));
 }finally{
  // Release only after both bounded physical-arrival observations settle. The
  // SAME original Stop promises must then return their exact callback results.
  for(const{session,request:mutation}of held)session.fake.sendRaw({op:7,d:{requestType:'SetInputMute',requestId:mutation.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{}}},mutation.connectionId);
  await Promise.all(held.map(h=>h.job));results=await Promise.all(stops);
  durable=receipts(native);wire=trace(native,request);
  await closeSession(source);await closeSession(native);
 }
 saveProgress(`${request}-${legacy?'legacy':'modern'}-held-mutation`,{name,request,legacy,sourcePhysicalBeforeRelease:observed[0],nativePhysicalBeforeRelease:observed[1],originalStopReplies:results,durable,wire,helpersJoined:true});
 expect(observed[0],`Source ${request} physical frame must precede mutation ACK release`).toHaveProperty('wire');
 expect(results).toHaveLength(2);expect(results[1]).toEqual(results[0]);
 for(const result of results){expect(result).toHaveProperty('value');expect(result).not.toHaveProperty('error');expect(result).toMatchObject({value:{content:[{type:'text',text:message}],structuredContent:{message}}});expect((result as {value:{isError?:boolean}}).value.isError).not.toBe(true);}
 expect(wire).toEqual([undefined]);expect(durable).toHaveLength(2);
 expect(durable.find(row=>row.requestType===request)).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
 expect(observed[1],`native ${request} physical frame must precede mutation ACK release`).toHaveProperty('wire');
});
it('cancels an undispatched public Stop while public lane occupied',async()=>{
 const native=await start(true,false,false,false,{},fake=>fake.queueSuccess('StopStream',{},60_000));
 const after=native.fake.cursor();const first=native.client.callTool({name:'obs-stop-stream',arguments:{}}).then(value=>({value}),error=>({error:String(error)}));
 const held=await native.fake.waitForRequest('StopStream',{after,timeoutMs:10_000});
 const abort=new AbortController();const second=native.client.callTool({name:'obs-stop-replay-buffer',arguments:{}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));
 try{
  // This sentinel only orders frontend calls; the direct Rust contract proves
  // the internal queued state. Holding the first public lane bounds dispatch.
  await native.client.callTool({name:'obs-control-status',arguments:{}});abort.abort();
  expect(await second).toHaveProperty('error');expect(trace(native,'StopReplayBuffer')).toEqual([]);
 }finally{
  native.fake.sendRaw({op:7,d:{requestType:'StopStream',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{}}},held.connectionId);
  const accepted=await first;expect(accepted).toMatchObject({value:{structuredContent:{message:'Successfully stopped streaming'}}});await second;
 }
 await vi.waitFor(()=>expect(receipts(native)[0]).toMatchObject({phase:'accepted'}));
 expect(trace(native,'StopReplayBuffer')).toEqual([]);expect(receipts(native)).toHaveLength(1);
});
it('preserves the owned recording Stop priority and reservation while a public Stop awaits its ACK',async()=>{
 const source=await start(false,false,true),native=await start(true,false,true);for(const session of[source,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);const held:{session:Session;request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;job:Promise<unknown>}[]=[];const stops:Promise<unknown>[]=[];
 try{for(const session of[source,native]){session.fake.queueSuccess('StopVirtualCam',{},60_000);const after=session.fake.cursor();const job=session.client.callTool({name:'obs-stop-virtual-cam',arguments:{}}).then(value=>({value}),error=>({error:String(error)}));held.push({session,request:await session.fake.waitForRequest('StopVirtualCam',{after,timeoutMs:10_000}),job});}for(const session of[source,native]){expect(trace(session,'StopRecord')).toHaveLength(0);stops.push(session.client.callTool({name:'obs-take-stop',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));}for(const{session,request}of held)await session.fake.waitForRequest('StopRecord',{after:request.cursor,timeoutMs:4000});
 }finally{for(const{session,request}of held)session.fake.sendRaw({op:7,d:{requestType:'StopVirtualCam',requestId:request.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{outputActive:true}}},request.connectionId);const ordinary=await Promise.all(held.map(h=>h.job));for(const result of ordinary){expect(result).toHaveProperty('value');expect(result).not.toHaveProperty('error');expect(result).toMatchObject({value:{structuredContent:{message:'Virtual camera stopped'}}});}const firstStops=await Promise.all(stops);expect(firstStops).toHaveLength(2);for(const result of firstStops){expect(result).toHaveProperty('value');expect(result).not.toHaveProperty('error');expect(result).toMatchObject({value:{structuredContent:{stopConfirmed:true}}});}for(const session of[source,native]){const joined=await session.client.callTool({name:'obs-take-stop',arguments:{}});expect(joined.structuredContent).toMatchObject({stopConfirmed:true});expect(trace(session,'StopRecord')).toHaveLength(1);expect(session.recording()).toBe(false);}}
 expect(receipts(native).find(r=>r.requestType==='StopVirtualCam')).toMatchObject({phase:'accepted',postconditionObserved:false});
});
it('uses the one explicit Source-complement inventory plan without admitting critical or arbitrary Stops',async()=>{
 const source=await start(false,false,true),native=await start(true,false,true);const expected=(await source.client.listTools()).tools,actual=(await native.client.listTools()).tools;expectNativeInventory(actual,expected);expect(native.fake.connectionCount).toBe(0);expect(children(native)).toEqual([]);
});
it('keeps all three destructive Stop tools hidden under actual Source read-only filtering',async()=>{
 const source=await start(false,false,false,false,{OBS_MCP_READ_ONLY:'true'}),native=await start(true,false,false,false,{OBS_MCP_READ_ONLY:'true'});for(const session of[source,native])for(const name of names)expect((await session.client.listTools()).tools.map(t=>t.name)).not.toContain(name);expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
});
it('retains an ACK-only Stop through frontend EOF until its same ACK and joins without opening compatibility',async()=>{
 const fake=await FakeOBSServer.start({platform:'macos',availableRequests:['StopReplayBuffer']});fake.respondWith('StopReplayBuffer',()=>({}));fake.queueSuccess('StopReplayBuffer',{},60_000);const directory=join(root,'native/target',`stop3-eof-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const child=spawn(binary,[],{cwd:root,stdio:['pipe','pipe','pipe'],env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_NATIVE_RECORDING_ROOT:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'}});child.stdout.on('data',()=>undefined);child.stderr.on('data',()=>undefined);child.stdin.on('error',()=>undefined);const closed=once(child,'close');void closed.catch(()=>undefined);let request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>|undefined;let released=false;const release=()=>{if(released||!request||!fake.openConnectionCount)return;fake.sendRaw({op:7,d:{requestType:'StopReplayBuffer',requestId:request.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);released=true;};
 try{child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:8100,method:'tools/call',params:{name:'obs-stop-replay-buffer',arguments:{},_meta:meta('stop3-eof')}})+'\n');request=await fake.waitForRequest('StopReplayBuffer',{timeoutMs:10_000});expect(receipts({directory})[0]).toMatchObject({phase:'dispatching',requestAccepted:null});expect(children({transport:{pid:child.pid}})).toEqual([]);child.stdin.end();await new Promise(resolve=>setTimeout(resolve,50));expect(child.exitCode).toBeNull();expect(fake.openConnectionCount).toBe(1);release();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:5000});expect(await closed).toEqual([0,null]);expect(fake.openConnectionCount).toBe(0);expect(trace({fake},'StopReplayBuffer')).toHaveLength(1);expect(receipts({directory})[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
 }finally{release();child.stdin.end();if(child.exitCode===null&&child.signalCode===null)await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:5000});await closed;await fake.close();rmSync(directory,{recursive:true,force:true});}
});
