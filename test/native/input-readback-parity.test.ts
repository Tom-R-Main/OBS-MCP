/* SPDX-License-Identifier: GPL-2.0-only */
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
const cases = [
 ['obs-create-input','CreateInput',{sceneName:'Main',inputName:'Fixture',inputKind:'screen_capture'}],
 ['obs-set-input-settings','SetInputSettings',{inputName:'Fixture',inputSettings:{window:42}}],
] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;recordingRoot:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,unadvertised=false,extra:Record<string,string>={},readbackOnly=false):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`input2-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({sceneItemId:42}));fake.respondWith('GetInputSettings',()=>({inputKind:'screen_capture',inputSettings:{type:1,window:42}}));fake.respondWith('GetSceneItemList',()=>({sceneItems:[{sceneItemId:42,sourceName:'Fixture',sceneItemEnabled:true,sceneItemTransform:{sourceWidth:640,sourceHeight:480}}]}));fake.respondWith('GetSourceScreenshot',()=>({imageData:'data:image/jpeg;base64,YWJjZA=='}));
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));let recordDirectory=recording?recordingRoot:undefined;fake.respondWith('SetRecordDirectory',data=>{recordDirectory=String(data.recordDirectory);return {};});fake.respondWith('GetRecordDirectory',()=>{if(recordDirectory)return {recordDirectory};throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:readbackOnly?['GetVersion','CreateInput','SetInputSettings']:['GetVersion'],supportedImageFormats:['png']}));
 const client=new Client({name:'input2',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false',...extra},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recordingRoot,recording:()=>active};sessions.push(session);await client.connect(transport);return session;
}
function normalized(value:unknown):unknown{if(Array.isArray(value))return value.map(normalized);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,value])=>[key,normalized(value)]));if(typeof value==='string'){try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==='object')return {jsonValue:normalized(parsed)};}catch{}}return value;}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(normalized(actual)).toEqual(normalized(expected));return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});

const settingsRecord = () => JSON.parse('{"$serde_json::private::RawValue":"123","__proto__":{"outer":"stripped"},"constructor":"kept","nested":{"__proto__":"kept","$serde_json::private::RawValue":{"value":true},"$serde_json::private::Number":"123"}}') as Record<string,unknown>;
const variants: [string,Record<string,unknown>][] = [
 ['obs-create-input',{sceneName:'',inputName:'Fixture',inputKind:'',includeScreenshot:false}],
 ['obs-create-input',{canvasUuid:'',sceneName:'Main',inputName:'Fixture',inputKind:'screen_capture',inputSettings:settingsRecord(),sceneItemEnabled:false,includeScreenshot:true,discarded:'stripped'}],
 ['obs-set-input-settings',{inputName:'Fixture',inputSettings:settingsRecord(),overlay:false,includeScreenshot:true}],
 ['obs-set-input-settings',{inputUuid:'opaque',inputName:'Fixture',inputSettings:{'$serde_json::private::RawValue':{value:true},nested:settingsRecord()},overlay:true,includeScreenshot:false}],
];
const invalid: [string,Record<string,unknown>][] = [
 ['obs-create-input',{}],['obs-create-input',{sceneName:42,inputName:null,inputKind:false}],
 ['obs-create-input',{sceneName:'S',inputName:'I',inputKind:'K',includeScreenshot:null}],
 ['obs-create-input',{sceneName:'S',inputName:'I',inputKind:'K',inputSettings:[]}],
 ['obs-set-input-settings',{}],['obs-set-input-settings',{inputName:'I',inputSettings:null}],
 ['obs-set-input-settings',{inputName:'I',inputSettings:{},overlay:0,includeScreenshot:'yes'}],
];
for(const legacy of [false,true]) {
 describe(`input readback complete public Source contracts (legacy=${legacy})`,()=>{
  it('matches metadata, exact mutation/readback order, opaque settings and screenshot sidecar omission',async()=>{
   const oracle=await start(false,legacy),native=await start(true,legacy);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;
   for(const[name,,args]of cases){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));await same(oracle,native,name,args);}
   for(const[name,args]of variants)await same(oracle,native,name,args);
   for(const request of['CreateInput','SetInputSettings','GetInputSettings','GetCurrentProgramScene','GetSceneItemList','GetSourceScreenshot'])expect(trace(native,request)).toEqual(trace(oracle,request));
   for(const[,request]of cases)for(const args of trace(native,request))expect(args).not.toHaveProperty('includeScreenshot');
   expect(receipts(native)).toHaveLength(cases.length+variants.length);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.requestAccepted===true&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
  });
  it('matches invalid arguments before any socket, receipt or opaque effect',async()=>{
   const oracle=await start(false,legacy),native=await start(true,legacy);for(const[name,args]of invalid)expect((await same(oracle,native,name,args)).isError).toBe(true);expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
  });
  it('preserves normal readback warnings, screenshot failure notes and durable acceptance after successful mutation',async()=>{
   const oracle=await start(false,legacy),native=await start(true,legacy);for(const session of[oracle,native])for(const request of['GetInputSettings','GetCurrentProgramScene','GetSourceScreenshot'])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,`fixture ${request} failure`);});
   for(const[name,,args]of cases){const result=await same(oracle,native,name,{...args,includeScreenshot:true});expect(result.isError).not.toBe(true);expect(JSON.stringify(result)).toContain('Could not read the input back');expect(JSON.stringify(result)).toContain('No screenshot:');}
   expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);
  });
  it('preserves invalid screenshot configuration after mutation/readback without falsely rejecting accepted work',async()=>{
   const oracle=await start(false,legacy,false,false,{OBS_MCP_MAX_SCREENSHOT_BYTES:'not-a-number'}),native=await start(true,legacy,false,false,{OBS_MCP_MAX_SCREENSHOT_BYTES:'not-a-number'});
   for(const[name,,args]of cases){expect((await same(oracle,native,name,{...args,includeScreenshot:true})).isError).not.toBe(true);}
   expect(trace(native,'GetSourceScreenshot')).toHaveLength(2);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);
  });
  it('preserves cached unsupported readback capability warnings without extra RPCs',async()=>{
   const oracle=await start(false,legacy,false,true,{},true),native=await start(true,legacy,false,true,{},true);
   for(const[name,,args]of cases)expect((await same(oracle,native,name,{...args,includeScreenshot:true})).isError).not.toBe(true);
   for(const request of['GetInputSettings','GetCurrentProgramScene','GetSceneItemList','GetSourceScreenshot'])expect(trace(native,request)).toHaveLength(0);expect(trace(native,'GetVersion')).toHaveLength(1);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);
  });
  it('preserves full mutation rejection and sends no follow-up',async()=>{
   const oracle=await start(false,legacy),native=await start(true,legacy);for(const session of[oracle,native])for(const[,request]of cases)session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'mutation rejected');});for(const[name,,args]of cases)expect((await same(oracle,native,name,{...args,includeScreenshot:true})).isError).toBe(true);for(const request of['GetInputSettings','GetCurrentProgramScene','GetSceneItemList','GetSourceScreenshot'])expect(trace(native,request)).toHaveLength(0);expect(receipts(native).every(receipt=>receipt.phase==='rejected'&&receipt.postconditionObserved===false)).toBe(true);
  });
 });
}
it('keeps Accepted when Source result construction fails after an ACK and never starts readback',async()=>{
 const oracle=await start(false,false),native=await start(true,false);for(const session of[oracle,native])session.fake.respondWith('CreateInput',()=>({sceneItemId:{toString:null}}));expect((await same(oracle,native,'obs-create-input',cases[0][2])).isError).toBe(true);expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(trace(native,'GetInputSettings')).toHaveLength(0);
});
it('retains Accepted input readback while physical owned Stop dispatches before the readback reply',async()=>{
 const oracle=await start(false,false,true),native=await start(true,false,true);for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);
 const held: {session:Session;request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;after:number;pending:Promise<unknown>}[]=[];const stops:Promise<unknown>[]=[];
 try {
  for(const session of[oracle,native]){session.fake.queueSuccess('GetInputSettings',{inputKind:'screen_capture',inputSettings:{window:42}},60_000);const after=session.fake.cursor();const pending=session.client.callTool({name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{once:true}}}).then(value=>({value}),error=>({error:String(error)}));const request=await session.fake.waitForRequest('GetInputSettings',{after,timeoutMs:10_000});held.push({session,request,after,pending});}
  expect(receipts(native).find(receipt=>receipt.requestType==='SetInputSettings')).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
  for(const session of[oracle,native]){expect(trace(session,'StopRecord')).toHaveLength(0);stops.push(session.client.callTool({name:'obs-take-stop',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));}
  await oracle.fake.waitForRequest('StopRecord',{after:held[0]!.after,timeoutMs:4000});await expect(native.fake.waitForRequest('StopRecord',{after:held[1]!.after,timeoutMs:1000}),'physical Stop must reach OBS before GetInputSettings is released').resolves.toBeDefined();for(const session of[oracle,native])expect(trace(session,'StopRecord')).toHaveLength(1);
 } finally {
  for(const{session,request}of held)session.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetInputSettings',requestStatus:{result:true,code:100},responseData:{inputKind:'screen_capture',inputSettings:{window:42}}}},request.connectionId);
  await Promise.all([...held.map(item=>item.pending),...stops]);for(const session of[oracle,native]){await session.client.callTool({name:'obs-take-stop',arguments:{}});expect(session.recording()).toBe(false);}
 }
 expect(receipts(native).find(receipt=>receipt.requestType==='SetInputSettings')).toMatchObject({phase:'accepted',postconditionObserved:false});expect(trace(native,'SetInputSettings')).toHaveLength(1);
});
it('retains a cancelled frontend input job without replay and permits an independent explicit command after its read drains',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('GetInputSettings',{inputKind:'screen_capture',inputSettings:{window:42}},60_000);const after=native.fake.cursor();const abort=new AbortController();
 const pending=native.client.callTool({name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{once:true}}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));const request=await native.fake.waitForRequest('GetInputSettings',{after,timeoutMs:10_000});
 expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});abort.abort();expect(await pending).toHaveProperty('error');await native.client.listTools();
 expect(trace(native,'GetCurrentProgramScene')).toHaveLength(0);native.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetInputSettings',requestStatus:{result:true,code:100},responseData:{inputKind:'screen_capture',inputSettings:{window:42}}}},request.connectionId);
 expect((await native.client.callTool({name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{explicitLater:true}}})).isError).not.toBe(true);expect(trace(native,'SetInputSettings')).toHaveLength(2);expect(trace(native,'GetCurrentProgramScene')).toHaveLength(1);expect(trace(native,'GetSceneItemList')).toHaveLength(1);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
});
it('joins EOF during accepted readback without replay, postcondition promotion, child process or socket leak',async()=>{
 const fake=await FakeOBSServer.start({platform:'macos'});serveProduction(fake);fake.respondWith('SetInputSettings',()=>({}));fake.queueSuccess('GetInputSettings',{inputKind:'screen_capture',inputSettings:{window:42}},60_000);
 const directory=join(root,'native/target',`input2-eof-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});
 const child=spawn(binary,[],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});child.stdout.on('data',()=>undefined);child.stderr.on('data',()=>undefined);
 let released=false;let request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>|undefined;
 try {
  child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{once:true}},_meta:{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name:'input2-eof',version:'1'},'io.modelcontextprotocol/clientCapabilities':{}}}})+'\n');
  request=await fake.waitForRequest('GetInputSettings',{timeoutMs:10_000});expect(receipts({directory})[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(children({transport:{pid:child.pid}})).toEqual([]);child.stdin.end();await new Promise(resolve=>setTimeout(resolve,40));expect(child.exitCode).toBeNull();expect(fake.openConnectionCount).toBe(1);
  fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetInputSettings',requestStatus:{result:true,code:100},responseData:{inputKind:'screen_capture',inputSettings:{window:42}}}},request.connectionId);released=true;
  await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(fake.openConnectionCount).toBe(0));expect(trace({fake},'SetInputSettings')).toHaveLength(1);expect(receipts({directory})[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});released=true;
 } finally {
  if(!released&&request&&fake.openConnectionCount)fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetInputSettings',requestStatus:{result:true,code:100},responseData:{inputKind:'screen_capture',inputSettings:{window:42}}}},request.connectionId);
  child.stdin.end();if(child.exitCode===null&&child.signalCode===null){await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:5000});}await fake.close();rmSync(directory,{recursive:true,force:true});
 }
});

async function numericWireSession(native:boolean,legacy:boolean){
 const socket=new WebSocketServer({host:'127.0.0.1',port:0});await once(socket,'listening');
 const wire:string[]=[];const availableRequests=['GetVersion',...cases.map(([,request])=>request)];
 socket.on('connection',peer=>{peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));peer.on('message',data=>{const text=data.toString();const frame=JSON.parse(text) as {op:number;d:{requestType?:string;requestId?:string}};if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));if(frame.op===6){wire.push(text);peer.send(JSON.stringify({op:7,d:{requestType:frame.d.requestType,requestId:frame.d.requestId,requestStatus:{result:true,code:100},responseData:frame.d.requestType==='GetVersion'?{obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests,supportedImageFormats:['png']}:{}}}));}});});
 const directory=join(root,'native/target',`input2-numbers-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const address=socket.address();if(!address||typeof address==='string')throw new Error('numeric fixture listener unavailable');
 const child=spawn(native?binary:process.execPath,native?[]:[sdk],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:`ws://127.0.0.1:${address.port}`,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});
 const replies:Record<string,unknown>[]=[];let buffer='';child.stderr.on('data',()=>undefined);child.stdout.on('data',(chunk:Buffer)=>{buffer+=chunk.toString();let newline:number;while((newline=buffer.indexOf('\n'))>=0){replies.push(JSON.parse(buffer.slice(0,newline)) as Record<string,unknown>);buffer=buffer.slice(newline+1);}});
 const response=async(id:number)=>{await vi.waitFor(()=>expect(replies.some(message=>message.id===id)).toBe(true));return replies.find(message=>message.id===id)!;};
 const close=async()=>{child.stdin.end();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(socket.clients.size).toBe(0));await new Promise<void>((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));rmSync(directory,{recursive:true,force:true});};
 try{
  if(legacy){child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'numeric-wire',version:'1'},capabilities:{}}})+'\n');await response(1);child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');}
  return {wire,directory,transport:{pid:child.pid},close,async call(id:number,name:string,args:string){const metadata=legacy?'':`,"_meta":${JSON.stringify(meta('numeric-wire'))}`;child.stdin.write(`{"jsonrpc":"2.0","id":${id},"method":"tools/call","params":{"name":${JSON.stringify(name)},"arguments":${args}${metadata}}}\n`);return (await response(id)).result;}};
 }catch(error){await close();throw error;}
}

const numericArgs = '{"inputName":"Fixture","inputSettings":{"negativeZero":-0.0,"integer":1.0,"unsafeInteger":9007199254740993,"fullU64":18446744073709551615,"tie":144713588892437.625,"nested":{"$serde_json::private::RawValue":"123","$serde_json::private::Number":"123"}}}';
const lexemes = {negativeZero:'0',integer:'1',unsafeInteger:'9007199254740992',fullU64:'18446744073709552000',tie:'144713588892437.62'};
function checkLexemes(frame:string){for(const[key,value]of Object.entries(lexemes)){const extracted=frame.match(new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)`))?.[1];expect(extracted,key).toBe(value);}expect((JSON.parse(frame) as {d:{requestData:{inputSettings:{nested:unknown}}}}).d.requestData.inputSettings.nested).toEqual({'$serde_json::private::RawValue':'123','$serde_json::private::Number':'123'});}
for(const legacy of[false,true]) {
 it(`matches Source input2 raw MCP numeric tokens and private sentinel fields (legacy=${legacy})`,async()=>{
  const oracle=await numericWireSession(false,legacy),native=await numericWireSession(true,legacy);
  try {
   expect(normalized(await native.call(10,'obs-set-input-settings',numericArgs))).toEqual(normalized(await oracle.call(10,'obs-set-input-settings',numericArgs)));
   for(const session of[oracle,native])checkLexemes(session.wire.find(frame=>(JSON.parse(frame) as {d:{requestType:string}}).d.requestType==='SetInputSettings')!);
   expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(children(native)).toEqual([]);
  } finally {await native.close();await oracle.close();}
 });
}

function meta(name:string){return {'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{}};}

it('reopens a lost input acknowledgement as Unknown and grants no automatic or explicit replay permission',async()=>{
 const original=await start(true,false);original.fake.queueDisconnect('SetInputSettings','lost input ACK');
 expect((await original.client.callTool({name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{once:true},includeScreenshot:true}})).isError).toBe(true);
 const receipt=receipts(original)[0];expect(receipt).toMatchObject({requestType:'SetInputSettings',phase:'unknown',requestAccepted:null,postconditionObserved:false});expect(trace(original,'SetInputSettings')).toHaveLength(1);expect(trace(original,'GetInputSettings')).toHaveLength(0);
 await original.client.close();await original.transport.close();await vi.waitFor(()=>expect(original.fake.openConnectionCount).toBe(0));
 const client=new Client({name:'input2',version:'1'},{versionNegotiation:{mode:{pin:'2026-07-28'}}});
 const transport=new StdioClientTransport({command:binary,args:[],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:original.fake.url,OBS_MCP_STATE_DIR:original.directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(original.directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(original.directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
 const restarted={...original,client,transport};sessions[sessions.indexOf(original)]=restarted;await client.connect(transport);
 expect((await client.callTool({name:'obs-set-input-settings',arguments:{inputName:'Fixture',inputSettings:{explicitLater:true},includeScreenshot:true}})).isError).toBe(true);
 expect(receipts(restarted)).toEqual([receipt]);expect(trace(restarted,'SetInputSettings')).toHaveLength(1);for(const request of['GetInputSettings','GetCurrentProgramScene','GetSceneItemList','GetSourceScreenshot'])expect(trace(restarted,request)).toHaveLength(0);expect(children(restarted)).toEqual([]);
});
