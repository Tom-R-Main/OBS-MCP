/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { WebSocketServer } from 'ws';
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');const sdk=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,'build/index.js');let seq=0;
const cases = [['obs-set-scene-item-transform','SetSceneItemTransform',{sceneName:'Main',sceneItemId:42,positionX:1}]] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;recordingRoot:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,unadvertised=false,extra:Record<string,string>={},readbackOnly=false):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`transform1-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({sceneItemId:42}));fake.respondWith('GetInputSettings',()=>({inputKind:'screen_capture',inputSettings:{type:1,window:42}}));fake.respondWith('GetSceneItemList',()=>({sceneItems:[{sceneItemId:42,sourceName:'Fixture',sceneItemEnabled:true,sceneItemTransform:{sourceWidth:640,sourceHeight:480}}]}));fake.respondWith('GetSourceScreenshot',()=>({imageData:'data:image/jpeg;base64,YWJjZA=='}));fake.respondWith('GetSceneItemTransform',()=>({sceneItemTransform:{z:3,a:1}}));
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));let recordDirectory=recording?recordingRoot:undefined;fake.respondWith('SetRecordDirectory',data=>{recordDirectory=String(data.recordDirectory);return {};});fake.respondWith('GetRecordDirectory',()=>{if(recordDirectory)return {recordDirectory};throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:readbackOnly?['GetVersion','SetSceneItemTransform']:['GetVersion'],supportedImageFormats:['png']}));
 const client=new Client({name:'transform1',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false',...extra},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recordingRoot,recording:()=>active};sessions.push(session);await client.connect(transport);if(!native&&unadvertised){const version=await client.callTool({name:'obs-get-version',arguments:{}});const block=version.content.find(block=>block.type==='text');expect(block?.type).toBe('text');expect(JSON.parse((block as {text:string}).text).availableRequests).toEqual(readbackOnly?['GetVersion','SetSceneItemTransform']:['GetVersion']);}return session;
}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(actual).toEqual(expected);return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});

const variants:Record<string,unknown>[]=[
 {sceneItemId:0},
 {canvasUuid:'',sceneName:'',sceneUuid:'uuid',sceneItemId:42,sceneItemTransform:{positionY:20,keep:true},positionX:1},
 {sceneUuid:'uuid',sceneItemId:42,sceneItemTransform:{positionX:20,keep:true},positionY:1},
 {sceneName:'Main',sceneItemId:42,sceneItemTransform:{scaleY:20,keep:true},scaleX:1},
 {sceneName:'Main',sceneItemId:42,sceneItemTransform:{scaleX:20,keep:true},scaleY:1},
 {sceneName:'Main',sceneItemId:42,sceneItemTransform:{cropTop:1,cropBottom:2,cropLeft:3,cropRight:4},cropTop:0},
 {sceneName:'Main',sceneItemId:42,sceneItemTransform:JSON.parse('{"__proto__":"stripped","$serde_json::private::RawValue":"123","nested":{"__proto__":"kept","$serde_json::private::RawValue":{"value":true}}}'),rotation:144713588892437.625},
 {sceneName:'Main',sceneItemId:9007199254740991,positionX:-0,positionY:-2,rotation:0,scaleX:0,scaleY:-1,cropTop:-1,cropBottom:0,cropLeft:1,cropRight:2,discarded:'stripped'},
];
const invalid:Record<string,unknown>[]=[{}, {sceneItemId:-1},{sceneItemId:1.5},{sceneItemId:9007199254740992},{sceneItemId:1,sceneItemTransform:[]},{sceneItemId:1,sceneName:null},{sceneItemId:1,positionX:null},{sceneItemId:1,scaleY:'2'}];
for(const legacy of[false,true])describe(`transform public Source contract (legacy=${legacy})`,()=>{
 it('matches metadata, paired deletion, independent crops, numeric projection and opaque wire effects',async()=>{
  const oracle=await start(false,legacy),native=await start(true,legacy);const expected=(await oracle.client.listTools()).tools,actual=(await native.client.listTools()).tools;expect(actual.find(tool=>tool.name==='obs-set-scene-item-transform')).toEqual(expected.find(tool=>tool.name==='obs-set-scene-item-transform'));
  for(const args of variants)await same(oracle,native,'obs-set-scene-item-transform',args);for(const request of['SetSceneItemTransform','GetSceneItemTransform'])expect(trace(native,request)).toEqual(trace(oracle,request));expect(receipts(native)).toHaveLength(variants.length);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.requestAccepted===true&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
 it('matches complete invalid schema errors before any native socket or intent',async()=>{
  const oracle=await start(false,legacy),native=await start(true,legacy);for(const args of invalid)expect((await same(oracle,native,'obs-set-scene-item-transform',args)).isError).toBe(true);expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
 });
 it('keeps accepted readback rejection a warning and preserves complete mutation rejection with no readback',async()=>{
  const oracle=await start(false,legacy),native=await start(true,legacy);for(const session of[oracle,native])session.fake.respondWith('GetSceneItemTransform',()=>{throw new FakeOBSRequestError(500,'PRIVATE readback fixture');});const warning=await same(oracle,native,'obs-set-scene-item-transform',cases[0][2]);expect(warning.isError).not.toBe(true);expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
  for(const session of[oracle,native])session.fake.respondWith('SetSceneItemTransform',()=>{throw new FakeOBSRequestError(500,'PRIVATE mutation fixture');});expect((await same(oracle,native,'obs-set-scene-item-transform',cases[0][2])).isError).toBe(true);expect(trace(native,'GetSceneItemTransform')).toHaveLength(1);expect(receipts(native).filter(receipt=>receipt.phase==='rejected')).toHaveLength(1);
 });
 it('uses cached unsupported getter capability without another packet and ignores irrelevant invalid screenshot policy',async()=>{
  const oracle=await start(false,legacy,false,true,{OBS_MCP_MAX_SCREENSHOT_BYTES:'invalid'},true),native=await start(true,legacy,false,true,{OBS_MCP_MAX_SCREENSHOT_BYTES:'invalid'},true);const result=await same(oracle,native,'obs-set-scene-item-transform',cases[0][2]);expect(result.isError).not.toBe(true);expect(JSON.stringify(result)).toContain("does not advertise support for request 'GetSceneItemTransform'");expect(trace(native,'GetSceneItemTransform')).toHaveLength(0);expect(trace(native,'GetVersion')).toHaveLength(1);expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false});
 });
 it('does not consult invalid screenshot policy when the typed getter is supported',async()=>{
  const oracle=await start(false,legacy,false,false,{OBS_MCP_MAX_SCREENSHOT_BYTES:'invalid'}),native=await start(true,legacy,false,false,{OBS_MCP_MAX_SCREENSHOT_BYTES:'invalid'});expect((await same(oracle,native,'obs-set-scene-item-transform',cases[0][2])).isError).not.toBe(true);expect(trace(native,'GetSceneItemTransform')).toHaveLength(1);expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false});
 });
});

async function rawSession(native:boolean,legacy:boolean){
 const socket=new WebSocketServer({host:'127.0.0.1',port:0});await once(socket,'listening');const wire:string[]=[];let body='{"sceneItemTransform":{"z":3,"a":1}}';
 socket.on('connection',peer=>{peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));peer.on('message',bytes=>{const text=bytes.toString();const frame=JSON.parse(text) as {op:number;d:{requestType:string;requestId:string}};if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));if(frame.op===6){wire.push(text);const response=frame.d.requestType==='GetVersion'?JSON.stringify({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion','SetSceneItemTransform','GetSceneItemTransform'],supportedImageFormats:[]}):frame.d.requestType==='GetSceneItemTransform'?body:'{}';peer.send(`{"op":7,"d":{"requestId":${JSON.stringify(frame.d.requestId)},"requestType":${JSON.stringify(frame.d.requestType)},"requestStatus":{"result":true,"code":100},"responseData":${response}}}`);}});});
 const directory=join(root,'native/target',`transform1-raw-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const address=socket.address();if(!address||typeof address==='string')throw new Error('listener unavailable');const child=spawn(native?binary:process.execPath,native?[]:[sdk],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:`ws://127.0.0.1:${address.port}`,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});
 let buffer='';const replies:Record<string,unknown>[]=[];child.stderr.on('data',()=>undefined);child.stdout.on('data',(part:Buffer)=>{buffer+=part.toString();let line:number;while((line=buffer.indexOf('\n'))>=0){replies.push(JSON.parse(buffer.slice(0,line)) as Record<string,unknown>);buffer=buffer.slice(line+1);}});
 const response=async(id:number)=>{await vi.waitFor(()=>expect(replies.some(message=>message.id===id)).toBe(true),{timeout:10_000});return replies.find(message=>message.id===id)!;};
 const close=async()=>{child.stdin.end();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(socket.clients.size).toBe(0));await new Promise<void>((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));rmSync(directory,{recursive:true,force:true});};
 try {if(legacy){child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'transform1-raw',version:'1'},capabilities:{}}})+'\n');await response(1);child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');}
 return {wire,directory,transport:{pid:child.pid},close,setBody(value:string){body=value;},async call(id:number,args:string){const meta=legacy?'':',"_meta":{"io.modelcontextprotocol/protocolVersion":"2026-07-28","io.modelcontextprotocol/clientInfo":{"name":"transform1-raw","version":"1"},"io.modelcontextprotocol/clientCapabilities":{}}';child.stdin.write(`{"jsonrpc":"2.0","id":${id},"method":"tools/call","params":{"name":"obs-set-scene-item-transform","arguments":${args}${meta}}}\n`);return (await response(id)).result;}};
 }catch(error){await close();throw error;}
}
const rawBodies=[
 '{"sceneItemTransform":{"z":3,"a":1}}',
 '{"sceneItemTransform":{"z":3,"2":"two","10":"ten","z":4,"a":1,"01":"leading","4294967294":"index","4294967295":"ordinary","nested":{"b":1,"a":2},"huge":18446744073709551615,"tie":144713588892437.625}}',
 '{"sceneItemTransform":{"$serde_json::private::RawValue":"123","__proto__":"kept","constructor":"kept","nested":{"$serde_json::private::RawValue":{"value":true},"z":3,"a":1}}}',
 '{"sceneItemTransform":{"escaped\\u006bey":"newline\\nquote\\\"","z":3,"a":1}}',
 '{"sceneItemTransform":false}','{"sceneItemTransform":0}','{"sceneItemTransform":""}','{"sceneItemTransform":[false,null,{"z":3,"a":1}]}',
 '{"sceneItemTransform":null}','{}','null','false','[]',
 '{"sceneItemTransform":{"sourceWidth":"0","sourceHeight":false}}',
 '{"sceneItemTransform":{"sourceWidth":12,"sourceHeight":-0.0}}',
];
for(const legacy of[false,true])it(`preserves exact applied JSON order, defaults, raw numeric wire tokens and structured Number semantics (legacy=${legacy})`,async()=>{
 const oracle=await rawSession(false,legacy),native=await rawSession(true,legacy);const args='{"sceneName":"Main","sceneItemId":1.0,"sceneItemTransform":{"negativeZero":-0.0,"integer":1.0,"unsafeInteger":9007199254740993,"fullU64":18446744073709551615,"tie":144713588892437.625},"positionX":144713588892437.625,"scaleX":-0.0,"scaleY":1.0}';
 try {
  for(const[index,body]of rawBodies.entries()){oracle.setBody(body);native.setBody(body);const expected=await oracle.call(index+10,args),actual=await native.call(index+10,args);expect(actual,body).toEqual(expected);}
  for(const session of[oracle,native]){const frames=session.wire.filter(text=>(JSON.parse(text) as {d:{requestType:string}}).d.requestType==='SetSceneItemTransform');expect(frames).toHaveLength(rawBodies.length);for(const frame of frames)for(const[key,value]of Object.entries({sceneItemId:'1',negativeZero:'0',integer:'1',unsafeInteger:'9007199254740992',fullU64:'18446744073709552000',tie:'144713588892437.62',positionX:'144713588892437.62',scaleX:'0',scaleY:'1'})){expect(frame.match(new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)`))?.[1],key).toBe(value);}}
  expect(receipts(native)).toHaveLength(rawBodies.length);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.requestAccepted===true&&receipt.postconditionObserved===false)).toBe(true);expect(children(native)).toEqual([]);
 }finally{await native.close();await oracle.close();}
});

it('retains Accepted transform readback while physical owned Stop dispatches before the readback reply',async()=>{
 const oracle=await start(false,false,true),native=await start(true,false,true);for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);
 const held: {session:Session;request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;after:number;pending:Promise<unknown>}[]=[];const stops:Promise<unknown>[]=[];
 try {
  for(const session of[oracle,native]){session.fake.queueSuccess('GetSceneItemTransform',{sceneItemTransform:{z:3,a:1}},60_000);const after=session.fake.cursor();const pending=session.client.callTool({name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{once:true}}}).then(value=>({value}),error=>({error:String(error)}));const request=await session.fake.waitForRequest('GetSceneItemTransform',{after,timeoutMs:10_000});held.push({session,request,after,pending});}
  expect(receipts(native).find(receipt=>receipt.requestType==='SetSceneItemTransform')).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
  for(const session of[oracle,native]){expect(trace(session,'StopRecord')).toHaveLength(0);stops.push(session.client.callTool({name:'obs-take-stop',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));}
  await oracle.fake.waitForRequest('StopRecord',{after:held[0]!.after,timeoutMs:4000});await expect(native.fake.waitForRequest('StopRecord',{after:held[1]!.after,timeoutMs:1000}),'physical Stop must reach OBS before GetSceneItemTransform is released').resolves.toBeDefined();for(const session of[oracle,native])expect(trace(session,'StopRecord')).toHaveLength(1);
 } finally {
  for(const{session,request}of held)session.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetSceneItemTransform',requestStatus:{result:true,code:100},responseData:{sceneItemTransform:{z:3,a:1}}}},request.connectionId);
  await Promise.all([...held.map(item=>item.pending),...stops]);for(const session of[oracle,native]){await session.client.callTool({name:'obs-take-stop',arguments:{}});expect(session.recording()).toBe(false);}
 }
 expect(receipts(native).find(receipt=>receipt.requestType==='SetSceneItemTransform')).toMatchObject({phase:'accepted',postconditionObserved:false});expect(trace(native,'SetSceneItemTransform')).toHaveLength(1);
});
it('retains a cancelled frontend transform job without replay and permits an independent explicit command after its read drains',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('GetSceneItemTransform',{sceneItemTransform:{z:3,a:1}},60_000);const after=native.fake.cursor();const abort=new AbortController();
 const pending=native.client.callTool({name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{once:true}}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));const request=await native.fake.waitForRequest('GetSceneItemTransform',{after,timeoutMs:10_000});
 expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});abort.abort();expect(await pending).toHaveProperty('error');await native.client.listTools();
 expect(trace(native,'GetCurrentProgramScene')).toHaveLength(0);native.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetSceneItemTransform',requestStatus:{result:true,code:100},responseData:{sceneItemTransform:{z:3,a:1}}}},request.connectionId);
 expect((await native.client.callTool({name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{explicitLater:true}}})).isError).not.toBe(true);expect(trace(native,'SetSceneItemTransform')).toHaveLength(2);expect(trace(native,'GetSceneItemTransform')).toHaveLength(2);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
});
it('joins EOF during accepted readback without replay, postcondition promotion, child process or socket leak',async()=>{
 const fake=await FakeOBSServer.start({platform:'macos'});serveProduction(fake);fake.respondWith('SetSceneItemTransform',()=>({}));fake.queueSuccess('GetSceneItemTransform',{sceneItemTransform:{z:3,a:1}},60_000);
 const directory=join(root,'native/target',`transform1-eof-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});
 const child=spawn(binary,[],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});child.stdout.on('data',()=>undefined);child.stderr.on('data',()=>undefined);
 let released=false;let request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>|undefined;
 try {
  child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'tools/call',params:{name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{once:true}},_meta:{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name:'transform1-eof',version:'1'},'io.modelcontextprotocol/clientCapabilities':{}}}})+'\n');
  request=await fake.waitForRequest('GetSceneItemTransform',{timeoutMs:10_000});expect(receipts({directory})[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(children({transport:{pid:child.pid}})).toEqual([]);child.stdin.end();await new Promise(resolve=>setTimeout(resolve,40));expect(child.exitCode).toBeNull();expect(fake.openConnectionCount).toBe(1);
  fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetSceneItemTransform',requestStatus:{result:true,code:100},responseData:{sceneItemTransform:{z:3,a:1}}}},request.connectionId);released=true;
  await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(fake.openConnectionCount).toBe(0));expect(trace({fake},'SetSceneItemTransform')).toHaveLength(1);expect(receipts({directory})[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});released=true;
 } finally {
  if(!released&&request&&fake.openConnectionCount)fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'GetSceneItemTransform',requestStatus:{result:true,code:100},responseData:{sceneItemTransform:{z:3,a:1}}}},request.connectionId);
  child.stdin.end();if(child.exitCode===null&&child.signalCode===null){await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:5000});}await fake.close();rmSync(directory,{recursive:true,force:true});
 }
});

it('reopens a lost transform acknowledgement as Unknown and grants no automatic or explicit replay permission',async()=>{
 const original=await start(true,false);original.fake.queueDisconnect('SetSceneItemTransform','lost transform ACK');
 expect((await original.client.callTool({name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{once:true}}})).isError).toBe(true);
 const receipt=receipts(original)[0];expect(receipt).toMatchObject({requestType:'SetSceneItemTransform',phase:'unknown',requestAccepted:null,postconditionObserved:false});expect(trace(original,'SetSceneItemTransform')).toHaveLength(1);expect(trace(original,'GetSceneItemTransform')).toHaveLength(0);
 await original.client.close();await original.transport.close();await vi.waitFor(()=>expect(original.fake.openConnectionCount).toBe(0));
 const client=new Client({name:'transform1',version:'1'},{versionNegotiation:{mode:{pin:'2026-07-28'}}});
 const transport=new StdioClientTransport({command:binary,args:[],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:original.fake.url,OBS_MCP_STATE_DIR:original.directory,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_COMPAT_NODE:join(original.directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(original.directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
 const restarted={...original,client,transport};sessions[sessions.indexOf(original)]=restarted;await client.connect(transport);
 expect((await client.callTool({name:'obs-set-scene-item-transform',arguments:{sceneName:'Main',sceneItemId:42,sceneItemTransform:{explicitLater:true}}})).isError).toBe(true);
 expect(receipts(restarted)).toEqual([receipt]);expect(trace(restarted,'SetSceneItemTransform')).toHaveLength(1);for(const request of['GetSceneItemTransform'])expect(trace(restarted,request)).toHaveLength(0);expect(children(restarted)).toEqual([]);
});
for(const legacy of[false,true])it(`preserves the complete post-ACK getter-disconnect warning while retaining Accepted (legacy=${legacy})`,async()=>{
 const oracle=await start(false,legacy),native=await start(true,legacy);for(const session of[oracle,native])session.fake.queueDisconnect('GetSceneItemTransform','lost readback reply');
 const expected=await oracle.client.callTool({name:'obs-set-scene-item-transform',arguments:cases[0][2]}),result=await native.client.callTool({name:'obs-set-scene-item-transform',arguments:cases[0][2]});
 const durable=receipts(native);expect(durable[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(JSON.stringify(durable)).not.toContain('lost readback reply');expect(trace(native,'SetSceneItemTransform')).toHaveLength(1);expect(trace(native,'GetSceneItemTransform')).toHaveLength(1);expect(children(native)).toEqual([]);
 const output=process.env.OBS_TRANSFORM_COUNTER_DIR;if(output){mkdirSync(output,{recursive:true});writeFileSync(join(output,`getter-close-${legacy?'legacy':'modern'}.json`),JSON.stringify({legacy,expected,actual:result,receipts:durable,mutation:trace(native,'SetSceneItemTransform'),readback:trace(native,'GetSceneItemTransform')},null,2)+'\n');}
 expect(result).toEqual(expected);expect(result.isError).not.toBe(true);
});
