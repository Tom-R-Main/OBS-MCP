/* SPDX-License-Identifier: GPL-2.0-only */
import { expectNativeInventory } from '../support/native-inventory.js';
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
const cases = [
 ['obs-broadcast-custom-event','BroadcastCustomEvent',{eventData:{message:'fixture',nested:[null,true,42]}}],
 ['obs-set-transition-settings','SetCurrentSceneTransitionSettings',{transitionSettings:{duration:125.5,nested:{preserved:true}}}],
 ['obs-create-source-filter','CreateSourceFilter',{canvasUuid:'canvas',sourceName:'Fixture source',filterName:'日本語 filter',filterKind:'opaque_plugin',filterSettings:{value:42}}],
 ['obs-set-source-filter-settings','SetSourceFilterSettings',{sourceName:'Fixture source',filterName:'Filter',filterSettings:{value:null,nested:[false,{}]},overlay:false}],
 ['obs-press-input-property-button','PressInputPropertiesButton',{inputUuid:'fixture-input',propertyName:'plugin_defined_button'}],
 ['obs-save-source-screenshot','SaveSourceScreenshot',{sourceName:'Fixture source',imageFormat:'opaque_format',imageFilePath:'/remote/not-approved/screenshot.png',imageWidth:-1.5,imageHeight:0,imageCompressionQuality:999}],
] as const;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;recordingRoot:string;state:ReturnType<typeof serveProduction>;recording:()=>boolean;externalRecording:(active:boolean,path:string)=>void};const sessions:Session[]=[];
async function start(native:boolean,legacy:boolean,recording=false,unadvertised=false,extra:Record<string,string>={}):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png'],autoGetVersion:!unadvertised});const state=serveProduction(fake);state.preview='Main';const directory=join(root,'native/target',`generic6-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});let active=false;
 for(const[,request]of cases)fake.respondWith(request,()=>({sceneItemId:42,extension:{preserved:true,values:[null,1,false]}}));fake.respondWith('SetPersistentData',()=>({}));
 const path=join(recordingRoot,'clip.mkv');fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));let recordDirectory=recording?recordingRoot:undefined;fake.respondWith('SetRecordDirectory',data=>{recordDirectory=String(data.recordDirectory);return {};});fake.respondWith('GetRecordDirectory',()=>{if(recordDirectory)return {recordDirectory};throw new FakeOBSRequestError(604,'fixture directory unavailable');});fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
 if(unadvertised)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 const client=new Client({name:'generic6',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recording?recordingRoot:'',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false',...extra},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,state,directory,recordingRoot,recording:()=>active,externalRecording:(next:boolean,nextPath:string)=>{active=next;fake.sendEvent('RecordStateChanged',{outputActive:next,outputState:next?'OBS_WEBSOCKET_OUTPUT_STARTED':'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:nextPath});}};sessions.push(session);await client.connect(transport);if(unadvertised&&!native){const version=await client.callTool({name:'obs-get-version',arguments:{}});expect(version.structuredContent).toMatchObject({availableRequests:['GetVersion']});}return session;
}
function normalized(value:unknown):unknown{if(Array.isArray(value))return value.map(normalized);if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([key,value])=>[key,normalized(value)]));if(typeof value==='string'){try{const parsed=JSON.parse(value);if(parsed&&typeof parsed==='object')return {jsonValue:normalized(parsed)};}catch{}}return value;}
async function same(a:Session,b:Session,name:string,args:Record<string,unknown>={}){const expected=await a.client.callTool({name,arguments:args});const actual=await b.client.callTool({name,arguments:args});expect(normalized(actual)).toEqual(normalized(expected));return actual;}
function trace(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request).map(item=>item.frame.d.requestData);}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',name),'utf8')) as Record<string,unknown>);}catch{return [];}}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
afterEach(async()=>{for(const session of sessions.splice(0)){try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0));}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});

const record = () => JSON.parse('{"__proto__":{"outer":"Source strips"},"constructor":"kept","prototype":"kept","$serde_json::private::RawValue":"123","nested":{"__proto__":"nested kept","$serde_json::private::Number":"123","private":[null,false,{"value":42}]}}') as Record<string,unknown>;
const variants: [string,Record<string,unknown>][] = [
 ['obs-broadcast-custom-event',{eventData:{}}],['obs-broadcast-custom-event',{eventData:record()}],
 ['obs-set-transition-settings',{transitionSettings:record()}],
 ['obs-create-source-filter',{sourceName:'',filterName:'',filterKind:''}],
 ['obs-create-source-filter',{sourceName:'S',filterName:'F',filterKind:'K',filterSettings:{}}],
 ['obs-create-source-filter',{sourceName:'S',filterName:'F',filterKind:'K',filterSettings:record()}],
 ['obs-set-source-filter-settings',{sourceName:'S',filterName:'F',filterSettings:record()}],
 ['obs-set-source-filter-settings',{sourceName:'S',filterName:'F',filterSettings:{},overlay:false}],
 ['obs-set-source-filter-settings',{sourceName:'S',filterName:'F',filterSettings:{},overlay:true}],
 ['obs-press-input-property-button',{propertyName:''}],
 ['obs-press-input-property-button',{inputName:'',inputUuid:'',propertyName:'P'}],
 ['obs-save-source-screenshot',{imageFormat:'',imageFilePath:''}],
 ['obs-save-source-screenshot',{canvasUuid:'',sourceUuid:'',imageFormat:'png',imageFilePath:'relative-path',imageWidth:144713588892437.625,imageHeight:-0,imageCompressionQuality:1e21}],
];
const invalid: [string,Record<string,unknown>][] = [
 ['obs-broadcast-custom-event',{}],['obs-broadcast-custom-event',{eventData:null}],['obs-broadcast-custom-event',{eventData:[]}],
 ['obs-set-transition-settings',{transitionSettings:42}],
 ['obs-create-source-filter',{sourceName:'S',filterName:'F',filterKind:'K',filterSettings:null}],
 ['obs-create-source-filter',{sourceName:null,filterName:42}],
 ['obs-set-source-filter-settings',{sourceName:'S',filterName:'F',filterSettings:{},overlay:null}],
 ['obs-set-source-filter-settings',{sourceName:'S',filterName:'F',filterSettings:[]}],
 ['obs-press-input-property-button',{}],['obs-press-input-property-button',{inputName:null,propertyName:'P'}],
 ['obs-save-source-screenshot',{imageFormat:'png',imageFilePath:'P',imageWidth:null}],
 ['obs-save-source-screenshot',{imageFormat:'png',imageFilePath:42}],
];
function meta(name:string){return {'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{}};}
async function raw(session:Session,id:number,name:string,args:Record<string,unknown>,caller:string){const prior=session.transport.onmessage;let answer:Record<string,unknown>|undefined;session.transport.onmessage=message=>{const value=message as unknown as Record<string,unknown>;if(value.id===id)answer=value;prior?.(message);};try{await session.transport.send({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,_meta:meta(caller)}} as Parameters<StdioClientTransport['send']>[0]);await vi.waitFor(()=>expect(answer).toBeDefined(),{timeout:10_000});return answer!;}finally{session.transport.onmessage=prior;}}
for (const legacy of [false,true]) {
 it(`Source-only opaque6 golden callbacks and wire distinctions (legacy=${legacy})`,async()=>{
  const oracle=await start(false,legacy);const listed=(await oracle.client.listTools()).tools;
  const messages=['Custom event broadcast successfully','Successfully updated current transition settings',"Successfully created filter '日本語 filter' of kind 'opaque_plugin' on source 'Fixture source'","Successfully updated settings for filter 'Filter' on source 'Fixture source'",'Press Input Property Button completed successfully','Successfully saved screenshot to: /remote/not-approved/screenshot.png'];
  for(const[index,[name,request,args]]of cases.entries()){const tool=listed.find(tool=>tool.name===name);expect(tool).toBeDefined();expect(tool?.annotations?.readOnlyHint).toBe(false);const result=await oracle.client.callTool({name,arguments:args});expect(result.isError).not.toBe(true);expect(result).toMatchObject({content:[{type:'text',text:messages[index]}],structuredContent:{message:messages[index]}});expect(trace(oracle,request)).toHaveLength(1);}
  for(const[name,args]of variants)expect((await oracle.client.callTool({name,arguments:args})).isError).not.toBe(true);
  const emitted=(trace(oracle,'BroadcastCustomEvent').at(-1) as Record<string,unknown>)?.eventData as Record<string,unknown>;expect(Object.hasOwn(emitted,'__proto__')).toBe(false);expect(emitted.constructor).toBe('kept');expect(emitted['$serde_json::private::RawValue']).toBe('123');expect(emitted.nested).toEqual(record().nested);
  const created=trace(oracle,'CreateSourceFilter');expect(created.at(-3)).not.toHaveProperty('filterSettings');expect(created.at(-2)).toHaveProperty('filterSettings',{});
  const changed=trace(oracle,'SetSourceFilterSettings');expect(changed.at(-3)).not.toHaveProperty('overlay');expect(changed.at(-2)).toHaveProperty('overlay',false);expect(changed.at(-1)).toHaveProperty('overlay',true);
  const before=cases.reduce((count,[,request])=>count+trace(oracle,request).length,0);for(const[name,args]of invalid)expect((await oracle.client.callTool({name,arguments:args})).isError).toBe(true);expect(cases.reduce((count,[,request])=>count+trace(oracle,request).length,0)).toBe(before);
  const prefixes=['Broadcast Custom Event failed: ','Set Transition Settings failed: ','Error creating source filter: ','Error setting source filter settings: ','Press Input Property Button failed: ','Save Source Screenshot failed: '];for(const[index,[name,request,args]]of cases.entries()){oracle.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'opaque private rejection');});const rejected=await oracle.client.callTool({name,arguments:args});expect(rejected).toMatchObject({isError:true,content:[{type:'text',text:`${prefixes[index]}OBS request ${request} failed with code 500: opaque private rejection`}]});expect(rejected.structuredContent).toBeUndefined();}

 });
 describe(`opaque6 actual public SDK parity (legacy=${legacy})`,()=>{
  it('owns six complete Source contracts, full success results and single physical-session traces',async()=>{
   const oracle=await start(false,legacy);const native=await start(true,legacy);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;
   for(const[name,request,args]of cases){expect(actual.find(tool=>tool.name===name)).toEqual(expected.find(tool=>tool.name===name));await same(oracle,native,name,{...args,discarded:'Source strips root unknown'});expect(trace(native,request)).toEqual(trace(oracle,request));}
   expect(receipts(native)).toHaveLength(6);expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.requestAccepted===true&&receipt.postconditionObserved===false)).toBe(true);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
  });
  it('preserves Source opaque records, private serde sentinels, omissions, explicit false and numeric edges',async()=>{
   const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,args]of variants)await same(oracle,native,name,args);for(const[,request]of cases)expect(trace(native,request)).toEqual(trace(oracle,request));expect(receipts(native).every(receipt=>receipt.phase==='accepted'&&receipt.postconditionObserved===false)).toBe(true);
  });
  it('returns complete explicit OBS rejection bodies and never promotes rejection to acceptance',async()=>{
   const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,request,args]of cases){for(const session of[oracle,native])session.fake.respondWith(request,()=>{throw new FakeOBSRequestError(500,'PRIVATE opaque callback rejection');});expect((await same(oracle,native,name,args)).isError).toBe(true);}expect(receipts(native)).toHaveLength(6);expect(receipts(native).every(receipt=>receipt.phase==='rejected'&&receipt.postconditionObserved===false)).toBe(true);
  });
  it('projects full Source invalid-argument errors with zero native socket, receipt or opaque effects',async()=>{
   const oracle=await start(false,legacy);const native=await start(true,legacy);for(const[name,args]of invalid)expect((await same(oracle,native,name,args)).isError).toBe(true);expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
  });
  it('preserves advertised-capability refusal without intent or mutation',async()=>{
   const oracle=await start(false,legacy,false,true);const native=await start(true,legacy,false,true);for(const[name,,args]of cases)expect((await same(oracle,native,name,args)).isError).toBe(true);for(const[,request]of cases)expect(trace(native,request)).toHaveLength(0);expect(receipts(native)).toEqual([]);
  });
 });
}
it('checks a foreign lease before opaque capability refusal and never sends a plugin or remote-file request',async()=>{
 const oracle=await start(false,false,false,true);const native=await start(true,false,false,true);
 for(const session of[oracle,native])expect((await raw(session,9100,'obs-claim-control',{minutes:1,reason:'opaque control'},'other-client')).result).toMatchObject({structuredContent:{holder:'other-client'}});
 for(const[name,request,args]of cases){const expected=(await raw(oracle,9101,name,args,'generic6')).result;const actual=(await raw(native,9101,name,args,'generic6')).result;expect(actual).toEqual(expected);expect(JSON.stringify(actual)).toContain('other-client');expect(trace(native,request)).toHaveLength(0);}expect(receipts(native)).toEqual([]);
});
it('does not replay a lost opaque effect or grant observation authority to its unknown receipt',async()=>{
 const native=await start(true,false);native.fake.queueDisconnect('BroadcastCustomEvent','lost opaque reply');expect((await native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{once:true}}})).isError).toBe(true);expect(receipts(native)[0]).toMatchObject({requestType:'BroadcastCustomEvent',phase:'unknown',requestAccepted:null,postconditionObserved:false});expect((await native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{explicitLater:true}}})).isError).toBe(true);expect(trace(native,'BroadcastCustomEvent')).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
});
it('retains a cancelled frontend opaque job through its original known ACK and accepts an independent explicit command',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('BroadcastCustomEvent',{},60_000);const cursor=native.fake.cursor();const abort=new AbortController();const pending=native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{once:true}}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));const request=await native.fake.waitForRequest('BroadcastCustomEvent',{after:cursor,timeoutMs:10_000});abort.abort();expect(await pending).toHaveProperty('error');native.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'BroadcastCustomEvent',requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);await vi.waitFor(()=>expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false}));expect((await native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{independent:true}}})).isError).not.toBe(true);expect(trace(native,'BroadcastCustomEvent')).toHaveLength(2);expect(native.fake.connectionCount).toBe(1);
});
it('treats a remote screenshot ACK as acceptance without granting local file-root authority or touching a human file',async()=>{
 const native=await start(true,false,true);const outside=join(native.directory,'outside');mkdirSync(outside);const human=join(outside,'human.png');writeFileSync(human,'human data');expect((await native.client.callTool({name:'obs-save-source-screenshot',arguments:{sourceName:'Fixture',imageFormat:'png',imageFilePath:human}})).isError).not.toBe(true);expect(receipts(native)[0]).toMatchObject({requestType:'SaveSourceScreenshot',phase:'accepted',postconditionObserved:false});const sheet=await native.client.callTool({name:'obs-contact-sheet',arguments:{path:human}});expect(sheet.isError).toBe(true);expect(JSON.stringify(sheet)).toMatch(/outside|approved|confined/i);expect(readFileSync(human,'utf8')).toBe('human data');expect(readdirSync(outside)).toEqual(['human.png']);expect(native.fake.connectionCount).toBe(1);
});

async function numericWireSession(native:boolean,legacy:boolean){
 const socket=new WebSocketServer({host:'127.0.0.1',port:0});await once(socket,'listening');
 const wire:string[]=[];const availableRequests=['GetVersion',...cases.map(([,request])=>request)];
 socket.on('connection',peer=>{peer.send(JSON.stringify({op:0,d:{rpcVersion:1,obsWebSocketVersion:'5.7.0'}}));peer.on('message',data=>{const text=data.toString();const frame=JSON.parse(text) as {op:number;d:{requestType?:string;requestId?:string}};if(frame.op===1)peer.send(JSON.stringify({op:2,d:{negotiatedRpcVersion:1}}));if(frame.op===6){wire.push(text);peer.send(JSON.stringify({op:7,d:{requestType:frame.d.requestType,requestId:frame.d.requestId,requestStatus:{result:true,code:100},responseData:frame.d.requestType==='GetVersion'?{obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests,supportedImageFormats:['png']}:{}}}));}});});
 const directory=join(root,'native/target',`generic6-numbers-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const address=socket.address();if(!address||typeof address==='string')throw new Error('numeric fixture listener unavailable');
 const child=spawn(native?binary:process.execPath,native?[]:[sdk],{cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:`ws://127.0.0.1:${address.port}`,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stdio:['pipe','pipe','pipe']});
 const replies:Record<string,unknown>[]=[];let buffer='';child.stderr.on('data',()=>undefined);child.stdout.on('data',(chunk:Buffer)=>{buffer+=chunk.toString();let newline:number;while((newline=buffer.indexOf('\n'))>=0){replies.push(JSON.parse(buffer.slice(0,newline)) as Record<string,unknown>);buffer=buffer.slice(newline+1);}});
 const response=async(id:number)=>{await vi.waitFor(()=>expect(replies.some(message=>message.id===id)).toBe(true));return replies.find(message=>message.id===id)!;};
 const close=async()=>{child.stdin.end();await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:4000});expect(child.exitCode).toBe(0);await vi.waitFor(()=>expect(socket.clients.size).toBe(0));await new Promise<void>((resolve,reject)=>socket.close(error=>error?reject(error):resolve()));rmSync(directory,{recursive:true,force:true});};
 try{
  if(legacy){child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'numeric-wire',version:'1'},capabilities:{}}})+'\n');await response(1);child.stdin.write(JSON.stringify({jsonrpc:'2.0',method:'notifications/initialized'})+'\n');}
  return {wire,directory,transport:{pid:child.pid},close,async call(id:number,name:string,args:string){const metadata=legacy?'':`,"_meta":${JSON.stringify(meta('numeric-wire'))}`;child.stdin.write(`{"jsonrpc":"2.0","id":${id},"method":"tools/call","params":{"name":${JSON.stringify(name)},"arguments":${args}${metadata}}}\n`);return (await response(id)).result;}};
 }catch(error){await close();throw error;}
}

const numericArgs = '{"eventData":{"negativeZero":-0.0,"integer":1.0,"unsafeInteger":9007199254740993,"fullU64":18446744073709551615,"tie":144713588892437.625,"nested":{"$serde_json::private::RawValue":"123","$serde_json::private::Number":"123"}}}';
const lexemes = {negativeZero:'0',integer:'1',unsafeInteger:'9007199254740992',fullU64:'18446744073709552000',tie:'144713588892437.62'};
function checkLexemes(frame:string){for(const[key,value]of Object.entries(lexemes)){const extracted=frame.match(new RegExp(`"${key}"\\s*:\\s*(-?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?)`))?.[1];expect(extracted,key).toBe(value);}expect((JSON.parse(frame) as {d:{requestData:{eventData:{nested:unknown}}}}).d.requestData.eventData.nested).toEqual({'$serde_json::private::RawValue':'123','$serde_json::private::Number':'123'});}
for(const legacy of[false,true]){
 it(`Source-only opaque6 literal numeric frame tokens and private sentinels (legacy=${legacy})`,async()=>{const oracle=await numericWireSession(false,legacy);try{expect(await oracle.call(10,'obs-broadcast-custom-event',numericArgs)).toMatchObject({structuredContent:{message:'Custom event broadcast successfully'}});checkLexemes(oracle.wire.find(frame=>(JSON.parse(frame) as {d:{requestType:string}}).d.requestType==='BroadcastCustomEvent')!);}finally{await oracle.close();}});
 it(`matches Source opaque6 actual numeric wire lexemes without private serde interpretation (legacy=${legacy})`,async()=>{const oracle=await numericWireSession(false,legacy);const native=await numericWireSession(true,legacy);try{expect(await native.call(10,'obs-broadcast-custom-event',numericArgs)).toEqual(await oracle.call(10,'obs-broadcast-custom-event',numericArgs));for(const session of[oracle,native])checkLexemes(session.wire.find(frame=>(JSON.parse(frame) as {d:{requestType:string}}).d.requestType==='BroadcastCustomEvent')!);expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false});}finally{await native.close();await oracle.close();}});
}

type HeldMutation = {session:Session;request:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;pending:Promise<unknown>};
async function holdOwnedMutation(session:Session):Promise<HeldMutation>{
 expect(trace(session,'StopRecord'),'fixture must still own the recording before plugin setup').toHaveLength(0);
 session.fake.queueSuccess('SetPersistentData',{},60_000);const after=session.fake.cursor();
 const pending=session.client.callTool({name:'obs-set-persistent-data',arguments:{realm:'fixture',slotName:'held',slotValue:{held:true}}}).then(value=>({value}),error=>({error:String(error)}));
 const request=await session.fake.waitForRequest('SetPersistentData',{after,timeoutMs:10_000});
 expect(trace(session,'StopRecord'),'Stop must not predate the held request').toHaveLength(0);
 return {session,request,pending};
}
async function releaseAndJoin(held:HeldMutation[],stops:Promise<unknown>[]=[]){
 for(const{session,request}of held)session.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'SetPersistentData',requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);
 await Promise.all([...held.map(item=>item.pending),...stops]);
 for(const{session}of held){await session.client.callTool({name:'obs-take-stop',arguments:{}});await vi.waitFor(()=>expect(session.recording()).toBe(false),{timeout:4000});expect(trace(session,'StopRecord')).toHaveLength(1);}
}
it('preserves Source owned timer Stop progress while an already-owned mutation ACK remains unresolved',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);
 for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:3}})).isError).not.toBe(true);
 const held:HeldMutation[]=[];
 try{
  for(const session of[oracle,native])held.push(await holdOwnedMutation(session));
  for(const session of[oracle,native])expect(trace(session,'StopRecord'),'both requests must be held before the timer Stop').toHaveLength(0);
  await oracle.fake.waitForRequest('StopRecord',{after:held[0]!.request.cursor,timeoutMs:5000});
  expect(receipts(native).find(receipt=>receipt.requestType==='SetPersistentData')).toMatchObject({phase:'dispatching',postconditionObserved:false});
  await expect(native.fake.waitForRequest('StopRecord',{after:held[1]!.request.cursor,timeoutMs:1000}),'native physical timer Stop must dispatch after the held request and before its ACK is released').resolves.toBeDefined();
 }finally{await releaseAndJoin(held);}
});
it('preserves Source manual Stop priority after both mutation ACKs are known held',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);
 for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);
 const held:HeldMutation[]=[];const stops:Promise<unknown>[]=[];
 try{
  for(const session of[oracle,native])held.push(await holdOwnedMutation(session));
  for(const session of[oracle,native])expect(trace(session,'StopRecord'),'manual Stop setup must have no earlier Stop').toHaveLength(0);
  for(const session of[oracle,native])stops.push(session.client.callTool({name:'obs-take-stop',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));
  await oracle.fake.waitForRequest('StopRecord',{after:held[0]!.request.cursor,timeoutMs:4000});
  expect(receipts(native).find(receipt=>receipt.requestType==='SetPersistentData')).toMatchObject({phase:'dispatching',postconditionObserved:false});
  await expect(native.fake.waitForRequest('StopRecord',{after:held[1]!.request.cursor,timeoutMs:1000}),'native physical manual Stop must dispatch after both held requests and before ACK release').resolves.toBeDefined();
 }finally{await releaseAndJoin(held,stops);}
});
it('Source-only opaque6 deadline Stop can dispatch after the held plugin request and before its ACK',async()=>{
 const oracle=await start(false,false,true);expect((await oracle.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:3}})).isError).not.toBe(true);
 expect(trace(oracle,'StopRecord')).toHaveLength(0);oracle.fake.queueSuccess('SetSourceFilterSettings',{},60_000);const after=oracle.fake.cursor();
 const pending=oracle.client.callTool({name:'obs-set-source-filter-settings',arguments:{sourceName:'S',filterName:'F',filterSettings:{held:true}}}).then(value=>({value}),error=>({error:String(error)}));
 const request=await oracle.fake.waitForRequest('SetSourceFilterSettings',{after,timeoutMs:10_000});
 try{expect(trace(oracle,'StopRecord'),'no timer Stop may precede the held request').toHaveLength(0);await oracle.fake.waitForRequest('StopRecord',{after:request.cursor,timeoutMs:5000});expect(trace(oracle,'StopRecord')).toHaveLength(1);}
 finally{oracle.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'SetSourceFilterSettings',requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);await pending;await oracle.client.callTool({name:'obs-take-stop',arguments:{}});expect(oracle.recording()).toBe(false);}
});
it('bounds queued opaque argument bytes before intent and cancels retained commands without sending them',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('BroadcastCustomEvent',{},60_000);const before=native.fake.cursor();
 const active=native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{active:true}}}).then(value=>({value}),error=>({error:String(error)}));const request=await native.fake.waitForRequest('BroadcastCustomEvent',{after:before,timeoutMs:10_000});
 const controllers=[new AbortController(),new AbortController()];const payload='x'.repeat(7*1024*1024);
 const cancellationSends:Promise<void>[]=[];
 const send=native.transport.send.bind(native.transport);
 native.transport.send=(message,...options)=>{
  const sent=send(message,...options);
  if('method' in message&&message.method==='notifications/cancelled')cancellationSends.push(sent);
  return sent;
 };
 const queued=controllers.map(controller=>native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{payload}}},{signal:controller.signal}).then(value=>({value}),error=>({error:String(error)})));
 try{
  const excess=await native.client.callTool({name:'obs-broadcast-custom-event',arguments:{eventData:{payload}}});expect(excess.isError).toBe(true);expect(JSON.stringify(excess)).toContain('Capacity');
  expect(trace(native,'BroadcastCustomEvent')).toHaveLength(1);expect(receipts(native)).toHaveLength(1);expect(receipts(native)[0]).toMatchObject({phase:'dispatching'});
 }finally{
  try{
   for(const controller of controllers)controller.abort();
   expect(cancellationSends).toHaveLength(2);
   await Promise.all(cancellationSends);
   // Abort settles the client promise, not the server's cancellation. This
   // ordered round trip fences both notifications before the independent OBS ACK.
   await native.client.listTools();
   expect(trace(native,'BroadcastCustomEvent')).toHaveLength(1);
   expect(receipts(native)).toHaveLength(1);
  }finally{
   native.transport.send=send;
   native.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'BroadcastCustomEvent',requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);
   await active;await Promise.all(queued);
  }
 }
 await vi.waitFor(()=>expect(receipts(native)[0]).toMatchObject({phase:'accepted',postconditionObserved:false}));expect(trace(native,'BroadcastCustomEvent')).toHaveLength(1);expect(receipts(native)).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);
});
it('exposes the planned possible native inventory with complete Source contracts and approved adapters',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);const expected=(await oracle.client.listTools()).tools;const actual=(await native.client.listTools()).tools;expectNativeInventory(actual,expected);expect(native.fake.connectionCount).toBe(0);expect(children(native)).toEqual([]);
 await expect(native.client.callTool({name:'obs-batch',arguments:{requests:[]}})).rejects.toMatchObject({code:-32601});expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);expect(children(native)).toEqual([]);
});

it('uses priority status as fresh evidence without stopping a replacement human recording',async()=>{
 const oracle=await start(false,false,true);const native=await start(true,false,true);
 for(const session of[oracle,native])expect((await session.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:30}})).isError).not.toBe(true);
 const held:HeldMutation[]=[];const stops:Promise<unknown>[]=[];
 try{
  for(const session of[oracle,native])held.push(await holdOwnedMutation(session));
  for(const session of[oracle,native]){session.externalRecording(false,join(session.recordingRoot,'clip.mkv'));session.externalRecording(true,join(session.recordingRoot,'human-replacement.mkv'));}
  for(const session of[oracle,native])stops.push(session.client.callTool({name:'obs-take-stop',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));
  for(const{session,request}of held){await session.fake.waitForRequest('GetRecordStatus',{after:request.cursor,timeoutMs:4000});expect(trace(session,'StopRecord')).toHaveLength(0);expect(session.recording()).toBe(true);}
 }finally{
  for(const{session,request}of held)session.fake.sendRaw({op:7,d:{requestId:request.frame.d.requestId,requestType:'SetPersistentData',requestStatus:{result:true,code:100},responseData:{}}},request.connectionId);
  await Promise.all(held.map(item=>item.pending));
  const results=await Promise.all(stops);
  try{for(const result of results)expect(result).toMatchObject({value:{isError:true,structuredContent:{stopConfirmed:false,ownership:'unavailable'}}});for(const session of[oracle,native]){expect(trace(session,'StopRecord')).toHaveLength(0);expect(session.recording(),'replacement must remain active until explicit external fixture reset').toBe(true);}}
  finally{for(const session of[oracle,native])session.externalRecording(false,join(session.recordingRoot,'human-replacement.mkv'));}
 }
});
