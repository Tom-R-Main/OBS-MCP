/* SPDX-License-Identifier: GPL-2.0-only */
import { spawn, execFileSync } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { once } from 'node:events';
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, rmSync, unlinkSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { StringDecoder } from 'node:string_decoder';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');const sdk=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,'build/index.js');let seq=0;
type Session={client:Client;transport:StdioClientTransport;fake:FakeOBSServer;directory:string;release:()=>void;ownedCleanup?:()=>Promise<void>};const sessions:Session[]=[];
type Options={readonly?:boolean;missingCapability?:boolean};
function env(native:boolean,fake:FakeOBSServer,directory:string,options:Options={}){return{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:options.readonly?'true':'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'};}
async function start(native:boolean,legacy:boolean,options:Options={}):Promise<Session>{
 const fake=await FakeOBSServer.start({platform:'macos',...(options.missingCapability?{autoGetVersion:false}:{})});fake.respondWith('StopRecord',()=>({outputPath:'/fixture/α.mkv'}));fake.respondWith('SetInputMute',()=>({}));if(options.missingCapability)fake.respondWith('GetVersion',()=>({obsVersion:'32.2.2',obsWebSocketVersion:'5.7.0',rpcVersion:1,platform:'macos',availableRequests:['GetVersion'],supportedImageFormats:['png']}));
 const directory=join(root,'native/target',`manual-stop-public-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const client=new Client({name:'manual-stop',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[sdk],cwd:root,env:env(native,fake,directory,options),stderr:'pipe'});transport.stderr?.on('data',()=>undefined);const session={client,transport,fake,directory,release:()=>undefined};sessions.push(session);await client.connect(transport);
 if(options.missingCapability)expect((await client.callTool({name:'obs-get-version',arguments:{}})).structuredContent).toMatchObject({availableRequests:['GetVersion']});return session;
}
function receipts(session:Pick<Session,'directory'>):Record<string,unknown>[] {try{return readdirSync(join(session.directory,'operation-receipts')).filter(n=>n.endsWith('.json')).map(n=>JSON.parse(readFileSync(join(session.directory,'operation-receipts',n),'utf8')) as Record<string,unknown>);}catch{return[];}}
function frames(session:Pick<Session,'fake'>,request:string){return session.fake.history().filter(item=>item.frame.op===6&&item.frame.d.requestType===request);}
function children(session:{transport:{pid:number|null|undefined}}){const pid=session.transport.pid;return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8',timeout:3000,maxBuffer:1024*1024}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([child])=>child);}
async function same(source:Session,native:Session,args:Record<string,unknown>={}){const expected=await source.client.callTool({name:'obs-stop-record',arguments:args});const actual=await native.client.callTool({name:'obs-stop-record',arguments:args});expect(actual).toEqual(expected);return actual;}
afterEach(async()=>{for(const session of sessions.splice(0)){if(session.ownedCleanup){await session.ownedCleanup();continue;}session.release();try{await session.client.close();await session.transport.close();await vi.waitFor(()=>expect(session.fake.openConnectionCount).toBe(0),{timeout:10_000});}finally{await session.fake.close();rmSync(session.directory,{recursive:true,force:true});}}});
for(const legacy of[false,true])describe(`manual recording Stop public parity legacy=${legacy}`,()=>{
 it('preserves complete Source metadata, success body, no-data request and distinct manual receipt without a Take',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);const expected=(await source.client.listTools()).tools,actual=(await native.client.listTools()).tools;expect(actual.find(t=>t.name==='obs-stop-record')).toEqual(expected.find(t=>t.name==='obs-stop-record'));
  expect(await same(source,native,{discarded:{value:'must not enter OBS'}})).toMatchObject({content:[{type:'text',text:'Recording stopped, saved to: /fixture/α.mkv'}],structuredContent:{message:'Recording stopped, saved to: /fixture/α.mkv'}});expect(frames(native,'StopRecord')).toHaveLength(1);expect(frames(native,'StopRecord')[0]?.frame.d).not.toHaveProperty('requestData');expect(native.fake.history().filter(row=>row.frame.op===6).map(row=>row.frame.d.requestType)).toEqual(['GetVersion','StopRecord']);expect(receipts(native)).toEqual([expect.objectContaining({phase:'accepted',requestAccepted:true,postconditionObserved:false})]);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
});

async function acknowledgedBody(session:Session,responseData:unknown){
 session.fake.queueSuccess('StopRecord',{},60_000);const after=session.fake.cursor();const job=session.client.callTool({name:'obs-stop-record',arguments:{}}).then(value=>({value}),error=>({error:String(error)}));const held=await Promise.race([session.fake.waitForRequest('StopRecord',{after,timeoutMs:10_000}),job.then(result=>{throw Error(`manual Stop did not reach wire: ${JSON.stringify(result)}`);})]);let released=false;const previous=session.release;const release=()=>{if(released||!session.fake.openConnectionCount)return;released=true;const d:Record<string,unknown>={requestType:'StopRecord',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100}};if(responseData!==undefined)d.responseData=responseData;session.fake.sendRaw({op:7,d},held.connectionId);};session.release=()=>{release();previous();};release();return job;
}
const bodies:unknown[]=[undefined,null,false,true,0,-0,1,'scalar',[],[null],{},...[
 undefined,null,false,true,0,-0,1,1.25,1e21,'','/fixture/α.mkv','line\n日本語💡',[],[null],[null,false,1],['a','b'],[['a'],['b']],{}, {toString:0},{toString:null},{toString:'own'},{valueOf:0},{toString:0,valueOf:0},[{toString:0}],[[{toString:null}]],{a:1},[{},[]],['x',null,['y',null]],{toString:[]}
].map(outputPath=>({outputPath}))];
function meta(name:string){return{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{}};}
async function raw(session:Session,id:number,name:string,args:Record<string,unknown>,caller:string){const previous=session.transport.onmessage;let answer:Record<string,unknown>|undefined;session.transport.onmessage=message=>{const frame=message as unknown as Record<string,unknown>;if(frame.id===id)answer=frame;else previous?.(message);};try{await session.transport.send({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,_meta:meta(caller)}} as Parameters<StdioClientTransport['send']>[0]);await vi.waitFor(()=>expect(answer).toBeDefined(),{timeout:10_000});return answer!;}finally{session.transport.onmessage=previous;}}
for(const legacy of[false,true])describe(`manual Stop responses and refusal legacy=${legacy}`,()=>{
 it('preserves Source projection and caught coercion errors across40 raw ACK bodies without asserting observed stop',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);expect(bodies).toHaveLength(40);
  for(const body of bodies){const expected=await acknowledgedBody(source,body),actual=await acknowledgedBody(native,body);expect(actual,`ACK body ${JSON.stringify(body)}`).toEqual(expected);expect(actual).toHaveProperty('value');expect(actual).not.toHaveProperty('error');}
  expect(frames(native,'StopRecord')).toHaveLength(bodies.length);expect(receipts(native)).toHaveLength(bodies.length);for(const receipt of receipts(native))expect(receipt).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 });
 it('refuses a known missing StopRecord capability before intent and preserves the Source error',async()=>{
  const source=await start(false,legacy,{missingCapability:true}),native=await start(true,legacy,{missingCapability:true});const result=await same(source,native);expect(result.isError).toBe(true);expect(JSON.stringify(result)).toContain("does not advertise support for request 'StopRecord'");expect(frames(native,'StopRecord')).toHaveLength(0);expect(receipts(native)).toEqual([]);
 });
 it('preserves exact Source rejection details with a Rejected receipt and no stop observation',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);for(const session of[source,native])session.fake.respondWith('StopRecord',()=>{throw new FakeOBSRequestError(500,'PRIVATE manual refusal π');});expect((await same(source,native)).isError).toBe(true);expect(receipts(native)).toEqual([expect.objectContaining({phase:'rejected',requestAccepted:false,postconditionObserved:false})]);expect(JSON.stringify(receipts(native))).not.toContain('PRIVATE manual refusal');
 });
 it('keeps a lost ACK Unknown without replay or inventing a Take',async()=>{
  const source=await start(false,legacy),native=await start(true,legacy);for(const session of[source,native])session.fake.queueDisconnect('StopRecord','PRIVATE manual reply lost π');expect((await same(source,native)).isError).toBe(true);expect(receipts(native)).toEqual([expect.objectContaining({phase:'unknown',requestAccepted:null,postconditionObserved:false})]);expect((await native.client.callTool({name:'obs-stop-record',arguments:{}})).isError).toBe(true);expect(frames(native,'StopRecord')).toHaveLength(1);expect(native.fake.connectionCount).toBe(1);
 });
 it('preserves Source read-only discovery filtering without connecting OBS',async()=>{
  const source=await start(false,legacy,{readonly:true}),native=await start(true,legacy,{readonly:true});for(const session of[source,native])expect((await session.client.listTools()).tools.map(t=>t.name)).not.toContain('obs-stop-record');expect(native.fake.connectionCount).toBe(0);expect(receipts(native)).toEqual([]);
 });
});
it('keeps Source manual Stop lease exemption ahead of missing-capability refusal for a foreign caller',async()=>{
 const source=await start(false,false,{missingCapability:true}),native=await start(true,false,{missingCapability:true});for(const session of[source,native])expect((await raw(session,8900,'obs-claim-control',{minutes:1,reason:'manual stop lease exemption'},'other-client')).result).toMatchObject({structuredContent:{holder:'other-client'}});const result=await same(source,native);expect(JSON.stringify(result)).toContain('does not advertise support');expect(JSON.stringify(result)).not.toContain('other-client has control');expect(frames(native,'StopRecord')).toHaveLength(0);expect(receipts(native)).toEqual([]);
});
it('allows a manual Stop through a foreign lease and records only the physical command ACK',async()=>{
 const source=await start(false,false),native=await start(true,false);for(const session of[source,native])expect((await raw(session,8910,'obs-claim-control',{minutes:1,reason:'manual stop lease exemption'},'other-client')).result).toMatchObject({structuredContent:{holder:'other-client'}});expect((await same(source,native)).isError).not.toBe(true);expect(frames(native,'StopRecord')).toHaveLength(1);expect(receipts(native)).toEqual([expect.objectContaining({phase:'accepted',postconditionObserved:false})]);
});

it('retains a canceled manual frontend until its same ACK and permits only a later explicit command',async()=>{
 const native=await start(true,false);native.fake.queueSuccess('StopRecord',{},60_000);const after=native.fake.cursor(),abort=new AbortController();const job=native.client.callTool({name:'obs-stop-record',arguments:{}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));const held=await native.fake.waitForRequest('StopRecord',{after,timeoutMs:10_000});let released=false;native.release=()=>{if(released||!native.fake.openConnectionCount)return;released=true;native.fake.sendRaw({op:7,d:{requestType:'StopRecord',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{outputPath:'/fixture/original.mkv'}}},held.connectionId);};
 try{abort.abort();expect(await job).toHaveProperty('error');expect(receipts(native)).toEqual([expect.objectContaining({phase:'dispatching'})]);expect((await native.client.callTool({name:'obs-control-status',arguments:{}})).isError).not.toBe(true);expect(native.fake.openConnectionCount).toBe(1);}finally{native.release();}
 await vi.waitFor(()=>expect(receipts(native)[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false}),{timeout:10_000});expect(frames(native,'StopRecord')).toHaveLength(1);expect((await native.client.callTool({name:'obs-stop-record',arguments:{}})).isError).not.toBe(true);expect(frames(native,'StopRecord')).toHaveLength(2);expect(receipts(native)).toHaveLength(2);
});
async function rawNative(legacy:boolean){
 const fake=await FakeOBSServer.start({platform:'macos'});fake.respondWith('StopRecord',()=>({outputPath:'/fixture/raw.mkv'}));fake.respondWith('SetInputMute',()=>({}));const directory=join(root,'native/target',`manual-stop-raw-${process.pid}-${seq++}`);mkdirSync(directory,{mode:0o700});const child=spawn(binary,[],{cwd:root,stdio:['pipe','pipe','pipe'],env:env(true,fake,directory)});const closed=once(child,'close');void closed.catch(()=>undefined);const replies=new Map<number,Record<string,unknown>>();const decoder=new StringDecoder('utf8');let buffer='',parseError:unknown,bytes=0;child.stderr.on('data',()=>undefined);child.stdin.on('error',()=>undefined);child.stdout.on('data',(chunk:Buffer)=>{bytes+=chunk.length;if(bytes>1024*1024){parseError='raw response capture exceeded1MiB';return;}buffer+=decoder.write(chunk);let end:number;while((end=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,end);buffer=buffer.slice(end+1);try{const frame=JSON.parse(line) as Record<string,unknown>;if(typeof frame.id==='number')replies.set(frame.id,frame);}catch(error){parseError=error;}}});
 const send=async(...frames:unknown[])=>new Promise<void>((resolve,reject)=>child.stdin.write(frames.map(frame=>JSON.stringify(frame)+'\n').join(''),error=>error?reject(error):resolve()));
 const response=async(id:number)=>{await vi.waitFor(()=>{expect(parseError).toBeUndefined();expect(replies.has(id)).toBe(true);},{timeout:10_000});return replies.get(id)!;};
 const frame=(id:number,name:string,args:Record<string,unknown>={})=>({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,...(legacy?{}:{_meta:meta('manual-stop-raw')})}});
 const close=async()=>{child.stdin.end();try{try{await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:10_000});}catch(error){child.kill('SIGKILL');await closed;throw error;}expect(await closed).toEqual([0,null]);expect(parseError).toBeUndefined();expect(buffer.trim()).toBe('');await vi.waitFor(()=>expect(fake.openConnectionCount).toBe(0),{timeout:10_000});}finally{await fake.close();rmSync(directory,{recursive:true,force:true});}};
 try{if(legacy){await send({jsonrpc:'2.0',id:1,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'manual-stop-raw',version:'1'},capabilities:{}}});expect((await response(1)).error).toBeUndefined();await send({jsonrpc:'2.0',method:'notifications/initialized'});}return{fake,directory,child,closed,replies,send,response,frame,close};}catch(error){await close();throw error;}
}
for(const legacy of[false,true])it(`retains exactly one manual Stop through raw frontend EOF (legacy=${legacy})`,async()=>{
 const raw=await rawNative(legacy);raw.fake.queueSuccess('StopRecord',{},60_000);let held:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>|undefined,released=false;const release=()=>{if(released||!held||!raw.fake.openConnectionCount)return;released=true;raw.fake.sendRaw({op:7,d:{requestType:'StopRecord',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{outputPath:'/fixture/raw.mkv'}}},held.connectionId);};
 try{await raw.send(raw.frame(7000,'obs-stop-record'));held=await raw.fake.waitForRequest('StopRecord',{timeoutMs:10_000});expect(receipts(raw)).toEqual([expect.objectContaining({phase:'dispatching'})]);expect(children({transport:{pid:raw.child.pid}})).toEqual([]);raw.child.stdin.end();await new Promise(resolve=>setTimeout(resolve,50));expect(raw.child.exitCode).toBeNull();expect(raw.fake.openConnectionCount).toBe(1);release();await vi.waitFor(()=>expect(raw.child.exitCode!==null||raw.child.signalCode!==null).toBe(true),{timeout:10_000});expect(await raw.closed).toEqual([0,null]);expect(receipts(raw)).toEqual([expect.objectContaining({phase:'accepted',requestAccepted:true,postconditionObserved:false})]);expect(frames(raw,'StopRecord')).toHaveLength(1);expect(raw.fake.openConnectionCount).toBe(0);
 }finally{release();await raw.close();}
});
for(const legacy of[false,true])it(`admits manual Stop in the existing shared safety slot with32 ordinary requests pending (legacy=${legacy})`,async()=>{
 const raw=await rawNative(legacy);raw.fake.queueSuccess('StopRecord',{},60_000);let held:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>|undefined,released=false;const release=()=>{if(released||!held||!raw.fake.openConnectionCount)return;released=true;raw.fake.sendRaw({op:7,d:{requestType:'StopRecord',requestId:held.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{outputPath:'/fixture/raw.mkv'}}},held.connectionId);};
 try{const timers=Array.from({length:32},(_,n)=>100+n);await raw.send(...timers.map(id=>raw.frame(id,'obs-sleep',{sleepMillis:30_000})),{jsonrpc:'2.0',id:500,method:'tools/list',params:legacy?{}:{_meta:meta('manual-stop-raw')}});await raw.response(500);expect(timers.some(id=>raw.replies.has(id))).toBe(false);await raw.send(raw.frame(7001,'obs-stop-record'));held=await raw.fake.waitForRequest('StopRecord',{timeoutMs:10_000});expect(timers.some(id=>raw.replies.has(id))).toBe(false);await raw.send(raw.frame(7002,'obs-stop-stream'));expect(await raw.response(7002)).toMatchObject({error:{code:-32000}});expect(frames(raw,'StopRecord')).toHaveLength(1);expect(frames(raw,'StopStream')).toHaveLength(0);release();expect(await raw.response(7001)).toMatchObject({result:{structuredContent:{message:'Recording stopped, saved to: /fixture/raw.mkv'}}});expect(receipts(raw)).toEqual([expect.objectContaining({phase:'accepted',postconditionObserved:false})]);
 }finally{release();await raw.close();}
});
for(const legacy of[false,true])it(`manual Stop reaches OBS while unrelated mutation ACK remains held (legacy=${legacy})`,async()=>{
 const source=await start(false,legacy),native=await start(true,legacy);const held:{session:Session;wire:Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;job:Promise<unknown>}[]=[];const stops:Promise<unknown>[]=[];
 try{for(const session of[source,native]){session.fake.respondWith('SetInputMute',()=>({}));session.fake.queueSuccess('SetInputMute',{},60_000);const after=session.fake.cursor();const job=session.client.callTool({name:'obs-set-input-mute',arguments:{inputName:'Fixture',inputMuted:true}}).then(value=>({value}),error=>({error:String(error)}));held.push({session,wire:await session.fake.waitForRequest('SetInputMute',{after,timeoutMs:10_000}),job});}for(const session of[source,native])stops.push(session.client.callTool({name:'obs-stop-record',arguments:{}}).then(value=>({value}),error=>({error:String(error)})));for(const {session,wire}of held)await session.fake.waitForRequest('StopRecord',{after:wire.cursor,timeoutMs:3000});const results=await Promise.all(stops);expect(results[1]).toEqual(results[0]);expect(results[1]).toMatchObject({value:{structuredContent:{message:'Recording stopped, saved to: /fixture/α.mkv'}}});expect(receipts(native).find(r=>r.requestType==='StopRecord')).toMatchObject({phase:'accepted',postconditionObserved:false});
 }finally{for(const {session,wire}of held)if(session.fake.openConnectionCount)session.fake.sendRaw({op:7,d:{requestType:'SetInputMute',requestId:wire.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{}}},wire.connectionId);await Promise.allSettled([...held.map(row=>row.job),...stops]);}
});

type OwnedTrace = (phase:string, details?:Record<string,unknown>)=>void;
function ownedTrace(legacy:boolean|'watch-control'):OwnedTrace {
 const started=performance.now();const output=process.env.NATIVE_MANUAL_STOP_DIAGNOSTICS;
 if(output)mkdirSync(output,{recursive:true});
 return(phase,details={})=>{if(output)appendFileSync(join(output,`legacy-${legacy}.jsonl`),JSON.stringify({phase,elapsedMs:Math.round((performance.now()-started)*1000)/1000,...details})+'\n');};
}
type OwnedExit={pid:number|null;code:number|null;signal:NodeJS.Signals|null;completion:'exit'|'close'|'cached-exit'|'spawn-error';spawnError?:string};
function watchOwnedTransport(transport:StdioClientTransport,trace:OwnedTrace){
 const seen=new Map<ChildProcess,Promise<OwnedExit>>();let samples=0;
 const inspect=()=>{
  const child=Reflect.get(transport,'_process') as ChildProcess|undefined;if(!child||seen.has(child))return;
  trace('native-spawn',{pid:child.pid??null});let resolve!:(exit:OwnedExit)=>void;let finished=false;const completed=new Promise<OwnedExit>(done=>{resolve=done;});seen.set(child,completed);
  const finish=(completion:OwnedExit['completion'],code:number|null,signal:NodeJS.Signals|null,spawnError?:string)=>{
   if(finished)return;finished=true;child.off('exit',onExit);child.off('close',onClose);child.off('error',onError);const exit:OwnedExit={pid:child.pid??null,code,signal,completion,...(spawnError?{spawnError}:{})};trace('native-exit',exit);resolve(exit);
  };
  const onExit=(code:number|null,signal:NodeJS.Signals|null)=>finish('exit',code,signal);
  const onClose=(code:number|null,signal:NodeJS.Signals|null)=>finish('close',code,signal);
  const onError=(error:Error)=>{if(child.pid===undefined)finish('spawn-error',child.exitCode,child.signalCode,String(error));};
  child.once('exit',onExit);child.once('close',onClose);child.once('error',onError);
  // A completed process is a cached fact, never a future event subscription.
  if(child.exitCode!==null||child.signalCode!==null)finish('cached-exit',child.exitCode,child.signalCode);
 };
 // Pinned SDK start() calls spawn synchronously before its first returned
 // Promise can settle. Hook the public method and inspect in the SAME turn,
 // before awaiting start or allowing spawn/error/exit events to be delivered.
 const originalStart=transport.start;const hookedStart=()=>{const starting=originalStart.call(transport);inspect();return starting;};transport.start=hookedStart;
 inspect();const timer=setInterval(()=>{trace('phase-heartbeat',{pid:transport.pid});if(++samples>=30)clearInterval(timer);},500);
 return{inspect,stop:()=>{clearInterval(timer);if(transport.start===hookedStart)transport.start=originalStart;},join:()=>Promise.all(seen.values())};
}
async function positiveControl(tool:string,trace:OwnedTrace){
 trace('positive-control-start');const child=spawn(tool,[],{stdio:'ignore'});let timedOut=false;let spawnError:string|undefined;let escalation:ReturnType<typeof setTimeout>|undefined;
 const closed=new Promise<{code:number|null;signal:NodeJS.Signals|null}>(resolve=>{child.once('error',error=>{spawnError=String(error);});child.once('close',(code,signal)=>resolve({code,signal}));});
 trace('positive-control-spawn',{pid:child.pid});
 const deadline=setTimeout(()=>{timedOut=true;child.kill('SIGTERM');escalation=setTimeout(()=>child.kill('SIGKILL'),250);},3000);
 const exit=await closed;clearTimeout(deadline);if(escalation)clearTimeout(escalation);
 const result={pid:child.pid,...exit,timedOut,spawnError};trace('positive-control-complete',result);return result;
}
async function startOwnedNative(legacy:boolean,trace:OwnedTrace){
 trace('fixture-start');
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png']});serveProduction(fake);
 const directory=join(root,'native/target',`manual-stop-take-${process.pid}-${seq++}`);let cleanup:(()=>Promise<void>)|undefined;
 try{
  mkdirSync(directory,{mode:0o700});const recordings=join(directory,'recordings');mkdirSync(recordings,{mode:0o700});const path=join(recordings,'same-path.mkv');writeFileSync(path,'owned fixture bytes; media is never inspected');const marker=join(directory,'media-invoked');const tool=join(directory,'media-observer.mjs');writeFileSync(tool,`#!${process.execPath}\nimport {writeFileSync} from 'node:fs';writeFileSync(${JSON.stringify(marker)},'invoked');process.exit(73);\n`,{mode:0o700});
  // Observe the actual child exit after bounded asynchronous cleanup before
  // asserting this positive control or admitting a recording operation.
  const control=await positiveControl(tool,trace);expect(control).toMatchObject({code:73,signal:null,timedOut:false,spawnError:undefined});expect(readFileSync(marker,'utf8')).toBe('invoked');unlinkSync(marker);
  let active=false;let startWireAt:number|undefined;
  fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));fake.respondWith('StartRecord',()=>{startWireAt=performance.now();trace('wire-start-record');active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return{};});fake.respondWith('StopRecord',()=>{trace('wire-stop-record');active=false;return{outputPath:path};});fake.respondWith('GetRecordDirectory',()=>({recordDirectory:recordings}));fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'}as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));
  const client=new Client({name:'manual-stop-take',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});const transport=new StdioClientTransport({command:binary,args:[],cwd:root,env:{...env(true,fake,directory),OBS_MCP_NATIVE_RECORDING_ROOT:recordings,OBS_MCP_NATIVE_FFMPEG:tool,OBS_MCP_NATIVE_FFPROBE:tool},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
  const watcher=watchOwnedTransport(transport,trace);const session:Session={client,transport,fake,directory,release:watcher.stop};let closing:Promise<void>|undefined;let cleanupMediaInvoked=false;
  cleanup=()=>closing??=(async()=>{watcher.inspect();const failures:unknown[]=[];try{await client.close();}catch(error){failures.push(error);}try{await transport.close();}catch(error){failures.push(error);}await watcher.join();watcher.stop();try{await fake.close();}catch(error){failures.push(error);}finally{cleanupMediaInvoked=existsSync(marker);rmSync(directory,{recursive:true,force:true});}trace('cleanup-complete',{failures:failures.map(String)});if(failures.length)throw new AggregateError(failures,'owned manual Stop fixture cleanup failed');})();
  session.ownedCleanup=cleanup;sessions.push(session);trace('native-connect-start');await client.connect(transport);watcher.inspect();trace('native-connect-complete',{pid:transport.pid});
  return{...session,path,marker,cleanupMediaInvoked:()=>cleanupMediaInvoked,startWireTime:()=>startWireAt,active:()=>active,replace(){trace('human-replacement-start');active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});},clear(){active=false;}};
 }catch(error){
  trace('fixture-setup-failure',{error:String(error)});
  if(cleanup)await cleanup();else{await fake.close();rmSync(directory,{recursive:true,force:true});}
  throw error;
 }
}
it('joins fast-exit and failed-start transport fixtures without missing owned completion',async()=>{
 const trace=ownedTrace('watch-control');
 for(const failedStart of[false,true]){
  const transport=new StdioClientTransport({command:failedStart?join(root,'native/target',`missing-owned-control-${process.pid}-${seq++}`):process.execPath,args:failedStart?[]:['-e','process.exit(19)'],stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
  const watcher=watchOwnedTransport(transport,trace);const closed=new Promise<void>(resolve=>{transport.onclose=resolve;});const started=transport.start().then(()=>({started:true}),error=>({started:false,error:String(error)}));
  let deadline:ReturnType<typeof setTimeout>|undefined;let joinDeadline:ReturnType<typeof setTimeout>|undefined;let closedInBudget=false;let joined:OwnedExit[]|undefined;let startResult:Awaited<typeof started>|undefined;
  try{
   closedInBudget=await Promise.race([Promise.all([started,closed]).then(()=>true),new Promise<false>(resolve=>{deadline=setTimeout(()=>resolve(false),3000);})]);
  }finally{
   if(deadline)clearTimeout(deadline);
   // Supported close is primary ownership cleanup. Only afterward challenge
   // the observation join, including the already-exited/failed-start case.
   await transport.close();await closed;startResult=await started;
   joined=await Promise.race([watcher.join(),new Promise<undefined>(resolve=>{joinDeadline=setTimeout(()=>resolve(undefined),3000);})]);
   if(joinDeadline)clearTimeout(joinDeadline);watcher.stop();
  }
  expect(closedInBudget).toBe(true);expect(startResult?.started).toBe(!failedStart);expect(joined).toHaveLength(1);
  if(failedStart){expect(startResult).toHaveProperty('error');expect(joined?.[0]).toMatchObject({pid:null,completion:'spawn-error'});}else{expect(joined?.[0]).toMatchObject({code:19,signal:null});expect(joined?.[0]?.pid).toEqual(expect.any(Number));}
 }
});
for(const legacy of[false,true])it(`public manual ACK revokes old Take before its deadline and preserves a human same-path replacement (legacy=${legacy})`,async()=>{
 const trace=ownedTrace(legacy);const native=await startOwnedNative(legacy,trace);
 const snapshot=(phase:string)=>trace(phase,{pid:native.transport.pid,active:native.active(),requests:native.fake.history().filter(row=>row.frame.op===6).map(row=>row.frame.d.requestType),receipts:receipts(native).map(row=>({kind:row.kind,requestType:row.requestType,phase:row.phase}))});
 try{
  snapshot('take-start-call');const started=await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:1}});const startReturnedAt=performance.now();snapshot('take-start-return');expect(started.isError).not.toBe(true);expect(started.structuredContent).toHaveProperty('takeId');expect(native.active()).toBe(true);
  snapshot('manual-stop-call');const stopped=await native.client.callTool({name:'obs-stop-record',arguments:{}});const ackReturnedAt=performance.now();snapshot('manual-stop-return');const startWireAt=native.startWireTime();expect(startWireAt).toBeDefined();trace('before-old-deadline-proof',{ackAfterStartWireMs:ackReturnedAt-startWireAt!,minimumRemainingMs:startWireAt!+1000-ackReturnedAt});expect(ackReturnedAt).toBeLessThan(startWireAt!+1000);expect(stopped).toMatchObject({content:[{type:'text',text:`Recording stopped, saved to: ${native.path}`}],structuredContent:{message:`Recording stopped, saved to: ${native.path}`}});expect(native.active()).toBe(false);expect(frames(native,'StopRecord')).toHaveLength(1);expect(receipts(native).find(row=>row.requestType==='StopRecord')).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false});
  // The fixture deliberately emits no STOPPED event. The ACK's impact owner
  // must revoke the old timer even when a replacement starts at the same path.
  const after=native.fake.cursor();native.replace();await new Promise(resolve=>setTimeout(resolve,1200));snapshot('replacement-crossing-wait-complete');trace('crossed-old-deadline-proof',{afterDeadlineUpperBoundMs:performance.now()-(startReturnedAt+1000)});expect(performance.now()).toBeGreaterThanOrEqual(startReturnedAt+1000);expect(native.active()).toBe(true);expect(frames(native,'StopRecord')).toHaveLength(1);
  snapshot('current-resource-call');const current=await native.client.readResource({uri:'obs://take/current'});expect(current.contents).toEqual([{uri:'obs://take/current',mimeType:'application/json',text:JSON.stringify({running:false},null,2)}]);
  const record=await native.client.callTool({name:'obs-get-record-status',arguments:{}});expect(record.structuredContent).toMatchObject({outputActive:true});const postAckReads=native.fake.history().filter(row=>row.cursor>after&&row.frame.op===6).map(row=>row.frame.d.requestType);expect(postAckReads.length).toBeGreaterThan(0);expect(postAckReads.every(request=>request==='GetRecordStatus'||request==='GetStats')).toBe(true);expect(existsSync(native.marker)).toBe(false);expect(native.fake.connectionCount).toBe(1);expect(children(native)).toEqual([]);
 }finally{
  // Remove this fixture's external replacement state directly; it was never
  // owned by a Take. Shutdown must not manufacture a replacement StopRecord.
  native.clear();snapshot('cleanup-start');
  const index=sessions.findIndex(session=>session.transport===native.transport);if(index>=0)sessions.splice(index,1);
  // Capture media evidence before teardown removes fixture files. Observe
  // shutdown wire history after the owned native PID and listener are joined.
  await native.ownedCleanup!();snapshot('cleanup-after-native-close');expect(native.fake.openConnectionCount).toBe(0);expect(frames(native,'StopRecord')).toHaveLength(1);expect(native.cleanupMediaInvoked()).toBe(false);
 }
});
