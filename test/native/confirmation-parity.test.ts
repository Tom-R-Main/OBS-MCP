/* SPDX-License-Identifier: GPL-2.0-only */
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { mkdirSync, rmSync, writeFileSync, readdirSync, readFileSync, appendFileSync } from 'node:fs';
import {spawn,execFileSync,type ChildProcessWithoutNullStreams} from 'node:child_process';
import {once} from 'node:events';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, expect, it, vi } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import {serveProduction} from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));
const review=process.env.CONFIRMATION_WIRE_EVIDENCE_DIR;
type Row=Record<string,any>;
type Session={fake:FakeOBSServer;directory:string;client:Client;transport:StdioClientTransport;wire:Row[]};
const transcript:Row[]=[];const sessions:Session[]=[];let sequence=0;
async function start(legacy:boolean,answer:Row|((request:Row,context:Row)=>Promise<Row>)|undefined,confirmLive=true,challenge?:string,native=false):Promise<Session>{
 const fake=await FakeOBSServer.start();fake.respondWith('StopRecord',()=>({outputPath:'/fixture/clip.mp4'}));fake.respondWith('StopStream',()=>({}));
 const directory=join(root,'native/target',`confirmation-wire-${process.pid}-${sequence++}`);mkdirSync(directory,{recursive:true,mode:0o700});
 const wire:Row[]=[];const client=new Client({name:'confirmation-wire',version:'1'},{capabilities:answer?{elicitation:{form:{}}}:{},versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
 if(answer)client.setRequestHandler('elicitation/create',async (request,context)=>{wire.push({direction:'client-handler',message:structuredClone(request)});return (typeof answer==='function'?await answer(request as Row,context as Row):answer)as never;});
 const transport=new StdioClientTransport({command:native?(process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native')):process.execPath,args:native?[]:[join(root,'build/index.js')],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_CONFIRM_LIVE:confirmLive?'true':'false',OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_TOOLSETS:'all',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});transport.stderr?.on('data',()=>undefined);
 const send=transport.send.bind(transport);transport.send=async(...args)=>{wire.push({direction:'client-to-server',message:structuredClone(args[0])});return send(...args);};
 const session={fake,directory,client,transport,wire};sessions.push(session);await client.connect(transport);
 const incoming=transport.onmessage;transport.onmessage=message=>{wire.push({direction:'server-to-client',message:structuredClone(message)});if(challenge&&(message as Row).result?.resultType==='input_required'){(message as Row).result.requestState=challenge;wire.push({direction:'fixture-added-state',message:structuredClone(message)});}incoming?.(message);};
 return session;
}
afterEach(async()=>{for(const s of sessions.splice(0)){await s.client.close();await s.transport.close();await s.fake.close();rmSync(s.directory,{recursive:true,force:true});}if(review)writeFileSync(join(review,`source-stdio-wire-${process.pid}.json`),JSON.stringify(transcript,null,2)+'\n');});
for(const legacy of[false,true])for(const mode of['accept','false','decline','cancel','fallback-missing','fallback-true','model-true'])it(`captures actual Source SDK2.4 confirmation wire (${legacy?'legacy':'modern'},${mode})`,async()=>{
 const answer=mode.startsWith('fallback')?undefined:mode==='accept'||mode==='model-true'?{action:'accept',content:{confirm:true}}:mode==='false'?{action:'accept',content:{confirm:false}}:{action:mode};
 const s=await start(legacy,answer);const tool=(await s.client.listTools()).tools.find(t=>t.name==='obs-stop-record');const args=mode==='fallback-true'||mode==='model-true'?{confirm:true}:{};
 const result=await s.client.callTool({name:'obs-stop-record',arguments:args});const sent=s.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord');
 expect(sent.length).toBe(['accept','fallback-true','model-true'].includes(mode)?1:0);expect(tool?.inputSchema.properties).toHaveProperty('confirm');
 transcript.push({legacy,mode,tool,result,effects:sent.map(row=>row.frame),wire:s.wire});
});

it('actual modern SDK automatically echoes an opaque requestState with original metadata and args',async()=>{
 const state='native-confirmation/fixture-opaque/\\"42';const s=await start(false,{action:'accept',content:{confirm:true}},true,state);
 const result=await s.client.callTool({name:'obs-stop-record',arguments:{confirm:true}});expect(result.isError).not.toBe(true);
 const calls=s.wire.filter(r=>r.direction==='client-to-server'&&r.message.method==='tools/call').map(r=>r.message);
 expect(calls).toHaveLength(2);expect(calls[1].id).not.toEqual(calls[0].id);expect(calls[1].params.requestState).toBe(state);expect(calls[1].params._meta).toEqual(calls[0].params._meta);expect(calls[1].params.arguments).toEqual(calls[0].params.arguments);expect(calls[1].params.inputResponses).toEqual({confirm:{action:'accept',content:{confirm:true}}});
 transcript.push({case:'explicit-fixture-injected-requestState-client-positive',state,result,wire:s.wire});
});
for(const legacy of[false,true])it(`Source deduplicates repeated batch questions (${legacy?'legacy':'modern'})`,async()=>{
 const s=await start(legacy,{action:'decline'});const result=await s.client.callTool({name:'obs-batch',arguments:{requests:[{requestType:'StopStream'},{requestType:'StopStream'},{requestType:'StopRecord'}]}});
 expect(result.content).toEqual([{type:'text',text:'Cancelled; nothing was changed'}]);const questions=s.wire.filter(r=>r.direction==='client-handler');expect(questions).toHaveLength(1);expect(questions[0]!.message.params.message).toBe('Stop the live stream? Stop the recording?');expect(s.fake.history().filter(row=>row.frame.op===8)).toEqual([]);transcript.push({legacy,case:'deduplicated-batch-question',result,wire:s.wire});
});

for(const legacy of[false,true])it(`native inventory exposes actual enabled Source confirmation schemas (${legacy?'legacy':'modern'})`,async()=>{
 const source=await start(legacy,{action:'decline'}),native=await start(legacy,{action:'decline'},true,undefined,true);const expected=(await source.client.listTools()).tools;const actual=(await native.client.listTools()).tools;
 for(const tool of actual)expect(tool).toEqual(expected.find(row=>row.name===tool.name));expect(actual.find(t=>t.name==='obs-stop-record')?.inputSchema.properties).toHaveProperty('confirm');expect(native.fake.connectionCount).toBe(0);
});
for(const legacy of[false,true])for(const mode of['accept','false','decline','cancel','fallback-missing','fallback-true','model-true'])it(`native matches actual Source confirmation decision (${legacy?'legacy':'modern'},${mode})`,async()=>{
 const answer=mode.startsWith('fallback')?undefined:mode==='accept'||mode==='model-true'?{action:'accept',content:{confirm:true}}:mode==='false'?{action:'accept',content:{confirm:false}}:{action:mode};
 const source=await start(legacy,answer),native=await start(legacy,answer,true,undefined,true);const args=mode==='fallback-true'||mode==='model-true'?{confirm:true}:{};
 const a=await source.client.callTool({name:'obs-stop-record',arguments:args}),b=await native.client.callTool({name:'obs-stop-record',arguments:args});expect(b).toEqual(a);
 const sent=(s:typeof native)=>s.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord');expect(sent(native).length).toBe(sent(source).length);
});
for(const legacy of[false,true])it(`native preserves default-false schemas and unknown confirm stripping (${legacy?'legacy':'modern'})`,async()=>{
 const source=await start(legacy,undefined,false),native=await start(legacy,undefined,false,undefined,true);const expected=(await source.client.listTools()).tools;const actual=(await native.client.listTools()).tools;for(const tool of actual)expect(tool).toEqual(expected.find(row=>row.name===tool.name));expect(actual.find(t=>t.name==='obs-stop-record')?.inputSchema.properties).not.toHaveProperty('confirm');
 const args={confirm:'ignored when feature off'};expect(await native.client.callTool({name:'obs-stop-record',arguments:args})).toEqual(await source.client.callTool({name:'obs-stop-record',arguments:args}));
});
for(const legacy of[false,true])it(`native invalid confirm diagnostics match Source before any OBS (${legacy?'legacy':'modern'})`,async()=>{
 const source=await start(legacy,undefined),native=await start(legacy,undefined,true,undefined,true);for(const args of[{confirm:null},{confirm:1},{confirm:'true'}])expect(await native.client.callTool({name:'obs-stop-record',arguments:args})).toEqual(await source.client.callTool({name:'obs-stop-record',arguments:args}));for(const args of[{stageId:3,confirm:'true'},{stageId:'bad',confirm:1}])expect(await native.client.callTool({name:'obs-take-scene',arguments:args})).toEqual(await source.client.callTool({name:'obs-take-scene',arguments:args}));expect(native.fake.connectionCount).toBe(0);
});
type RawSession={fake:FakeOBSServer;directory:string;child:ChildProcessWithoutNullStreams;closed:Promise<unknown[]>;wire:Row[];send:(value:Row)=>Promise<void>;response:(id:number)=>Promise<Row>;call:(id:number,name?:string,args?:Row,caller?:string,extra?:Row)=>Row;fence:(id?:number)=>Promise<void>;joinChild:()=>Promise<void>;release:()=>void};
const rawSessions:RawSession[]=[];
function metadata(name='original'){return{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name,version:'1'},'io.modelcontextprotocol/clientCapabilities':{elicitation:{form:{}}}};}
function receiptRows(directory:string):Row[]{try{return readdirSync(join(directory,'operation-receipts')).filter(n=>n.endsWith('.json')).map(n=>JSON.parse(readFileSync(join(directory,'operation-receipts',n),'utf8'))as Row);}catch{return[];}}
async function rawNative(legacy=false,native=true,offers:Row={elicitation:{form:{}}},dynamic=false):Promise<RawSession>{
 const fake=await FakeOBSServer.start();serveProduction(fake);fake.respondWith('StopRecord',()=>({outputPath:'/fixture/clip.mp4'}));fake.respondWith('StartStream',()=>({}));fake.respondWith('GetStreamStatus',()=>({outputActive:false}));
 const directory=join(root,'native/target',`confirmation-raw-${process.pid}-${sequence++}`);mkdirSync(directory,{recursive:true,mode:0o700});const child=spawn(native?(process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native')):process.execPath,native?[]:[join(root,'build/index.js')],{cwd:root,stdio:['pipe','pipe','pipe'],env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_CONFIRM_LIVE:'true',OBS_MCP_TOOLSETS:'all',OBS_MCP_DYNAMIC_TOOLSETS:dynamic?'true':'false',OBS_MCP_COMPAT_NODE:join(directory,'missing-node'),OBS_MCP_COMPAT_ENTRY:join(directory,'missing-sdk.js'),OBS_MCP_READ_OBS_CONFIG:'false'}});const closed=once(child,'close');void closed.catch(()=>undefined);const wire:Row[]=[],replies=new Map<number,Row>();let buffer='',parseError:unknown;
 child.stdin.on('error',()=>undefined);child.stderr.on('data',()=>undefined);child.stdout.setEncoding('utf8');child.stdout.on('data',(chunk:string)=>{buffer+=chunk;if(buffer.length>2*1024*1024){parseError='bounded capture exceeded';return;}let at:number;while((at=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);try{const value=JSON.parse(line)as Row;wire.push(value);if(value.method===undefined&&typeof value.id==='number')replies.set(value.id,value);}catch(error){parseError=error;}}});
 const send=(value:Row)=>new Promise<void>((resolve,reject)=>child.stdin.write(JSON.stringify(value)+'\n',error=>error?reject(error):resolve()));const response=async(id:number)=>{await vi.waitFor(()=>{expect(parseError).toBeUndefined();expect(replies.has(id),`confirmation id${id} exit${child.exitCode}`).toBe(true);},{timeout:10_000});return replies.get(id)!;};
 const call=(id:number,name='obs-stop-record',args:Row={},caller='original',extra:Row={}):Row=>({jsonrpc:'2.0',id,method:'tools/call',params:{name,arguments:args,...extra,...(legacy?{}:{_meta:{...metadata(caller),'io.modelcontextprotocol/clientCapabilities':offers}})}});
 const fence=async(id=9901)=>{await send({jsonrpc:'2.0',id,method:'tools/list',params:legacy?{}:{_meta:metadata()}});await response(id);};
 const joinChild=async()=>{try{await vi.waitFor(()=>expect(child.exitCode!==null||child.signalCode!==null).toBe(true),{timeout:10_000});}catch(error){child.kill('SIGKILL');await closed;throw error;}expect(await closed).toEqual([0,null]);expect(parseError).toBeUndefined();};
 const s:RawSession={fake,directory,child,closed,wire,send,response,call,fence,joinChild,release:()=>undefined};rawSessions.push(s);
 if(legacy){await send({jsonrpc:'2.0',id:9000,method:'initialize',params:{protocolVersion:'2025-11-25',clientInfo:{name:'original',version:'1'},capabilities:offers}});expect((await response(9000)).error).toBeUndefined();await send({jsonrpc:'2.0',method:'notifications/initialized'});}
 return s;
}
afterEach(async()=>{for(const s of rawSessions.splice(0)){s.release();s.child.stdin.end();try{await s.joinChild();expect(s.fake.openConnectionCount).toBe(0);}finally{await s.fake.close();rmSync(s.directory,{recursive:true,force:true});}}});
function mutations(s:Awaited<ReturnType<typeof rawNative>>){return s.fake.history().filter(row=>row.frame.op===6&&['StopRecord','StartStream','TriggerStudioModeTransition'].includes(String(row.frame.d.requestType)));}
async function question(s:Awaited<ReturnType<typeof rawNative>>,id:number){await vi.waitFor(()=>expect(s.wire.some(v=>v.method==='elicitation/create'&&v.id===id)).toBe(true),{timeout:10_000});return s.wire.find(v=>v.method==='elicitation/create'&&v.id===id)!;}
it('native modern opaque state rejects forged, changed-caller/args/tool/meta and consumed replay without losing victim',async()=>{
 const s=await rawNative();await s.send(s.call(100));const first=await s.response(100);expect(first.result.resultType).toBe('input_required');const state=first.result.requestState;expect(typeof state).toBe('string');expect(receiptRows(s.directory)).toEqual([]);expect(s.fake.connectionCount).toBe(0);
 const response={inputResponses:{confirm:{action:'accept',content:{confirm:true}}},requestState:state};
 for(const[id,name,args,caller,extra]of[[101,'obs-stop-record',{},'original',{...response,requestState:'forged'}],[102,'obs-stop-record',{},'intruder',response],[103,'obs-stop-record',{confirm:true},'original',response],[104,'obs-stop-stream',{},'original',response],[105,'obs-stop-record',{},'original',{inputResponses:response.inputResponses}]]as const){await s.send(s.call(id,name,args,caller,extra));expect((await s.response(id)).error?.code).toBe(-32602);}
 const changed=s.call(106,'obs-stop-record',{},'original',response);changed.params._meta=metadata('original');changed.params._meta['io.modelcontextprotocol/clientCapabilities']={};await s.send(changed);expect((await s.response(106)).error?.code).toBe(-32602);
 expect(mutations(s)).toEqual([]);await s.send(s.call(110,'obs-stop-record',{},'original',response));expect((await s.response(110)).result.isError).not.toBe(true);expect(mutations(s)).toHaveLength(1);
 await s.send(s.call(111,'obs-stop-record',{},'original',response));expect((await s.response(111)).error?.code).toBe(-32602);await s.fence();expect(mutations(s)).toHaveLength(1);
});
it('native legacy server-request ids are directional and late/unknown/replayed responses cannot grant effects',async()=>{
 const s=await rawNative(true);await s.send(s.call(0));await question(s,0);expect(s.fake.connectionCount).toBe(0);expect(receiptRows(s.directory)).toEqual([]);
 await s.send({jsonrpc:'2.0',id:1,result:{action:'accept',content:{confirm:true}}});await s.fence();expect(mutations(s)).toEqual([]);
 await s.send({jsonrpc:'2.0',id:0,result:{action:'accept',content:{confirm:true}}});expect((await s.response(0)).result.isError).not.toBe(true);expect(mutations(s)).toHaveLength(1);
 await s.send({jsonrpc:'2.0',id:0,result:{action:'accept',content:{confirm:true}}});await s.fence(9902);expect(mutations(s)).toHaveLength(1);
});
for(const legacy of[false,true])it(`native original request cancellation invalidates only inert consent, then EOF joins (${legacy?'legacy':'modern'})`,async()=>{
 const s=await rawNative(legacy);await s.send(s.call(100));const challenge=legacy?await question(s,0):(await s.response(100)).result;expect(s.fake.connectionCount).toBe(0);expect(receiptRows(s.directory)).toEqual([]);
 await s.send({jsonrpc:'2.0',method:'notifications/cancelled',params:{requestId:100}});await s.fence();
 if(legacy){expect(s.wire).toContainEqual({jsonrpc:'2.0',method:'notifications/cancelled',params:{requestId:0,reason:'Confirmation cancelled'}});await s.send({jsonrpc:'2.0',id:0,result:{action:'accept',content:{confirm:true}}});}else{await s.send(s.call(101,'obs-stop-record',{},'original',{requestState:challenge.requestState,inputResponses:{confirm:{action:'accept',content:{confirm:true}}}}));expect((await s.response(101)).error?.code).toBe(-32602);}
 await s.fence(9902);expect(mutations(s)).toEqual([]);expect(receiptRows(s.directory)).toEqual([]);s.child.stdin.end();await s.joinChild();
});
for(const legacy of[false,true])it(`native pending confirmation EOF retains no effect owner or SDK child (${legacy?'legacy':'modern'})`,async()=>{
 const s=await rawNative(legacy);await s.send(s.call(100));if(legacy)await question(s,0);else await s.response(100);expect(s.fake.connectionCount).toBe(0);expect(receiptRows(s.directory)).toEqual([]);
 const children=execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(row=>row.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===s.child.pid);expect(children).toEqual([]);s.child.stdin.end();await s.joinChild();expect(mutations(s)).toEqual([]);
});
it('native bounded continuation count is independent of effect admission and cancellation returns capacity',async()=>{
 const s=await rawNative();const tokens:string[]=[];for(let id=100;id<116;id++){await s.send(s.call(id));tokens.push((await s.response(id)).result.requestState);}expect(new Set(tokens).size).toBe(16);await s.send(s.call(116));expect((await s.response(116)).error?.code).toBe(-32000);expect(receiptRows(s.directory)).toEqual([]);expect(s.fake.connectionCount).toBe(0);
 await s.send({jsonrpc:'2.0',method:'notifications/cancelled',params:{requestId:100}});await s.send(s.call(117));expect((await s.response(117)).result.resultType).toBe('input_required');await s.send(s.call(118,'obs-stop-record',{},'original',{requestState:tokens[0],inputResponses:{confirm:{action:'accept',content:{confirm:true}}}}));expect((await s.response(118)).error?.code).toBe(-32602);expect(mutations(s)).toEqual([]);
});
it('native pending-call logical byte bound refuses before OBS or receipt allocation',async()=>{
 const s=await rawNative();await s.send(s.call(100,'obs-stop-record',{},'x'.repeat(2*1024*1024+64)));expect((await s.response(100)).error?.code).toBe(-32000);expect(s.fake.connectionCount).toBe(0);expect(receiptRows(s.directory)).toEqual([]);
});
it('native accepted continuation consumes consent before fresh foreign lease refusal, without OBS lookup',async()=>{
 const s=await rawNative();const args={stageId:'12345678-1234-4234-8234-123456789abc'};await s.send(s.call(100,'obs-take-scene',args));const state=(await s.response(100)).result.requestState;
 await s.send(s.call(101,'obs-claim-control',{minutes:1},'other'));expect((await s.response(101)).result.structuredContent.holder).toBe('other');
 const resume={requestState:state,inputResponses:{confirm:{action:'accept',content:{confirm:true}}}};await s.send(s.call(102,'obs-take-scene',args,'original',resume));expect((await s.response(102)).result).toMatchObject({isError:true,content:[{type:'text',text:expect.stringContaining('other has control')}]});
 await s.send(s.call(103,'obs-release-control',{},'other'));await s.response(103);await s.send(s.call(104,'obs-take-scene',args,'original',resume));expect((await s.response(104)).error?.code).toBe(-32602);expect(s.fake.connectionCount).toBe(0);expect(receiptRows(s.directory)).toEqual([]);
});

for(const legacy of[false,true])it(`captures actual Source original request abort while awaiting user (${legacy?'legacy':'modern'})`,async()=>{
 let ready!:()=>void,release!:(answer:Row)=>void;const waiting=new Promise<void>(resolve=>{ready=resolve;});const answer=new Promise<Row>(resolve=>{release=resolve;});
 const s=await start(legacy,async()=>{ready();return answer;});const abort=new AbortController();const job=s.client.callTool({name:'obs-stop-record',arguments:{}},{signal:abort.signal});void job.catch(()=>undefined);
 try{await waiting;expect(s.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toEqual([]);abort.abort();release({action:'accept',content:{confirm:true}});await expect(job).rejects.toThrow();await s.client.request({method:'tools/list',params:{}});expect(s.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toEqual([]);transcript.push({legacy,case:'original-request-abort-while-human-pending',wire:s.wire});}
 finally{release({action:'cancel'});await job.catch(()=>undefined);}
});
it('native physical send rechecks the original caller after accepted resume queues behind a held read',async()=>{
 const s=await rawNative();const stats={cpuUsage:1,memoryUsage:1000,availableDiskSpace:50000,activeFps:30,averageFrameRenderTime:1,renderSkippedFrames:0,renderTotalFrames:1,outputSkippedFrames:0,outputTotalFrames:1};s.fake.respondWith('GetStats',()=>stats);s.fake.queueSuccess('GetStats',stats,60_000);await s.send(s.call(80,'obs-get-stats'));const held=await Promise.race([s.fake.waitForRequest('GetStats',{timeoutMs:10_000}),s.response(80).then(value=>{throw Error(`Read settled before hold: ${JSON.stringify(value)}`);})]);let released=false;s.release=()=>{if(!released&&s.fake.openConnectionCount){s.fake.sendRaw({op:7,d:{requestId:held.frame.d.requestId,requestType:'GetStats',requestStatus:{result:true,code:100},responseData:stats}},held.connectionId);released=true;}};
 try{await s.send(s.call(81,'obs-start-stream'));const state=(await s.response(81)).result.requestState;expect(receiptRows(s.directory)).toEqual([]);expect(mutations(s)).toEqual([]);await s.send(s.call(82,'obs-start-stream',{},'original',{requestState:state,inputResponses:{confirm:{action:'accept',content:{confirm:true}}}}));await vi.waitFor(()=>expect(receiptRows(s.directory)[0]).toMatchObject({phase:'dispatching'}),{timeout:10_000});expect(mutations(s)).toEqual([]);await s.send(s.call(83,'obs-claim-control',{minutes:1},'other'));expect((await s.response(83)).result.structuredContent.holder).toBe('other');s.release();await s.response(80);expect((await s.response(82)).result).toMatchObject({isError:true,content:[{type:'text',text:expect.stringContaining('other has control')}]});expect(mutations(s)).toEqual([]);expect(receiptRows(s.directory)[0]).toMatchObject({phase:'not-dispatched',requestDispatched:false});expect(s.fake.connectionCount).toBe(1);}
 finally{s.release();}
});
it('native accepted resume is consumed before fresh ordinary-work capacity refusal',async()=>{
 const s=await rawNative();const args={stageId:'12345678-1234-4234-8234-123456789abc'};await s.send(s.call(100,'obs-take-scene',args));const state=(await s.response(100)).result.requestState;
 const stats={cpuUsage:1,memoryUsage:1000,availableDiskSpace:50000,activeFps:30,averageFrameRenderTime:1,renderSkippedFrames:0,renderTotalFrames:1,outputSkippedFrames:0,outputTotalFrames:1};s.fake.respondWith('GetStats',()=>stats);s.fake.queueSuccess('GetStats',stats,60_000);for(let id=300;id<332;id++)await s.send(s.call(id,'obs-get-stats'));const held=await s.fake.waitForRequest('GetStats',{timeoutMs:10_000});let released=false;s.release=()=>{if(!released&&s.fake.openConnectionCount){s.fake.sendRaw({op:7,d:{requestId:held.frame.d.requestId,requestType:'GetStats',requestStatus:{result:true,code:100},responseData:stats}},held.connectionId);released=true;}};
 try{await s.fence();const resume={requestState:state,inputResponses:{confirm:{action:'accept',content:{confirm:true}}}};await s.send(s.call(110,'obs-take-scene',args,'original',resume));expect((await s.response(110)).error?.code).toBe(-32000);s.release();for(let id=300;id<332;id++)await s.response(id);await s.send(s.call(111,'obs-take-scene',args,'original',resume));expect((await s.response(111)).error?.code).toBe(-32602);expect(mutations(s)).toEqual([]);expect(receiptRows(s.directory)).toEqual([]);}finally{s.release();}
});

for(const legacy of[false,true])for(const offers of[{}, {elicitation:{}}, {elicitation:{url:{}}}])it(`captures Source form-offer gate (${legacy?'legacy':'modern'},${JSON.stringify(offers)})`,async()=>{
 const s=await rawNative(legacy,false,offers);await s.send(s.call(100));await vi.waitFor(()=>expect(s.wire.some(v=>(v.id===100&&v.method===undefined)||v.method==='elicitation/create')).toBe(true),{timeout:10_000});const request=s.wire.find(v=>v.method==='elicitation/create');if(request)await s.send({jsonrpc:'2.0',id:request.id,result:{action:'decline'}});const result=await s.response(100);expect(mutations(s)).toEqual([]);transcript.push({legacy,case:'form-offer-gate',offers,request,result,wire:s.wire});
});
async function formOfferResult(s:RawSession,legacy:boolean){await s.send(s.call(100));await vi.waitFor(()=>expect(s.wire.some(v=>(v.id===100&&v.method===undefined)||v.method==='elicitation/create')).toBe(true),{timeout:10_000});const request=s.wire.find(v=>v.method==='elicitation/create');if(request)await s.send({jsonrpc:'2.0',id:request.id,result:{action:'decline'}});const result=structuredClone(await s.response(100));if(!legacy&&result.result?.resultType==='input_required'){expect(typeof result.result.requestState==='string'||result.result.requestState===undefined).toBe(true);delete result.result.requestState;}return{result,request};}
for(const legacy of[false,true])for(const offers of[{}, {elicitation:{}}, {elicitation:{url:{}}}])it(`native matches actual Source form capability gate (${legacy?'legacy':'modern'},${JSON.stringify(offers)})`,async()=>{
 const source=await rawNative(legacy,false,offers),native=await rawNative(legacy,true,offers);const expected=await formOfferResult(source,legacy),actual=await formOfferResult(native,legacy);expect(actual).toEqual(expected);expect(mutations(native)).toEqual([]);expect(receiptRows(native.directory)).toEqual([]);expect(native.fake.connectionCount).toBe(0);
});



for(const legacy of [false,true])it(`native consent is consumed before dynamic visibility refusal and cannot revive after re-enable (legacy=${legacy})`,async()=>{
 const rows:Row[]=[];
 for(const native of[false,true]){
  const s=await rawNative(legacy,native,{elicitation:{form:{}}},true);
  await s.send(s.call(10,'obs-enable-toolset',{groups:['record']}));expect((await s.response(10)).error).toBeUndefined();
  await s.send(s.call(100));
  await vi.waitFor(()=>expect(s.wire.some(v=>(v.id===100&&v.method===undefined)||v.method==='elicitation/create')).toBe(true),{timeout:10_000});
  const question=s.wire.find(v=>v.method==='elicitation/create');
  let initial:Row|undefined,state:unknown;
  if(!question){initial=await s.response(100);state=initial.result?.requestState;if(native)expect(typeof state).toBe('string');else expect(initial.result).not.toHaveProperty('requestState');}
  await s.send(s.call(101,'obs-disable-toolset',{groups:['record']}));expect((await s.response(101)).error).toBeUndefined();
  let hidden:Row,replayed:Row|undefined,hiddenEffects:Row[]=[];
  if(question){
   await s.send({jsonrpc:'2.0',id:question.id,result:{action:'accept',content:{confirm:true}}});hidden=await s.response(100);hiddenEffects=mutations(s);
   await s.send(s.call(103,'obs-enable-toolset',{groups:['record']}));expect((await s.response(103)).error).toBeUndefined();
   await s.send({jsonrpc:'2.0',id:question.id,result:{action:'accept',content:{confirm:true}}});await s.fence();
  }else{
   const resume={...(state===undefined?{}:{requestState:state}),inputResponses:{confirm:{action:'accept',content:{confirm:true}}}};
   await s.send(s.call(102,'obs-stop-record',{},'original',resume));hidden=await s.response(102);hiddenEffects=mutations(s);
   await s.send(s.call(103,'obs-enable-toolset',{groups:['record']}));expect((await s.response(103)).error).toBeUndefined();
   await s.send(s.call(104,'obs-stop-record',{},'original',resume));replayed=await s.response(104);
  }
  rows.push({native,legacy,initial,question,hidden,replayed,hiddenEffects,effects:mutations(s),receipts:receiptRows(s.directory),connections:s.fake.connectionCount,wire:s.wire});
 }
 if(process.env.CONFIRMATION_VISIBILITY_EVIDENCE)appendFileSync(process.env.CONFIRMATION_VISIBILITY_EVIDENCE,JSON.stringify({legacy,rows})+'\n');
 const source=rows[0]!,native=rows[1]!;
 expect(source.hiddenEffects).toEqual([]);expect(native.hiddenEffects).toEqual([]);
 expect(native.effects).toEqual([]);expect(native.receipts).toEqual([]);expect(native.connections).toBe(0);
 if(legacy){expect(source.question).toBeDefined();expect(native.question).toBeDefined();expect(source.effects).toEqual([]);}
 else{
  // This pinned Source emits no opaque state: its inputResponses retry after
  // re-enable is a fresh call. Native's issued opaque grant remains consumed.
  expect(source.initial.result).not.toHaveProperty('requestState');
  expect(source.replayed.result.resultType).toBe('complete');expect(source.effects).toHaveLength(1);
  expect(native.replayed.error).toEqual({code:-32602,message:'Invalid or expired requestState'});
 }
 expect(source.hidden.error).toEqual({code:-32602,message:'Tool obs-stop-record disabled'});
 expect(native.hidden.error).toEqual(source.hidden.error);
});
