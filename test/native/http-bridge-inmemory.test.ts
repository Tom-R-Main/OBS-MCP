/* SPDX-License-Identifier: GPL-2.0-only */
// The actual SDK's fetch entry is wired to an owned Rust fixture process.
// This exercises exchange semantics, never TCP HTTP parsing or socket flush.
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdtempSync,mkdirSync,writeFileSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Client,StreamableHTTPClientTransport,type FetchLike} from '@modelcontextprotocol/client';
import {expect,it,vi} from 'vitest';
import WebSocket from 'ws';
import {z} from 'zod';
import {FakeOBSServer,type FakeOBSOptions} from '../support/fake-obs-server.js';
import {serveProduction} from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));
const binary=process.env.HTTP_BRIDGE_PROBE_BINARY;
type Row=Record<string,any>;
async function probe(confirm:boolean,options:FakeOBSOptions={}){
 if(!binary)throw Error('HTTP_BRIDGE_PROBE_BINARY must name an immutable test executable');
 const fake=await FakeOBSServer.start(options);serveProduction(fake);fake.respondWith('StopRecord',()=>({outputPath:'/fixture/clip.mkv'}));
 const evidence=process.env.HTTP_BRIDGE_EVIDENCE_DIR??join(root,'native/target');mkdirSync(evidence,{recursive:true});
 const state=mkdtempSync(join(evidence,'sdk-inmemory-'));const wire:Row[]=[];
 const child=spawn(binary,['--exact','native_service::http_bridge::tests::probe_child','--ignored','--nocapture'],{cwd:root,stdio:['pipe','pipe','pipe'],env:{...process.env,NATIVE_HTTP_BRIDGE_PROBE:'1',NATIVE_HTTP_BRIDGE_STATE:state,NATIVE_HTTP_BRIDGE_OBS:fake.url,NATIVE_HTTP_BRIDGE_CONFIRM:confirm?'1':'0'}});
 const closed=once(child,'close');void closed.catch(()=>undefined);child.stdin.on('error',()=>undefined);let buffer='',capture='',ready=false;
 child.stderr.setEncoding('utf8');child.stderr.on('data',(chunk:string)=>{capture=(capture+chunk).slice(-8192);});
 type Exchange={resolve:(r:Response)=>void;reject:(e:Error)=>void;controller?:ReadableStreamDefaultController<Uint8Array>;done:boolean;abort:()=>void};
 const exchanges=new Map<number,Exchange>();let serial=0;
 const send=(row:Row)=>{child.stdin.write(JSON.stringify(row)+'\n');};
 child.stdout.setEncoding('utf8');child.stdout.on('data',(chunk:string)=>{
  buffer+=chunk;if(buffer.length>2*1024*1024)throw Error('fixture output capture bound');
  let at:number;while((at=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);const prefix=line.indexOf('HTTP_BRIDGE_PROBE ');if(prefix<0)continue;const row=JSON.parse(line.slice(prefix+18))as Row;if(row.ready){ready=true;continue;}wire.push({direction:'bridge',...row});const exchange=exchanges.get(row.id);if(!exchange)continue;
   if(row.failure){exchange.reject(Error(String(row.failure)));exchanges.delete(row.id);continue;}
   if(row.packet){const packet=row.packet;if(!exchange.controller){const stream=new ReadableStream<Uint8Array>({start(controller){exchange.controller=controller;},cancel(){exchange.abort();}});const headers=new Headers();if(packet.contentType)headers.set('content-type',packet.contentType);if(packet.authenticateBearer)headers.set('www-authenticate','Bearer');exchange.resolve(new Response(packet.status===202||packet.status===204?null:stream,{status:packet.status,headers}));}if(packet.body)exchange.controller!.enqueue(new TextEncoder().encode(packet.body));}
   if(row.ended||row.closed){exchange.done=true;try{exchange.controller?.close();}catch{}exchanges.delete(row.id);}
  }
 });
 await vi.waitFor(()=>expect(ready,`fixture exit=${child.exitCode} ${capture}`).toBe(true),{timeout:5000});
 const fetch:FetchLike=async(input,init)=>{
  const request=input instanceof Request?input:new Request(input,init);const url=new URL(request.url);const headers=Object.fromEntries(request.headers.entries());headers.host=url.host;
  const body=await request.text();const id=++serial;wire.push({direction:'sdk',id,method:request.method,target:url.pathname+url.search,headers,body:body?JSON.parse(body):null});
  return new Promise<Response>((resolve,reject)=>{
   const exchange:Exchange={resolve,reject,done:false,abort:()=>{if(exchange.done)return;exchange.done=true;send({action:'close',id});try{exchange.controller?.error(new DOMException('Cancelled','AbortError'));}catch{}exchanges.delete(id);}};
   exchanges.set(id,exchange);request.signal.addEventListener('abort',exchange.abort,{once:true});
   if(request.signal.aborted){exchange.abort();reject(new DOMException('Cancelled','AbortError'));return;}
   send({action:'open',id,method:request.method,target:url.pathname+url.search,headers,body});
  });
 };
 const clients:Client[]=[];
 return{fake,wire,state,async client(legacy:boolean,form=false,name=legacy?'sdk-legacy':'sdk-modern',timeout=5000){const client=new Client({name,version:'1'},{capabilities:form?{elicitation:{form:{}}}:{},versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});if(form)client.setRequestHandler('elicitation/create',async()=>({action:'accept',content:{confirm:true}}));const transport=new StreamableHTTPClientTransport(new URL('http://127.0.0.1:4455/mcp?client=sdk-legacy'),{fetch});clients.push(client);await client.connect(transport,{timeout});return client;},async close(){for(const client of clients)await client.close();child.stdin.end(JSON.stringify({action:'shutdown',id:0})+'\n');try{const exit=await Promise.race([closed,new Promise<never>((_,reject)=>setTimeout(()=>reject(Error('Rust fixture failed to join')),5000).unref())]);expect(exit, capture).toEqual([0,null]);await fake.waitForNoOpenConnections();expect(fake.openConnectionCount).toBe(0);}finally{if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');await closed;}writeFileSync(join(state,'wire.json'),JSON.stringify(wire,null,2)+'\n');await fake.close();}}};
}
for(const legacy of[false,true])it(`actual SDK2.4 exchanges discover, call and reuse one OBS owner (${legacy?'legacy':'modern'})`,async()=>{
 const p=await probe(false);try{const client=await p.client(legacy);expect((await client.listTools()).tools.some(row=>row.name==='obs-get-version')).toBe(true);expect((await client.readResource({uri:'obs://take/current'})).contents).toHaveLength(1);const result=await client.callTool({name:'obs-get-version',arguments:{}});expect(result.isError).not.toBe(true);expect(p.fake.connectionCount).toBe(1);const status=await client.callTool({name:'obs-control-status',arguments:{}});expect(status.structuredContent).toMatchObject({caller:legacy?'sdk-legacy':'sdk-modern'});expect(p.fake.connectionCount).toBe(1);}finally{await p.close();}
});
it('actual SDK2.4 modern confirmation resumes once across stateless POST exchanges',async()=>{
 const p=await probe(true);try{const client=await p.client(false,true);const result=await client.callTool({name:'obs-stop-record',arguments:{}});expect(result.isError).not.toBe(true);expect(p.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);const calls=p.wire.filter(row=>row.direction==='sdk'&&row.body?.method==='tools/call');expect(calls).toHaveLength(2);expect(calls[0]!.body.id).not.toEqual(calls[1]!.body.id);expect(calls[1]!.body.params.requestState).toMatch(/^native-confirmation\//);expect(calls[1]!.body.params.arguments).toEqual(calls[0]!.body.params.arguments);expect(calls[1]!.body.params._meta).toEqual(calls[0]!.body.params._meta);}finally{await p.close();}
});
it('actual SDK2.4 legacy initialize form offer grants no later exchange authority',async()=>{
 const p=await probe(true);try{const client=await p.client(true,true);expect((await client.callTool({name:'obs-stop-record',arguments:{}})).isError).toBe(true);expect(p.fake.connectionCount).toBe(0);expect((await client.callTool({name:'obs-stop-record',arguments:{confirm:true}})).isError).not.toBe(true);expect(p.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);}finally{await p.close();}
});

it('peer-close fence never terminates an active peer and resolves on its close event',async()=>{
 const fake=await FakeOBSServer.start();const peer=new WebSocket(fake.url);const closed=once(peer,'close');
 try{await once(peer,'open');await fake.waitForConnectionCount(1);await expect(fake.waitForNoOpenConnections(20)).rejects.toThrow('Timed out waiting for all OBS peers to close');expect(fake.openConnectionCount).toBe(1);const waiting=fake.waitForNoOpenConnections();peer.close();await waiting;await closed;expect(fake.openConnectionCount).toBe(0);}finally{if(peer.readyState!==WebSocket.CLOSED){peer.terminate();await closed;}await fake.close();}
});

it('actual SDK peers discover and refuse unknown RPC during shared initialization; cancelled mutation and Stop never dispatch',async()=>{
 const p=await probe(false,{holdHello:true});const abortA=new AbortController();const abortStop=new AbortController();
 try{
  const a=await p.client(false);const mutation=a.callTool({name:'obs-set-input-mute',arguments:{inputName:'Mic',inputMuted:true}},{signal:abortA.signal});const mutationOutcome=mutation.then(value=>({value}),reason=>({error:String((reason as Error).message)}));
  await p.fake.waitForConnectionCount(1);expect(p.fake.history()).toEqual([]);
  const b=await p.client(false,false,'sdk-peer',2000);expect((await b.listTools()).tools.some(row=>row.name==='obs-stop-record')).toBe(true);
  await expect(b.request({method:'fixture/unknown',params:{}} as never,z.object({}))).rejects.toThrow();
  const stop=b.callTool({name:'obs-stop-record',arguments:{}},{signal:abortStop.signal});const stopOutcome=stop.then(value=>({value}),reason=>({error:String((reason as Error).message)}));
  await b.listTools();expect(p.fake.history()).toEqual([]);expect(p.fake.connectionCount).toBe(1);
  abortA.abort();abortStop.abort();await a.close();expect(await mutationOutcome).toHaveProperty('error');expect(await stopOutcome).toHaveProperty('error');await b.listTools();
  p.fake.releaseHello();expect((await b.callTool({name:'obs-get-version',arguments:{}})).isError).not.toBe(true);
  const types=p.fake.history().filter(row=>row.frame.op===6).map(row=>row.frame.d.requestType);expect(types.filter(type=>type==='GetVersion')).toHaveLength(2);expect(types).not.toContain('SetInputMute');expect(types).not.toContain('StopRecord');expect(p.fake.connectionCount).toBe(1);
 }finally{abortA.abort();abortStop.abort();p.fake.releaseHello();await p.close();}
});

it('actual SDK peer can preempt control during initialization and the original caller is rechecked before any effect',async()=>{
 const p=await probe(false,{holdHello:true});
 try{const a=await p.client(false);const pending=a.callTool({name:'obs-set-input-mute',arguments:{inputName:'Mic',inputMuted:true}});const outcome=pending.then(value=>value,reason=>({isError:true,content:[{type:'text',text:String((reason as Error).message)}]}));await p.fake.waitForConnectionCount(1);const b=await p.client(false,false,'peer-holder');expect((await b.callTool({name:'obs-claim-control',arguments:{minutes:1}})).structuredContent).toMatchObject({holder:'peer-holder'});expect(p.fake.history()).toEqual([]);p.fake.releaseHello();const refused=await outcome;expect(refused.isError).toBe(true);expect(JSON.stringify(refused)).toContain('peer-holder');expect(p.fake.history().some(row=>row.frame.op===6&&row.frame.d.requestType==='SetInputMute')).toBe(false);expect(p.fake.connectionCount).toBe(1);}finally{p.fake.releaseHello();await p.close();}
});
