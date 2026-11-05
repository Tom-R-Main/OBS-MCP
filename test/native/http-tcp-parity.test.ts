/* SPDX-License-Identifier: MIT */
import {spawn} from 'node:child_process';
import {once} from 'node:events';
import {mkdirSync,mkdtempSync,realpathSync,rmSync,readFileSync,readdirSync,writeFileSync,appendFileSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import net from 'node:net';
import http from 'node:http';
import {Client,StreamableHTTPClientTransport} from '@modelcontextprotocol/client';
import {McpServer} from '@modelcontextprotocol/server';
import {afterEach,expect,it,vi} from 'vitest';
import {FakeOBSServer} from '../support/fake-obs-server.js';
import {serveProduction} from '../support/production-fixture.js';
import {initialize} from '../../src/tools/index.js';
import {serveHttp} from '../../src/http.js';
import {closeHttpNativeEntries,launchHttpNativeEntry} from '../support/http-native-entry.js';
const root=fileURLToPath(new URL('../../',import.meta.url));
const binary=process.env.HTTP_TCP_PROBE_BINARY;
type Row=Record<string,unknown>;
const closes:(()=>Promise<void>)[]=[];
afterEach(async()=>{for(const close of closes.splice(0).reverse())await close();});
afterEach(closeHttpNativeEntries);
function modern(id:number,method:string,params:Row={}){return{jsonrpc:'2.0',id,method,params:{...params,_meta:{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name:'wire-caller',version:'1'},'io.modelcontextprotocol/clientCapabilities':{}}}};}
async function native(confirm=false,read=5000,take=false,token?:string,write=5000){
 if(!binary)throw Error('HTTP_TCP_PROBE_BINARY must name an immutable candidate test executable');
 const fake=await FakeOBSServer.start({platform:'macos'});serveProduction(fake);fake.respondWith('GetStats',()=>Object.fromEntries(Object.entries({activeFps:60,availableDiskSpace:51200,averageFrameRenderTime:2.5,cpuUsage:12.5,memoryUsage:200.25,outputSkippedFrames:0,outputTotalFrames:1200,renderSkippedFrames:0,renderTotalFrames:1200,webSocketSessionIncomingMessages:7,webSocketSessionOutgoingMessages:9}).sort(([a],[b])=>a.localeCompare(b))));fake.respondWith('StopRecord',()=>({outputPath:'/fixture/clip.mkv'}));
 const parent=join(root,'native/target');mkdirSync(parent,{recursive:true});const state=realpathSync(mkdtempSync(join(parent,'tcp-fixture-')));
 const recordingRoot=join(state,'recordings');mkdirSync(recordingRoot,{mode:0o700});let recording=false;const path=join(recordingRoot,'clip.mkv');
 if(take){
  fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));
  fake.respondWith('GetRecordDirectory',()=>({recordDirectory:recordingRoot}));fake.respondWith('GetRecordStatus',()=>({outputActive:recording,outputPaused:false,outputBytes:1048576}));
  fake.respondWith('StartRecord',()=>{recording=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return{};});
  fake.respondWith('StopRecord',()=>{recording=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return{outputPath:path};});
  fake.respondWith('CreateRecordChapter',()=>({}));fake.respondWith('StopVirtualCam',()=>({}));
 }
 const child=spawn(binary,['--exact','native_service::http_bridge::http_tcp::tests::probe_child','--ignored','--nocapture'],{cwd:root,stdio:['pipe','pipe','pipe'],env:{PATH:process.env.PATH,NATIVE_HTTP_TCP_PROBE:'1',NATIVE_HTTP_TCP_STATE:state,NATIVE_HTTP_TCP_OBS:fake.url,NATIVE_HTTP_TCP_CONFIRM:confirm?'1':'0',NATIVE_HTTP_TCP_READ_MS:String(read),NATIVE_HTTP_TCP_WRITE_MS:String(write),...(take?{NATIVE_HTTP_TCP_RECORDING_ROOT:recordingRoot}:{}),...(token?{NATIVE_HTTP_TCP_TOKEN:token}:{})}});
 const closed=once(child,'close');void closed.catch(()=>{});child.stdin.on('error',()=>{});const lines:Row[]=[];let buffer='',diagnostics='';
 child.stdout.on('data',(bytes:Buffer)=>{buffer+=bytes.toString();let at:number;while((at=buffer.indexOf('\n'))>=0){const line=buffer.slice(0,at);buffer=buffer.slice(at+1);const start=line.indexOf('HTTP_TCP_PROBE ');if(start>=0)lines.push(JSON.parse(line.slice(start+15)) as Row);}});
 child.stderr.on('data',(bytes:Buffer)=>{diagnostics=(diagnostics+bytes.toString()).slice(-8192);});
 closes.push(async()=>{child.stdin.end('shutdown\n');try{await Promise.race([closed,new Promise<never>((_,reject)=>setTimeout(()=>reject(Error('HTTP TCP child did not join')),10000).unref())]);expect(child.exitCode,diagnostics).toBe(0);expect(lines.some(row=>row.joined)).toBe(true);await fake.waitForNoOpenConnections();}finally{if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');await closed;}await fake.close();rmSync(state,{recursive:true,force:true});}});
 await vi.waitFor(()=>expect(lines.some(row=>row.ready),diagnostics).toBe(true),{timeout:5000});const port=Number(lines.find(row=>row.ready)!.port);
 async function stats(){const before=lines.length;child.stdin.write('stats\n');await vi.waitFor(()=>expect(lines.slice(before).some(row=>'bridgeOpens'in row)).toBe(true));return lines.slice(before).find(row=>'bridgeOpens'in row)!;}
 return{fake,state,recording:()=>recording,port,url:`http://127.0.0.1:${port}/mcp?client=sdk-legacy`,stats};
}
async function source(confirm=false,token?:string){
 const fake=await FakeOBSServer.start({platform:'macos'});serveProduction(fake);fake.respondWith('GetStats',()=>Object.fromEntries(Object.entries({activeFps:60,availableDiskSpace:51200,averageFrameRenderTime:2.5,cpuUsage:12.5,memoryUsage:200.25,outputSkippedFrames:0,outputTotalFrames:1200,renderSkippedFrames:0,renderTotalFrames:1200,webSocketSessionIncomingMessages:7,webSocketSessionOutgoingMessages:9}).sort(([a],[b])=>a.localeCompare(b))));fake.respondWith('StopRecord',()=>({outputPath:'/fixture/clip.mkv'}));
 const {OBSWebSocketClient}=await import('../../src/client.js');const obs=new OBSWebSocketClient(fake.url);
 const server=await serveHttp(()=>{const server=new McpServer({name:'obs-mcp',version:'1.1.1'});initialize(server,obs,{confirmLive:confirm});return server;},{port:0,token});
 closes.push(async()=>{await server.close();await obs.disconnect();await fake.close();});return{fake,url:server.url+'?client=sdk-legacy'};
}
async function client(url:string,legacy:boolean,form=false){
 const client=new Client({name:legacy?'sdk-legacy':'sdk-modern',version:'1'},{capabilities:form?{elicitation:{form:{}}}:{},versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
 if(form)client.setRequestHandler('elicitation/create',async()=>({action:'accept',content:{confirm:true}}));
 const transport=new StreamableHTTPClientTransport(new URL(url));await client.connect(transport,{timeout:5000});closes.push(async()=>{await client.close();});return client;
}
async function raw(port:number,request:string){const socket=net.connect({host:'127.0.0.1',port});const closed=new Promise<void>(resolve=>socket.once('close',()=>resolve()));socket.on('error',()=>{});await once(socket,'connect');let response='';socket.on('data',chunk=>{response+=chunk.toString('latin1');});socket.write(request);const timer=setTimeout(()=>socket.destroy(),3000);await closed;clearTimeout(timer);return response;}
for(const legacy of[false,true])it(`actual SDK2.4 discovers, reads and reconnects on explicit-close native TCP (${legacy})`,async()=>{
 const s=await source(),n=await native();const a=await client(s.url,legacy),b=await client(n.url,legacy);
 const expected=await a.callTool({name:'obs-get-stats',arguments:{}}),actual=await b.callTool({name:'obs-get-stats',arguments:{}});expect(actual).toEqual(expected);
 expect(await b.callTool({name:'obs-get-stats',arguments:{}})).toEqual(await a.callTool({name:'obs-get-stats',arguments:{}}));expect(n.fake.connectionCount).toBe(1);expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='GetStats')).toHaveLength(2);
 expect(Number((await n.stats()).accepted)).toBeGreaterThan(2);
});
it('actual SDK modern confirmation resumes once over separate native TCP exchanges',async()=>{const n=await native(true),c=await client(n.url,false,true);expect((await c.callTool({name:'obs-stop-record',arguments:{}})).isError).not.toBe(true);expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);});
type DriverState={id:number;peerPort:number;phase:'headers'|'http'};
function driverStates(stats:Row):DriverState[]{return stats.driverStates as DriverState[];}
async function singleHttpDriver(n:{stats:()=>Promise<Row>}){let driver:DriverState|undefined;await vi.waitFor(async()=>{const live=driverStates(await n.stats()).filter(row=>row.phase==='http');expect(live).toHaveLength(1);driver=live[0];});return driver!;}
it('actual SDK modern SSE listen stays open while a distinct TCP exchange reads and closes',async()=>{
 const n=await native(),c=await client(n.url,false),subscription=await c.listen({resourcesListChanged:true});
 const sse=await singleHttpDriver(n);expect(await c.listTools()).toHaveProperty('tools');expect((await c.callTool({name:'obs-get-stats',arguments:{}})).isError).not.toBe(true);
 expect(driverStates(await n.stats())).toContainEqual(sse);await subscription.close();expect(await subscription.closed).toBe('local');
 // Pool-created pre-head sockets own separate bounded readers. The exact SSE
 // driver must join, independently of whether the fetch pool retains one.
 await vi.waitFor(async()=>expect(driverStates(await n.stats()).some(row=>row.id===sse.id)).toBe(false));
});
it('closing an SDK SSE response joins that exchange while an independent idle header reader remains owned',async()=>{
 const n=await native();const idle=net.connect({host:'127.0.0.1',port:n.port});idle.on('error',()=>{});const closed=once(idle,'close');await once(idle,'connect');const port=idle.localPort;
 let idleDriver:DriverState|undefined;
 try{
  await vi.waitFor(async()=>{idleDriver=driverStates(await n.stats()).find(row=>row.peerPort===port);expect(idleDriver?.phase).toBe('headers');});expect((await n.stats()).bridgeOpens).toBe(0);
  const c=await client(n.url,false),subscription=await c.listen({resourcesListChanged:true}),sse=await singleHttpDriver(n);
  const roles=new Set([idleDriver!.id,sse.id]);const controlled=(stats:Row)=>driverStates(stats).filter(row=>roles.has(row.id));
  expect(controlled(await n.stats())).toHaveLength(2);expect((await c.callTool({name:'obs-get-stats',arguments:{}})).isError).not.toBe(true);
  await subscription.close();expect(await subscription.closed).toBe('local');
  await vi.waitFor(async()=>expect(controlled(await n.stats())).toEqual([idleDriver]));expect(idle.destroyed).toBe(false);
  idle.destroy();await closed;await vi.waitFor(async()=>expect(controlled(await n.stats())).toEqual([]));
 }finally{idle.destroy();await closed;}
});
it('one native TCP connection emits explicit close and cannot execute a pipelined second MCP call',async()=>{const n=await native();const body=JSON.stringify(modern(1,'tools/call',{name:'obs-get-stats',arguments:{}}));const request=`POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json, text/event-stream\r\nMcp-Protocol-Version: 2026-07-28\r\nMcp-Method: tools/call\r\nMcp-Name: obs-get-stats\r\nContent-Length: ${Buffer.byteLength(body)}\r\n\r\n${body}`;const response=await raw(n.port,request+request);expect(response.toLowerCase()).toContain('connection: close');expect([...response.matchAll(/HTTP\/1.1 200/g)]).toHaveLength(1);expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='GetStats')).toHaveLength(1);});
for(const lines of[
 ['Transfer-Encoding: chunked','Content-Length: 0'],['Content-Length: 0','Transfer-Encoding: chunked'],['Content-Length: 0','Content-Length: 0'],
])it(`raw framing conflict refuses before Bridge/OBS: ${lines.join(';')}`,async()=>{const n=await native();const response=await raw(n.port,['POST /mcp HTTP/1.1','Host: localhost',...lines,'Connection: close','','0\r\n\r\n'].join('\r\n'));expect(response).toMatch(/^HTTP\/1.1 400/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('partial headers consume the original accept-time deadline and join without effect',async()=>{const n=await native(false,100);const response=await raw(n.port,'POST /mcp HTTP/1.1\r\nHost:');expect(response).toBe('');expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);await vi.waitFor(async()=>expect(Number((await n.stats()).drivers)).toBe(0));});

for(const idleHeader of[false,true])it(`actual SDK abort closes HTTP observation but retains the same Stop ACK and durable receipt (idle header: ${idleHeader})`,async()=>{
 const n=await native();n.fake.queueSuccess('StopRecord',{outputPath:'/fixture/clip.mkv'},60000);const c=await client(n.url,false);const abort=new AbortController();
 const idle=idleHeader?net.connect({host:'127.0.0.1',port:n.port}):undefined;
 const idleClosed=idle?new Promise<void>(resolve=>idle.once('close',()=>resolve())):Promise.resolve();
 let idleDriver:DriverState|undefined;
 if(idle){idle.on('error',()=>{});await once(idle,'connect');await vi.waitFor(async()=>{idleDriver=driverStates(await n.stats()).find(row=>row.peerPort===idle.localPort);expect(idleDriver?.phase).toBe('headers');});}
 try{
 const work=c.callTool({name:'obs-stop-record',arguments:{}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)}));
 const held=await n.fake.waitForRequest('StopRecord');const requestDriver=await singleHttpDriver(n);abort.abort();expect(await work).toHaveProperty('error');
 // A fetch pool may retain independent pre-head readers. Check the exact
 // cancelled request owner rather than treating every TCP socket as that owner.
 await vi.waitFor(async()=>expect(driverStates(await n.stats()).some(row=>row.id===requestDriver.id)).toBe(false));
 if(idle){expect(driverStates(await n.stats())).toContainEqual(idleDriver);expect(idle.destroyed).toBe(false);}
 const receipts=()=>readdirSync(join(n.state,'operation-receipts')).map(name=>JSON.parse(readFileSync(join(n.state,'operation-receipts',name),'utf8'))as Row);
 expect(receipts()[0]).toMatchObject({phase:'dispatching'});
 n.fake.sendRaw({op:7,d:{requestId:held.frame.d.requestId,requestType:'StopRecord',requestStatus:{result:true,code:100},responseData:{outputPath:'/fixture/clip.mkv'}}},held.connectionId);
 await vi.waitFor(()=>expect(receipts()[0]).toMatchObject({phase:'accepted',requestAccepted:true,postconditionObserved:false}));
 expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);
 }finally{idle?.destroy();await idleClosed;}
 if(idleDriver)await vi.waitFor(async()=>expect(driverStates(await n.stats()).some(row=>row.id===idleDriver!.id)).toBe(false));
});
it('actual TCP32 ordinary timers cannot consume either typed global Stop lane',async()=>{
 const n=await native(false,5000,true),c=await client(n.url,false);expect((await c.callTool({name:'obs-take-start',arguments:{expectSilent:false,maxDurationSeconds:30}})).isError).not.toBe(true);expect(n.recording()).toBe(true);
 const controllers=Array.from({length:32},()=>new AbortController());const sleepers=controllers.map(abort=>c.callTool({name:'obs-sleep',arguments:{sleepMillis:50000}},{signal:abort.signal}).then(value=>({value}),error=>({error:String(error)})));
 let release=()=>{};
 try{
  await vi.waitFor(async()=>expect(Number((await n.stats()).drivers)).toBe(32),{timeout:5000});const statsBefore=n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='GetStats').length;
  for(const[name,args]of[['obs-get-stats',{}],['obs-call-request',{requestType:'StopRecord'}]] as const){
   const refused=await httpExchange(n.url,[['Host','localhost']],'/mcp','tools/call',{name,arguments:args});expect(refused.status).toBe(503);
   expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(0);
   expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='GetStats')).toHaveLength(statsBefore);
   await vi.waitFor(async()=>expect(Number((await n.stats()).drivers)).toBe(32));
  }
  n.fake.queueSuccess('StopVirtualCam',{},60000);
  const publicStop=c.callTool({name:'obs-stop-virtual-cam',arguments:{}});void publicStop.catch(()=>{});const held=await n.fake.waitForRequest('StopVirtualCam');
  let released=false;release=()=>{if(released)return;released=true;n.fake.sendRaw({op:7,d:{requestId:held.frame.d.requestId,requestType:'StopVirtualCam',requestStatus:{result:true,code:100},responseData:{}}},held.connectionId);};
  const owned=c.callTool({name:'obs-take-stop',arguments:{}});void owned.catch(()=>{});
  // Match the already-qualified service control: physical owned Stop must
  // progress before the other original ACK. Confirmation may await that ACK.
  await n.fake.waitForRequest('StopRecord',{after:held.cursor,timeoutMs:2500});expect(n.recording()).toBe(false);
  expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);expect(Number((await n.stats()).drivers)).toBeGreaterThanOrEqual(32);
  release();expect((await publicStop).isError).not.toBe(true);expect((await owned).structuredContent).toMatchObject({stopConfirmed:true});
 }finally{release();for(const abort of controllers)abort.abort();await Promise.all(sleepers);}
 await vi.waitFor(async()=>expect(Number((await n.stats()).drivers)).toBe(0),{timeout:5000});
});

async function httpExchange(url:string,headers:[string,string][],path='/mcp',method='prompts/list',params:Row={}){
 const endpoint=new URL(url),body=JSON.stringify(modern(7100,method,params));
 return await new Promise<{status:number;type:string|undefined;authenticate:string|undefined;body:unknown;connection:string|undefined}>((resolve,reject)=>{
  const request=http.request({host:'127.0.0.1',port:endpoint.port,path,method:'POST',headers:[...headers,['Content-Type','application/json'],['Accept','application/json, text/event-stream'],['Mcp-Protocol-Version','2026-07-28'],['Mcp-Method',method],...(method==='tools/call'&&typeof params.name==='string'?[['Mcp-Name',params.name]]:[]),['Content-Length',String(Buffer.byteLength(body))],['Connection','close']].flat()},response=>{
   let text='';response.setEncoding('utf8');response.on('data',chunk=>{text+=chunk;});response.on('end',()=>{let parsed:unknown=text;try{parsed=JSON.parse(text);}catch{}resolve({status:response.statusCode!,type:response.headers['content-type'],authenticate:response.headers['www-authenticate'],body:parsed,connection:response.headers.connection});});response.on('error',reject);
  });request.once('error',reject);request.end(body);
 });
}
const headerCases:{name:string;headers:[string,string][];path?:string;token?:string}[]=[
 {name:'canonical localhost',headers:[['Host','localhost']]},
 {name:'host case',headers:[['Host','LOCALHOST']]},
 {name:'canonical invalid host',headers:[['Host','other.invalid']]},
 {name:'allowed Origin port and path',headers:[['Host','localhost'],['Origin','http://localhost:54321/path']]},
 {name:'invalid Origin',headers:[['Host','localhost'],['Origin','https://other.invalid']]},
 {name:'null Origin',headers:[['Host','localhost'],['Origin','null']]},
 {name:'Host first allowed',headers:[['Host','localhost'],['Host','other.invalid']]},
 {name:'Host first denied',headers:[['Host','other.invalid'],['Host','localhost']]},
 {name:'wrong path before host',headers:[['Host','other.invalid']],path:'/wrong'},
 {name:'token missing',headers:[['Host','localhost']],token:'fixture-token'},
 {name:'token present',headers:[['Host','localhost'],['Authorization','Bearer fixture-token']],token:'fixture-token'},
 {name:'token wrong',headers:[['Host','localhost'],['Authorization','Bearer fixture-wrong']],token:'fixture-token'},
 {name:'Auth first allowed',headers:[['Host','localhost'],['Authorization','Bearer fixture-token'],['Authorization','Bearer fixture-wrong']],token:'fixture-token'},
 {name:'Auth first denied',headers:[['Host','localhost'],['Authorization','Bearer fixture-wrong'],['Authorization','Bearer fixture-token']],token:'fixture-token'},
 {name:'Origin refusal before token',headers:[['Host','localhost'],['Origin','http://other.invalid']],token:'fixture-token'},
 {name:'path refusal before token',headers:[['Host','localhost']],path:'/wrong',token:'fixture-token'},
];
it.each(headerCases)('actual Source exact HTTP status/type/body and no OBS for $name',async({headers,path,token})=>{
 const s=await source(false,token),n=await native(false,5000,false,token);const expected=await httpExchange(s.url,headers,path),actual=await httpExchange(n.url,headers,path);
 expect({...actual,connection:undefined}).toEqual({...expected,connection:undefined});expect(actual.connection).toBe('close');expect(n.fake.connectionCount).toBe(0);expect(s.fake.connectionCount).toBe(0);
});
for(const target of['/x/../mcp','//localhost/mcp','/m%63p'])it(`URL normalization remains explicit unqualified with zero dispatch: ${target}`,async()=>{const n=await native();const result=await httpExchange(n.url,[['Host','localhost']],target);expect(result.status).toBe(400);expect(String(result.body)).toContain('unqualified');expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('Expect remains explicit unqualified rather than an invented100 handshake',async()=>{const n=await native();const result=await httpExchange(n.url,[['Host','localhost'],['Expect','100-continue']]);expect(result.status).toBe(417);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});

it('matches the strict Source unknown modern ping projection without changing authority',async()=>{
 const s=await source(),n=await native();const sourceResult=await httpExchange(s.url,[['Host','localhost']],'/mcp','ping');const nativeResult=await httpExchange(n.url,[['Host','localhost']],'/mcp','ping');
 if(process.env.HTTP_TCP_UNKNOWN_RECEIPT)writeFileSync(process.env.HTTP_TCP_UNKNOWN_RECEIPT,JSON.stringify({input:modern(7100,'ping'),source:sourceResult,native:nativeResult,sourceObsConnections:s.fake.connectionCount,nativeObsConnections:n.fake.connectionCount},null,2)+'\n');
 expect({...nativeResult,connection:undefined}).toEqual({...sourceResult,connection:undefined});
});

it('prefetched JSON body bytes are replayed once into the existing ingress owner',async()=>{
 const n=await native();const body=JSON.stringify(modern(7200,'prompts/list'))+' '.repeat(12000);const request=`POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json\r\nMcp-Protocol-Version: 2026-07-28\r\nMcp-Method: prompts/list\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`;
 expect(await raw(n.port,request)).toMatch(/^HTTP\/1.1 200/);expect((await n.stats()).bridgeOpens).toBe(1);expect(n.fake.connectionCount).toBe(0);
});
it('65 raw header fields refuse before body, Bridge or OBS admission',async()=>{const n=await native();const fields=Array.from({length:64},(_,i)=>`X-Fixture-${i}: value`);const response=await raw(n.port,['POST /mcp HTTP/1.1','Host: localhost',...fields,'Content-Length: 0','Connection: close','',''].join('\r\n'));expect(response).toMatch(/^HTTP\/1.1 431/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('16KiB raw head cap refuses without growing into a declared request body',async()=>{const n=await native();const response=await raw(n.port,`POST /mcp HTTP/1.1\r\nHost: localhost\r\nX-Fixture: ${'x'.repeat(17000)}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n`);expect(response).toMatch(/^HTTP\/1.1 431/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('known body over4MiB refuses before allocating a Bridge command or connecting OBS',async()=>{const n=await native();const response=await raw(n.port,`POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: ${4*1024*1024+1}\r\nConnection: close\r\n\r\n{`);expect(response).toMatch(/^HTTP\/1.1 413/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('blocked real TCP output keeps its packet owner, permits typed Stop and closes at its write deadline',async()=>{
 const n=await native(false,5000,false,undefined,500);const c=await client(n.url,false);n.fake.respondWith('GetSourceScreenshot',()=>({imageData:'data:image/png;base64,'+'A'.repeat(8*1024*1024)}));
 const socket=net.connect({host:'127.0.0.1',port:n.port});socket.on('error',()=>{});const closed=once(socket,'close');await once(socket,'connect');
 try{
  const body=JSON.stringify(modern(7300,'tools/call',{name:'obs-get-source-screenshot',arguments:{sourceName:'Application',imageFormat:'png'}}));
  socket.write(`POST /mcp HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nAccept: application/json\r\nMcp-Protocol-Version: 2026-07-28\r\nMcp-Method: tools/call\r\nMcp-Name: obs-get-source-screenshot\r\nContent-Length: ${Buffer.byteLength(body)}\r\nConnection: close\r\n\r\n${body}`);
  await n.fake.waitForRequest('GetSourceScreenshot');expect(Number((await n.stats()).drivers)).toBe(1);expect((await c.callTool({name:'obs-stop-record',arguments:{}})).isError).not.toBe(true);expect(n.fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType==='StopRecord')).toHaveLength(1);
  await vi.waitFor(async()=>expect(Number((await n.stats()).drivers)).toBe(0),{timeout:5000});
 }finally{socket.destroy();await closed;}
});

const closedAuthorities:{name:string;headers:[string,string][];target?:string}[]=[
 {name:'numeric IPv4',headers:[['Host','2130706433']]},
 {name:'short IPv4',headers:[['Host','127.1']]},
 {name:'hex IPv4',headers:[['Host','0x7f000001']]},
 {name:'noncanonical IPv6',headers:[['Host','[0:0:0:0:0:0:0:1]']]},
 {name:'unicode authority',headers:[['Host','localhöst']]},
 {name:'noncanonical Origin authority',headers:[['Host','localhost'],['Origin','http://127.1']]},
];
it.each(closedAuthorities)('closed URL domain refuses $name explicitly with zero Bridge/OBS',async({headers,target})=>{const n=await native();const result=await httpExchange(n.url,headers,target);expect(result.status).toBe(400);expect(String(result.body)).toContain('unqualified');expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
it('raw unicode target is explicitly unqualified with zero Bridge/OBS',async()=>{const n=await native();const response=await raw(n.port,'POST /章/mcp HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n');expect(response).toMatch(/^HTTP\/1.1 400/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});
for(const[name,request]of[
 ['bare LF','POST /mcp HTTP/1.1\nHost: localhost\nContent-Length: 0\nConnection: close\n\n'],
 ['folded header','POST /mcp HTTP/1.1\r\nHost: localhost\r\nX-Test: a\r\n b\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'],
 ['space before colon','POST /mcp HTTP/1.1\r\nHost : localhost\r\nContent-Length: 0\r\nConnection: close\r\n\r\n'],
]as const)it(`Source-observed strict syntax refuses ${name} before Bridge/OBS`,async()=>{const n=await native();expect(await raw(n.port,request)).toMatch(/^HTTP\/1.1 400/);expect((await n.stats()).bridgeOpens).toBe(0);expect(n.fake.connectionCount).toBe(0);});

// Compare policy headers and the complete decoded body without relying on JSON
// key order, Date, or the transport implementation's framing choice.
async function methodWire(url:string,verb:string,legacy:boolean,headers:[string,string][]=[],path='/mcp'){
 const endpoint=new URL(url);
 return await new Promise<{status:number;type:string|undefined;allow:string|undefined;authenticate:string|undefined;connection:string|undefined;body:unknown;bodyText:string}>((resolve,reject)=>{
  const request=http.request({host:'127.0.0.1',port:endpoint.port,path,method:verb,headers:[['Host','localhost'],...headers,...(legacy?[]:[['Mcp-Protocol-Version','2026-07-28']]),['Connection','close']].flat()},response=>{
   let text='';response.setEncoding('utf8');response.on('data',chunk=>{text+=chunk;});response.on('end',()=>{let body:unknown=text;try{body=JSON.parse(text);}catch{}resolve({status:response.statusCode!,type:response.headers['content-type'],allow:response.headers.allow,authenticate:response.headers['www-authenticate'],connection:response.headers.connection,body,bodyText:text});});response.on('error',reject);
  });request.once('error',reject);request.end();
 });
}
const methodSemantic=({bodyText:_raw,...reply}:Awaited<ReturnType<typeof methodWire>>)=>reply;
for(const legacy of[true,false])for(const verb of['GET','PUT'])it(`HTTP method refusal preserves Source Allow POST over actual app and TCP (${verb},legacy=${legacy})`,async()=>{
 const s=await source(),n=await native(),entry=await launchHttpNativeEntry();
 const expected=await methodWire(s.url,verb,legacy),probe=await methodWire(n.url,verb,legacy),product=await methodWire((await entry.url()).href,verb,legacy);
 const facts={bridgeOpens:Number((await n.stats()).bridgeOpens),sourceConnections:s.fake.connectionCount,probeConnections:n.fake.connectionCount,productConnections:entry.fake.connectionCount,sourceHistory:s.fake.history(),probeHistory:n.fake.history(),productHistory:entry.fake.history(),stdout:entry.stdout()};
 if(process.env.HTTP_METHOD_EVIDENCE)appendFileSync(process.env.HTTP_METHOD_EVIDENCE,JSON.stringify({kind:'method',legacy,verb,source:expected,probe,product,facts})+'\n');
 expect(expected).toMatchObject({status:405,allow:'POST',body:{jsonrpc:'2.0',error:{code:-32000,message:'Method not allowed.'},id:null}});
 expect(methodSemantic(probe)).toEqual(methodSemantic(expected));expect(methodSemantic(product)).toEqual(methodSemantic(expected));
 expect((await n.stats()).bridgeOpens).toBe(0);for(const fake of[s.fake,n.fake,entry.fake]){expect(fake.connectionCount).toBe(0);expect(fake.history()).toEqual([]);}expect(entry.stdout()).toBe('');
});
const methodPriorRefusals=[
 {name:'path',path:'/wrong',headers:[] as [string,string][],token:undefined,status:404},
 {name:'auth',path:'/mcp',headers:[] as [string,string][],token:'fixture-method-token',status:401},
 {name:'Origin before auth',path:'/mcp',headers:[['Origin','http://other.invalid']] as [string,string][],token:'fixture-method-token',status:403},
];
for(const legacy of[true,false])it.each(methodPriorRefusals)(`HTTP method refusal preserves prior $name security reply without Allow (legacy=${legacy})`,async({name,path,headers,token,status})=>{
 const s=await source(false,token),n=await native(false,5000,false,token),entry=await launchHttpNativeEntry({token});
 const expected=await methodWire(s.url,'GET',legacy,headers,path),probe=await methodWire(n.url,'GET',legacy,headers,path),product=await methodWire((await entry.url()).href,'GET',legacy,headers,path);
 if(process.env.HTTP_METHOD_EVIDENCE)appendFileSync(process.env.HTTP_METHOD_EVIDENCE,JSON.stringify({kind:'prior',name,legacy,source:expected,probe,product})+'\n');
 expect(expected.status).toBe(status);expect(expected.allow).toBeUndefined();expect(methodSemantic(probe)).toEqual(methodSemantic(expected));expect(methodSemantic(product)).toEqual(methodSemantic(expected));
 expect((await n.stats()).bridgeOpens).toBe(0);for(const fake of[s.fake,n.fake,entry.fake]){expect(fake.connectionCount).toBe(0);expect(fake.history()).toEqual([]);}expect(entry.stdout()).toBe('');
});
