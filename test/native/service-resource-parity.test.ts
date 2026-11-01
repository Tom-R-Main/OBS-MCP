/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdirSync, realpathSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));
const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');
const compat=process.env.NATIVE_TEST_COMPAT_ENTRY??join(root,'build/index.js');
const sessions:{client:Client;transport:StdioClientTransport;fake:FakeOBSServer}[]=[];
async function start(native:boolean,legacy:boolean,response:Record<string,unknown>={scenes:[]},take=false) {
  const fake=await FakeOBSServer.start({platform:'macos',availableRequests:['GetSceneList','GetSceneItemList','GetVersion']});
  if(take)serveProduction(fake);else fake.respondWith('GetSceneList',()=>response);
  const state=join(realpathSync(process.env.OBS_MCP_STATE_DIR!),`owned-resource-${sessions.length}`);mkdirSync(state,{mode:0o700});
  const recordingRoot=take?join(state,'recordings'):'';
  if(take){
    mkdirSync(recordingRoot,{mode:0o700});let active=false;const path=join(recordingRoot,'fixture.mkv');
    fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));
    fake.respondWith('GetRecordDirectory',()=>{throw new FakeOBSRequestError(604,'fixture directory unavailable');});
    fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));fake.respondWith('GetRecordStatus',()=>({outputActive:active,outputPaused:false,outputBytes:1048576}));
    fake.respondWith('StartRecord',()=>{active=true;fake.sendEvent('RecordStateChanged',{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED',outputPath:path});return {};});
    fake.respondWith('StopRecord',()=>{active=false;fake.sendEvent('RecordStateChanged',{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED',outputPath:path});return {outputPath:path};});fake.respondWith('CreateRecordChapter',()=>({}));
  }
  const client=new Client({name:'resource-owner',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
  const transport=new StdioClientTransport({command:native?binary:process.execPath,args:native?[]:[compat],cwd:root,stderr:'pipe',env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:state,OBS_MCP_NATIVE_STATEFUL:native?'1':'0',OBS_MCP_NATIVE_RECORDING_ROOT:recordingRoot,OBS_MCP_COMPAT_NODE:join(state,'absent-node'),OBS_MCP_COMPAT_ENTRY:join(state,'absent-sdk'),OBS_MCP_READ_OBS_CONFIG:'false',OBS_MCP_TOOLSETS:take?'all':'general',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:take?'false':'true',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false'}});
  transport.stderr?.on('data',()=>undefined);const session={client,transport,fake};sessions.push(session);await client.connect(transport);return session;
}
afterEach(async()=>{for(const s of sessions.splice(0)){try{await s.client.close();await s.transport.close();}finally{await s.fake.close();}}});
function children(pid:number){return execFileSync('ps',['-axo','pid=,ppid='],{encoding:'utf8'}).trim().split('\n').map(line=>line.trim().split(/\s+/).map(Number)).filter(([,parent])=>parent===pid).map(([id])=>id);}
const readTypes=(fake:FakeOBSServer)=>fake.history().filter(item=>item.frame.op===6).map(item=>item.frame.d.requestType);
const outcome=async(work:Promise<unknown>)=>{try{return await work;}catch(reason){const e=reason as Error&{code?:number};return {error:e.message,code:e.code};}};
function semantic(result:Awaited<ReturnType<Client['readResource']>>){return {...result,contents:result.contents.map(item=>'text'in item?{...item,text:JSON.parse(String(item.text)) as unknown}:item)};}
for(const legacy of [false,true])describe(`persistent resource owner legacy=${legacy}`,()=>{
  it('lists complete Source static/dynamic metadata independently of tool filters on one owner',async()=>{
    const scenes={scenes:[null,{sceneName:'日本語 /?%'},{sceneName:'A'},{sceneName:'A'},{wrong:'skip'}]};const oracle=await start(false,legacy,scenes),native=await start(true,legacy,scenes);
    const expected=await oracle.client.listResources();expect(expected.resources).toHaveLength(6);
    expect(await native.client.listResources()).toEqual(expected);expect(await native.client.listResources()).toEqual(expected);
    expect(native.fake.connectionCount).toBe(1);expect(readTypes(native.fake).filter(type=>type==='GetSceneList')).toHaveLength(2);expect(children(native.transport.pid!)).toEqual([]);
  });
  it('reads full scene state and inactive take without a status-tool substitution',async()=>{
    const scenes={currentProgramSceneName:'A',scenes:[{sceneName:'A'}],extension:[1,null]};const oracle=await start(false,legacy,scenes),native=await start(true,legacy,scenes);
    expect(semantic(await native.client.readResource({uri:'obs://take/current'}))).toEqual(semantic(await oracle.client.readResource({uri:'obs://take/current'})));expect(native.fake.connectionCount).toBe(0);
    expect(semantic(await native.client.readResource({uri:'obs://scenes'}))).toEqual(semantic(await oracle.client.readResource({uri:'obs://scenes'})));expect(native.fake.connectionCount).toBe(1);expect(readTypes(native.fake).filter(type=>type!=='GetVersion')).toEqual(['GetSceneList']);expect(children(native.transport.pid!)).toEqual([]);
  });
  it('matches Source prefix completion, ordered duplicates and 100-item projection on the same session',async()=>{
    const scenes={scenes:[...Array.from({length:105},(_,i)=>({sceneName:`Alpha ${i}`})),{sceneName:'日本語'},null,{sceneName:'Alpha 0'}]};const oracle=await start(false,legacy,scenes),native=await start(true,legacy,scenes);
    for(const value of ['Alpha','日本','missing']){const request={ref:{type:'ref/resource' as const,uri:'obs://scene/{sceneName}/items'},argument:{name:'sceneName',value},context:{arguments:{unused:'kept'}}};expect(await native.client.complete(request)).toEqual(await oracle.client.complete(request));}
    expect(native.fake.connectionCount).toBe(1);expect(readTypes(native.fake).filter(type=>type!=='GetVersion')).toEqual(['GetSceneList','GetSceneList','GetSceneList']);expect(children(native.transport.pid!)).toEqual([]);
  });
  it('keeps empty/static completions and precise missing references offline',async()=>{
    const oracle=await start(false,legacy),native=await start(true,legacy);
    const refs=[{type:'ref/resource' as const,uri:'obs://scene/{sceneName}/items'},{type:'ref/resource' as const,uri:'obs://scene/{sceneName}/screenshot'},{type:'ref/resource' as const,uri:'obs://status'},{type:'ref/resource' as const,uri:'obs://scenes'},{type:'ref/resource' as const,uri:'obs://take/current'},{type:'ref/prompt' as const,name:'software-demo'},{type:'ref/prompt' as const,name:'missing'},{type:'ref/resource' as const,uri:'obs://missing'}];
    for(const ref of refs){const request={ref,argument:{name:'notACompleter',value:''}};expect(await outcome(native.client.complete(request))).toEqual(await outcome(oracle.client.complete(request)));}
    expect(native.fake.connectionCount).toBe(0);expect(children(native.transport.pid!)).toEqual([]);
  });
  it('preserves malformed scene-list fallbacks without inventing a partial inventory',async()=>{
    const response={scenes:'not an array'};const oracle=await start(false,legacy,response),native=await start(true,legacy,response);
    expect(await native.client.listResources()).toEqual(await oracle.client.listResources());
    const request={ref:{type:'ref/resource' as const,uri:'obs://scene/{sceneName}/items'},argument:{name:'sceneName',value:''}};
    expect(await native.client.complete(request)).toEqual(await oracle.client.complete(request));expect(native.fake.connectionCount).toBe(1);
  });
  it('refuses oversized inventory without truncation or handoff and keeps the owner usable',async()=>{
    const native=await start(true,legacy,{scenes:Array.from({length:4097},(_,i)=>({sceneName:`Scene ${i}`}))});
    expect(await outcome(native.client.listResources())).toEqual({error:'Native resource inventory limit exceeded; live state retained',code:-32603});
    expect(await native.client.complete({ref:{type:'ref/prompt',name:'software-demo'},argument:{name:'x',value:''}})).toMatchObject({completion:{values:[],hasMore:false}});
    expect((await native.client.callTool({name:'obs-get-version',arguments:{}})).isError).not.toBe(true);expect(children(native.transport.pid!)).toEqual([]);expect(native.fake.connectionCount).toBe(1);
  });
  it('preserves peer refusal without opening a compatibility runtime',async()=>{
    const oracle=await start(false,legacy),native=await start(true,legacy);
    for(const s of [oracle,native])s.fake.respondWith('GetSceneList',()=>{throw new FakeOBSRequestError(500,'fixture scene refusal');});
    expect(await outcome(native.client.listResources())).toEqual(await outcome(oracle.client.listResources()));
    const request={ref:{type:'ref/resource' as const,uri:'obs://scene/{sceneName}/items'},argument:{name:'sceneName',value:''}};
    expect(await outcome(native.client.complete(request))).toEqual(await outcome(oracle.client.complete(request)));expect(children(native.transport.pid!)).toEqual([]);expect(native.fake.connectionCount).toBe(1);
  });
});

it('retains an admitted resource read through frontend EOF and joins its sole socket',async()=>{
  const fake=await FakeOBSServer.start({availableRequests:['GetSceneList','GetVersion']});let release=()=>{};
  const state=join(realpathSync(process.env.OBS_MCP_STATE_DIR!),'resource-eof');mkdirSync(state,{mode:0o700});
  const child=spawn(binary,[],{cwd:root,stdio:['pipe','pipe','pipe'],env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:state,OBS_MCP_NATIVE_STATEFUL:'1',OBS_MCP_NATIVE_RECORDING_ROOT:'',OBS_MCP_COMPAT_NODE:join(state,'absent-node'),OBS_MCP_COMPAT_ENTRY:join(state,'absent-sdk'),OBS_MCP_READ_OBS_CONFIG:'false'}});child.stdout.on('data',()=>undefined);child.stderr.on('data',()=>undefined);child.stdin.on('error',()=>undefined);const closed=once(child,'close');void closed.catch(()=>undefined);
  try{
    child.stdin.write(JSON.stringify({jsonrpc:'2.0',id:1,method:'resources/list',params:{_meta:{'io.modelcontextprotocol/protocolVersion':'2026-07-28','io.modelcontextprotocol/clientInfo':{name:'resource-eof',version:'1'},'io.modelcontextprotocol/clientCapabilities':{}}}})+'\n');
    const request=await fake.waitForRequest('GetSceneList');release=()=>{if(fake.openConnectionCount>0)fake.sendRaw({op:7,d:{requestType:'GetSceneList',requestId:request.frame.d.requestId,requestStatus:{result:true,code:100},responseData:{scenes:[]}}});release=()=>{};};child.stdin.end();await new Promise(resolve=>setTimeout(resolve,60));expect(child.exitCode).toBeNull();expect(fake.connectionCount).toBe(1);expect(children(child.pid!)).toEqual([]);release();
    const exit=await Promise.race([closed,new Promise<never>((_,reject)=>setTimeout(()=>reject(new Error('retained resource child failed to join')),4000).unref())]);expect(exit).toEqual([0,null]);expect(fake.openConnectionCount).toBe(0);expect(readTypes(fake)).toEqual(['GetVersion','GetSceneList']);
  }finally{release();if(child.exitCode===null&&child.signalCode===null){child.kill('SIGTERM');await closed;}await fake.close();}
});

it('reads only the active owned monitor and retires that resource after bounded stop',async()=>{
  const native=await start(true,false,{},true);const read=async()=>{const result=await native.client.readResource({uri:'obs://take/current'});return JSON.parse(String((result.contents[0] as {text:string}).text)) as Record<string,unknown>;};
  expect(await read()).toEqual({running:false});
  const started=await native.client.callTool({name:'obs-take-start',arguments:{expectSilent:true,maxDurationSeconds:5}});expect(started.isError).not.toBe(true);
  try{
    expect((await native.client.callTool({name:'obs-take-mark',arguments:{name:'Resource chapter'}})).isError).not.toBe(true);
    const summary=await read();expect(summary).toMatchObject({running:true,expectSilent:true,chapters:[{name:'Resource chapter'}]});expect(summary).not.toHaveProperty('takeId');expect(summary).not.toHaveProperty('record');expect(Number(summary.durationSeconds)).toBeGreaterThanOrEqual(0);expect(native.fake.connectionCount).toBe(1);expect(children(native.transport.pid!)).toEqual([]);
  }finally{await native.client.callTool({name:'obs-take-stop',arguments:{}});}
  expect(await read()).toEqual({running:false});expect(readTypes(native.fake).filter(type=>type==='StartRecord')).toHaveLength(1);expect(readTypes(native.fake).filter(type=>type==='StopRecord')).toHaveLength(1);
});
