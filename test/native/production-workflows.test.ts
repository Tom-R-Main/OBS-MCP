/* SPDX-License-Identifier: MIT */
import {mkdirSync,mkdtempSync,readFileSync,readdirSync,rmSync} from 'node:fs';
import {join} from 'node:path';
import {fileURLToPath} from 'node:url';
import {Client} from '@modelcontextprotocol/client';
import {StdioClientTransport,getDefaultEnvironment} from '@modelcontextprotocol/client/stdio';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {FakeOBSServer,FakeOBSRequestError} from '../support/fake-obs-server.js';
import {serveProduction} from '../support/production-fixture.js';
const root=fileURLToPath(new URL('../../',import.meta.url));
const binary=process.env.NATIVE_TEST_BINARY??join(root,'native/target/debug/obs-mcp-native');
type Session={client:Client;transport:StdioClientTransport;closed:boolean};
const owners:{directory:string;fake:FakeOBSServer;sessions:Session[]}[]=[];
const tracks=()=>({'1':true,'2':false,'3':false,'4':false,'5':false,'6':false});
async function fixture(legacy:boolean,guarded=false){
 const parent=join(root,'native/target');mkdirSync(parent,{recursive:true});const directory=mkdtempSync(join(parent,'production-workflows-'));
 const fake=await FakeOBSServer.start({platform:'macos',supportedImageFormats:['png']});
 const owner={directory,fake,sessions:[] as Session[]};owners.push(owner);
 const scene=serveProduction(fake);scene.preview='Main';
 const audio={muted:true,tracks:tracks(),monitor:'OBS_MONITORING_TYPE_NONE'};
 const output={camera:false,stream:false};
 fake.respondWith('GetInputMute',()=>({inputMuted:audio.muted}));
 fake.respondWith('SetInputMute',data=>{audio.muted=data.inputMuted as boolean;return {};});
 fake.respondWith('GetInputAudioTracks',()=>({inputAudioTracks:structuredClone(audio.tracks)}));
 fake.respondWith('SetInputAudioTracks',data=>{audio.tracks=structuredClone(data.inputAudioTracks) as ReturnType<typeof tracks>;return {};});
 fake.respondWith('GetInputAudioMonitorType',()=>({monitorType:audio.monitor}));
 fake.respondWith('SetInputAudioMonitorType',data=>{audio.monitor=String(data.monitorType);return {};});
 for(const [request,event,key]of [['StartVirtualCam','VirtualcamStateChanged','camera'],['StartStream','StreamStateChanged','stream']] as const)fake.respondWith(request,()=>{output[key]=true;fake.sendEvent(event,{outputActive:true,outputState:'OBS_WEBSOCKET_OUTPUT_STARTED'});return {};});
 for(const [request,event,key]of [['StopVirtualCam','VirtualcamStateChanged','camera'],['StopStream','StreamStateChanged','stream']] as const)fake.respondWith(request,()=>{output[key]=false;fake.sendEvent(event,{outputActive:false,outputState:'OBS_WEBSOCKET_OUTPUT_STOPPED'});return {};});
 fake.respondWith('GetVirtualCamStatus',()=>({outputActive:output.camera}));
 fake.respondWith('GetStreamStatus',()=>({outputActive:output.stream,outputReconnecting:false}));
 fake.respondWith('GetProfileParameter',data=>({parameterValue:({'Output/Mode':'Simple','SimpleOutput/RecQuality':'Small','SimpleOutput/RecEncoder':'apple_h264','SimpleOutput/RecTracks':'1','SimpleOutput/RecFormat2':'mkv'} as Record<string,string>)[`${data.parameterCategory}/${data.parameterName}`]??null}));
 fake.respondWith('GetRecordDirectory',()=>{throw new FakeOBSRequestError(604,'fixture remote directory unavailable');});
 fake.respondWith('GetStats',()=>({availableDiskSpace:50000,renderSkippedFrames:0,renderTotalFrames:0}));
 const recordingRoot=join(directory,'recordings');mkdirSync(recordingRoot,{mode:0o700});
 const connect=async()=>{
  const client=new Client({name:'production-workflows',version:'1'},{versionNegotiation:{mode:legacy?'legacy':{pin:'2026-07-28'}}});
  const transport=new StdioClientTransport({command:binary,args:[],cwd:root,env:{...getDefaultEnvironment(),OBS_WEBSOCKET_URL:fake.url,OBS_MCP_STATE_DIR:directory,OBS_MCP_NATIVE_PROFILE:'production',OBS_MCP_NATIVE_RECORDING_ROOT:guarded?recordingRoot:'',OBS_MCP_NATIVE_FFMPEG:join(directory,'missing-ffmpeg'),OBS_MCP_NATIVE_FFPROBE:join(directory,'missing-ffprobe'),OBS_MCP_TOOLSETS:'all',OBS_MCP_TOOLS:'',OBS_MCP_READ_ONLY:'false',OBS_MCP_DYNAMIC_TOOLSETS:'false',OBS_MCP_CONFIRM_LIVE:'false',OBS_MCP_READ_OBS_CONFIG:'false'},stderr:'pipe'});
  transport.stderr?.on('data',()=>undefined);const s={client,transport,closed:false};owner.sessions.push(s);await client.connect(transport);return s;
 };
 const frames=(request:string)=>fake.history().filter(row=>row.frame.op===6&&row.frame.d.requestType===request);
 const receipts=()=>{const path=join(directory,'operation-receipts');try{return readdirSync(path).filter(name=>name.endsWith('.json')).map(name=>JSON.parse(readFileSync(join(path,name),'utf8')) as Record<string,unknown>);}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return [];throw error;}};
 const stage=(s:Session)=>s.client.callTool({name:'obs-stage-scene',arguments:{sceneCollection:scene.collection,sceneName:scene.sceneName}});
 return {...owner,scene,audio,output,connect,frames,receipts,stage};
}
async function close(s:Session){if(s.closed)return;await s.client.close();await s.transport.close();s.closed=true;}
async function call(s:Session,name:string,args:Record<string,unknown>={}){const result=await s.client.callTool({name,arguments:args});expect(result.isError,JSON.stringify(result)).not.toBe(true);return result;}
async function refused(s:Session,name:string,args:Record<string,unknown>={}){
 const result=await s.client.callTool({name,arguments:args}).then(value=>({value}),error=>({error}));
 if('value' in result)expect(result.value.isError,JSON.stringify(result.value)).toBe(true);
 else expect(String(result.error)).toMatch(/profile|stage|unavailable|incompatible/i);
 return result;
}
afterEach(async()=>{for(const owner of owners.splice(0)){try{for(const s of owner.sessions)await close(s);await vi.waitFor(()=>expect(owner.fake.openConnectionCount).toBe(0));}finally{await owner.fake.close();rmSync(owner.directory,{recursive:true,force:true});}}});
for(const legacy of[false,true])describe(`stateful production journeys (legacy=${legacy})`,()=>{
 it('reports conservative configured-route evidence without measuring monitoring or promising silence',async()=>{
  const f=await fixture(legacy,true),s=await f.connect();await call(s,'obs-set-input-mute',{inputName:'Application',inputMuted:false});
  for(const monitorType of ['OBS_MONITORING_TYPE_NONE','OBS_MONITORING_TYPE_MONITOR_ONLY','OBS_MONITORING_TYPE_MONITOR_AND_OUTPUT',null,'INVALID']){
   f.fake.respondWith('GetInputAudioMonitorType',()=>({monitorType}));
   const result=await call(s,'obs-preflight',{expectSilent:true});const report=result.structuredContent as {ready:boolean;checks:{id:string;status:string;message:string}[]};
   expect(report.ready).toBe(false);expect(report.checks.find(check=>check.id==='audio')).toMatchObject({status:'fail',message:expect.stringContaining('Configured unmuted audio routes')});expect(report.checks.find(check=>check.id==='audio')!.message).toContain('Signal levels and monitoring/loopback paths were not measured');
  }
  expect(f.frames('GetInputAudioMonitorType')).toHaveLength(0);expect(f.frames('StartRecord')).toHaveLength(0);expect(f.frames('StartStream')).toHaveLength(0);
 });
 it('cues a meeting scene while virtual camera runs and verifies microphone routing without recording',async()=>{
  const f=await fixture(legacy),s=await f.connect();
  await call(s,'obs-start-virtual-cam');expect(f.output.camera).toBe(true);
  const map={...tracks(),'2':true};await call(s,'obs-set-input-mute',{inputName:'Application',inputMuted:false});await call(s,'obs-set-input-audio-tracks',{inputName:'Application',inputAudioTracks:map});
  expect((await call(s,'obs-get-input-mute',{inputName:'Application'})).structuredContent).toMatchObject({inputMuted:false});
  expect((await call(s,'obs-get-input-audio-tracks',{inputName:'Application'})).structuredContent).toMatchObject({inputAudioTracks:map});
  const staged=await f.stage(s);expect(staged.isError).not.toBe(true);expect(f.scene.program).toBe('Main');expect(f.scene.preview).toBe('Review');
  const id=(staged.structuredContent as {stageId:string}).stageId;expect(id).toBeTypeOf('string');
  const taken=await call(s,'obs-take-scene',{stageId:id});expect(taken.structuredContent).toMatchObject({intendedSceneObservedOnProgram:true});expect(f.scene.program).toBe('Review');expect(f.output.camera).toBe(true);
  await call(s,'obs-stop-virtual-cam');expect((await call(s,'obs-get-virtual-cam-status')).structuredContent).toMatchObject({outputActive:false});expect(f.audio).toMatchObject({muted:false,tracks:map});
  for(const request of['StartVirtualCam','StopVirtualCam','TriggerStudioModeTransition','SetInputMute','SetInputAudioTracks'])expect(f.frames(request)).toHaveLength(1);
  for(const request of['StartRecord','StopRecord','StartStream','StopStream','StartOutput','StopOutput'])expect(f.frames(request)).toHaveLength(0);
  expect(f.fake.connectionCount).toBe(1);expect(f.receipts().filter(row=>row.requestType==='StartVirtualCam')).toEqual([expect.objectContaining({phase:'accepted',postconditionObserved:false})]);
 });
 it('keeps stream controls and diagnostics usable while durable stream history blocks staged transitions across restart',async()=>{
  const f=await fixture(legacy),s=await f.connect();const staged=await f.stage(s);expect(staged.isError).not.toBe(true);const id=(staged.structuredContent as {stageId:string}).stageId;
  await call(s,'obs-start-stream');expect(f.output.stream).toBe(true);await refused(s,'obs-take-scene',{stageId:id});await refused(s,'obs-stage-scene',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName});
  await call(s,'obs-stop-stream');expect(f.output.stream).toBe(false);expect(f.frames('TriggerStudioModeTransition')).toHaveLength(0);
  const before=f.receipts();expect(before.filter(row=>row.requestType==='StartStream')).toEqual([expect.objectContaining({phase:'accepted',requestAccepted:true})]);
  await close(s);await vi.waitFor(()=>expect(f.fake.openConnectionCount).toBe(0));const connections=f.fake.connectionCount;const restarted=await f.connect();
  await refused(restarted,'obs-stage-scene',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName});expect(f.fake.connectionCount).toBe(connections);expect(f.fake.openConnectionCount).toBe(0);
  expect((await call(restarted,'obs-get-stream-status')).structuredContent).toMatchObject({outputActive:false});await call(restarted,'obs-get-status');
  await refused(restarted,'obs-stage-scene',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName});await refused(restarted,'obs-take-scene',{stageId:id});
  await call(restarted,'obs-stop-stream');expect(f.output.stream).toBe(false);expect(f.frames('StartStream')).toHaveLength(1);expect(f.frames('StopStream')).toHaveLength(2);expect(f.frames('TriggerStudioModeTransition')).toHaveLength(0);const after=f.receipts();expect(after).toHaveLength(before.length+1);expect(after).toEqual(expect.arrayContaining(before));expect(after.filter(row=>row.requestType==='StopStream')).toEqual([expect.objectContaining({phase:'accepted',requestAccepted:true}),expect.objectContaining({phase:'accepted',requestAccepted:true})]);
 });
 it('permits only diagnostics for retained Unknown stream history after restart without replay or Stop authority',async()=>{
  const f=await fixture(legacy),s=await f.connect();
  // close() switches the socket to CLOSING synchronously, so the fake's
  // sendResponse cannot deliver the ACK returned by this applied mutation.
  f.fake.respondWith('StartStream',()=>{f.output.stream=true;f.fake.disconnect(undefined,1011,'applied stream reply lost');return {};});
  const started=await s.client.callTool({name:'obs-start-stream',arguments:{}});expect(started.isError).toBe(true);expect(f.output.stream).toBe(true);
  const unknown=f.receipts().filter(row=>row.requestType==='StartStream');expect(unknown).toEqual([expect.objectContaining({phase:'unknown',requestAccepted:null,postconditionObserved:false})]);expect(f.frames('StartStream')).toHaveLength(1);
  await close(s);await vi.waitFor(()=>expect(f.fake.openConnectionCount).toBe(0));const connections=f.fake.connectionCount;const restarted=await f.connect();
  await refused(restarted,'obs-stage-scene',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName});expect(f.fake.connectionCount).toBe(connections);expect(f.fake.openConnectionCount).toBe(0);
  expect((await call(restarted,'obs-get-stream-status')).structuredContent).toMatchObject({outputActive:true});
  await refused(restarted,'obs-set-input-mute',{inputName:'Application',inputMuted:false});expect(f.frames('SetInputMute')).toHaveLength(0);expect(f.audio.muted).toBe(true);
  // receipt_broker::scope admits safety work around LIVE owned inflight IDs,
  // not persisted Unknown from a previous process. See stop3_contract.rs:
  // lost_stop_ack_is_unknown_without_replay_or_uncertainty_bypass. No old
  // observer or Stop owner survives this restart; reconciliation is separate.
  const stop=await refused(restarted,'obs-stop-stream');expect(JSON.stringify(stop)).toMatch(/uncertain/i);expect(f.output.stream).toBe(true);
  expect(f.frames('StartStream')).toHaveLength(1);expect(f.frames('StopStream')).toHaveLength(0);expect(f.frames('TriggerStudioModeTransition')).toHaveLength(0);expect(f.frames('StartRecord')).toHaveLength(0);
  const after=f.receipts();expect(after).toEqual(unknown);expect(after).toHaveLength(1);
  // A human stopping OBS can change observed status, never this MCP journal's
  // outcome or authority. This fixture edit performs no MCP/OBS request.
  f.output.stream=false;expect((await call(restarted,'obs-get-stream-status')).structuredContent).toMatchObject({outputActive:false});await refused(restarted,'obs-stage-scene',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName});expect(f.receipts()).toEqual(unknown);
 });
 it('refuses stale stage IDs after restart and human edits, then verifies a fresh cue without replay',async()=>{
  const f=await fixture(legacy),s=await f.connect();const staged=await f.stage(s);expect(staged.isError).not.toBe(true);const old=(staged.structuredContent as {stageId:string}).stageId;
  await close(s);await vi.waitFor(()=>expect(f.fake.openConnectionCount).toBe(0));f.scene.setting=77;f.scene.preview='Main';
  const restarted=await f.connect();expect((await call(restarted,'obs-get-input-settings',{inputName:'Application'})).structuredContent).toMatchObject({inputSettings:{setting:77}});await call(restarted,'obs-observe');
  await refused(restarted,'obs-take-scene',{stageId:old});expect(f.frames('TriggerStudioModeTransition')).toHaveLength(0);
  const fresh=await f.stage(restarted);expect(fresh.isError).not.toBe(true);const id=(fresh.structuredContent as {stageId:string}).stageId;expect(id).not.toBe(old);
  expect((await call(restarted,'obs-take-scene',{stageId:id})).structuredContent).toMatchObject({intendedSceneObservedOnProgram:true});expect(f.scene.program).toBe('Review');expect(f.scene.setting).toBe(77);expect(f.frames('TriggerStudioModeTransition')).toHaveLength(1);
  for(const request of['StartRecord','StopRecord','StartStream','StopStream'])expect(f.frames(request)).toHaveLength(0);
 });
 it('retains preparation recovery provenance across restart and refuses to overwrite an operator transform edit',async()=>{
  const f=await fixture(legacy);f.scene.program=f.scene.sceneName;f.audio.muted=false;
  const item=f.scene.items[0]!;item.sceneItemTransform.rotation=10;item.sceneItemTransform.positionX=25;
  f.fake.respondWith('GetSceneItemList',data=>({sceneItems:data.sceneName===f.scene.sceneName||data.sceneUuid===f.scene.sceneUuid?structuredClone(f.scene.items):[]}));
  f.fake.respondWith('SetSceneItemTransform',data=>{Object.assign(item.sceneItemTransform,data.sceneItemTransform);return {};});
  const effects=()=>f.fake.history().flatMap(({frame})=>{const requests=frame.op===6?[frame.d]:frame.op===8&&Array.isArray(frame.d.requests)?frame.d.requests:[];return requests.filter((request:{requestType?:unknown})=>typeof request.requestType==='string'&&!request.requestType.startsWith('Get')&&request.requestType!=='Sleep');});
  const s=await f.connect();const prepared=await call(s,'obs-prepare-demo',{sceneCollection:f.scene.collection,sceneName:f.scene.sceneName,inputName:'Application',apply:true});expect(prepared.structuredContent).toMatchObject({status:'prepared',applied:true});
  const operation=(prepared.structuredContent as {operationId:string}).operationId;const path=join(f.directory,'demo-recovery',operation+'.json');const journalBytes=readFileSync(path,'utf8');const journal=JSON.parse(journalBytes);expect(journal.version).toBe(2);expect(journal.provenance.fields.every((field:{preparation:{steps:string[]}})=>field.preparation.steps.at(-1)==='accepted')).toBe(true);
  expect(f.audio.muted).toBe(true);expect(Object.values(f.audio.tracks).every(value=>value===false)).toBe(true);expect(item.sceneItemTransform.rotation).toBe(0);
  await close(s);await vi.waitFor(()=>expect(f.fake.openConnectionCount).toBe(0));item.sceneItemTransform.rotation=77;const before=effects();const restarted=await f.connect();
  for(const apply of[false,true]){const result=await restarted.client.callTool({name:'obs-recover-demo',arguments:{operationId:operation,apply}});expect(result.isError).toBe(true);expect(result.structuredContent).toMatchObject({status:'conflict',applied:false});expect(effects()).toEqual(before);expect(item.sceneItemTransform.rotation).toBe(77);expect(f.audio.muted).toBe(true);expect(readFileSync(path,'utf8')).toBe(journalBytes);}
  expect(f.frames('StartRecord')).toHaveLength(0);expect(f.frames('StartStream')).toHaveLength(0);
 });
 it('refuses stream activation before any effect when guarded Take/media owners are bound',async()=>{
  const f=await fixture(legacy,true),s=await f.connect();expect((await s.client.listTools()).tools.map(t=>t.name)).not.toContain('obs-start-stream');await refused(s,'obs-start-stream');expect(f.frames('StartStream')).toHaveLength(0);expect(f.receipts()).toEqual([]);
 });
});
