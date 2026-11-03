/* SPDX-License-Identifier: GPL-2.0-only */
// Development Source oracle. The Rust service owns durable request receipts.
import {createHash} from 'node:crypto';
import {constants,closeSync,fstatSync,lstatSync,mkdtempSync,openSync,readSync,readdirSync,realpathSync,rmdirSync,unlinkSync,writeSync} from 'node:fs';
import type {BigIntStats} from 'node:fs';
import {dirname,isAbsolute,join,relative,resolve} from 'node:path';
import {performance} from 'node:perf_hooks';
import type {CallToolResult,McpServer} from '@modelcontextprotocol/server';
import {z} from 'zod';
import {OBSConnectionChangedError,OBSRequestError,type OBSWebSocketClient} from '../client.js';
import {localMediaInputOptions,runBoundedMediaProcess,runStreamingMediaProcess} from './media-verification.js';
import {activeLease,callerName} from './lease.js';
import {currentTake} from './takes.js';
import {NON_IDEMPOTENT_WRITE_TOOL} from './request-tool.js';
const MAX_BYTES=512*1024*1024,MAX_PATH=4096;
type FileEvidence={path:string;sha256:string;bytes:number;durationSeconds:number;audioStreams:number;videoStreams:number};
export type ReplayFacts={status:'verified'|'unverified';requestAccepted:boolean|null;saveEventObserved:boolean;newPathObserved:boolean;fileStableDuringVerification:boolean;strictDecode:boolean;requestAttribution:'unverified';file?:FileEvidence};
type Settings={root?:string;state?:string;ffmpeg?:string;ffprobe?:string;profile?:string};
const runs=new WeakMap<OBSWebSocketClient,Promise<CallToolResult>>();
const doubtful=new WeakSet<OBSWebSocketClient>();
const cleanupBlocked=new WeakSet<OBSWebSocketClient>();
const obj=(v:unknown):v is Record<string,unknown>=>v!==null&&typeof v==='object'&&!Array.isArray(v);
const identity=(a:BigIntStats,b:BigIntStats)=>a.dev===b.dev&&a.ino===b.ino;
const stamp=(a:BigIntStats,b:BigIntStats)=>identity(a,b)&&a.size===b.size&&a.mode===b.mode&&a.nlink===b.nlink&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
const stat=(path:string)=>lstatSync(path,{bigint:true});
function directory(path:string|undefined){
 if(!path||!isAbsolute(path)||resolve(path)!==path||realpathSync(path)!==path)throw Error('An explicit canonical approved directory is required');
 let current=path;while(true){const s=stat(current);if(!s.isDirectory()||s.isSymbolicLink())throw Error('Approved directory cannot traverse symbolic links');const parent=dirname(current);if(parent===current)break;current=parent;}
 return {path,identity:stat(path)};
}
function executable(path:string|undefined){if(!path||!isAbsolute(path)||realpathSync(path)!==path)throw Error('Explicit canonical media executables are required');const s=stat(path);if(!s.isFile()||s.isSymbolicLink()||(s.mode&0o111n)===0n)throw Error('Media executable is unavailable');return{path,identity:s};}
function checkDirectory(root:ReturnType<typeof directory>){const now=stat(root.path);if(!identity(root.identity,now)||!now.isDirectory()||now.isSymbolicLink())throw Error('Approved directory identity changed');}
function checkExecutable(tool:ReturnType<typeof executable>){if(!stamp(tool.identity,stat(tool.path)))throw Error('Media executable identity changed');}
function inventory(root:ReturnType<typeof directory>,check:()=>void){
 const paths=new Set<string>();let entries=0;
 const walk=(path:string,depth:number)=>{check();checkDirectory(root);for(const name of readdirSync(path)){check();if(++entries>2048||depth+1>8)throw Error('Replay inventory exceeds its complete bounded scope');const full=join(path,name);if(Buffer.byteLength(full)>MAX_PATH)throw Error('Replay inventory path exceeds its bound');const s=stat(full);paths.add(full);if(s.isSymbolicLink()||(!s.isDirectory()&&!s.isFile()))throw Error('Replay inventory contains an unsupported entry');if(s.isDirectory())walk(full,depth+1);}};
 walk(root.path,0);return paths;
}
function checkedFile(root:ReturnType<typeof directory>,path:string){checkDirectory(root);if(!isAbsolute(path)||resolve(path)!==path||!relative(root.path,path)||relative(root.path,path).startsWith('..')||isAbsolute(relative(root.path,path))||realpathSync(dirname(path))!==dirname(path))throw Error('Replay path is outside the approved root');let parent=dirname(path);while(parent!==root.path){const s=stat(parent);if(!s.isDirectory()||s.isSymbolicLink())throw Error('Replay path traverses an unsafe directory');parent=dirname(parent);}const s=stat(path);if(!s.isFile()||s.isSymbolicLink()||s.nlink!==1n||s.size<=0n||s.size>BigInt(MAX_BYTES))throw Error('Replay file must be a new bounded independent regular file');return s;}
function hash(fd:number,check:()=>void,copy?:number){const before=fstatSync(fd,{bigint:true}),hash=createHash('sha256'),buffer=Buffer.alloc(64*1024);let position=0;while(true){check();const count=readSync(fd,buffer,0,buffer.length,position);if(!count)break;position+=count;if(position>MAX_BYTES)throw Error('Replay file exceeds its byte limit');hash.update(buffer.subarray(0,count));if(copy!==undefined){let offset=0;while(offset<count){check();const written=writeSync(copy,buffer,offset,count-offset);if(written<=0)throw Error('Replay snapshot write made no progress');offset+=written;}}}if(!stamp(before,fstatSync(fd,{bigint:true})))throw Error('Replay file changed while reading');return{sha256:hash.digest('hex'),bytes:position};}
function result(facts:ReplayFacts,message:string):CallToolResult{return{content:[{type:'text',text:message}],structuredContent:facts,isError:facts.status!=='verified'};}
/** Join retained Source work in test/host shutdown. No replay or new effect. */
export async function joinReplayVerification(client:OBSWebSocketClient){await runs.get(client);}
export function initialize(server:McpServer,client:OBSWebSocketClient):void{
 const settings:Settings={root:process.env.OBS_MCP_NATIVE_RECORDING_ROOT,state:process.env.OBS_MCP_STATE_DIR,ffmpeg:process.env.OBS_MCP_NATIVE_FFMPEG,ffprobe:process.env.OBS_MCP_NATIVE_FFPROBE,profile:process.env.OBS_MCP_NATIVE_PROFILE};
 server.registerTool('obs-save-replay-verified',{title:'Save and Verify Replay Buffer',description:'Requests one replay save and verifies a new local file under the approved recording root. Request attribution remains unverified: OBS save events contain no request ID. Requires explicit approved root, state directory and media executables; never starts/stops outputs or retries a save. OBS naming can overwrite existing replay files; verification does not prevent overwrites.',inputSchema:z.object({timeoutSeconds:z.number().int().min(1).max(50).default(15).describe('Original total operation budget, including save, event and local file verification')}),annotations:{...NON_IDEMPOTENT_WRITE_TOOL,destructiveHint:true}},async({timeoutSeconds},ctx)=>{
  if(settings.profile==='advanced')return result(empty(),'Advanced profile does not grant replay file verification');
  if(runs.has(client))return result(empty(),'Replay verification is already in progress');
  if(cleanupBlocked.has(client))return result(empty(),'Previous media cleanup is unconfirmed; private evidence remains retained and no new save was attempted');
  if(doubtful.has(client))return result(empty(),'A previous save outcome is uncertain; no replay was attempted');
  const signal=(ctx as {mcpReq?:{signal?:AbortSignal}}|undefined)?.mcpReq?.signal;
  const caller=callerName(server,ctx);const policy=()=>{const lease=activeLease(client);if(lease&&lease.holder!==caller)throw Error('Control lease refuses this save');if(currentTake(client))throw Error('An owned Take prevents replay file inspection');};
  const run=verify(client,settings,timeoutSeconds,policy,signal);runs.set(client,run);
  try{return await run;}finally{runs.delete(client);}
 });
}
function empty():ReplayFacts{return{status:'unverified',requestAccepted:false,saveEventObserved:false,newPathObserved:false,fileStableDuringVerification:false,strictDecode:false,requestAttribution:'unverified'};}
async function verify(client:OBSWebSocketClient,settings:Settings,seconds:number,policy:()=>void,signal?:AbortSignal):Promise<CallToolResult>{
 const facts=empty(),deadline=performance.now()+seconds*1000,abort=new AbortController();const timer=setTimeout(()=>abort.abort(),seconds*1000);
 const check=()=>{if(performance.now()>=deadline||abort.signal.aborted)throw Error('Original replay verification deadline expired');};
 const remaining=()=>{check();return Math.max(1,Math.floor(deadline-performance.now()));};
 let armed=false,generation=0,count=0,candidate:string|undefined,badEvent=false,disconnected=false,wake:(()=>void)|undefined;
 const saved=(data:unknown)=>{if(!armed||performance.now()>deadline||client.getConnectionStatus().generation!==generation)return;facts.saveEventObserved=true;count=Math.min(2,count+1);if(count!==1||!obj(data)||typeof data.savedReplayPath!=='string'||!data.savedReplayPath||Buffer.byteLength(data.savedReplayPath)>MAX_PATH||data.savedReplayPath.includes('\0'))badEvent=true;else candidate=data.savedReplayPath;wake?.();};
 const lost=()=>{if(armed)disconnected=true;wake?.();};
 let work:ReturnType<typeof directory>|undefined,pin:BigIntStats|undefined,sourceFd:number|undefined,pinFd:number|undefined;let message='Replay verification did not complete',retainWork=false;
 try{
  signal?.throwIfAborted();policy();check();const root=directory(settings.root),state=directory(settings.state),ffmpeg=executable(settings.ffmpeg),ffprobe=executable(settings.ffprobe);
  // Exclusive private writable probe, removed before inventory and before Save.
  const probe=mkdtempSync(join(root.path,'.replay-writable-')),probeIdentity=stat(probe),probeFile=join(probe,'write-check');let probeFd:number|undefined,probeStamp:BigIntStats|undefined;try{checkDirectory(root);probeFd=openSync(probeFile,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL,0o600);probeStamp=fstatSync(probeFd,{bigint:true});if(writeSync(probeFd,Buffer.from([0]))!==1)throw Error('Replay writable probe made no progress');}finally{if(probeFd!==undefined)closeSync(probeFd);if(!identity(probeIdentity,stat(probe)))throw Error('Writable probe identity changed; cleanup withheld');if(probeStamp){if(!identity(probeStamp,stat(probeFile)))throw Error('Writable probe file identity changed; cleanup withheld');unlinkSync(probeFile);}rmdirSync(probe);}
  await client.connect();check();generation=client.getConnectionStatus().generation;
  for(const name of['SaveReplayBuffer','GetReplayBufferStatus','GetLastReplayBufferReplay'])if(client.supportsRequest(name)!==true)throw Error('Replay workflow capabilities are unavailable');
  const read=(name:string)=>client.sendRequestForGeneration<Record<string,unknown>>(generation,name,undefined,remaining());
  if((await read('GetReplayBufferStatus')).outputActive!==true)throw Error('Replay buffer is not active');
  const previous=(await read('GetLastReplayBufferReplay')).savedReplayPath;if(typeof previous!=='string')throw Error('Previous replay filename evidence is unavailable');
  const baseline=inventory(root,check);
  client.on('ReplayBufferSaved',saved);client.on('disconnected',lost);signal?.throwIfAborted();policy();check();checkDirectory(root);const sendBudget=remaining();
  // Source has event-loop ordinals, not the Rust observer's physical cursor.
  // There is no await between arming and the generation-bound synchronous send.
  armed=true;facts.requestAccepted=null;
  try{await client.sendRequestForGeneration(generation,'SaveReplayBuffer',undefined,sendBudget);facts.requestAccepted=true;}catch(error){if(error instanceof OBSRequestError||error instanceof OBSConnectionChangedError)facts.requestAccepted=false;else doubtful.add(client);throw error;}
  while(count===0&&!disconnected){check();await new Promise<void>(resolve=>{const done=()=>{clearTimeout(t);abort.signal.removeEventListener('abort',done);resolve();};const t=setTimeout(done,remaining());wake=done;abort.signal.addEventListener('abort',done,{once:true});});wake=undefined;}
  check();if(disconnected||badEvent||count!==1||!candidate)throw Error('Replay saved-event evidence is missing or ambiguous');
  if(candidate===previous||baseline.has(candidate))throw Error('Replay event names an existing or previous file');
  const before=checkedFile(root,candidate);facts.newPathObserved=true;
  if((await read('GetLastReplayBufferReplay')).savedReplayPath!==candidate)throw Error('Current replay filename does not match the observed event');
  checkDirectory(state);work=directory(mkdtempSync(join(state.path,'.replay-verification-')));const snapshot=join(work.path,'replay.snapshot');
  sourceFd=openSync(candidate,constants.O_RDONLY|constants.O_NOFOLLOW|constants.O_NONBLOCK);if(!stamp(before,fstatSync(sourceFd,{bigint:true})))throw Error('Replay file changed before opening');
  pinFd=openSync(snapshot,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL,0o600);const copied=hash(sourceFd,check,pinFd);closeSync(pinFd);pinFd=undefined;pin=stat(snapshot);
  const stdout:Buffer[]=[];let outputBytes=0;checkExecutable(ffprobe);
  await runStreamingMediaProcess(ffprobe.path,['-v','error',...localMediaInputOptions(),'-show_format','-show_streams','-of','json',snapshot],chunk=>{outputBytes+=chunk.length;if(outputBytes>1024*1024)throw Error('Media probe output exceeds its bound');stdout.push(Buffer.from(chunk));},{signal:abort.signal,timeoutMs:remaining()});
  const info:unknown=JSON.parse(Buffer.concat(stdout).toString('utf8'));if(!obj(info)||!obj(info.format)||!Array.isArray(info.streams))throw Error('Replay media metadata is incomplete');
  const rawDuration=info.format.duration;if(typeof rawDuration!=='number'&&(typeof rawDuration!=='string'||!rawDuration.trim()))throw Error('Replay duration is unavailable');
  const duration=Number(rawDuration),videos=info.streams.filter(stream=>obj(stream)&&stream.codec_type==='video'),audios=info.streams.filter(stream=>obj(stream)&&stream.codec_type==='audio');
  if(!Number.isFinite(duration)||duration<=0||videos.length!==1||!obj(videos[0])||typeof videos[0].width!=='number'||typeof videos[0].height!=='number'||!Number.isSafeInteger(videos[0].width)||!Number.isSafeInteger(videos[0].height)||videos[0].width<=0||videos[0].height<=0||videos[0].width>8192||videos[0].height>8192||videos[0].width*videos[0].height>33_554_432||audios.length>6||info.streams.length>16)throw Error('Replay requires bounded video, audio and stream metadata with positive duration');
  checkExecutable(ffmpeg);await runBoundedMediaProcess(ffmpeg.path,['-nostdin','-v','error','-xerror','-err_detect','explode',...localMediaInputOptions(),'-i',snapshot,'-map','0:v','-map','0:a?','-f','null','-'],{signal:abort.signal,timeoutMs:remaining()});facts.strictDecode=true;
  check();if(badEvent||count!==1||disconnected)throw Error('Replay save-event interval became ambiguous');if((await read('GetLastReplayBufferReplay')).savedReplayPath!==candidate)throw Error('Replay filename changed during verification');
  if(!stamp(before,checkedFile(root,candidate))||!stamp(before,fstatSync(sourceFd,{bigint:true}))||!stamp(pin,stat(snapshot)))throw Error('Replay file changed during verification');
  const rechecked=hash(sourceFd,check);if(rechecked.sha256!==copied.sha256||rechecked.bytes!==copied.bytes||!stamp(before,checkedFile(root,candidate)))throw Error('Replay source bytes changed during verification');
  facts.fileStableDuringVerification=true;facts.file={path:candidate,sha256:copied.sha256,bytes:copied.bytes,durationSeconds:duration,audioStreams:audios.length,videoStreams:videos.length};facts.status='verified';message='A new local replay file was verified. OBS save events have no request ID; request attribution remains unverified.';
 }catch(error){retainWork=error instanceof Error&&/cleanup unconfirmed/i.test(error.message);if(retainWork)cleanupBlocked.add(client);message=facts.requestAccepted===null?'Replay save outcome is uncertain; no retry was attempted':'Replay file verification is incomplete; inspect OBS and local evidence before any new save';}
 finally{
  armed=false;clearTimeout(timer);client.off('ReplayBufferSaved',saved);client.off('disconnected',lost);if(sourceFd!==undefined)closeSync(sourceFd);if(pinFd!==undefined)closeSync(pinFd);
  if(work&&retainWork){facts.status='unverified';message='Replay media cleanup is unconfirmed; private snapshot evidence was retained';}
  else if(work){try{checkDirectory(work);const names=readdirSync(work.path);if(names.length!==(pin?1:0)||names.some(name=>name!=='replay.snapshot'))throw Error('Private snapshot cleanup is ambiguous');if(pin){const path=join(work.path,'replay.snapshot');if(!identity(pin,stat(path)))throw Error('Private snapshot identity changed');unlinkSync(path);}rmdirSync(work.path);}catch{cleanupBlocked.add(client);facts.status='unverified';message='Replay verification cleanup is unconfirmed; private evidence was retained';}}
 }
 if(facts.status!=='verified')delete facts.file;
 return result(facts,message);
}
