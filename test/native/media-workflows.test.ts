/* SPDX-License-Identifier: MIT */
import { appendFileSync, chmodSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { execFileSync, spawnSync } from 'node:child_process';
import { join } from 'node:path';
import { createServer } from 'node:http';
import type { Socket } from 'node:net';
import { fileURLToPath } from 'node:url';
import { Client, type CallToolResult } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { InMemoryTransport, McpServer } from '@modelcontextprotocol/server';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { initialize } from '../../src/tools/trim.js';
import type { OBSWebSocketClient } from '../../src/client.js';
import { PACKAGE_VERSION } from '../../src/version.js';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { nativeDefaultFixture } from '../support/native-default-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const ffmpeg = realpathSync(execFileSync('/usr/bin/which', ['ffmpeg'], { encoding: 'utf8' }).trim());
const ffprobe = realpathSync(execFileSync('/usr/bin/which', ['ffprobe'], { encoding: 'utf8' }).trim());
const multitrackMessage = 'Multitrack trim requires supported codecs, mono/stereo 44.1/48 kHz audio and representable track metadata';
const sessions: { close(): Promise<void> }[] = [];

function pidAbsent(pid: number): boolean {
  try { process.kill(pid, 0); return false; } catch (error) { return (error as NodeJS.ErrnoException).code === 'ESRCH'; }
}
function command(executable: string, args: string[]): Buffer {
  return execFileSync(executable, args, { timeout: 15_000, maxBuffer: 2 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
}
function makeMedia(directory: string, kind: 'multitrack' | 'narrated' | 'audio-only' | '10bit', voiceTitle = 'Narration', extraMetadata: string[] = []): string {
  const path = join(directory, `${kind}.mkv`);
  const args = ['-v', 'error', '-nostdin', '-n', '-threads', '1', '-filter_threads', '1'];
  if (kind === 'audio-only') {
    args.push('-f', 'lavfi', '-i', 'sine=frequency=997:sample_rate=48000:duration=3', '-c:a', 'pcm_s16le');
  } else {
    args.push('-f', 'lavfi', '-i', `color=c=red:s=160x90:r=10:d=${kind === 'multitrack' || kind === 'narrated' ? 6 : 3}`);
    if (kind === 'multitrack' || kind === 'narrated') {
      args.push('-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=mono', '-f', 'lavfi', '-i', `sine=frequency=997:sample_rate=${kind === 'narrated' ? 48000 : 32000}:duration=6`, '-map', '0:v:0', '-map', '1:a:0', '-map', '2:a:0', '-c:v', 'ffv1', '-pix_fmt', 'yuv420p', '-c:a', kind === 'narrated' ? 'aac' : 'pcm_s16le', '-t', '6');
      if (kind === 'narrated') args.push('-metadata:s:a:0', 'title=Silence', '-metadata:s:a:0', 'language=eng', '-metadata:s:a:1', `title=${voiceTitle}`, '-metadata:s:a:1', 'language=fra');
    } else {
      args.push('-vf', 'format=yuv420p10le', '-c:v', 'ffv1', '-pix_fmt', 'yuv420p10le', '-an');
    }
  }
  command(ffmpeg, [...args, ...extraMetadata, '-f', 'matroska', path]);
  const probe = JSON.parse(command(ffprobe, ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', path]).toString()) as { streams: { codec_type: string; pix_fmt?: string; color_transfer?: string }[] };
  if (kind === 'multitrack' || kind === 'narrated') {
    expect(probe.streams.filter(stream => stream.codec_type === 'audio')).toHaveLength(2);
    for (const track of [0, 1]) {
      const pcm = command(ffmpeg, ['-v', 'error', '-nostdin', '-i', path, '-map', `0:a:${track}`, '-t', '0.1', '-c:a', 'pcm_s16le', '-f', 's16le', '-']);
      expect(pcm.length).toBeGreaterThan(0);
      let peak = 0; for (let offset = 0; offset + 1 < pcm.length; offset += 2) peak = Math.max(peak, Math.abs(pcm.readInt16LE(offset)));
      if (track === 0) expect(peak).toBe(0); else expect(peak).toBeGreaterThan(1000);
    }
  } else if (kind === 'audio-only') {
    expect(probe.streams.every(stream => stream.codec_type === 'audio')).toBe(true);
  } else {
    expect(probe.streams[0]).toMatchObject({ pix_fmt: 'yuv420p10le' });
  }
  return path;
}
function text(result: CallToolResult): string {
  return result.content.map(part => part.type === 'text' ? part.text : '').join('\n');
}
function makeEditableTracks(directory: string, title = 'Game'): string {
  const path = join(directory, 'editable.mkv');
  command(ffmpeg, ['-v','error','-nostdin','-n','-threads','1','-filter_threads','1','-f','lavfi','-i','color=c=red:s=160x90:r=10:d=6','-f','lavfi','-i','sine=frequency=440:sample_rate=48000:duration=2,apad=whole_dur=6','-f','lavfi','-i','sine=frequency=997:sample_rate=44100:duration=2,adelay=4000,apad=whole_dur=6,aformat=channel_layouts=stereo','-map','0:v:0','-map','1:a:0','-map','2:a:0','-c:v','ffv1','-pix_fmt','yuv420p','-c:a','aac','-metadata:s:a:0',`title=${title}`,'-metadata:s:a:0','language=eng','-metadata:s:a:1','title=Voix – français','-metadata:s:a:1','language=fra','-t','6',path]);
  return path;
}
function inspectEditedTracks(path: string): void {
  const data = JSON.parse(command(ffprobe,['-v','error','-show_streams','-of','json',path]).toString()) as {streams:Record<string,unknown>[]};
  const tracks = data.streams.filter(stream => stream.codec_type==='audio');
  expect(tracks).toHaveLength(2);
  for(const [index,track] of tracks.entries()) {
    expect(track).toMatchObject({codec_name:'aac',sample_rate:index===0?'48000':'44100',channels:index===0?1:2,channel_layout:index===0?'mono':'stereo'});
    expect(track.tags).toMatchObject({language:index===0?'eng':'fra',handler_name:index===0?'Game':'Voix – français'});
    expect(track.disposition).toMatchObject({default:index===0?1:0});
    for(const active of [false,true]) {
      const time = index===0 ? (active?'0.4':'3.2') : (active?'3.2':'0.4');
      const pcm = command(ffmpeg,['-v','error','-nostdin','-ss',time,'-i',path,'-map',`0:a:${index}`,'-t','0.5','-c:a','pcm_s16le','-f','s16le','-']);
      let peak=0,crossings=0;
      for(let offset=0;offset+1<pcm.length;offset+=2){const value=pcm.readInt16LE(offset);peak=Math.max(peak,Math.abs(value));if(offset>0&&pcm.readInt16LE(offset-2)<=0&&value>0)crossings++;}
      if(active){expect(peak).toBeGreaterThan(1000);expect(crossings).toBeGreaterThan(index===0?205:480);expect(crossings).toBeLessThan(index===0?235:515);}
      else expect(peak).toBeLessThan(32);
    }
  }
  command(ffmpeg,['-v','error','-nostdin','-xerror','-err_detect','explode','-i',path,'-map','0:v:0','-map','0:a?','-f','null','-']);
}
function evidence(kind: string, legacy: boolean, args: Record<string, unknown>, source: CallToolResult, native: CallToolResult): void {
  if (process.env.MEDIA_WORKFLOW_EVIDENCE) appendFileSync(process.env.MEDIA_WORKFLOW_EVIDENCE, JSON.stringify({ kind, legacy, args, source, native }) + '\n');
}
function sameToolBody(native: CallToolResult, source: CallToolResult, legacy: boolean): void {
  const { _meta, ...body } = native;
  if (legacy) expect(_meta).toBeUndefined();
  else expect(_meta).toEqual({ 'io.modelcontextprotocol/serverInfo': { name: 'obs-mcp', version: PACKAGE_VERSION } });
  expect(body).toEqual(source);
}
async function pair(legacy: boolean, muteMediaDiagnostics = false, holdAudioEvidence = false) {
  const env = nativeDefaultFixture(root);
  const directory = env.OBS_MCP_STATE_DIR!;
  vi.stubEnv('TMPDIR',directory);
  const recordingDirectory = join(directory, 'recordings'); mkdirSync(recordingDirectory, { mode: 0o700 });
  let mediaExecutable=ffmpeg;const samplerMarker=join(directory,'sampler-calls.jsonl'),holderScript=join(directory,'audio-evidence-holder.cjs'),holderRecords=join(directory,'audio-evidence-holders.jsonl');
  type HeldAudio={pid:number;file:string;closed:string};
  const heldAudio=():HeldAudio[]=>existsSync(holderRecords)?readFileSync(holderRecords,'utf8').trim().split('\n').filter(Boolean).map(line=>JSON.parse(line) as HeldAudio):[];
  const joinAudioHolders=async()=>{
    for(const held of heldAudio()){
      if(!pidAbsent(held.pid)){
        const commandLine=execFileSync('/bin/ps',['-p',String(held.pid),'-o','args='],{encoding:'utf8'});if(!commandLine.includes(holderScript)||!commandLine.includes(held.file))throw Error('Audio holder identity changed; signal and cleanup withheld');process.kill(held.pid,'SIGTERM');
      }
      await vi.waitFor(()=>expect(pidAbsent(held.pid)).toBe(true),{timeout:5000});expect(JSON.parse(readFileSync(held.closed,'utf8'))).toEqual({descriptorClosed:true,filePresent:true});
    }
  };
  if(muteMediaDiagnostics||holdAudioEvidence){
    if(holdAudioEvidence)writeFileSync(holderScript,`const fs=require('node:fs');const file=process.argv[2],closed=process.argv[3],fd=fs.openSync(file,'r');const timer=setInterval(()=>fs.readSync(fd,Buffer.alloc(1),0,1,0),50);process.on('SIGTERM',()=>{clearInterval(timer);fs.closeSync(fd);fs.writeFileSync(closed,JSON.stringify({descriptorClosed:true,filePresent:fs.existsSync(file)}));process.exit(0);});process.send('ready');`);
    const bin=join(directory,'diagnostic-free-media');mkdirSync(bin);mediaExecutable=join(bin,'ffmpeg');
    writeFileSync(mediaExecutable,'#!/usr/bin/env node\n'+`
import{spawn}from'node:child_process';import{appendFileSync}from'node:fs';
const args=process.argv.slice(2),sampling=args.includes('rawvideo')&&args.includes('pipe:1');
if(sampling)appendFileSync(${JSON.stringify(samplerMarker)},JSON.stringify(args)+${JSON.stringify('\n')});
const child=spawn(${JSON.stringify(ffmpeg)},args,{stdio:['ignore','pipe','pipe']});child.stdout.pipe(process.stdout);child.stderr.resume();
process.on('SIGTERM',()=>child.kill('SIGTERM'));child.on('error',()=>{process.exitCode=1;});
child.on('close',code=>{
 if(code!==0||!sampling||!${JSON.stringify(holdAudioEvidence)}){process.exitCode=code??1;return;}
 const filter=args.find((_,i)=>args[i-1]==='-af'),raw=filter?.split('ametadata=mode=print:file=')[1];
 if(!raw){process.exitCode=8;return;}
 const unescape=value=>{let out='';for(let i=0;i<value.length;i++){if(value.charCodeAt(i)===92&&i+1<value.length)i++;out+=value[i];}return out;};const file=unescape(unescape(raw));
 const closed=${JSON.stringify(directory)}+'/audio-holder-closed-'+process.pid;
 const holder=spawn(process.execPath,[${JSON.stringify(holderScript)},file,closed],{detached:true,stdio:['ignore','inherit','inherit','ipc']});let ready=false;
 holder.once('exit',()=>{if(!ready)process.exitCode=9;});holder.once('message',message=>{if(message!=='ready'){process.exitCode=7;return;}ready=true;appendFileSync(${JSON.stringify(holderRecords)},JSON.stringify({pid:holder.pid,file,closed})+${JSON.stringify('\n')});holder.disconnect();holder.unref();process.exitCode=0;});
});
`);chmodSync(mediaExecutable,0o755);vi.stubEnv('PATH',`${bin}:${process.env.PATH??''}`);
  }

  const fake = await FakeOBSServer.start({ availableRequests: ['GetRecordDirectory','GetRecordStatus',...(holdAudioEvidence?['StartRecord','StopRecord','SaveReplayBuffer','GetReplayBufferStatus','GetLastReplayBufferReplay']:[])] });
  fake.respondWith('GetRecordDirectory', () => ({ recordDirectory: recordingDirectory }));
  fake.respondWith('GetRecordStatus', () => ({ outputActive: false, outputPaused: false }));
  const options = { versionNegotiation: { mode: legacy ? 'legacy' as const : { pin: '2026-07-28' as const } } };
  const native = new Client({ name: 'media-workflows', version: '1' }, options);
  const transport = new StdioClientTransport({ command: binary, args: [], cwd: root, env: { ...getDefaultEnvironment(), ...env, HOME: directory, TMPDIR: directory, OBS_WEBSOCKET_URL: fake.url, OBS_WEBSOCKET_PASSWORD: '', OBS_MCP_NATIVE_RECORDING_ROOT: recordingDirectory, OBS_MCP_NATIVE_FFMPEG: mediaExecutable, OBS_MCP_NATIVE_FFPROBE: ffprobe, OBS_MCP_NATIVE_PROFILE: 'production', OBS_MCP_TOOLSETS: '', OBS_MCP_TOOLS: holdAudioEvidence?'obs-contact-sheet,obs-trim-take,obs-take-start,obs-save-replay-verified,obs-get-record-status':'obs-contact-sheet,obs-trim-take', OBS_MCP_READ_ONLY: 'false', OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false', OBS_MCP_HTTP_PORT: '', OBS_MCP_LOG_LEVEL: 'silent' }, stderr: 'pipe' });
  transport.stderr?.on('data', () => undefined);
  const server = new McpServer({ name: 'canonical-source-media-oracle', version: '1' });
  initialize(server, { async sendRequest(request: string) { expect(request).toBe('GetRecordDirectory'); return { recordDirectory: recordingDirectory }; } } as unknown as OBSWebSocketClient);
  // Canonical Source callback through public MCP; modern serving-entry framing
  // belongs to the separate process parity suites. Native stdio uses both eras.
  const source = new Client({ name: 'media-workflows-source', version: '1' }, { versionNegotiation: { mode: 'legacy' } });
  const [clientSide, serverSide] = InMemoryTransport.createLinkedPair();
  let pid: number | null = null, closed = false, resolveClosed!: () => void;
  const closeEvent = new Promise<void>(done => { resolveClosed = done; });
  sessions.push({ async close() {
    await joinAudioHolders();
    await source.close(); await server.close();
    pid ??= transport.pid;
    try { await native.close(); await transport.close(); }
    finally {
      if (pid) {
        let timer: ReturnType<typeof setTimeout> | undefined;
        try { await Promise.race([closeEvent, new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error('Native close event was not joined')), 5000); })]); }
        finally { clearTimeout(timer); }
        expect(closed).toBe(true); expect(pidAbsent(pid)).toBe(true);
      }
      await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0), { timeout: 5000 });
      await fake.close();
    }
    expect(existsSync(env.OBS_MCP_TEST_NODE_TRAP!)).toBe(false);
  } });
  await Promise.all([server.connect(serverSide), source.connect(clientSide)]);
  await native.connect(transport); pid = transport.pid;
  const previousClose = transport.onclose;
  transport.onclose = () => { closed = true; resolveClosed(); previousClose?.(); };
  return { source, native, directory, recordingDirectory, fake, samplerMarker, heldAudio, joinAudioHolders };
}
afterEach(async () => { try {for (const session of sessions.splice(0)) await session.close();} finally {vi.unstubAllEnvs();} });

for (const legacy of [false, true]) describe(`actual-file public media safety (legacy=${legacy})`, () => {
  it('retains private audio evidence while an escaped holder keeps its descriptor and pipe alive',async()=>{
    const session=await pair(legacy,true,true),path=makeEditableTracks(session.recordingDirectory),before=readFileSync(path);
    const args={path,apply:false,minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false};
    const registered=(await session.native.listTools()).tools.map(tool=>tool.name);for(const name of['obs-trim-take','obs-take-start','obs-save-replay-verified','obs-get-record-status'])expect(registered).toContain(name);
    const samplers=()=>readFileSync(session.samplerMarker,'utf8').trim().split('\n').filter(Boolean).length;
    const assertNativeRetained=async()=>{
      const count=samplers(),frames=session.fake.history().length;
      for(const[name,arguments_]of[['obs-trim-take',args],['obs-take-start',{}],['obs-save-replay-verified',{timeoutSeconds:1}]]as const){const result=await session.native.callTool({name,arguments:arguments_});expect(result.isError,JSON.stringify(result)).toBe(true);expect(text(result)).toContain('(Uncertain)');}
      expect(samplers()).toBe(count);const requests=session.fake.history().slice(frames).filter(row=>row.frame.op===6);expect(requests).toEqual([]);for(const held of session.heldAudio())expect(existsSync(held.file)).toBe(true);
    };
    for(const[engine,client]of[['source',session.source],['native',session.native]]as const){
      const result=await client.callTool({name:'obs-trim-take',arguments:args});expect(result.isError,JSON.stringify(result)).toBe(true);expect(text(result)).toMatch(/cleanup.*unconfirmed|pipe.*(?:close|drain)/i);const held=session.heldAudio().at(-1)!;expect(held,engine).toBeDefined();expect(pidAbsent(held.pid)).toBe(false);expect(existsSync(held.file)).toBe(true);expect(readFileSync(held.file,'utf8')).toContain('lavfi.silence_');expect(readFileSync(path)).toEqual(before);if(process.env.MEDIA_WORKFLOW_EVIDENCE)appendFileSync(process.env.MEDIA_WORKFLOW_EVIDENCE,JSON.stringify({kind:'private-audio-custody-unjoined',legacy,engine,args,result,held})+'\n');
    }
    await assertNativeRetained();expect((await session.native.callTool({name:'obs-get-record-status',arguments:{}})).isError).not.toBe(true);
    const owned=session.heldAudio();expect(owned).toHaveLength(2);await session.joinAudioHolders();for(const held of owned){expect(existsSync(held.file)).toBe(true);expect(pidAbsent(held.pid)).toBe(true);}
    // Joining the fixture descendant supplies no new service authority.
    await assertNativeRetained();expect(samplers()).toBe(2);expect(session.fake.history().filter(row=>row.frame.op===6&&typeof row.frame.d.requestType==='string'&&['StartRecord','SaveReplayBuffer'].includes(row.frame.d.requestType))).toEqual([]);
  },30_000);
  it('finds shared silence from private audio files when every FFmpeg diagnostic is suppressed',async()=>{
    const session=await pair(legacy,true),path=makeEditableTracks(session.recordingDirectory),before=readFileSync(path);
    const args={path,apply:false,minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false};
    const source=await session.source.callTool({name:'obs-trim-take',arguments:args}),native=await session.native.callTool({name:'obs-trim-take',arguments:args});
    evidence('private-audio-without-diagnostics',legacy,args,source,native);expect(source.isError,JSON.stringify(source)).not.toBe(true);expect(native.isError,JSON.stringify(native)).not.toBe(true);sameToolBody(native,source,legacy);expect(source.structuredContent).toMatchObject({audioConsidered:true,removedSeconds:1.6});expect((source.structuredContent as {cuts:unknown[]}).cuts).toHaveLength(1);expect(readFileSync(path)).toEqual(before);
    const samplers=readFileSync(session.samplerMarker,'utf8').trim().split('\n').map(line=>JSON.parse(line) as string[]);expect(samplers).toHaveLength(2);for(const args of samplers){const filters=args.filter((_,i)=>args[i-1]==='-af');expect(filters).toHaveLength(2);for(const filter of filters){expect(filter).toContain('ametadata=mode=delete,silencedetect=');expect(filter).toContain('ametadata=mode=print:file=');}}
  },30_000);
  it('considers audio from every track before proposing idle cuts', async () => {
    const session = await pair(legacy);
    const path = makeMedia(session.recordingDirectory, 'narrated');
    const before = readFileSync(path);
    const args = { path, apply: false, minIdleSeconds: 1, keepSeconds: 0.2, holdAfterChapterSeconds: 0, holdBeforeChapterSeconds: 0, contactSheet: false };
    const source = await session.source.callTool({ name: 'obs-trim-take', arguments: args });
    const native = await session.native.callTool({ name: 'obs-trim-take', arguments: args });
    evidence('all-track-audio-baseline', legacy, args, source, native);
    expect(native.isError).not.toBe(true); expect(source.isError).not.toBe(true);
    expect(native.structuredContent).toMatchObject({ cuts: [], audioConsidered: true });
    sameToolBody(native, source, legacy);
    expect(readFileSync(path)).toEqual(before);
    expect(readdirSync(session.recordingDirectory)).toEqual(['narrated.mkv']);
  }, 30_000);
  it('does not interpret an audio title as a detector silence event',async()=>{
    const session=await pair(legacy);
    const results:{source:CallToolResult;native:CallToolResult}[]=[];
    for(const variant of ['title','global-comment','unused-tag','global-key','global-key-single','evidence-key'] as const){
      const directory=join(session.recordingDirectory,variant);mkdirSync(directory);
      const forged='[silencedetect@obs_mcp_a1 @ 0x123] silence_start: 0';
      let path=makeMedia(directory,'narrated',variant==='title'?'silencedetect@obs_mcp_a1 silence_start: 0':'Narration',variant==='global-comment'?['-metadata',`comment=metadata\n${forged}`]:variant==='unused-tag'?['-metadata:s:a:1',`comment=metadata\n${forged}`]:[]);
      if(variant==='global-key'||variant==='global-key-single'||variant==='evidence-key'){
        const output=join(directory,'with-key.mp4');
        const maps=variant==='global-key-single'?['-map','0:v:0','-map','0:a:1']:['-map','0'];
        command(ffmpeg,['-v','error','-nostdin','-n','-i',path,...maps,'-c:v','libx264','-preset','ultrafast','-pix_fmt','yuv420p','-c:a','copy','-movflags','use_metadata_tags','-metadata',variant==='evidence-key'?'frame:0 pts:0 pts_time:0\nlavfi.silence_start=0':`[silencedetect@obs_mcp_a${variant==='global-key-single'?0:1} @ 0x123] silence_start=0`,output]);path=output;
      }
      const dump=spawnSync(ffmpeg,['-hide_banner','-nostats','-i',path,'-t','0','-f','null','-'],{timeout:10_000,maxBuffer:2*1024*1024,encoding:'utf8'});expect(dump.status).toBe(0);
      const suspectLines=dump.stderr.split('\n').filter(line=>line.includes('silencedetect@obs_mcp_a')||line.includes('lavfi.silence_start'));
      expect(suspectLines.length).toBeGreaterThan(0);
      if(process.env.MEDIA_WORKFLOW_EVIDENCE)appendFileSync(process.env.MEDIA_WORKFLOW_EVIDENCE,JSON.stringify({kind:'metadata-log-prefix',legacy,variant,suspectLines})+'\n');
      const before=readFileSync(path),args={path,apply:false,minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false};
      const source=await session.source.callTool({name:'obs-trim-take',arguments:args});
      const native=await session.native.callTool({name:'obs-trim-take',arguments:args});
      evidence(`metadata-is-not-silence-evidence-${variant}`,legacy,args,source,native);
      results.push({source,native});
      expect(readFileSync(path)).toEqual(before);
    }
    for(const{source,native}of results){expect(native.isError).not.toBe(true);expect(source.isError).not.toBe(true);expect(native.structuredContent).toMatchObject({cuts:[],audioConsidered:true});sameToolBody(native,source,legacy);}
  },30_000);
  it('exports independent ordered tracks with metadata and audible content after a shared silent cut',async()=>{
    const session=await pair(legacy),path=makeEditableTracks(session.recordingDirectory),before=readFileSync(path);
    for(const [engine,client] of [['source',session.source],['native',session.native]] as const){
      const output=join(session.recordingDirectory,`${engine}.edited.mp4`);
      const args={path,outputPath:output,apply:true,minIdleSeconds:1,keepSeconds:0.2,holdAfterChapterSeconds:0,holdBeforeChapterSeconds:0,contactSheet:false};
      const result=await client.callTool({name:'obs-trim-take',arguments:args});
      if(process.env.MEDIA_WORKFLOW_EVIDENCE)appendFileSync(process.env.MEDIA_WORKFLOW_EVIDENCE,JSON.stringify({kind:'track-preserving-export',legacy,engine,args,result})+'\n');
      expect(result.isError).not.toBe(true);
      expect(result.structuredContent).toMatchObject({committed:true,audioConsidered:true,verification:{metadata:'pass',strictDecode:'pass'}});
      const structured=result.structuredContent as Record<string,unknown>;
      expect(structured.cuts as {start:number;end:number}[]).toHaveLength(1);
      const cut=(structured.cuts as {start:number;end:number}[])[0]!;
      expect(cut.start).toBeGreaterThan(2);expect(cut.start).toBeLessThan(2.4);expect(cut.end).toBeGreaterThan(3.7);expect(cut.end).toBeLessThan(4.1);
      inspectEditedTracks(output);expect(readFileSync(path)).toEqual(before);
    }
  },30_000);
  it('refuses the reserved SoundHandler title before either planning or export',async()=>{
    const session=await pair(legacy),path=makeEditableTracks(session.recordingDirectory,'SoundHandler'),before=readFileSync(path);
    const output=join(session.recordingDirectory,'human-output.mp4');writeFileSync(output,'human output must survive');const sentinel=readFileSync(output);
    for(const apply of[false,true]){
      const args={path,outputPath:output,apply,minIdleSeconds:1,contactSheet:false};
      const source=await session.source.callTool({name:'obs-trim-take',arguments:args});
      const native=await session.native.callTool({name:'obs-trim-take',arguments:args});
      evidence('reserved-audio-title',legacy,args,source,native);
      expect(native.isError).toBe(true);expect(source.isError).toBe(true);expect(text(native)).toBe(`Trim failed: ${multitrackMessage}`);sameToolBody(native,source,legacy);
      expect(readFileSync(path)).toEqual(before);expect(readFileSync(output)).toEqual(sentinel);expect(readdirSync(session.recordingDirectory).sort()).toEqual(['editable.mkv','human-output.mp4']);
    }
  },30_000);
  it('refuses intentional stream offsets before either planning or export',async()=>{
    const session=await pair(legacy),original=makeMedia(session.recordingDirectory,'narrated');
    const results:{source:CallToolResult;native:CallToolResult}[]=[];
    const output=join(session.recordingDirectory,'human-output.mp4');writeFileSync(output,'human output must survive');const sentinel=readFileSync(output);
    for(const offset of[0.5,2]){
      const path=join(session.recordingDirectory,`offset-${offset}.mkv`);
      command(ffmpeg,['-v','error','-nostdin','-n','-i',original,'-itsoffset',String(offset),'-i',original,'-map','0:v:0','-map','0:a:0','-map','1:a:1','-c','copy',path]);
      const probe=JSON.parse(command(ffprobe,['-v','error','-show_streams','-of','json',path]).toString()) as{streams:{codec_type:string;start_time?:string}[]};
      expect(Number(probe.streams.filter(stream=>stream.codec_type==='audio')[1]!.start_time)).toBeGreaterThan(offset-0.05);
      const before=readFileSync(path),names=readdirSync(session.recordingDirectory).sort();
      for(const apply of[false,true]){
        const args={path,outputPath:output,apply,minIdleSeconds:1,contactSheet:false};
        const source=await session.source.callTool({name:'obs-trim-take',arguments:args});
        const native=await session.native.callTool({name:'obs-trim-take',arguments:args});
        evidence('unsupported-stream-offset',legacy,args,source,native);
        results.push({source,native});
        expect(readFileSync(path)).toEqual(before);expect(readFileSync(output)).toEqual(sentinel);expect(readdirSync(session.recordingDirectory).sort()).toEqual(names);
      }
    }
    for(const{source,native}of results){expect(native.isError).toBe(true);expect(source.isError).toBe(true);expect(text(native)).toBe('Trim failed: Multitrack trim requires declared audio/video/format starts within 50 ms of zero and each other');sameToolBody(native,source,legacy);}
  },30_000);
  for (const apply of [false, true]) it(`refuses unqualified multitrack metadata before ${apply ? 'export' : 'planning'} while preserving files`, async () => {
    const session = await pair(legacy);
    const path = makeMedia(session.recordingDirectory, 'multitrack');
    const output = join(session.recordingDirectory, 'human-output.mp4'); writeFileSync(output, 'human output must survive');
    const inputBefore = readFileSync(path), outputBefore = readFileSync(output), namesBefore = readdirSync(session.recordingDirectory).sort();
    const args = { path, outputPath: output, apply, minIdleSeconds: 1, keepSeconds: 0.2, holdAfterChapterSeconds: 0, holdBeforeChapterSeconds: 0, contactSheet: false };
    const source = await session.source.callTool({ name: 'obs-trim-take', arguments: args });
    const native = await session.native.callTool({ name: 'obs-trim-take', arguments: args });
    evidence('multitrack', legacy, args, source, native);
    expect(readFileSync(path)).toEqual(inputBefore); expect(readFileSync(output)).toEqual(outputBefore);
    expect(readdirSync(session.recordingDirectory).sort()).toEqual(namesBefore);
    expect(native.isError).toBe(true); expect(source.isError).toBe(true);
    expect(text(native)).toBe(`Trim failed: ${multitrackMessage}`); sameToolBody(native, source, legacy);
    for (const client of [session.source, session.native]) expect((await client.listTools()).tools.find(tool => tool.name === 'obs-trim-take')?.description).toContain('Preserves up to six qualified mono/stereo 44.1/48 kHz audio tracks with near-zero timestamps as lossy AAC in MP4');
    expect(session.fake.history().every(row => row.frame.op === 1 || (row.frame.op === 6 && ['GetVersion', 'GetRecordStatus', 'GetRecordDirectory'].includes(String(row.frame.d.requestType))))).toBe(true);
  }, 30_000);

  for (const kind of ['corrupt', 'audio-only', '10bit'] as const) it(`refuses unsupported ${kind} export without replacing a sentinel`, async () => {
    const session = await pair(legacy);
    const path = kind === 'corrupt' ? join(session.recordingDirectory, 'corrupt.mkv') : makeMedia(session.recordingDirectory, kind);
    if (kind === 'corrupt') writeFileSync(path, 'not a media container');
    const output = join(session.recordingDirectory, 'human-output.mp4'); writeFileSync(output, 'human output must survive');
    const inputBefore = readFileSync(path), outputBefore = readFileSync(output), namesBefore = readdirSync(session.recordingDirectory).sort();
    const args = { path, outputPath: output, apply: true, minIdleSeconds: 1, contactSheet: false };
    const source = await session.source.callTool({ name: 'obs-trim-take', arguments: args });
    const native = await session.native.callTool({ name: 'obs-trim-take', arguments: args });
    evidence(kind, legacy, args, source, native);
    expect(native.isError).toBe(true); expect(source.isError).toBe(true);
    if (kind === '10bit') { expect(text(native)).toBe('Trim failed: Trim export requires a known 8-bit pixel format; got yuv420p10le'); sameToolBody(native, source, legacy); }
    else if (kind === 'audio-only') { expect(text(native)).toContain('known 8-bit pixel format'); sameToolBody(native, source, legacy); }
    else { expect(text(native)).toContain('Trim failed:'); expect(text(source)).toContain('Trim failed:'); }
    expect(readFileSync(path)).toEqual(inputBefore); expect(readFileSync(output)).toEqual(outputBefore);
    expect(readdirSync(session.recordingDirectory).sort()).toEqual(namesBefore);
    expect(session.fake.history().every(row => row.frame.op === 1 || (row.frame.op === 6 && ['GetVersion', 'GetRecordStatus', 'GetRecordDirectory'].includes(String(row.frame.d.requestType))))).toBe(true);
  }, 30_000);

  it('rejects a disguised network playlist in both public media tools before secondary HTTP access', async () => {
    const session = await pair(legacy);
    let requests = 0;
    const sockets = new Set<Socket>();
    const server = createServer({ maxHeaderSize: 16 * 1024 }, (_request, response) => { requests++; response.writeHead(404); response.end('owned forbidden secondary input'); });
    server.on('connection', socket => { sockets.add(socket); socket.once('close', () => sockets.delete(socket)); });
    try {
      await new Promise<void>((done, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', done); });
      const address = server.address(); expect(address).not.toBeNull();
      if (!address || typeof address === 'string') throw new Error('Owned sentinel did not bind TCP');
      const path = join(session.recordingDirectory, 'looks-like-recording.mkv');
      writeFileSync(path, `#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:1\n#EXT-X-MEDIA-SEQUENCE:0\n#EXT-X-PLAYLIST-TYPE:VOD\n#EXTINF:1.0,\nhttp://127.0.0.1:${address.port}/segment.ts\n#EXT-X-ENDLIST\n`);
      const output = join(session.recordingDirectory, 'human-output.mp4'); writeFileSync(output, 'human output must survive');
      const inputBefore = readFileSync(path), outputBefore = readFileSync(output), namesBefore = readdirSync(session.recordingDirectory).sort();
      for (const tool of ['obs-contact-sheet', 'obs-trim-take']) {
        const args = tool === 'obs-contact-sheet' ? { path, times: [0], width: 160 } : { path, outputPath: output, apply: true, contactSheet: false };
        const source = await session.source.callTool({ name: tool, arguments: args });
        const native = await session.native.callTool({ name: tool, arguments: args });
        evidence(`disguised-playlist-${tool}`, legacy, args, source, native);
        for (const result of [source, native]) {
          expect(result.isError).toBe(true);
          expect(text(result)).toContain(tool === 'obs-contact-sheet' ? 'Contact sheet failed:' : 'Trim failed:');
          expect(result.content.every(part => part.type === 'text')).toBe(true);
        }
        expect(requests).toBe(0);
        expect(readFileSync(path)).toEqual(inputBefore); expect(readFileSync(output)).toEqual(outputBefore);
        expect(readdirSync(session.recordingDirectory).sort()).toEqual(namesBefore);
      }
      expect(session.fake.history().every(row => row.frame.op === 1 || (row.frame.op === 6 && ['GetVersion', 'GetRecordStatus', 'GetRecordDirectory'].includes(String(row.frame.d.requestType))))).toBe(true);
    } finally {
      for (const socket of sockets) socket.destroy();
      if (server.listening) await new Promise<void>((done, reject) => server.close(error => error ? reject(error) : done()));
      expect(server.listening).toBe(false); expect(sockets.size).toBe(0); expect(requests).toBe(0);
    }
  }, 30_000);
});
