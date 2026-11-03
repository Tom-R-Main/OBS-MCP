/* SPDX-License-Identifier: GPL-2.0-only */
import { appendFileSync, existsSync, mkdirSync, readFileSync, readdirSync, realpathSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
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
const multitrackMessage = 'Trim supports at most one audio track; multitrack input requires an explicit track-preserving workflow';
const sessions: { close(): Promise<void> }[] = [];

function pidAbsent(pid: number): boolean {
  try { process.kill(pid, 0); return false; } catch (error) { return (error as NodeJS.ErrnoException).code === 'ESRCH'; }
}
function command(executable: string, args: string[]): Buffer {
  return execFileSync(executable, args, { timeout: 15_000, maxBuffer: 2 * 1024 * 1024, stdio: ['ignore', 'pipe', 'pipe'] });
}
function makeMedia(directory: string, kind: 'multitrack' | 'audio-only' | '10bit'): string {
  const path = join(directory, `${kind}.mkv`);
  const args = ['-v', 'error', '-nostdin', '-n', '-threads', '1', '-filter_threads', '1'];
  if (kind === 'audio-only') {
    args.push('-f', 'lavfi', '-i', 'sine=frequency=997:sample_rate=48000:duration=3', '-c:a', 'pcm_s16le');
  } else {
    args.push('-f', 'lavfi', '-i', `color=c=red:s=160x90:r=10:d=${kind === 'multitrack' ? 6 : 3}`);
    if (kind === 'multitrack') {
      args.push('-f', 'lavfi', '-i', 'anullsrc=r=48000:cl=mono', '-f', 'lavfi', '-i', 'sine=frequency=997:sample_rate=48000:duration=6', '-map', '0:v:0', '-map', '1:a:0', '-map', '2:a:0', '-c:v', 'ffv1', '-pix_fmt', 'yuv420p', '-c:a', 'pcm_s16le', '-t', '6');
    } else {
      args.push('-vf', 'format=yuv420p10le', '-c:v', 'ffv1', '-pix_fmt', 'yuv420p10le', '-an');
    }
  }
  command(ffmpeg, [...args, '-f', 'matroska', path]);
  const probe = JSON.parse(command(ffprobe, ['-v', 'error', '-show_streams', '-show_format', '-of', 'json', path]).toString()) as { streams: { codec_type: string; pix_fmt?: string; color_transfer?: string }[] };
  if (kind === 'multitrack') {
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
function evidence(kind: string, legacy: boolean, args: Record<string, unknown>, source: CallToolResult, native: CallToolResult): void {
  if (process.env.MEDIA_WORKFLOW_EVIDENCE) appendFileSync(process.env.MEDIA_WORKFLOW_EVIDENCE, JSON.stringify({ kind, legacy, args, source, native }) + '\n');
}
function sameToolBody(native: CallToolResult, source: CallToolResult, legacy: boolean): void {
  const { _meta, ...body } = native;
  if (legacy) expect(_meta).toBeUndefined();
  else expect(_meta).toEqual({ 'io.modelcontextprotocol/serverInfo': { name: 'obs-mcp', version: PACKAGE_VERSION } });
  expect(body).toEqual(source);
}
async function pair(legacy: boolean) {
  const env = nativeDefaultFixture(root);
  const directory = env.OBS_MCP_STATE_DIR!;
  const recordingDirectory = join(directory, 'recordings'); mkdirSync(recordingDirectory, { mode: 0o700 });
  const fake = await FakeOBSServer.start({ availableRequests: ['GetRecordDirectory', 'GetRecordStatus'] });
  fake.respondWith('GetRecordDirectory', () => ({ recordDirectory: recordingDirectory }));
  fake.respondWith('GetRecordStatus', () => ({ outputActive: false, outputPaused: false }));
  const options = { versionNegotiation: { mode: legacy ? 'legacy' as const : { pin: '2026-07-28' as const } } };
  const native = new Client({ name: 'media-workflows', version: '1' }, options);
  const transport = new StdioClientTransport({ command: binary, args: [], cwd: root, env: { ...getDefaultEnvironment(), ...env, HOME: directory, TMPDIR: directory, OBS_WEBSOCKET_URL: fake.url, OBS_WEBSOCKET_PASSWORD: '', OBS_MCP_NATIVE_RECORDING_ROOT: recordingDirectory, OBS_MCP_NATIVE_FFMPEG: ffmpeg, OBS_MCP_NATIVE_FFPROBE: ffprobe, OBS_MCP_NATIVE_PROFILE: 'production', OBS_MCP_TOOLSETS: '', OBS_MCP_TOOLS: 'obs-contact-sheet,obs-trim-take', OBS_MCP_READ_ONLY: 'false', OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false', OBS_MCP_HTTP_PORT: '', OBS_MCP_LOG_LEVEL: 'silent' }, stderr: 'pipe' });
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
  return { source, native, directory, recordingDirectory, fake };
}
afterEach(async () => { for (const session of sessions.splice(0)) await session.close(); });

for (const legacy of [false, true]) describe(`actual-file public media safety (legacy=${legacy})`, () => {
  for (const apply of [false, true]) it(`refuses multitrack before ${apply ? 'export' : 'planning'} while preserving files`, async () => {
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
    for (const client of [session.source, session.native]) expect((await client.listTools()).tools.find(tool => tool.name === 'obs-trim-take')?.description).toContain('Supports video-only or one audio track; multitrack input is refused');
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
