/* SPDX-License-Identifier: GPL-2.0-only */
// Bounded native-only frontend admission control; all output effects are fake.
import { execFileSync, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { mkdirSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import { fileURLToPath } from 'node:url';
import { getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { expect, it, vi } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const WATCH = 10_000;
let sequence = 0;
type Rpc = { id?: number; result?: Record<string, unknown>; error?: { code: number; message: string } };
type Wire = Awaited<ReturnType<FakeOBSServer['waitForRequest']>>;
function meta() {
  return {
    'io.modelcontextprotocol/protocolVersion': '2026-07-28',
    'io.modelcontextprotocol/clientInfo': { name: 'public-stop-progress', version: '1' },
    'io.modelcontextprotocol/clientCapabilities': {},
  };
}
function receipts(directory: string): Record<string, unknown>[] {
  try { return readdirSync(join(directory, 'operation-receipts')).filter(name => name.endsWith('.json'))
    .map(name => JSON.parse(readFileSync(join(directory, 'operation-receipts', name), 'utf8')) as Record<string, unknown>); }
  catch { return []; }
}
function save(name: string, value: unknown) {
  const directory = process.env.OBS_PUBLIC_STOP_PROGRESS_EVIDENCE_DIR;
  if (!directory) return;
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, `${name}.json`), JSON.stringify(value, null, 2) + '\n');
}
class Raw {
  readonly child: ChildProcessWithoutNullStreams;
  readonly rows = new Map<number, Rpc>();
  readonly closed: Promise<{ code: number | null; signal: string | null }>;
  protocolError: string | undefined;
  private text = '';
  private decoder = new StringDecoder('utf8');
  private bytes = 0;
  constructor(readonly fake: FakeOBSServer, readonly directory: string, readonly legacy: boolean) {
    this.child = spawn(binary, [], { cwd: root, stdio: ['pipe', 'pipe', 'pipe'], env: {
      ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory,
      OBS_MCP_NATIVE_STATEFUL: '1', OBS_MCP_NATIVE_RECORDING_ROOT: join(directory, 'recordings'),
      OBS_MCP_COMPAT_NODE: join(directory, 'missing-node'), OBS_MCP_COMPAT_ENTRY: join(directory, 'missing-sdk'),
      OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '', OBS_MCP_READ_ONLY: 'false',
      OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false', OBS_MCP_READ_OBS_CONFIG: 'false',
      OBS_MCP_HTTP_PORT: '', OBS_MCP_HTTP_TOKEN: '',
    } });
    this.child.stderr.on('data', () => undefined);
    this.child.stdin.on('error', () => undefined);
    this.child.stdout.on('data', (chunk: Buffer) => {
      this.bytes += chunk.length;
      if (this.bytes > 1024 * 1024) { this.protocolError = 'bounded capture exceeded'; return; }
      this.text += this.decoder.write(chunk);
      let newline: number;
      while ((newline = this.text.indexOf('\n')) >= 0) {
        const line = this.text.slice(0, newline); this.text = this.text.slice(newline + 1);
        if (!line.trim()) continue;
        try { const frame = JSON.parse(line) as Rpc; if (typeof frame.id === 'number') this.rows.set(frame.id, frame); }
        catch { this.protocolError = 'invalid JSON stdout'; }
      }
    });
    this.child.stdout.once('end', () => { if (this.text.trim()) this.protocolError = 'incomplete JSON stdout'; });
    this.closed = new Promise(resolve => this.child.once('close', (code, signal) => resolve({ code, signal })));
  }
  frame(id: number, name: string, args: Record<string, unknown> = {}) {
    return { jsonrpc: '2.0', id, method: 'tools/call', params: {
      name, arguments: args, ...(this.legacy ? {} : { _meta: meta() }),
    } };
  }
  async batch(frames: unknown[]) {
    const bytes = frames.map(frame => JSON.stringify(frame) + '\n').join('');
    expect(Buffer.byteLength(bytes)).toBeLessThan(32 * 1024);
    await new Promise<void>((resolve, reject) => this.child.stdin.write(bytes, error => error ? reject(error) : resolve()));
  }
  async response(id: number): Promise<Rpc> {
    await vi.waitFor(() => { expect(this.protocolError).toBeUndefined(); expect(this.rows.has(id)).toBe(true); }, { timeout: WATCH });
    return this.rows.get(id)!;
  }
  async call(id: number, name: string, args: Record<string, unknown> = {}) {
    await this.batch([this.frame(id, name, args)]); return this.response(id);
  }
  async initialize() {
    if (!this.legacy) return;
    await this.batch([{ jsonrpc: '2.0', id: 1, method: 'initialize', params: {
      protocolVersion: '2025-11-25', clientInfo: { name: 'public-stop-progress', version: '1' }, capabilities: {},
    } }]);
    expect((await this.response(1)).error).toBeUndefined();
    await this.batch([{ jsonrpc: '2.0', method: 'notifications/initialized' }]);
  }
  async close() {
    this.child.stdin.end();
    try {
      try { await vi.waitFor(() => expect(this.child.exitCode !== null || this.child.signalCode !== null).toBe(true), { timeout: 6000 }); }
      catch (error) { this.child.kill('SIGKILL'); await this.closed; throw error; }
      const result = await this.closed;
      expect(result).toEqual({ code: 0, signal: null });
      expect(this.protocolError).toBeUndefined();
      await vi.waitFor(() => expect(this.fake.openConnectionCount).toBe(0));
      return result;
    } finally { await this.fake.close(); rmSync(this.directory, { recursive: true, force: true }); }
  }
}
for (const legacy of [false, true]) it(`admits independent public and owned Stop slots with32 ordinary local timers (legacy=${legacy})`, async () => {
  const directory = join(realpathSync(join(root, 'native/target')), `public-stop-progress-${process.pid}-${sequence++}`);
  mkdirSync(directory, { mode: 0o700 }); mkdirSync(join(directory, 'recordings'), { mode: 0o700 });
  const fake = await FakeOBSServer.start({ platform: 'macos', supportedImageFormats: ['png'] });
  const state = serveProduction(fake); state.preview = 'Main';
  const path = join(directory, 'recordings/clip.mkv'); let recording = false;
  fake.respondWith('GetProfileParameter', data => ({ parameterValue: ({
    'Output/Mode': 'Simple', 'SimpleOutput/RecQuality': 'Small', 'SimpleOutput/RecEncoder': 'apple_h264',
    'SimpleOutput/RecTracks': '1', 'SimpleOutput/RecFormat2': 'mkv',
  } as Record<string, string>)[`${data.parameterCategory}/${data.parameterName}`] ?? null }));
  fake.respondWith('GetRecordDirectory', () => ({ recordDirectory: join(directory, 'recordings') }));
  fake.respondWith('GetStats', () => ({ availableDiskSpace: 50000, renderSkippedFrames: 0, renderTotalFrames: 0 }));
  fake.respondWith('GetRecordStatus', () => ({ outputActive: recording, outputPaused: false, outputBytes: 1048576 }));
  fake.respondWith('StartRecord', () => { recording = true; fake.sendEvent('RecordStateChanged', {
    outputActive: true, outputState: 'OBS_WEBSOCKET_OUTPUT_STARTED', outputPath: path,
  }); return {}; });
  fake.respondWith('StopRecord', () => { recording = false; fake.sendEvent('RecordStateChanged', {
    outputActive: false, outputState: 'OBS_WEBSOCKET_OUTPUT_STOPPED', outputPath: path,
  }); return { outputPath: path }; });
  fake.respondWith('CreateRecordChapter', () => ({}));
  fake.respondWith('StopVirtualCam', () => ({ ignored: true, outputActive: true }));
  fake.queueSuccess('StopVirtualCam', {}, 60_000);
  // All responders/capabilities are established before the child connects.
  const raw = new Raw(fake, directory, legacy);
  const sleepIds = Array.from({ length: 32 }, (_, index) => 100 + index);
  let started = false, sentOwned = false, publicHeld: Wire | undefined, released = false;
  let phase = 'setup', failure: unknown, ordinaryRefusal: Rpc | undefined, publicReply: Rpc | undefined, ownedReply: Rpc | undefined;
  let physicalOwned = false, timersPendingAtStop = false, durable: Record<string, unknown>[] = [], cleanup: unknown;
  const release = () => {
    if (released || !publicHeld || !fake.openConnectionCount) return;
    fake.sendRaw({ op: 7, d: { requestType: 'StopVirtualCam', requestId: publicHeld.frame.d.requestId,
      requestStatus: { result: true, code: 100 }, responseData: { outputActive: true } } }, publicHeld.connectionId); released = true;
  };
  try {
    await raw.initialize();
    const begin = await raw.call(10, 'obs-take-start', { expectSilent: true, maxDurationSeconds: 30 });
    expect(begin.error).toBeUndefined(); expect(begin.result?.isError).not.toBe(true); expect(begin.result?.structuredContent).toHaveProperty('takeId'); expect(recording).toBe(true); started = true;
    const after = fake.cursor();
    // One bounded stdin write makes all32 effect-free jobs precede the sentinel.
    await raw.batch([...sleepIds.map(id => raw.frame(id, 'obs-sleep', { sleepMillis: 50000 })), raw.frame(200, 'obs-get-status')]);
    ordinaryRefusal = await raw.response(200);
    expect(ordinaryRefusal.error?.message).toContain('Native request admission unavailable');
    expect(sleepIds.some(id => raw.rows.has(id))).toBe(false);
    phase = 'ordinary32-admitted';
    await raw.batch([raw.frame(300, 'obs-stop-virtual-cam')]);
    publicHeld = await fake.waitForRequest('StopVirtualCam', { after, timeoutMs: 2500 }).catch(() => undefined);
    publicReply = raw.rows.get(300);
    if (publicHeld) {
      phase = 'public-physical-ack-held';
      expect(publicReply).toBeUndefined();
      await raw.batch([raw.frame(301, 'obs-take-stop')]); sentOwned = true;
      const physical = await fake.waitForRequest('StopRecord', { after: publicHeld.cursor, timeoutMs: 2500 }).then(() => true, () => false);
      physicalOwned = physical; timersPendingAtStop = sleepIds.every(id => !raw.rows.has(id));
      if (physical) {
        phase = 'owned-physical-before-public-ack';
        expect(raw.rows.has(300)).toBe(false); expect(timersPendingAtStop).toBe(true);
        const refused = await raw.call(201, 'obs-get-status'); expect(refused.error?.message).toContain('Native request admission unavailable');
      }
    }
  } catch (error) { failure = error; }
  finally {
    release();
    try {
      // Cancellation suppresses timer RPC replies; subsequent successful local
      // admission and process close witness that these same jobs were drained.
      await raw.batch(sleepIds.map(requestId => ({ jsonrpc: '2.0', method: 'notifications/cancelled', params: { requestId, reason: 'owned fixture cleanup' } })));
      let next = 500;
      await vi.waitFor(async () => { const result = await raw.call(next++, 'obs-get-status'); expect(result.error).toBeUndefined(); }, { timeout: 5000 });
      if (publicHeld) publicReply = await raw.response(300);
      if (sentOwned) ownedReply = await raw.response(301);
      // The SAME first owned Stop reply is recorded before any idempotent call.
      if (started && recording) await raw.call(600, 'obs-take-stop');
      expect(recording).toBe(false);
      durable = receipts(directory);
      const childRows = execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8' }).trim().split('\n')
        .map(line => line.trim().split(/\s+/).map(Number)).filter(([, parent]) => parent === raw.child.pid);
      expect(childRows).toEqual([]);
    } finally { cleanup = await raw.close(); }
  }
  save(`frontend34-${legacy ? 'legacy' : 'modern'}`, { phase, legacy, ordinaryRefusal, publicPhysicalBeforeRelease: Boolean(publicHeld), publicReply,
    physicalOwnedBeforePublicAck: physicalOwned, timersPendingAtStop, originalOwnedReply: ownedReply, durable, cleanup, helperJoined: true });
  if (failure) throw failure;
  expect(publicHeld, 'public Stop must have its own frontend admission while32 ordinary jobs remain occupied').toBeDefined();
  expect(publicReply?.error).toBeUndefined(); expect(publicReply?.result).toMatchObject({
    content: [{ type: 'text', text: 'Virtual camera stopped' }], structuredContent: { message: 'Virtual camera stopped' },
  });
  expect(physicalOwned, 'owned Stop must retain its independent slot before public Stop ACK release').toBe(true);
  expect(ownedReply?.error).toBeUndefined(); expect(ownedReply?.result?.structuredContent).toMatchObject({ stopConfirmed: true });
  expect(durable.find(row => row.requestType === 'StopVirtualCam')).toMatchObject({ phase: 'accepted', requestAccepted: true, postconditionObserved: false });
  expect(fake.history().filter(row => row.frame.op === 6 && row.frame.d.requestType === 'StopVirtualCam')).toHaveLength(1);
  expect(fake.history().filter(row => row.frame.op === 6 && row.frame.d.requestType === 'StopRecord')).toHaveLength(1);
  expect(fake.connectionCount).toBe(1);
});
