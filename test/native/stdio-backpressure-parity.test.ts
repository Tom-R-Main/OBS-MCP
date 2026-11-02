/* SPDX-License-Identifier: GPL-2.0-only */
// Root-authorized bounded raw-stdio regression; fake OBS only.
import { execFile, spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { StringDecoder } from 'node:string_decoder';
import { fileURLToPath } from 'node:url';
import { promisify } from 'node:util';
import { getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { expect, it } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const sdk = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, 'build/index.js');
const execFileAsync = promisify(execFile);
const MAX_LIST_REPLIES = 32;
const MAX_CAPTURE_BYTES = 16 * 1024 * 1024;
const WATCH = 10_000;
let sequence = 0;
type Frame = { value: Record<string, unknown>; bytes: number };
type Settled = { frame: Frame } | { error: string };
function bounded<T>(promise: Promise<T>, ms: number, label: string): Promise<T> {
  let timer: ReturnType<typeof setTimeout>;
  return Promise.race([
    promise,
    new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(label)), ms); }),
  ]).finally(() => clearTimeout(timer!));
}
function meta() {
  return {
    'io.modelcontextprotocol/protocolVersion': '2026-07-28',
    'io.modelcontextprotocol/clientInfo': { name: 'stdio-pressure', version: '1' },
    'io.modelcontextprotocol/clientCapabilities': {},
  };
}
class RawChild {
  readonly child: ChildProcessWithoutNullStreams;
  readonly frames = new Map<number, Frame>();
  readonly pending = new Map<number, { resolve: (frame: Frame) => void; reject: (error: Error) => void }>();
  readonly closed: Promise<{ code: number | null; signal: string | null }>;
  readonly directory: string;
  readonly fake: FakeOBSServer;
  readonly native: boolean;
  readonly legacy: boolean;
  readonly startAt = performance.now();
  readonly recording: () => boolean;
  stdoutBytes = 0;
  stdoutProtocolError: string | undefined;
  private text = '';
  private decoder = new StringDecoder('utf8');
  private nextId = 100;
  private stoppedReading = () => {};
  private constructor(fake: FakeOBSServer, directory: string, native: boolean, legacy: boolean, recording: () => boolean, private readonly captureLimit: number) {
    this.fake = fake; this.directory = directory; this.native = native; this.legacy = legacy; this.recording = recording;
    this.child = spawn(native ? binary : process.execPath, native ? [] : [sdk], {
      cwd: root, stdio: ['pipe', 'pipe', 'pipe'],
      env: {
        ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory,
        OBS_MCP_NATIVE_STATEFUL: native ? '1' : '0', OBS_MCP_NATIVE_RECORDING_ROOT: join(directory, 'recordings'),
        OBS_MCP_COMPAT_NODE: join(directory, 'missing-node'), OBS_MCP_COMPAT_ENTRY: join(directory, 'missing-sdk.js'),
        OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '', OBS_MCP_READ_ONLY: 'false', OBS_MCP_DYNAMIC_TOOLSETS: 'false',
        OBS_MCP_CONFIRM_LIVE: 'false', OBS_MCP_READ_OBS_CONFIG: 'false', OBS_MCP_HTTP_PORT: '', OBS_MCP_HTTP_TOKEN: '',
      },
    });
    this.child.stderr.on('data', () => undefined);
    this.child.stdin.on('error', () => undefined);
    this.child.stdout.on('error', error => { for (const waiter of this.pending.values()) waiter.reject(error); this.pending.clear(); });
    this.child.stdout.on('data', (bytes: Buffer) => {
      this.stdoutBytes += bytes.length;
      if (this.stdoutBytes > this.captureLimit) {
        this.stdoutProtocolError = 'bounded stdout capture exceeded';
        for (const waiter of this.pending.values()) waiter.reject(new Error('bounded stdout capture exceeded'));
        return;
      }
      this.text += this.decoder.write(bytes);
      let end: number;
      while ((end = this.text.indexOf('\n')) >= 0) {
        const line = this.text.slice(0, end); this.text = this.text.slice(end + 1);
        if (!line.trim()) continue;
        let value: Record<string, unknown>;
        try { value = JSON.parse(line) as Record<string, unknown>; }
        catch { this.stdoutProtocolError = 'invalid JSON stdout frame'; for (const waiter of this.pending.values()) waiter.reject(new Error('invalid JSON stdout frame')); this.pending.clear(); return; }
        if (typeof value.id !== 'number') continue;
        const frame = { value, bytes: Buffer.byteLength(line) + 1 };
        this.frames.set(value.id, frame); this.pending.get(value.id)?.resolve(frame); this.pending.delete(value.id);
      }
    });
    this.child.stdout.once('end', () => {
      if (this.text.trim()) this.stdoutProtocolError = 'incomplete stdout frame';
    });
    // close follows stdio drain; exit alone can precede the final pipe bytes.
    this.closed = new Promise(resolve => this.child.once('close', (code, signal) => {
      for (const waiter of this.pending.values()) waiter.reject(new Error(`child exited ${code ?? signal}`));
      this.pending.clear(); resolve({ code, signal });
    }));
  }
  static async create(native: boolean, legacy: boolean, captureLimit = MAX_CAPTURE_BYTES) {
    const fake = await FakeOBSServer.start({ platform: 'macos', supportedImageFormats: ['png'] });
    const state = serveProduction(fake); state.preview = 'Main';
    const directory = join(root, 'native/target', `stdio-pressure-${process.pid}-${sequence++}`);
    mkdirSync(directory, { mode: 0o700 }); mkdirSync(join(directory, 'recordings'), { mode: 0o700 });
    const path = join(directory, 'recordings/clip.mkv');
    let active = false, ticks = 0, recordDirectory = join(directory, 'recordings');
    fake.respondWith('GetProfileParameter', data => ({ parameterValue: ({
      'Output/Mode': 'Simple', 'SimpleOutput/RecQuality': 'Small', 'SimpleOutput/RecEncoder': 'apple_h264',
      'SimpleOutput/RecTracks': '1', 'SimpleOutput/RecFormat2': 'mkv',
    } as Record<string, string>)[`${data.parameterCategory}/${data.parameterName}`] ?? null }));
    fake.respondWith('SetRecordDirectory', data => { recordDirectory = String(data.recordDirectory); return {}; });
    fake.respondWith('GetRecordDirectory', () => ({ recordDirectory }));
    fake.respondWith('GetRecordStatus', () => ({ outputActive: active, outputPaused: false, outputPath: path, outputBytes: 1_048_576 + ticks++ * 4096 }));
    fake.respondWith('GetStats', () => ({ availableDiskSpace: 50000, renderSkippedFrames: 0, renderTotalFrames: ++ticks }));
    fake.respondWith('StartRecord', () => {
      active = true; fake.sendEvent('RecordStateChanged', { outputActive: true, outputState: 'OBS_WEBSOCKET_OUTPUT_STARTED', outputPath: path }); return {};
    });
    fake.respondWith('StopRecord', () => {
      active = false; fake.sendEvent('RecordStateChanged', { outputActive: false, outputState: 'OBS_WEBSOCKET_OUTPUT_STOPPED', outputPath: path }); return { outputPath: path };
    });
    fake.respondWith('CreateRecordChapter', () => ({}));
    fake.respondWith('OpenInputPropertiesDialog', () => ({}));
    const session = new RawChild(fake, directory, native, legacy, () => active, captureLimit);
    try { if (legacy) {
      const response = session.request('initialize', { protocolVersion: '2025-11-25', clientInfo: { name: 'stdio-pressure', version: '1' }, capabilities: {} });
      await response.sent; await bounded(response.result, WATCH, 'legacy initialization stalled');
      await session.write({ jsonrpc: '2.0', method: 'notifications/initialized' });
    }
    return session; } catch (error) { await session.cleanup(); throw error; }
  }
  async write(value: unknown) {
    await new Promise<void>((resolve, reject) => this.child.stdin.write(JSON.stringify(value) + '\n', error => error ? reject(error) : resolve()));
  }
  request(method: string, params: Record<string, unknown>) {
    const id = this.nextId++;
    const response = new Promise<Frame>((resolve, reject) => this.pending.set(id, { resolve, reject }));
    // Every request gets an immediate rejection handler, including unread list responses.
    const result: Promise<Settled> = response.then(frame => ({ frame }), error => ({ error: String(error) }));
    const sent = this.write({ jsonrpc: '2.0', id, method, params: this.legacy ? params : { ...params, _meta: meta() } });
    return { id, sent, result };
  }
  tool(name: string, args: Record<string, unknown> = {}) { return this.request('tools/call', { name, arguments: args }); }
  async startTake() {
    const pending = this.tool('obs-take-start', { expectSilent: true, maxDurationSeconds: 30 }); await pending.sent;
    const result = await bounded(pending.result, WATCH, 'owned take did not start');
    if (!('frame' in result)) throw new Error(result.error);
    expect((result.frame.value.result as { isError?: boolean }).isError).not.toBe(true);
    expect(this.recording()).toBe(true); expect(this.packets('StopRecord')).toHaveLength(0);
  }
  packets(name: string) { return this.fake.history().filter(row => row.frame.op === 6 && row.frame.d.requestType === name); }
  pause() { this.child.stdout.pause(); }
  async bufferedHighWaterMark() {
    const stream = this.child.stdout;
    const ready = () => stream.isPaused() && stream.readableLength >= stream.readableHighWaterMark;
    if (ready()) return;
    await bounded(new Promise<void>((resolve, reject) => {
      // Keep readable mode attached until explicit resume; removing the listener
      // here lets Node resume flow through the existing data listener.
      const inspect = () => { if (ready()) resolve(); };
      const ended = () => { cleanup(); reject(new Error('stdout ended before its unread high water mark')); };
      const cleanup = () => { stream.off('readable', inspect); stream.off('end', ended); };
      this.stoppedReading = cleanup; stream.on('readable', inspect); stream.once('end', ended); inspect();
    }), WATCH, 'unread stdout never reached its actual high water mark');
  }
  resume() { this.stoppedReading(); this.stoppedReading = () => {}; this.child.stdout.resume(); }
  async cleanup(assertCleanExit = true) {
    this.resume(); this.child.stdin.end();
    let exit;
    try { exit = await bounded(this.closed, 6000, 'owned helper did not exit after draining'); }
    catch (error) { this.child.kill('SIGKILL'); await bounded(this.closed, 2000, 'owned helper could not be reaped'); throw error; }
    finally { await this.fake.close(); rmSync(this.directory, { recursive: true, force: true }); }
    if (assertCleanExit) expect(exit).toEqual({ code: 0, signal: null });
    expect(this.fake.openConnectionCount).toBe(0);
  }
}
function save(name: string, value: unknown) {
  const output = process.env.OBS_STDIO_PRESSURE_EVIDENCE_DIR;
  if (!output) return;
  mkdirSync(output, { recursive: true }); writeFileSync(join(output, `${name}.json`), JSON.stringify(value, null, 2) + '\n');
}
async function actualNativeWriteWitness(session: RawChild, legacy: boolean) {
  // The behavioral regression always runs. Platform stack sampling is an
  // explicit forensic option; the preserved pre-fix receipt includes it.
  if (process.env.OBS_STDIO_PRESSURE_SAMPLE !== '1') return {
    witness: 'portable paused-HWM + physical Stop + same-reply drain assertions', sampled: false,
  };
  if (process.platform !== 'darwin') throw new Error('staged witness needs platform-specific syscall evidence; no pressure claim made');
  const output = process.env.OBS_STDIO_PRESSURE_EVIDENCE_DIR ?? session.directory;
  mkdirSync(output, { recursive: true }); const path = join(output, `native-write-${legacy ? 'legacy' : 'modern'}.sample.txt`);
  await execFileAsync('/usr/bin/sample', [String(session.child.pid!), '1', '1', '-file', path], { timeout: 5000, maxBuffer: 128 * 1024 });
  const text = readFileSync(path, 'utf8');
  // Evidence must identify both a syscall write wait and the actual stdout owner.
  // Root reviews the raw sample before accepting the platform-specific matcher.
  const threads = text.split(/^(?= {4}\d+ Thread_)/m);
  const hasBlockedStdout = threads.some(thread => /Stdout/.test(thread)
    && /\b(?:__)?write(?:_nocancel)?[^\n]*\(in libsystem_kernel/.test(thread));
  expect(hasBlockedStdout, `actual blocked stdout stack required; inspect ${path}`).toBe(true);
  return { path, bytes: Buffer.byteLength(text), witness: 'actual native stdout write stack, not a synthetic writer' };
}

for (const legacy of [false, true]) {
  it(`keeps explicit owned Stop dispatch live while bounded valid stdout replies are unread (legacy=${legacy})`, async () => {
    for (const native of [false, true]) {
      const session = await RawChild.create(native, legacy);
      let stop: ReturnType<RawChild['tool']> | undefined;
      const lists: ReturnType<RawChild['request']>[] = [];
      let failure: unknown, started = false;
      const observation: Record<string, unknown> = { native, legacy, phase: 'setup' };
      try {
        await session.startTake(); started = true;
        const probe = session.request('tools/list', {}); await probe.sent;
        const measured = await bounded(probe.result, WATCH, 'catalog measurement stalled');
        if (!('frame' in measured)) throw new Error(measured.error);
        expect((measured.frame.value.result as { tools: unknown[] }).tools.length).toBeGreaterThan(100);
        expect(measured.frame.bytes).toBeGreaterThanOrEqual(64 * 1024);
        session.pause();
        for (let index = 0; index < MAX_LIST_REPLIES; index++) { const item = session.request('tools/list', {}); lists.push(item); await item.sent; }
        await session.bufferedHighWaterMark();
        Object.assign(observation, { phase: 'unread-high-water-mark', measuredCatalogBytes: measured.frame.bytes,
          requestedLists: lists.length, unreadHighWaterMark: session.child.stdout.readableHighWaterMark,
          unreadBufferedBytes: session.child.stdout.readableLength, paused: session.child.stdout.isPaused() });
        expect(session.child.stdout.isPaused()).toBe(true);
        expect(session.child.stdout.readableLength).toBeGreaterThanOrEqual(session.child.stdout.readableHighWaterMark);
        const witness = native ? await actualNativeWriteWitness(session, legacy) : { witness: 'same bounded unread catalog budget; Source positive control' };
        expect(session.recording()).toBe(true); expect(session.packets('StopRecord')).toHaveLength(0);
        expect(performance.now() - session.startAt).toBeLessThan(20_000); // excludes the 30s automatic deadline
        expect(session.child.stdout.isPaused()).toBe(true);
        expect(session.child.stdout.readableLength).toBeGreaterThanOrEqual(session.child.stdout.readableHighWaterMark);
        const cursor = session.fake.cursor(); const stopRequestedAt = performance.now(); stop = session.tool('obs-take-stop', {}); await stop.sent;
        let physicalStopBeforeResume = false;
        try { await session.fake.waitForRequest('StopRecord', { after: cursor, timeoutMs: 1500 }); physicalStopBeforeResume = true; }
        catch (error) { failure = error; }
        Object.assign(observation, { phase: 'stop-observed-before-resume', measuredCatalogBytes: measured.frame.bytes, requestedLists: lists.length,
          unreadHighWaterMark: session.child.stdout.readableHighWaterMark, unreadBufferedBytes: session.child.stdout.readableLength,
          physicalStopBeforeResume, physicalStopWatchElapsedMs: performance.now() - stopRequestedAt,
          nativePid: session.child.pid, witness, traceBeforeResume: session.fake.history() });
        expect(physicalStopBeforeResume, `physical Stop must precede stdout resume (native=${native}, legacy=${legacy})`).toBe(true);
      } catch (error) { failure = error; }
      finally {
        session.resume();
        try {
          if (!stop && started) { stop = session.tool('obs-take-stop', {}); await stop.sent; }
          if (stop) {
            const stopped = await bounded(stop.result, WATCH, 'same Stop result failed to settle after stdout resume');
            expect(stopped).toHaveProperty('frame');
            if ('frame' in stopped) {
              expect(stopped.frame.value.id).toBe(stop.id);
              expect(stopped.frame.value.result).toMatchObject({ structuredContent: { stopConfirmed: true } });
              expect((stopped.frame.value.result as { isError?: boolean }).isError).not.toBe(true);
            }
          }
          const replies = await bounded(Promise.all(lists.map(item => item.result)), WATCH, 'bounded catalog replies did not drain');
          for (const result of replies) {
            expect(result).toHaveProperty('frame');
            if ('frame' in result) expect((result.frame.value.result as { tools: unknown[] }).tools.length).toBeGreaterThan(100);
          }
          expect(session.stdoutProtocolError).toBeUndefined();
          if (started) { expect(session.packets('StopRecord')).toHaveLength(1); expect(session.recording()).toBe(false); }
        } finally {
          save(`pressure-${legacy ? 'legacy' : 'modern'}-${native ? 'native' : 'source'}`, {
            observation, failure: failure ? String(failure) : undefined,
            finalTrace: session.fake.history(), remainingOpenSocketsBeforeCleanup: session.fake.openConnectionCount,
          });
          await session.cleanup();
        }
      }
      if (failure) throw failure;
    }
  }, 25_000);

  it(`bounds native output overload, stops the owned Take and drains prior frames before error exit (legacy=${legacy})`, async () => {
    // 160 valid catalogs exceed the native 128 total active-plus-queued frame budget.
    // This is a native lifecycle policy, not a Source overload parity claim.
    const requested = 160;
    const session = await RawChild.create(true, legacy, 24 * 1024 * 1024);
    const lists: ReturnType<RawChild['request']>[] = [];
    const observation: Record<string, unknown> = { native: true, legacy, requestedLists: requested };
    let failure: unknown;
    try {
      await session.startTake();
      const cursor = session.fake.cursor();
      session.pause();
      for (let index = 0; index < requested; index++) {
        const item = session.request('tools/list', {}); lists.push(item); await item.sent;
      }
      await session.bufferedHighWaterMark();
      Object.assign(observation, { paused: session.child.stdout.isPaused(), unreadBytes: session.child.stdout.readableLength,
        highWaterMark: session.child.stdout.readableHighWaterMark });
      expect(session.child.stdout.isPaused()).toBe(true);
      expect(session.child.stdout.readableLength).toBeGreaterThanOrEqual(session.child.stdout.readableHighWaterMark);
      // Queue-admission failure must signal shutdown despite blocked output.
      // Physical Stop must precede stdout resume within the bounded watchdog;
      // this is not a 1.5-second scheduling guarantee on a loaded runner.
      await session.fake.waitForRequest('StopRecord', { after: cursor, timeoutMs: WATCH });
      expect(session.child.stdout.isPaused()).toBe(true);
      expect(session.recording()).toBe(false);
      expect(session.packets('StopRecord')).toHaveLength(1);
      expect(performance.now() - session.startAt).toBeLessThan(20_000);
      observation.physicalStopBeforeResume = true;
    } catch (error) { failure = error; }
    finally {
      session.resume(); session.child.stdin.end();
      try {
        const exit = await bounded(session.closed, WATCH, 'overloaded helper did not exit after draining stdout');
        const replies = await bounded(Promise.all(lists.map(item => item.result)), WATCH, 'overloaded replies did not settle');
        let complete = 0, missing = 0;
        for (const [index, reply] of replies.entries()) {
          if ('frame' in reply) {
            complete++;
            expect(reply.frame.value.id).toBe(lists[index]!.id);
            expect(reply.frame.value.error).toBeUndefined();
            expect((reply.frame.value.result as { tools: unknown[] }).tools.length).toBeGreaterThan(100);
          } else missing++;
        }
        const ids = new Set(lists.map(item => item.id));
        const outputOrder = [...session.frames.keys()].filter(id => ids.has(id));
        expect(outputOrder).toEqual([...outputOrder].sort((a, b) => a - b));
        expect(session.stdoutProtocolError).toBeUndefined();
        expect(complete).toBeGreaterThan(0);
        expect(missing).toBeGreaterThan(0);
        expect(complete + missing).toBe(requested);
        expect(exit.signal).toBeNull(); expect(exit.code).not.toBeNull(); expect(exit.code).not.toBe(0);
        expect(session.packets('StopRecord')).toHaveLength(1); expect(session.recording()).toBe(false);
        Object.assign(observation, { completeReplies: complete, missingReplies: missing, exit, stdoutBytes: session.stdoutBytes,
          finalStopCount: session.packets('StopRecord').length, recordingAfterJoin: session.recording(), orderedIds: outputOrder });
      } finally {
        save(`overload-${legacy ? 'legacy' : 'modern'}-native`, {
          observation, failure: failure ? String(failure) : undefined, finalTrace: session.fake.history(),
        });
        await session.cleanup(false);
      }
    }
    if (failure) throw failure;
  }, 20_000);

  it(`signals owned Take shutdown before draining an admitted UI ACK after stdin EOF (legacy=${legacy})`, async () => {
    // Source is measured honestly. Native's Stop-before-release is a stronger
    // cleanup requirement when Source merely disconnects/exits without Stop.
    const baseline: unknown[] = [];
    for (const native of [false, true]) {
      const session = await RawChild.create(native, legacy); let held: Awaited<ReturnType<FakeOBSServer['waitForRequest']>> | undefined;
      let stoppedBeforeRelease = false, failure: unknown;
      try {
        await session.startTake(); session.fake.queueSuccess('OpenInputPropertiesDialog', {}, 60_000);
        const cursor = session.fake.cursor(); const dialog = session.tool('obs-open-input-properties', { inputName: 'Fixture' }); await dialog.sent;
        held = await session.fake.waitForRequest('OpenInputPropertiesDialog', { after: cursor, timeoutMs: WATCH });
        expect(session.recording()).toBe(true); expect(session.packets('StopRecord')).toHaveLength(0);
        session.child.stdin.end();
        try { await session.fake.waitForRequest('StopRecord', { after: held.cursor, timeoutMs: 1500 }); stoppedBeforeRelease = true; }
        catch (error) { if (native) failure = error; }
        baseline.push({ native, legacy, stoppedBeforeRelease, childExitBeforeRelease: session.child.exitCode,
          recordingBeforeRelease: session.recording(), traceBeforeRelease: session.fake.history(),
          interpretation: native ? 'required native cleanup ordering' : 'observed Source behavior; not assumed to stop or match native cleanup' });
        if (native) expect(stoppedBeforeRelease, 'owned Stop must dispatch before held UI ACK release').toBe(true);
      } catch (error) { failure = error; }
      finally {
        try {
          if (held && session.fake.openConnectionCount && session.child.exitCode === null) {
            try {
              session.fake.sendRaw({ op: 7, d: { requestType: 'OpenInputPropertiesDialog', requestId: held.frame.d.requestId,
                requestStatus: { result: true, code: 100 }, responseData: {} } }, held.connectionId);
            } catch (error) { if (native) throw error; }
          }
          const exit = await bounded(session.closed, WATCH, 'EOF helper did not join after ACK release');
          if (native) { expect(exit).toEqual({ code: 0, signal: null }); expect(session.packets('StopRecord')).toHaveLength(1); expect(session.recording()).toBe(false); }
        } finally {
          save(`eof-held-ui-${legacy ? 'legacy' : 'modern'}-${native ? 'native' : 'source'}`, {
            observation: baseline.at(-1), failure: failure ? String(failure) : undefined,
            finalTrace: session.fake.history(), exitCode: session.child.exitCode, recordingAfterJoin: session.recording(),
          });
          await session.cleanup(native);
        }
      }
      if (failure) throw failure;
    }
  }, 25_000);
}
