/* SPDX-License-Identifier: GPL-2.0-only */
// Staged source: install at test/native only for the coordinated counter window.
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, expect, it, vi } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const sdk = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, 'build/index.js');
let sequence = 0;
type Session = {
  native: boolean;
  client: Client;
  transport: StdioClientTransport;
  fake: FakeOBSServer;
  directory: string;
};
const sessions: Session[] = [];
async function start(native: boolean, legacy: boolean): Promise<Session> {
  const fake = await FakeOBSServer.start({ platform: 'macos' });
  serveProduction(fake);
  fake.respondWith('BroadcastCustomEvent', () => ({}));
  fake.respondWith('SetInputSettings', () => ({}));
  fake.respondWith('GetInputSettings', () => ({ inputKind: 'screen_capture', inputSettings: { window: 42 } }));
  fake.respondWith('GetCurrentProgramScene', () => ({ currentProgramSceneName: 'Main' }));
  fake.respondWith('GetSceneItemList', () => ({ sceneItems: [] }));
  const directory = join(root, 'native/target', `disconnect-reason-${process.pid}-${sequence++}`);
  mkdirSync(directory, { mode: 0o700 });
  const client = new Client({ name: 'disconnect-reason', version: '1' }, {
    versionNegotiation: { mode: legacy ? 'legacy' : { pin: '2026-07-28' } },
  });
  const transport = new StdioClientTransport({
    command: native ? binary : process.execPath,
    args: native ? [] : [sdk],
    cwd: root,
    env: {
      ...getDefaultEnvironment(),
      OBS_WEBSOCKET_URL: fake.url,
      OBS_MCP_STATE_DIR: directory,
      OBS_MCP_NATIVE_STATEFUL: native ? '1' : '0',
      OBS_MCP_COMPAT_NODE: join(directory, 'missing-node'),
      OBS_MCP_COMPAT_ENTRY: join(directory, 'missing-sdk.js'),
      OBS_MCP_TOOLSETS: 'all',
      OBS_MCP_TOOLS: '',
      OBS_MCP_READ_ONLY: 'false',
      OBS_MCP_DYNAMIC_TOOLSETS: 'false',
      OBS_MCP_CONFIRM_LIVE: 'false',
      OBS_MCP_READ_OBS_CONFIG: 'false',
    },
    stderr: 'pipe',
  });
  transport.stderr?.on('data', () => undefined);
  const session = { native, client, transport, fake, directory };
  sessions.push(session);
  await client.connect(transport);
  return session;
}
function receipts(session: Session): Record<string, unknown>[] {
  const directory = join(session.directory, 'operation-receipts');
  try {
    return readdirSync(directory).filter(name => name.endsWith('.json')).map(name => (
      JSON.parse(readFileSync(join(directory, name), 'utf8')) as Record<string, unknown>
    ));
  } catch { return []; }
}
function packets(session: Session, request: string) {
  return session.fake.history().filter(row => row.frame.op === 6 && row.frame.d.requestType === request);
}
function children(session: Session): number[] {
  const pid = session.transport.pid;
  return execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8' }).trim().split('\n')
    .map(line => line.trim().split(/\s+/).map(Number))
    .filter(([, parent]) => parent === pid).map(([child]) => child!);
}
function capture(name: string, value: unknown) {
  const directory = process.env.OBS_DISCONNECT_REASON_EVIDENCE_DIR;
  if (!directory) return;
  mkdirSync(directory, { recursive: true });
  writeFileSync(join(directory, `${name}.json`), JSON.stringify(value, null, 2) + '\n');
}
afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try {
      await session.client.close();
      await session.transport.close();
      await vi.waitFor(() => expect(session.fake.openConnectionCount).toBe(0));
    } finally {
      await session.fake.close();
      rmSync(session.directory, { recursive: true, force: true });
    }
  }
});

for (const legacy of [false, true]) {
  it(`preserves complete mutation close reason with Unknown receipt and no replay (legacy=${legacy})`, async () => {
    const oracle = await start(false, legacy);
    const native = await start(true, legacy);
    const reason = 'fixture mutation peer closed: π';
    for (const session of [oracle, native]) session.fake.queueDisconnect('BroadcastCustomEvent', reason);
    const args = { eventData: { fixture: 'once' } };
    const expected = await oracle.client.callTool({ name: 'obs-broadcast-custom-event', arguments: args });
    const actual = await native.client.callTool({ name: 'obs-broadcast-custom-event', arguments: args });
    expect(expected.isError).toBe(true);
    expect(actual.isError).toBe(true);
    const durable = receipts(native);
    expect(durable).toHaveLength(1);
    expect(durable[0]).toMatchObject({ requestType: 'BroadcastCustomEvent', phase: 'unknown', requestAccepted: null, postconditionObserved: false });
    expect(JSON.stringify(durable)).not.toContain(reason);
    const refused = await native.client.callTool({ name: 'obs-broadcast-custom-event', arguments: { eventData: { fixture: 'explicit later' } } });
    expect(refused.isError).toBe(true);
    expect(packets(oracle, 'BroadcastCustomEvent')).toHaveLength(1);
    expect(packets(native, 'BroadcastCustomEvent')).toHaveLength(1);
    expect(receipts(native)).toEqual(durable);
    expect(children(native)).toEqual([]);
    capture(`mutation-${legacy ? 'legacy' : 'modern'}`, {
      expected, actual, refused, receipts: durable,
      sourceTrace: oracle.fake.history(), nativeTrace: native.fake.history(),
      sourceConnections: oracle.fake.connectionCount, nativeConnections: native.fake.connectionCount,
    });
    expect(actual).toEqual(expected);
  });

  it(`preserves accepted input getter reason while exposing the Source reconnect generation boundary (legacy=${legacy})`, async () => {
    const oracle = await start(false, legacy);
    const native = await start(true, legacy);
    const reason = 'fixture input getter peer closed: π';
    for (const session of [oracle, native]) session.fake.queueDisconnect('GetInputSettings', reason);
    const args = { inputName: 'Fixture', inputSettings: { fixture: 'once' } };
    const expected = await oracle.client.callTool({ name: 'obs-set-input-settings', arguments: args });
    const actual = await native.client.callTool({ name: 'obs-set-input-settings', arguments: args });
    expect(expected.isError).not.toBe(true);
    expect(actual.isError).not.toBe(true);
    const durable = receipts(native);
    expect(durable).toHaveLength(1);
    expect(durable[0]).toMatchObject({ requestType: 'SetInputSettings', phase: 'accepted', requestAccepted: true, postconditionObserved: false });
    expect(JSON.stringify(durable)).not.toContain(reason);
    for (const session of [oracle, native]) {
      expect(packets(session, 'SetInputSettings')).toHaveLength(1);
      expect(packets(session, 'GetInputSettings')).toHaveLength(1);
    }
    const sourceGetter = packets(oracle, 'GetInputSettings')[0]!;
    const sourceProgram = packets(oracle, 'GetCurrentProgramScene').find(row => row.cursor > sourceGetter.cursor);
    expect(sourceProgram, 'Source must demonstrate its fresh read-only connection after getter loss').toBeDefined();
    expect(sourceProgram!.connectionId).not.toBe(sourceGetter.connectionId);
    expect(packets(native, 'GetCurrentProgramScene')).toHaveLength(0);
    expect(children(native)).toEqual([]);
    capture(`input-${legacy ? 'legacy' : 'modern'}`, {
      expected, actual, receipts: durable,
      sourceTrace: oracle.fake.history(), nativeTrace: native.fake.history(),
      sourceConnections: oracle.fake.connectionCount, nativeConnections: native.fake.connectionCount,
      generationBoundary: { sourceGetter: sourceGetter.connectionId, sourceProgram: sourceProgram!.connectionId },
      scope: 'Only the first warning is the reason-projection regression. Full bodies and traces preserve the intentional native same-session refusal of further reads; no reconnect is requested.',
    });
    const source = expected.structuredContent as { warnings: string[] };
    const result = actual.structuredContent as { warnings: string[] };
    expect(source.warnings).toHaveLength(1);
    expect(result.warnings).toHaveLength(2);
    expect(result.warnings[1]).toBe('Could not read the program scene: Input readback unavailable on the admitted OBS connection');
    expect(result.warnings[0]).toBe(source.warnings[0]);
  });
}
