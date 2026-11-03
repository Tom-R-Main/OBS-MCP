/* SPDX-License-Identifier: GPL-2.0-only */
import { readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { nativeDefaultFixture } from '../support/native-default-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];

async function start(legacy: boolean) {
  const fake = await FakeOBSServer.start({ platform: 'macos' });
  // An acknowledgement can precede ReplayBufferSaved and still expose an older path.
  fake.respondWith('SaveReplayBuffer', () => ({}));
  fake.respondWith('GetLastReplayBufferReplay', () => ({ savedReplayPath: '/old/highlight.mkv' }));
  const fixture = nativeDefaultFixture(root, true);
  const client = new Client({ name: 'replay-workflow', version: '1' }, {
    versionNegotiation: { mode: legacy ? 'legacy' : { pin: '2026-07-28' } },
  });
  const transport = new StdioClientTransport({ command: binary, cwd: root, env: {
    ...getDefaultEnvironment(), ...fixture, OBS_WEBSOCKET_URL: fake.url,
    OBS_MCP_NATIVE_PROFILE: 'production', OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '',
    OBS_MCP_READ_ONLY: 'false', OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false',
  }, stderr: 'pipe' });
  transport.stderr?.on('data', () => undefined);
  sessions.push({ client, transport, fake });
  await client.connect(transport);
  return { client, transport, fake, directory: fixture.OBS_MCP_STATE_DIR! };
}

afterEach(async () => {
  for (const session of sessions.splice(0)) {
    try {
      await session.client.close();
      await session.transport.close();
      await vi.waitFor(() => expect(session.fake.openConnectionCount).toBe(0));
    } finally { await session.fake.close(); }
  }
});

for (const legacy of [false, true]) describe(`highlight and long-take discovery (legacy=${legacy})`, () => {
  it('distinguishes an accepted save from a new verified highlight file', async () => {
    const { client, fake, directory } = await start(legacy);
    const result = await client.callTool({ name: 'obs-save-replay-buffer', arguments: {} });
    expect(result.isError).not.toBe(true);
    expect(JSON.stringify(result.content)).toContain('Replay buffer save requested');
    const latest = await client.callTool({ name: 'obs-get-last-replay-buffer-replay', arguments: {} });
    expect(JSON.stringify(latest)).toContain('/old/highlight.mkv');
    const receipts = readdirSync(join(directory, 'operation-receipts'))
      .filter(name => name.endsWith('.json'))
      .map(name => JSON.parse(readFileSync(join(directory, 'operation-receipts', name), 'utf8')));
    expect(receipts).toHaveLength(1);
    expect(receipts[0]).toMatchObject({ requestType: 'SaveReplayBuffer', phase: 'accepted',
      requestAccepted: true, postconditionObserved: false });
    expect(fake.history().filter(item => item.frame.op === 6 && item.frame.d.requestType === 'SaveReplayBuffer')).toHaveLength(1);
  });

  it('never retries a highlight save after losing its acknowledgement', async () => {
    const { client, fake, directory } = await start(legacy);
    fake.queueDisconnect('SaveReplayBuffer', 'lost highlight acknowledgement');
    for (let attempt = 0; attempt < 2; attempt++) {
      expect((await client.callTool({ name: 'obs-save-replay-buffer', arguments: {} })).isError).toBe(true);
    }
    expect(fake.history().filter(item => item.frame.op === 6 && item.frame.d.requestType === 'SaveReplayBuffer')).toHaveLength(1);
    expect(fake.connectionCount).toBe(1);
    const names = readdirSync(join(directory, 'operation-receipts')).filter(name => name.endsWith('.json'));
    expect(names).toHaveLength(1);
    expect(JSON.parse(readFileSync(join(directory, 'operation-receipts', names[0]!), 'utf8')))
      .toMatchObject({ requestType: 'SaveReplayBuffer', phase: 'unknown', requestAccepted: null,
        postconditionObserved: false });
  });

  it('directs longer recordings to the guarded Take workflow available in Production', async () => {
    const { client, fake } = await start(legacy);
    const tools = (await client.listTools()).tools;
    const clip = tools.find(tool => tool.name === 'obs-record-clip');
    expect(clip).toBeDefined();
    expect(clip!.description).toContain('obs-take-start');
    expect(clip!.description).toContain('obs-take-stop');
    expect(clip!.description).not.toContain('use obs-start-record');
    for (const name of ['obs-take-start', 'obs-take-mark', 'obs-take-status', 'obs-take-stop']) {
      expect(tools.some(tool => tool.name === name)).toBe(true);
    }
    expect(fake.connectionCount).toBe(0);
  });
});
