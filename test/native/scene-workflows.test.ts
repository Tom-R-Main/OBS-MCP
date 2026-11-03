/* SPDX-License-Identifier: GPL-2.0-only */
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { FakeOBSServer } from '../support/fake-obs-server.js';
import { nativeDefaultFixture } from '../support/native-default-fixture.js';
import { serveSceneAuditFixture, SCENE_AUDIT_READS } from '../support/scene-audit-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
const sessions: { client: Client; transport: StdioClientTransport; fake: FakeOBSServer }[] = [];

async function start(legacy: boolean, kind: string) {
  const fake = await FakeOBSServer.start({ platform: 'macos', supportedImageFormats: ['png'] });
  const scene = serveSceneAuditFixture(fake);
  scene.items[0]!.inputKind = kind;
  fake.respondWith('GetInputList', () => ({ inputs: [{ inputName: 'Application',
    inputUuid: 'application-source-uuid', inputKind: kind }] }));
  const client = new Client({ name: 'scene-workflow', version: '1' }, {
    versionNegotiation: { mode: legacy ? 'legacy' : { pin: '2026-07-28' } },
  });
  const transport = new StdioClientTransport({ command: binary, cwd: root, env: {
    ...getDefaultEnvironment(), ...nativeDefaultFixture(root), OBS_WEBSOCKET_URL: fake.url,
    OBS_MCP_NATIVE_PROFILE: 'production', OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '',
    OBS_MCP_READ_ONLY: 'false', OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false',
  }, stderr: 'pipe' });
  transport.stderr?.on('data', () => undefined);
  sessions.push({ client, transport, fake });
  await client.connect(transport);
  return { client, fake, scene };
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

for (const legacy of [false, true]) describe(`scene workflow evidence (legacy=${legacy})`, () => {
  it.each([
    ['subscriber alerts', 'browser_source'],
    ['webinar preroll', 'ffmpeg_source'],
    ['console capture card', 'av_capture_input_v2'],
    ['gameplay', 'game_capture'],
    ['lecture slides', 'image_source'],
  ])('audits %s structure without certifying delivered content', async (_workflow, kind) => {
    const { client, fake, scene } = await start(legacy, kind);
    const result = await client.callTool({ name: 'obs-audit-scene', arguments: {
      sceneCollection: scene.collection, sceneName: scene.sceneName, includeScreenshot: true,
      requiredItems: [{ sourceName: 'Application', sourceUuid: 'application-source-uuid' }],
    } });
    expect(result.isError).not.toBe(true);
    expect(result.structuredContent).toMatchObject({ scope: 'structural', status: 'pass',
      renderedContent: 'unverified', items: [{ sourceUuid: 'application-source-uuid', sceneItemId: 7 }] });
    expect(result.content.some(item => item.type === 'image')).toBe(true);
    expect(JSON.stringify(result.structuredContent)).toContain('recording readiness');
    expect(fake.history().filter(item => item.frame.op === 6)
      .every(item => item.frame.op === 6 && typeof item.frame.d.requestType === 'string'
        && SCENE_AUDIT_READS.has(item.frame.d.requestType))).toBe(true);
    expect(fake.connectionCount).toBe(1);
  });

  it('reports a missing lower third without creating a replacement or switching program', async () => {
    const { client, fake, scene } = await start(legacy, 'browser_source');
    const result = await client.callTool({ name: 'obs-audit-scene', arguments: {
      sceneCollection: scene.collection, sceneName: scene.sceneName,
      requiredItems: [{ sourceName: 'Speaker lower third' }],
    } });
    expect(result.structuredContent).toMatchObject({ status: 'fail', renderedContent: 'unverified' });
    expect(JSON.stringify(result.structuredContent)).toContain('Speaker lower third');
    expect(fake.history().filter(item => item.frame.op === 6)
      .every(item => item.frame.op === 6 && typeof item.frame.d.requestType === 'string'
        && SCENE_AUDIT_READS.has(item.frame.d.requestType))).toBe(true);
  });
});
