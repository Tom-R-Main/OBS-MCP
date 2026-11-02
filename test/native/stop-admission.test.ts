/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from 'node:child_process';
import { mkdirSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Client } from '@modelcontextprotocol/client';
import { StdioClientTransport, getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { expect, it, vi } from 'vitest';
import { FakeOBSServer, FakeOBSRequestError } from '../support/fake-obs-server.js';
import { serveProduction } from '../support/production-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const binary = process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/debug/obs-mcp-native');
let sequence = 0;
for (const legacy of [false, true]) {
  it(`admits owned Stop while all32 ordinary frontend jobs remain occupied (legacy=${legacy})`, async () => {
    const directory = join(root, 'native/target', `stop-admission-${process.pid}-${sequence++}`);
    mkdirSync(directory, { mode: 0o700 });
    const recordingRoot = join(directory, 'recordings');
    mkdirSync(recordingRoot, { mode: 0o700 });
    const fake = await FakeOBSServer.start({ platform: 'macos', supportedImageFormats: ['png'] });
    const state = serveProduction(fake);
    state.preview = 'Main';
    let recording = false;
    const path = join(recordingRoot, 'clip.mkv');
    fake.respondWith('GetProfileParameter', data => ({ parameterValue: ({
      'Output/Mode': 'Simple', 'SimpleOutput/RecQuality': 'Small',
      'SimpleOutput/RecEncoder': 'apple_h264', 'SimpleOutput/RecTracks': '1',
      'SimpleOutput/RecFormat2': 'mkv',
    } as Record<string, string>)[`${data.parameterCategory}/${data.parameterName}`] ?? null }));
    fake.respondWith('GetRecordDirectory', () => { throw new FakeOBSRequestError(604, 'fixture directory unavailable'); });
    fake.respondWith('GetStats', () => ({ availableDiskSpace: 50000, renderSkippedFrames: 0, renderTotalFrames: 0 }));
    fake.respondWith('GetRecordStatus', () => ({ outputActive: recording, outputPaused: false, outputBytes: 1048576 }));
    fake.respondWith('StartRecord', () => {
      recording = true;
      fake.sendEvent('RecordStateChanged', { outputState: 'OBS_WEBSOCKET_OUTPUT_STARTED', outputActive: true, outputPath: path });
      return {};
    });
    fake.respondWith('StopRecord', () => {
      recording = false;
      fake.sendEvent('RecordStateChanged', { outputState: 'OBS_WEBSOCKET_OUTPUT_STOPPED', outputActive: false, outputPath: path });
      return { outputPath: path };
    });
    const client = new Client({ name: 'stop-admission', version: '1' }, {
      versionNegotiation: { mode: legacy ? 'legacy' : { pin: '2026-07-28' } },
    });
    const transport = new StdioClientTransport({ command: binary, args: [], cwd: root, stderr: 'pipe', env: {
      ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url, OBS_MCP_STATE_DIR: directory,
      OBS_MCP_NATIVE_STATEFUL: '1', OBS_MCP_NATIVE_RECORDING_ROOT: recordingRoot,
      OBS_MCP_COMPAT_NODE: join(directory, 'missing-node'), OBS_MCP_COMPAT_ENTRY: join(directory, 'missing-sdk'),
      OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '', OBS_MCP_READ_ONLY: 'false',
      OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false', OBS_MCP_READ_OBS_CONFIG: 'false',
    } });
    transport.stderr?.on('data', () => undefined);
    const controllers: AbortController[] = [];
    const sleeps: Promise<void>[] = [];
    let settled = 0;
    let connected = false;
    try {
      await client.connect(transport);
      connected = true;
      expect((await client.callTool({ name: 'obs-take-start', arguments: { expectSilent: true, maxDurationSeconds: 30 } })).isError).not.toBe(true);
      const after = fake.cursor();
      for (let index = 0; index < 32; index++) {
        const controller = new AbortController();
        controllers.push(controller);
        sleeps.push(client.callTool({ name: 'obs-sleep', arguments: { sleepMillis: 50000 } },
          { signal: controller.signal }).then(() => { settled++; }, () => { settled++; }));
      }
      await expect(client.callTool({ name: 'obs-get-status', arguments: {} })).rejects.toThrow(/Native request admission unavailable/);
      expect(settled).toBe(0);
      const stopping = client.callTool({ name: 'obs-take-stop', arguments: {} });
      await expect(stopping, 'owned Stop must retain frontend admission while all32 ordinary jobs remain live').resolves.toHaveProperty('structuredContent.takeId');
      const stopped = await stopping;
      expect(stopped.structuredContent).toHaveProperty('takeId');
      await fake.waitForRequest('StopRecord', { after, timeoutMs: 1000 });
      expect(recording).toBe(false);
      expect(settled).toBe(0);
      await expect(client.callTool({ name: 'obs-get-status', arguments: {} })).rejects.toThrow(/Native request admission unavailable/);
      expect(fake.history().filter(item => item.frame.op === 6 && item.frame.d.requestType === 'StopRecord')).toHaveLength(1);
      expect(fake.connectionCount).toBe(1);
      const pid = transport.pid!;
      const children = execFileSync('ps', ['-axo', 'pid=,ppid='], { encoding: 'utf8' }).trim().split('\n')
        .map(line => line.trim().split(/\s+/).map(Number)).filter(([, parent]) => parent === pid);
      expect(children).toEqual([]);
    } finally {
      controllers.forEach(controller => controller.abort());
      try {
        await Promise.all(sleeps);
        // Capacity refusals have no effect. Once the admitted timers settle,
        // cleanup can still stop the fixture if the regression failed above.
        if (connected) {
          await vi.waitFor(async () => {
            const result = await client.callTool({ name: 'obs-get-status', arguments: {} });
            expect(result.isError).not.toBe(true);
          }, { timeout: 4000 });
          await client.callTool({ name: 'obs-take-stop', arguments: {} });
          expect(recording).toBe(false);
        }
      } finally {
        try {
          await client.close();
          await transport.close();
          await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0));
        } finally {
          await fake.close();
          rmSync(directory, { recursive: true, force: true });
        }
      }
    }
  });
}
