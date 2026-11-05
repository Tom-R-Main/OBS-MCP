/* SPDX-License-Identifier: MIT */
// Actual default executable + mature HTTP stack. This is separate from the
// retained in-memory SDK probe and lower-level TCP framing/lifetime controls.
import { afterEach, expect, it, vi } from 'vitest';
import { productionSourceTools } from '../support/native-default-fixture.js';
import { closeHttpNativeEntries, launchHttpNativeEntry, modernDiscovery } from '../support/http-native-entry.js';

afterEach(closeHttpNativeEntries);

for (const legacy of [false, true]) it(`actual native HTTP entry shares one OBS owner across SDK eras (first legacy=${legacy})`, async () => {
  const entry = await launchHttpNativeEntry();
  const url = await entry.url();
  expect(url.href).toMatch(/^http:\/\/127\.0\.0\.1:[1-9][0-9]*\/mcp$/);
  expect(entry.stdout()).toBe('');
  // HTTP ownership must outlive stdin EOF, matching the Source entry mode.
  entry.child.stdin.end();
  const first = await entry.client(legacy, 'entry-first');
  const peer = await entry.client(!legacy, 'entry-peer');
  expect((await first.listTools()).tools.some(tool => tool.name === 'obs-get-version')).toBe(true);
  expect(entry.fake.connectionCount).toBe(0);
  expect((await first.callTool({ name: 'obs-get-version', arguments: {} })).isError).not.toBe(true);
  expect((await peer.callTool({ name: 'obs-get-version', arguments: {} })).isError).not.toBe(true);
  expect(entry.fake.connectionCount).toBe(1);
  await first.close();
  expect((await peer.callTool({ name: 'obs-get-version', arguments: {} })).isError).not.toBe(true);
  expect(entry.fake.connectionCount).toBe(1);
  const requests = entry.fake.history().filter(row => row.frame.op === 6);
  expect(requests.every(row => row.frame.d.requestType === 'GetVersion')).toBe(true);
  expect(entry.fake.history().some(row => row.frame.op === 8)).toBe(false);
  expect(entry.stdout()).toBe('');
});

it('actual native HTTP auth refuses before OBS and never echoes its opaque token', async () => {
  const token = 'synthetic-native-http-entry-token';
  const entry = await launchHttpNativeEntry({ token });
  const url = await entry.url();
  expect(entry.stderr()).toContain('(bearer token required)');
  for (const authorization of [undefined, 'Bearer synthetic-wrong-token']) {
    const response = await fetch(url, { method: 'POST', headers: {
      'content-type': 'application/json', accept: 'application/json, text/event-stream',
      ...(authorization === undefined ? {} : { authorization }),
    }, body: JSON.stringify(modernDiscovery()) });
    expect(response.status).toBe(401);
    expect(response.headers.get('www-authenticate')).toBe('Bearer');
    expect(await response.text()).not.toContain(token);
    expect(entry.fake.connectionCount).toBe(0); expect(entry.fake.history()).toEqual([]);
  }
  const client = await entry.client(false, 'authorized-entry', { authorization: `Bearer ${token}` });
  expect((await client.callTool({ name: 'obs-get-version', arguments: {} })).isError).not.toBe(true);
  expect(entry.fake.connectionCount).toBe(1);
  expect(entry.stdout()).toBe(''); expect(entry.stderr()).not.toContain(token);
});

for (const legacy of [false, true]) it(`actual native HTTP ignores dynamic selection and preserves Source static groups (legacy=${legacy})`, async () => {
  const env = { OBS_MCP_DYNAMIC_TOOLSETS: 'true', OBS_MCP_TOOLSETS: 'general', OBS_MCP_TOOLS: '' };
  const source = await launchHttpNativeEntry({ native: false, env });
  const native = await launchHttpNativeEntry({ env });
  const expected = await (await source.client(legacy, 'static-source')).listTools();
  const actual = await (await native.client(legacy, 'static-native')).listTools();
  expect(actual.tools).toEqual(productionSourceTools(expected.tools));
  expect(actual.tools.some(tool => tool.name === 'obs-get-version')).toBe(true);
  expect(actual.tools.some(tool => tool.name === 'obs-enable-toolset')).toBe(false);
  expect(native.fake.connectionCount).toBe(0); expect(native.fake.history()).toEqual([]);
  expect(native.stdout()).toBe('');
});

for (const port of ['+1', '-1', '65536', '0x10', '1e3', '1.5', 'private-invalid-port']) it(`actual native HTTP entry rejects an unsupported decimal port without echo or OBS (${JSON.stringify(port)})`, async () => {
  const entry = await launchHttpNativeEntry({ port });
  expect(await entry.waitExit()).toEqual([1, null]);
  expect(entry.stdout()).toBe('');
  expect(entry.stderr()).not.toContain('Serving MCP over HTTP');
  expect(entry.stderr()).not.toContain(port);
  expect(entry.fake.connectionCount).toBe(0); expect(entry.fake.history()).toEqual([]);
});

for (const [label, port] of [['empty', ''], ['JS-whitespace', ' \t\n\u00a0\ufeff']] as const) it(`${label} HTTP port keeps native stdio ownership`, async () => {
  const entry = await launchHttpNativeEntry({ port });
  entry.child.stdin.write(JSON.stringify(modernDiscovery(91)) + '\n');
  await vi.waitFor(() => expect(entry.stdout()).toContain('\n'), { timeout: 5000 });
  const response = JSON.parse(entry.stdout().trim());
  expect(response.id).toBe(91); expect(response.result.supportedVersions).toEqual(['2026-07-28']);
  expect(entry.stderr()).not.toContain('Serving MCP over HTTP');
  expect(entry.fake.connectionCount).toBe(0);
  entry.child.stdin.end(); expect(await entry.waitExit()).toEqual([0, null]);
});

it('JS-trimmed decimal leading zeros select an ephemeral native HTTP listener', async () => {
  const entry = await launchHttpNativeEntry({ port: ' \t00\u00a0\ufeff' });
  const url = await entry.url();
  expect(Number(url.port)).toBeGreaterThan(0);
  const client = await entry.client(false, 'trimmed-zero-entry');
  expect((await client.listTools()).tools.some(tool => tool.name === 'obs-get-version')).toBe(true);
  expect(entry.fake.connectionCount).toBe(0); expect(entry.stdout()).toBe('');
});

for (const signal of ['SIGTERM', 'SIGINT'] as const) it(`actual native HTTP ${signal} joins its active OBS owner and HTTP driver`, async () => {
  const entry = await launchHttpNativeEntry();
  const url = await entry.url();
  const client = await entry.client(false, 'signal-entry');
  expect((await client.callTool({ name: 'obs-get-version', arguments: {} })).isError).not.toBe(true);
  expect(entry.fake.openConnectionCount).toBe(1);
  // Keep the SDK frontend alive while the process receives its shutdown signal.
  await entry.signalAndJoin(signal);
  expect(entry.stdout()).toBe('');
  await expect(fetch(url)).rejects.toThrow();
});
