/* SPDX-License-Identifier: MIT */
// Actual process + loopback HTTP entry controls. No probe protocol or fetch shim.
import { spawn, type ChildProcessWithoutNullStreams } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { join } from 'node:path';
import { Client, StreamableHTTPClientTransport } from '@modelcontextprotocol/client';
import { getDefaultEnvironment } from '@modelcontextprotocol/client/stdio';
import { expect } from 'vitest';
import { FakeOBSServer } from './fake-obs-server.js';
import { serveProduction } from './production-fixture.js';
import { nativeDefaultFixture } from './native-default-fixture.js';

const root = fileURLToPath(new URL('../../', import.meta.url));
const active = new Set<HttpNativeEntry>();
const modernMeta = {
  'io.modelcontextprotocol/protocolVersion': '2026-07-28',
  'io.modelcontextprotocol/clientInfo': { name: 'http-entry-raw', version: '1' },
  'io.modelcontextprotocol/clientCapabilities': {},
};
export function modernDiscovery(id = 1) {
  return { jsonrpc: '2.0', id, method: 'server/discover', params: { _meta: modernMeta } };
}
async function bounded<T>(work: Promise<T>, label: string, ms = 5000): Promise<T> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([work, new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(label)), ms);
    })]);
  } finally { clearTimeout(timer); }
}
export type HttpEntryOptions = {
  native?: boolean;
  port?: string;
  token?: string;
  env?: Record<string, string>;
};
export class HttpNativeEntry {
  readonly child: ChildProcessWithoutNullStreams;
  readonly closed: Promise<[number | null, NodeJS.Signals | null]>;
  private output = '';
  private errors = '';
  private captureError: Error | undefined;
  private announced = false;
  private readonly clients: Client[] = [];
  private readonly address: Promise<URL>;
  private addressResolve!: (url: URL) => void;
  private addressReject!: (error: Error) => void;
  readonly native: boolean;
  private stopped = false;
  constructor(readonly fake: FakeOBSServer, options: HttpEntryOptions) {
    this.native = options.native !== false;
    this.address = new Promise<URL>((resolve, reject) => { this.addressResolve = resolve; this.addressReject = reject; });
    void this.address.catch(() => undefined);
    const binary = process.env.HTTP_NATIVE_ENTRY_BINARY ?? process.env.NATIVE_TEST_BINARY ?? join(root, 'native/target/release/obs-mcp-native');
    const source = process.env.NATIVE_TEST_COMPAT_ENTRY ?? join(root, 'build/index.js');
    this.child = spawn(this.native ? binary : process.execPath, this.native ? [] : [source], {
      cwd: root, stdio: ['pipe', 'pipe', 'pipe'], env: {
        ...getDefaultEnvironment(), ...nativeDefaultFixture(root),
        OBS_WEBSOCKET_URL: fake.url, OBS_MCP_HTTP_PORT: options.port ?? '0',
        OBS_MCP_TOOLSETS: 'all', OBS_MCP_TOOLS: '', OBS_MCP_READ_ONLY: 'false',
        OBS_MCP_DYNAMIC_TOOLSETS: 'false', OBS_MCP_CONFIRM_LIVE: 'false',
        ...(options.token === undefined ? {} : { OBS_MCP_HTTP_TOKEN: options.token }), ...options.env,
      },
    });
    this.closed = new Promise(resolve => {
      this.child.once('close', (code, signal) => {
        if (!this.announced) this.addressReject(new Error('HTTP entry exited before safe announcement'));
        resolve([code, signal]);
      });
      this.child.once('error', () => this.addressReject(new Error('HTTP entry spawn failed')));
    });
    this.child.stdin.on('error', () => undefined);
    this.child.stdout.on('data', (bytes: Buffer) => {
      this.output += String(bytes);
      if (this.output.length > 64 * 1024) this.captureError = new Error('HTTP entry stdout capture exceeded bound');
      this.output = this.output.slice(-64 * 1024);
    });
    this.child.stderr.on('data', (bytes: Buffer) => {
      this.errors += String(bytes);
      if (this.errors.length > 64 * 1024) this.captureError = new Error('HTTP entry stderr capture exceeded bound');
      this.errors = this.errors.slice(-64 * 1024);
      const diagnosticLines = this.native ? this.errors : this.errors.replace(/^\[obs-mcp\] /gm, '');
      const match = /^Serving MCP over HTTP at (http:\/\/127\.0\.0\.1:([0-9]+)\/mcp)(?: \(bearer token required\))?\r?$/m.exec(diagnosticLines);
      if (match && !this.announced) {
        const port = Number(match[2]);
        if (port > 0 && port <= 65535) { this.announced = true; this.addressResolve(new URL(match[1]!)); }
      }
    });
  }
  stdout() { if (this.captureError) throw this.captureError; return this.output; }
  stderr() { if (this.captureError) throw this.captureError; return this.errors; }
  url() { return bounded(this.address, 'HTTP entry never published its safe loopback endpoint'); }
  waitExit() { return bounded(this.closed, 'HTTP entry did not exit', 8000); }
  async client(legacy: boolean, name: string, headers: Record<string, string> = {}) {
    const target = new URL(await this.url());
    if (legacy) target.searchParams.set('client', name);
    const client = new Client({ name, version: '1' }, { versionNegotiation: { mode: legacy ? 'legacy' : { pin: '2026-07-28' } } });
    this.clients.push(client);
    await client.connect(new StreamableHTTPClientTransport(target, { requestInit: { headers } }), { timeout: 5000 });
    return client;
  }
  async signalAndJoin(signal: 'SIGTERM' | 'SIGINT') {
    expect(this.child.exitCode).toBeNull();
    this.child.kill(signal);
    expect(await this.waitExit()).toEqual([0, null]);
    await this.fake.waitForNoOpenConnections(5000);
    expect(this.fake.openConnectionCount).toBe(0);
    this.stopped = true;
  }
  async close() {
    try {
      if (this.child.exitCode === null && this.child.signalCode === null) {
        this.child.kill('SIGTERM');
        const exit = await this.waitExit();
        if (this.native && this.announced) expect(exit).toEqual([0, null]);
      }
      if (this.announced || this.stopped) {
        await this.fake.waitForNoOpenConnections(5000);
        expect(this.fake.openConnectionCount).toBe(0);
      }
    } finally {
      await Promise.allSettled(this.clients.map(client => client.close()));
      if (this.child.exitCode === null && this.child.signalCode === null) {
        this.child.kill('SIGKILL'); await this.closed;
      }
      active.delete(this); await this.fake.close();
    }
  }
}
export async function launchHttpNativeEntry(options: HttpEntryOptions = {}) {
  const fake = await FakeOBSServer.start();
  serveProduction(fake);
  const entry = new HttpNativeEntry(fake, options); active.add(entry); return entry;
}
export async function closeHttpNativeEntries() {
  const results = await Promise.allSettled(Array.from(active, entry => entry.close()));
  const failure = results.find((result): result is PromiseRejectedResult => result.status === 'rejected');
  if (failure) throw failure.reason;
}
