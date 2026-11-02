/* SPDX-License-Identifier: GPL-2.0-only */
import { execFileSync } from "node:child_process";
import { fileURLToPath, pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

const script = pathToFileURL(fileURLToPath(new URL("../../scripts/benchmark-native.mjs", import.meta.url))).href;
function probe(body: string) {
  const code = `import assert from 'node:assert/strict';
import { createProcessSampler } from ${JSON.stringify(script)};
const table = process.pid + ' 1 ' + process.pid + ' 1024 Sat Oct 31 16:00:00 2026\\n';
${body}
console.log('sampler probe passed');`;
  return execFileSync(process.execPath, ["--input-type=module", "-e", code], { encoding: "utf8", timeout: 10_000 });
}

describe("benchmark process sampler without launching benchmark targets", () => {
  it("discards failed stdout, records diagnostics and recovers from a transient nonzero exit", () => {
    expect(probe(`let count = 0;
const sampler = createProcessSampler({ pause: async () => {}, run: async () => {
  if (++count === 1) throw Object.assign(new Error('ps failed'), { code: 1, signal: null, killed: false, stderr: 'temporary failure', stdout: table + '999999 1 999999 1 OLD\\n' });
  return { stdout: table, stderr: '' };
} });
const rows = await sampler.read('test:cleanup');
assert.equal(rows.length, 1);
assert.equal(rows[0].pid, process.pid);
assert.equal(sampler.diagnostics.commandAttempts, 2);
assert.equal(sampler.diagnostics.failures, 1);
assert.equal(sampler.diagnostics.recoveredReads, 1);
assert.equal(sampler.diagnostics.events[0].stderr, 'temporary failure');
assert.equal(sampler.diagnostics.events[0].stage, 'test:cleanup');
assert.ok(sampler.diagnostics.events[0].stdoutBytes > Buffer.byteLength(table));`)).toContain("passed");
  });
  it("bounds retries and fails explicitly after persistent sampler failure", () => {
    expect(probe(`const sampler = createProcessSampler({ pause: async () => {}, run: async () => {
throw Object.assign(new Error('ps failed'), { code: 1, signal: 'SIGTERM', killed: true, stderr: 'fixture failure', stdout: table });
} });
await assert.rejects(sampler.read('test:cleanup'), error => error.code === 'PROCESS_SAMPLER_UNAVAILABLE' && error.message.includes('fixture failure'));
assert.equal(sampler.diagnostics.commandAttempts, 3);
assert.equal(sampler.diagnostics.failedReads, 1);
assert.equal(sampler.diagnostics.events[2].signal, 'SIGTERM');
assert.equal(sampler.diagnostics.events[2].killed, true);`)).toContain("passed");
  });
  it("rejects malformed successful stdout instead of treating a partial table as cleanup evidence", () => {
    expect(probe(`const sampler = createProcessSampler({ pause: async () => {}, run: async () => ({ stdout: table + 'truncated-row\\n', stderr: '' }) });
await assert.rejects(sampler.read('test:cleanup'), { code: 'PROCESS_SAMPLER_UNAVAILABLE' });
assert.equal(sampler.diagnostics.commandAttempts, 1);
assert.equal(sampler.diagnostics.events[0].code, 'INVALID_PROCESS_TABLE');`)).toContain("passed");
  });
  it("serializes periodic and cleanup readers onto one command lane", () => {
    expect(probe(`let active = 0, maximum = 0;
const sampler = createProcessSampler({ run: async () => {
active++; maximum = Math.max(maximum, active);
await new Promise(done => setTimeout(done, 5)); active--;
return { stdout: table, stderr: '' };
} });
await Promise.all([sampler.read('periodic'), sampler.read('cleanup'), sampler.read('cleanup')]);
assert.equal(maximum, 1);
assert.equal(sampler.diagnostics.maxConcurrentCommands, 1);
assert.equal(sampler.diagnostics.requestedTables, 3);`)).toContain("passed");
  });
  it("keeps counters after its bounded diagnostic-event list fills", () => {
    expect(probe(`const sampler = createProcessSampler({ run: async () => { throw Object.assign(new Error('denied'), { code: 'EACCES', stderr: 'x'.repeat(2000) }); } });
for (let index = 0; index < 40; index++) await assert.rejects(sampler.read('periodic'));
assert.equal(sampler.diagnostics.failures, 40);
assert.equal(sampler.diagnostics.failedReads, 40);
assert.equal(sampler.diagnostics.events.length, 32);
assert.equal(sampler.diagnostics.events[0].stderr.length, 1024);`)).toContain("passed");
  });
});
