/* SPDX-License-Identifier: GPL-2.0-only */
import { describe, expect, it } from "vitest";
// JavaScript orchestration is exercised through its public injected boundary.
// @ts-expect-error The benchmark helper intentionally has no product TypeScript API.
import { createProcessSampler } from "../../scripts/benchmark-native.mjs";
const table = (extra = "") => `${process.pid} 1 ${process.pid} 100 Sat Oct 31 16:00:00 2026\n${extra}`;
const failure = () => Object.assign(new Error("transient ps failure"), { code: 1, stdout: table("999999 1 999999 500 Sat Oct 31 16:00:00 2026\n"), stderr: "" });
describe("benchmark process lifetime observations", () => {
  it("serializes simultaneous periodic and cleanup reads", async () => {
    let active = 0, max = 0;
    const sampler = createProcessSampler({ run: async () => { active++; max = Math.max(max, active); await new Promise(setImmediate); active--; return { stdout: table(), stderr: "" }; } });
    await Promise.all([sampler.read("periodic"), sampler.read("cleanup"), sampler.read("manual")]);
    expect(max).toBe(1); expect(sampler.diagnostics.maxConcurrentCommands).toBe(1);
  });
  it("never promotes failed-command stdout into an ownership observation", async () => {
    let calls = 0;
    const sampler = createProcessSampler({ pause: async () => {}, run: async () => { if (++calls === 1) throw failure(); return { stdout: table(), stderr: "" }; } });
    const rows = await sampler.read("cleanup");
    expect(rows.map((row: { pid: number }) => row.pid)).toEqual([process.pid]);
    expect(sampler.diagnostics).toMatchObject({ failures: 1, recoveredReads: 1, failedReads: 0 });
  });
  it("leaves persistent sampling failure unavailable after exactly three attempts", async () => {
    const sampler = createProcessSampler({ pause: async () => {}, run: async () => { throw failure(); } });
    await expect(sampler.read("cleanup")).rejects.toMatchObject({ code: "PROCESS_SAMPLER_UNAVAILABLE" });
    expect(sampler.diagnostics).toMatchObject({ commandAttempts: 3, failedReads: 1, recoveredReads: 0 });
  });
  it("does not retry a missing sampler executable", async () => {
    const sampler = createProcessSampler({ pause: async () => {}, run: async () => { throw Object.assign(new Error("missing"), { code: "ENOENT" }); } });
    await expect(sampler.read("periodic")).rejects.toMatchObject({ code: "PROCESS_SAMPLER_UNAVAILABLE" });
    expect(sampler.diagnostics.commandAttempts).toBe(1);
  });
  it("rejects a malformed table even when some rows look valid", async () => {
    const sampler = createProcessSampler({ run: async () => ({ stdout: table("invalid row\n"), stderr: "" }) });
    await expect(sampler.read("cleanup")).rejects.toMatchObject({ code: "PROCESS_SAMPLER_UNAVAILABLE" });
    expect(sampler.diagnostics.failedReads).toBe(1);
  });
});
