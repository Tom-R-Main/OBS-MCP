/* SPDX-License-Identifier: GPL-2.0-only */
import { mkdtempSync, mkdirSync, readFileSync, existsSync, rmSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, expect, it } from "vitest";
import { measurePcmPeak, runBoundedMediaProcess, runStreamingMediaProcess } from "./media-verification.js";

const root = fileURLToPath(new URL("../../", import.meta.url));
const directories: string[] = [];
function directory() {
  const parent = join(root, "native", "target"); mkdirSync(parent, { recursive: true });
  const path = mkdtempSync(join(parent, "source-media-")); directories.push(path); return path;
}
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));
async function until(test: () => boolean) {
  const end = Date.now() + 4000;
  while (!test()) { if (Date.now() > end) throw new Error("Media fixture did not become ready"); await sleep(10); }
}
function gone(pid: number) {
  try { process.kill(pid, 0); return false; }
  catch (error) { return (error as NodeJS.ErrnoException).code === "ESRCH"; }
}
afterEach(() => { for (const path of directories.splice(0)) rmSync(path, { recursive: true, force: true }); });

it.each([
  { name: "silence", samples: [0, 0, 0, 0], expected: -91 },
  { name: "both saturation signs", samples: [32767, -32768], expected: 0 },
  { name: "audible right channel", samples: [0, 4096, 0, -4096], expected: -18.1 },
])("measures $name from every little-endian sample across split chunks", async ({ samples, expected }) => {
  const code = `const b=Buffer.alloc(${samples.length * 2});${JSON.stringify(samples)}.forEach((s,i)=>b.writeInt16LE(s,i*2));process.stdout.write(b.subarray(0,1));setTimeout(()=>process.stdout.write(b.subarray(1)),10);`;
  expect(await measurePcmPeak(process.execPath, ["-e", code])).toBe(expected);
});

it("rejects incomplete PCM and terminal failure instead of publishing a partial peak", async () => {
  await expect(measurePcmPeak(process.execPath, ["-e", "process.stdout.write(Buffer.from([0]));"])).rejects.toThrow("Incomplete s16 PCM sample at EOF");
  await expect(measurePcmPeak(process.execPath, ["-e", "process.stdout.write(Buffer.from([0,128]));process.exitCode=7;"])).rejects.toThrow("Media decode failed (7)");
  expect(await measurePcmPeak(process.execPath, ["-e", ""])).toBeUndefined();
});

it("never interprets filename or metadata-like stderr as audio evidence", async () => {
  const value = await measurePcmPeak(process.execPath, ["-e", "process.stderr.write('filename max_volume: -inf dB\\n[Parsed_volumedetect_0 @ fake] max_volume: -inf dB\\n');process.stdout.write(Buffer.from([0,16]));"]);
  expect(value).toBe(-18.1);
});

it("cancels a resistant direct child, escalates, and waits for its actual close", async () => {
  const ready = join(directory(), "ready"); const controller = new AbortController();
  const code = "const fs=require('node:fs');process.on('SIGTERM',()=>{});fs.writeFileSync(process.argv[1],String(process.pid));setInterval(()=>{},1000);";
  const result = runStreamingMediaProcess(process.execPath, ["-e", code, ready], () => undefined, { signal: controller.signal }).then(() => null, error => error as Error);
  try {
    await until(() => existsSync(ready)); const pid = Number(readFileSync(ready, "utf8"));
    controller.abort(); expect((await result)?.message).toBe("Media verification cancelled");
    expect(gone(pid)).toBe(true);
  } finally { controller.abort(); await result; }
});

it("keeps its absolute deadline while the returned promise is not awaited", async () => {
  const ready = join(directory(), "ready");
  const result = runBoundedMediaProcess(process.execPath, ["-e", "require('node:fs').writeFileSync(process.argv[1],String(process.pid));setInterval(()=>{},1000);", ready], { timeoutMs: 200 }).then(() => null, error => error as Error);
  await until(() => existsSync(ready)); const pid = Number(readFileSync(ready, "utf8"));
  await sleep(400); expect(gone(pid)).toBe(true); expect((await result)?.message).toBe("Media verification timed out");
});

it.each([0, 7])("bounds inherited pipe drainage after leader exit %s and reports descendant uncertainty", async (exitCode) => {
  const done = join(directory(), "descendant-done");
  const code = "const{spawn}=require('node:child_process');const child=spawn(process.execPath,['-e',\"setTimeout(()=>require('node:fs').writeFileSync(process.argv[1],'done'),1100)\",process.argv[1]],{stdio:['ignore','ignore','inherit']});child.unref();process.exitCode=Number(process.argv[2]);";
  const result = await runBoundedMediaProcess(process.execPath, ["-e", code, done, String(exitCode)]).then(() => null, error => error as Error);
  // The intentionally escaped fixture exits on its own short timer. Wait for
  // its terminal marker before deleting its task-owned directory.
  await until(() => existsSync(done));
  expect(result?.message).toContain("cleanup unconfirmed");
  if (exitCode) expect(result?.message).toContain("Media decode failed (7)");
});

it("joins a producer after the streaming consumer refuses more bytes", async () => {
  const ready = join(directory(), "ready");
  const code = "require('node:fs').writeFileSync(process.argv[1],String(process.pid));setInterval(()=>process.stdout.write(Buffer.alloc(8192)),5);";
  const result = await runStreamingMediaProcess(process.execPath, ["-e", code, ready], () => { throw new Error("parser byte bound"); }).then(() => null, error => error as Error);
  expect(result?.message).toBe("parser byte bound"); expect(gone(Number(readFileSync(ready, "utf8")))).toBe(true);
});
