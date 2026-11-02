#!/usr/bin/env node
/* SPDX-License-Identifier: GPL-2.0-only */
import { Client } from "@modelcontextprotocol/client";
import { getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { WebSocketServer } from "ws";
import { createHash } from "node:crypto";
import { execFile, execFileSync, spawn } from "node:child_process";
import { once } from "node:events";
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { createServer, createConnection } from "node:net";
import { arch, cpus, platform, release, tmpdir } from "node:os";
import { dirname, resolve } from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath } from "node:url";
import { parseArgs, promisify } from "node:util";

const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const execute = promisify(execFile);
const sleep = (ms) => new Promise((done) => setTimeout(done, ms));
const hash = (value) => createHash("sha256").update(value).digest("hex");
const canonical = (value) => JSON.stringify(value, (_key, entry) => entry && typeof entry === "object" && !Array.isArray(entry)
  ? Object.fromEntries(Object.entries(entry).sort(([a], [b]) => a.localeCompare(b))) : entry);
function assertBytes(actual, expected, label) {
  const left = Buffer.from(canonical(actual)), right = Buffer.from(canonical(expected));
  if (!left.equals(right)) throw new Error(`${label} differs (actual=${hash(left)}, baseline=${hash(right)})`);
  return { sha256: hash(left), bytes: left.length };
}
function versionResult(value) {
  return { ...value, content: value.content?.map((block) => {
    if (block.type !== "text") return block;
    try {
      const parsed = JSON.parse(block.text);
      return parsed && typeof parsed === "object" ? { ...block, text: canonical(parsed) } : block;
    } catch { return block; }
  }) };
}
function assertVersion(actual, expected, label) {
  return assertBytes(versionResult(actual), versionResult(expected), label);
}
export function distribution(samples) {
  if (!samples.length) return null;
  const sorted = [...samples].sort((a, b) => a - b);
  return { count: samples.length, min: sorted[0], p50: sorted[Math.ceil(sorted.length * 0.5) - 1], p95: sorted[Math.ceil(sorted.length * 0.95) - 1], max: sorted.at(-1) };
}
function finiteInteger(value, fallback, minimum, name) {
  const number = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(number) || number < minimum) throw new Error(`${name} must be an integer >= ${minimum}`);
  return number;
}
async function deadline(promise, ms, label) {
  let timer;
  try { return await Promise.race([promise, new Promise((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`${label} exceeded ${ms}ms`)), ms); })]); }
  finally { clearTimeout(timer); }
}

function parseProcessTable(stdout) {
  const rows = stdout.split("\n").filter((line) => line.trim()).map((line) => {
    const match = /^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(.+?)\s*$/.exec(line);
    if (!match) throw Object.assign(new Error("Malformed process-table row"), { code: "INVALID_PROCESS_TABLE" });
    return { pid: Number(match[1]), parent: Number(match[2]), group: Number(match[3]), rssBytes: Number(match[4]) * 1024, identity: match[5] };
  });
  if (!rows.some((row) => row.pid === process.pid)) throw Object.assign(new Error("Process table omitted benchmark runner"), { code: "INVALID_PROCESS_TABLE" });
  return rows;
}

/** One serialized read lane for periodic samples and teardown. Failed command
 * stdout is diagnostic byte count only, never a process/cleanup observation. */
export function createProcessSampler({ run = execute, pause = sleep, attempts = 3 } = {}) {
  const diagnostics = { requestedTables: 0, commandAttempts: 0, failures: 0, recoveredReads: 0, failedReads: 0, maxConcurrentCommands: 0, events: [] };
  let tail = Promise.resolve(), active = 0;
  const read = (stage) => {
    diagnostics.requestedTables++;
    const pending = tail.then(async () => {
      for (let attempt = 1; attempt <= attempts; attempt++) {
        let stdout, stderr;
        const started = performance.now();
        diagnostics.commandAttempts++;
        active++; diagnostics.maxConcurrentCommands = Math.max(diagnostics.maxConcurrentCommands, active);
        try {
          const result = await run("ps", ["-axo", "pid=,ppid=,pgid=,rss=,lstart="], { timeout: 2_000, maxBuffer: 4 * 1024 * 1024 });
          stdout = result.stdout; stderr = result.stderr;
          const rows = parseProcessTable(stdout);
          if (attempt > 1) diagnostics.recoveredReads++;
          return rows;
        } catch (error) {
          diagnostics.failures++;
          const retryable = typeof error.code === "number" || error.killed === true || Boolean(error.signal)
            || ["EAGAIN", "EINTR", "ENOMEM", "EBUSY", "ETIMEDOUT"].includes(error.code);
          const event = { stage, attempt, code: error.code ?? null, signal: error.signal ?? null, killed: error.killed === true,
            stderr: String(error.stderr ?? stderr ?? "").slice(0, 1024), stdoutBytes: Buffer.byteLength(error.stdout ?? stdout ?? ""), elapsedMs: performance.now() - started, retryable };
          if (diagnostics.events.length < 32) diagnostics.events.push(event);
          if (!retryable || attempt === attempts) {
            diagnostics.failedReads++;
            throw Object.assign(new Error(`Process sampler unavailable: ${JSON.stringify(event)}`), { code: "PROCESS_SAMPLER_UNAVAILABLE", sampler: event });
          }
        } finally { active--; }
        await pause(20 * attempt);
      }
      throw new Error("Process sampler has no attempts configured");
    });
    tail = pending.then(() => undefined, () => undefined);
    return pending;
  };
  return { read, diagnostics };
}

/** Benchmark-only transport. The pinned SDK Client owns MCP negotiation; this
 * transport owns an isolated POSIX process group and its actual teardown. */
class OwnedStdioTransport {
  onclose; onerror; onmessage;
  child; exited; closed; pid; sampleError; buffer = ""; stderrBytes = 0;
  known = new Map(); samples = []; escalations = []; timer; sampling;
  constructor(command, args, env, cwd, sampleMs, sampler, mode) { Object.assign(this, { command, args, env, cwd, sampleMs, sampler, mode }); }
  async start() {
    this.child = spawn(this.command, this.args, { env: this.env, cwd: this.cwd, stdio: ["pipe", "pipe", "pipe"], detached: true });
    this.pid = this.child.pid;
    this.exited = new Promise((done) => {
      this.child.once("exit", () => { this.onclose?.(); done(); });
      this.child.once("error", () => { if (!this.child.pid) done(); });
    });
    this.child.on("error", (error) => this.onerror?.(error));
    this.child.stdin.on("error", (error) => this.onerror?.(error));
    this.child.stderr.on("data", (bytes) => { this.stderrBytes += bytes.length; });
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (bytes) => {
      this.buffer += bytes;
      if (Buffer.byteLength(this.buffer) > 16 * 1024 * 1024) { this.onerror?.(new Error("MCP stdout exceeded benchmark buffer")); return; }
      let end;
      while ((end = this.buffer.indexOf("\n")) >= 0) {
        const line = this.buffer.slice(0, end); this.buffer = this.buffer.slice(end + 1);
        if (!line.trim()) continue;
        try { this.onmessage?.(JSON.parse(line)); } catch { this.onerror?.(new Error("Invalid MCP JSON on stdout")); }
      }
    });
    this.timer = setInterval(() => { void this.sample().catch(() => { clearInterval(this.timer); }); }, this.sampleMs);
    await once(this.child, "spawn");
    if (!this.pid) throw new Error("Owned MCP child did not expose its PID");
  }
  async send(message) {
    if (!this.child?.stdin.writable) throw new Error("Owned MCP child is not writable");
    if (!this.child.stdin.write(`${JSON.stringify(message)}\n`)) await once(this.child.stdin, "drain");
  }
  async sample(stage = "periodic") {
    if (this.sampling) return this.sampling;
    this.sampling = (async () => {
      const rows = await this.sampler.read(`${this.mode}:${stage}`);
      const root = rows.find((row) => row.pid === this.pid);
      const rootKnown = this.known.get(this.pid);
      const rootMatches = root && (!rootKnown || root.identity === rootKnown.identity);
      const groupKnown = rows.some((row) => row.group === this.pid && this.known.get(row.pid)?.identity === row.identity);
      const owned = new Set(rows.filter((row) => this.known.get(row.pid)?.identity === row.identity).map((row) => row.pid));
      if (rootMatches) owned.add(this.pid);
      let previous = -1;
      while (previous !== owned.size) {
        previous = owned.size;
        for (const row of rows) if (((rootMatches || groupKnown) && row.group === this.pid) || owned.has(row.parent)) owned.add(row.pid);
      }
      const current = rows.filter((row) => owned.has(row.pid) && (!this.known.has(row.pid) || this.known.get(row.pid).identity === row.identity));
      for (const row of current) this.known.set(row.pid, row);
      const sample = { rssBytes: current.reduce((sum, row) => sum + row.rssBytes, 0), processes: current.length };
      this.samples.push(sample);
      return { ...sample, rows: current };
    })();
    try { return await this.sampling; }
    catch (error) { this.sampleError ??= error; throw error; }
    finally { this.sampling = null; }
  }
  async survivors() {
    return (await this.sample("cleanup")).rows;
  }
  async signalOwned(signal) {
    const active = await this.survivors();
    if (active.some((row) => row.group === this.pid)) {
      try { process.kill(-this.pid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
    }
    for (const row of active.filter((entry) => entry.group !== this.pid)) {
      try { process.kill(row.pid, signal); } catch (error) { if (error.code !== "ESRCH") throw error; }
    }
  }
  async close() {
    return this.closed ??= (async () => {
      clearInterval(this.timer);
      if (!this.child) return;
      this.child.stdin.end();
      try {
        await this.sample("cleanup");
        await Promise.race([this.exited, sleep(1_000)]);
        if ((await this.survivors()).length) {
          this.escalations.push("SIGTERM"); await this.signalOwned("SIGTERM");
          await Promise.race([this.exited, sleep(500)]);
        }
        if ((await this.survivors()).length) { this.escalations.push("SIGKILL"); await this.signalOwned("SIGKILL"); }
        await deadline(this.exited, 2_000, "owned process exit");
        for (let n = 0; n < 40; n++) { if (!(await this.survivors()).length) return; await sleep(25); }
        throw new Error("Owned MCP descendants remain after teardown");
      } catch (error) {
        // A failed sampler cannot certify descendant cleanup. Still stop our
        // live spawned process/group using its owned ChildProcess handle.
        if (this.child.exitCode === null && this.child.signalCode === null && this.pid) {
          for (const signal of ["SIGTERM", "SIGKILL"]) {
            this.escalations.push(signal);
            try { process.kill(-this.pid, signal); } catch (signalError) { if (signalError.code !== "ESRCH") error.emergencySignalError = signalError.code; }
            await Promise.race([this.exited, sleep(500)]);
            if (this.child.exitCode !== null || this.child.signalCode !== null) break;
          }
        }
        throw error;
      } finally {
        this.child.stdout.destroy(); this.child.stderr.destroy(); this.child.stdin.destroy();
      }
    })();
  }
}

async function unusedUrl() {
  const server = createServer(); server.listen({ host: "127.0.0.1", port: 0 }); await once(server, "listening");
  const port = server.address().port;
  await new Promise((done, reject) => server.close((error) => error ? reject(error) : done()));
  return `ws://127.0.0.1:${port}`;
}
async function fakeObs({ workflows = false } = {}) {
  const server = new WebSocketServer({ host: "127.0.0.1", port: 0 }); await once(server, "listening");
  const port = server.address().port;
  let requests = 0, connections = 0, unexpectedEffects = 0;
  const requestCounts = {};
  const transform = {positionX:0,positionY:0,rotation:0,scaleX:1,scaleY:1,sourceWidth:1280,sourceHeight:720,width:1280,height:720,alignment:5,boundsType:"OBS_BOUNDS_NONE",boundsAlignment:0,boundsWidth:0,boundsHeight:0,cropLeft:0,cropRight:0,cropTop:0,cropBottom:0};
  const item = {sceneItemId:7,sceneItemIndex:0,sourceName:"Application",sourceUuid:"application-source-uuid",inputKind:"screen_capture",isGroup:false,sceneItemEnabled:true,sceneItemLocked:false,sceneItemTransform:transform};
  const workflowResponses = workflows ? {
    GetSceneCollectionList:{currentSceneCollectionName:"Benchmark collection"},
    GetSceneList:{currentProgramSceneName:"Review",currentProgramSceneUuid:"review-scene-uuid",scenes:[{sceneName:"Review",sceneUuid:"review-scene-uuid"}]},
    GetVideoSettings:{baseWidth:1920,baseHeight:1080,outputWidth:1920,outputHeight:1080,fpsNumerator:30,fpsDenominator:1},
    GetInputList:{inputs:[{inputName:"Application",inputUuid:"application-source-uuid",inputKind:"screen_capture"}]},
    GetSceneItemList:{sceneItems:[item]}, GetSceneItemTransform:{sceneItemTransform:transform},
  } : {};
  const version = { obsVersion: "32.2.2", obsWebSocketVersion: "5.7.0", rpcVersion: 1, availableRequests: ["GetVersion", ...Object.keys(workflowResponses)], supportedImageFormats: ["png", "jpeg", "webp"], platform: "macos", platformDescription: "Benchmark fake OBS" };
  server.on("connection", (socket) => {
    connections++;
    socket.on("error", () => {});
    socket.send(JSON.stringify({ op: 0, d: { obsWebSocketVersion: "5.7.0", obsStudioVersion: "32.2.2", rpcVersion: 1 } }));
    socket.on("message", (bytes) => {
      const frame = JSON.parse(bytes.toString());
      if (frame.op === 1) socket.send(JSON.stringify({ op: 2, d: { negotiatedRpcVersion: 1 } }));
      if (frame.op === 6) {
        requests++;
        const type = frame.d.requestType;
        requestCounts[type] = (requestCounts[type] ?? 0) + 1;
        if (!String(type).startsWith("Get")) unexpectedEffects++;
        const known = type === "GetVersion" || Object.hasOwn(workflowResponses,type);
        socket.send(JSON.stringify({ op: 7, d: { requestId: frame.d.requestId, requestType: type, requestStatus: { result: known, code: known ? 100 : 204 }, responseData: type === "GetVersion" ? version : workflowResponses[type] ?? {} } }));
      }
      if (frame.op === 8) { unexpectedEffects++; socket.close(); }
    });
  });
  return { url: `ws://127.0.0.1:${port}`, stats: () => ({ requests, connections, requestCounts, unexpectedEffects }), close: async () => {
    for (const socket of server.clients) socket.terminate();
    await new Promise((done, reject) => server.close((error) => error ? reject(error) : done()));
    await new Promise((done, reject) => {
      const probe = createConnection({ host: "127.0.0.1", port });
      probe.once("connect", () => { probe.destroy(); reject(new Error("Owned fake OBS listener survived teardown")); });
      probe.once("error", (error) => error.code === "ECONNREFUSED" ? done() : reject(error));
    });
  } };
}

async function session(root, mode, obsUrl, stateDir, sampleMs, action, bun, sampler) {
  const native = resolve(root, "native/target/release/obs-mcp-native");
  const entry = resolve(root, "build/index.js");
  const env = { ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: obsUrl, OBS_MCP_READ_OBS_CONFIG: "0", OBS_MCP_STATE_DIR: stateDir, OBS_MCP_LOG_LEVEL: "error", OBS_MCP_COMPAT_ENTRY: entry, ...(mode === "forced-compat" ? { OBS_MCP_FORCE_COMPAT: "1" } : {}) };
  const hostBaseline = mode === "node-baseline" || mode === "bun-baseline";
  const command = mode === "bun-baseline" ? bun : mode === "node-baseline" ? process.execPath : native;
  const transport = new OwnedStdioTransport(command, hostBaseline ? [entry] : [], env, root, sampleMs, sampler, mode);
  const client = new Client({ name: "obs-native-benchmark", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  const start = performance.now();
  try {
    await deadline(client.connect(transport), 15_000, "MCP connect");
    const inventory = (await deadline(client.listTools(), 15_000, "tools/list")).tools;
    const coldMs = performance.now() - start;
    await transport.sample();
    const result = await action({ client, inventory, coldMs, transport });
    await sleep(50);
    const idle = await transport.sample();
    if (transport.sampleError) throw transport.sampleError;
    return { ...result, memory: { peakSampledRssBytes: Math.max(...transport.samples.map((s) => s.rssBytes)), idleAggregateRssBytes: idle.rssBytes, peakObservedProcesses: Math.max(...transport.samples.map((s) => s.processes)), samples: transport.samples.length }, cleanup: await (async () => { await client.close(); await transport.close(); return { verified: true, escalations: transport.escalations }; })() };
  } finally {
    try { await transport.close(); }
    catch (error) { error.cleanupFailure = true; throw error; }
  }
}

function sourceIdentity(root, bun) {
  const run = (command, args, cwd = root) => { try { return execFileSync(command, args, { cwd, encoding: "utf8", timeout: 5_000, killSignal: "SIGKILL", stdio: ["ignore", "pipe", "ignore"] }).trim(); } catch { return null; } };
  const fileHash = (path) => existsSync(resolve(root, path)) ? hash(readFileSync(resolve(root, path))) : null;
  let bunIdentity = null;
  if (bun) {
    bunIdentity = { executable: bun, version: run(bun, ["--version"]), binarySha256: null };
    try { bunIdentity.binarySha256 = hash(readFileSync(bun)); } catch { /* Optional host can remain unsupported without losing the main comparison. */ }
  }
  return { node: { version: process.version, executable: process.execPath }, bun: bunIdentity, cargo: run("cargo", ["--version"], resolve(root, "native")), rustc: run("rustc", ["--version"], resolve(root, "native")), sourceRevision: run("git", ["rev-parse", "HEAD"]), dirty: Boolean(run("git", ["status", "--porcelain"])), nativeBinarySha256: fileHash("native/target/release/obs-mcp-native"), nodeEntrySha256: fileHash("build/index.js"), nativeContractSha256: fileHash("native/assets/tools.json"), migrationLedgerSha256: fileHash("docs/native-migration-ledger.json"), packageLockSha256: fileHash("package-lock.json"), platform: platform(), arch: arch(), osRelease: release(), cpuModel: cpus()[0]?.model ?? null };
}

/** Runs are sequential and deliberately synthetic; no native/live OBS claims. */
export async function benchmarkNative({ root = defaultRoot, out, coldRuns = 15, warmRequests = 100, sampleMs = 25, bun, workflows = false } = {}) {
  if (process.platform === "win32") throw new Error("This benchmark currently requires POSIX owned process-group cleanup and ps RSS; Windows is unqualified");
  coldRuns = finiteInteger(coldRuns, 15, 15, "cold-runs");
  warmRequests = finiteInteger(warmRequests, 100, 100, "warm-requests");
  sampleMs = finiteInteger(sampleMs, 25, 10, "sample-ms");
  if (!out) throw new Error("--out must name the benchmark report");
  if (bun) bun = resolve(bun);
  for (const path of ["build/index.js", "native/target/release/obs-mcp-native", "native/assets/tools.json"]) if (!existsSync(resolve(root, path))) throw new Error(`Build artifact unavailable: ${path}`);
  const temporary = mkdtempSync(resolve(tmpdir(), "obs-native-benchmark-"));
  const modes = ["node-baseline", "native-readonly", "forced-compat"];
  const sampler = createProcessSampler();
  const report = { version: 1, startedAt: new Date().toISOString(), identity: sourceIdentity(root, bun), optionalBun: bun ? { requested: true, executable: bun, status: "pending-parity" } : { requested: false, status: "not-requested" }, method: { coldRuns, warmRequests, sampleMs, idleIntervalMs: 50, sequential: true, protocol: "2026-07-28", inventoryComparison: "Byte-equal JSON after recursively sorting object keys; array order preserved", getVersionComparison: "Full result equality, including structuredContent and extra fields; parseable JSON object/array text blocks have only JSON object ordering/serialization whitespace normalized. Other text and diagnostics remain exact.", rss: "Aggregate RSS of spawned server and all observed descendants, excluding benchmark runner/fake OBS. Periodic ps sampling gives a lower-bound peak and can perturb timing.", cold: "Spawn through pinned Client negotiation and completed tools/list; filesystem caches are not flushed.", warm: "Sequential obs-get-version round trips against task-owned in-process fake OBS; excludes initial request and does not measure real OBS or network latency." }, parity: null, cold: [], warm: [], workflows: [], workflowParity: [], cleanup: { verified: false }, limitations: ["Synthetic local fixture; no live OBS, native capture, recording, UI or delivery qualification.", "RSS is sampled, shared resident pages can be counted in multiple processes, and short-lived peaks may be missed.", "Whole-process comparison includes the compatibility Node descendant when handoff occurs.", "No speed or memory-reduction claim follows without inspecting parity and every cleanup result."] };
  report.processSampler = sampler.diagnostics;
  report.method.processSampler = { attempts: 3, timeoutMs: 2_000, retryBackoffMs: [20, 40], serialized: true, failedStdout: "Never parsed or accepted as an observation" };
  const unsupportedBun = (error, stage) => {
    // An optional host's semantic/runtime incompatibility can be reported, but
    // unproven cleanup must still fail the entire benchmark.
    if (error.cleanupFailure || error.code === "PROCESS_SAMPLER_UNAVAILABLE") throw error;
    Object.assign(report.optionalBun, { status: "unsupported", stage, reason: error.message });
    const index = modes.indexOf("bun-baseline");
    if (index >= 0) modes.splice(index, 1);
    report.cold = report.cold.filter((entry) => entry.mode !== "bun-baseline");
    report.warm = report.warm.filter((entry) => entry.mode !== "bun-baseline");
    report.workflows = report.workflows.filter((entry) => entry.mode !== "bun-baseline");
  };
  let fake;
  try {
    const offline = await unusedUrl();
    fake = await fakeObs({ workflows });
    let baselineInventory;
    const parity = [];
    // Correctness gate precedes every measured case, including offline discovery.
    for (const mode of modes) {
      const checked = await session(root, mode, offline, temporary, sampleMs, async ({ inventory, transport }) => {
        if (!baselineInventory) baselineInventory = inventory;
        const inventoryIdentity = assertBytes(inventory, baselineInventory, `${mode} tool inventory`);
        if (mode === "native-readonly" && (await transport.sample()).processes !== 1) throw new Error("Native-only discovery unexpectedly spawned a compatibility descendant");
        if (mode === "forced-compat" && (await transport.sample()).processes < 2) throw new Error("Forced compatibility discovery did not expose the expected compatibility child");
        return { mode, inventory: inventoryIdentity };
      }, bun, sampler);
      parity.push(checked);
    }
    let baselineVersion;
    for (const mode of modes) {
      const checked = await session(root, mode, fake.url, temporary, sampleMs, async ({ client, inventory, transport }) => {
        assertBytes(inventory, baselineInventory, `${mode} connected inventory`);
        const value = await deadline(client.callTool({ name: "obs-get-version", arguments: {} }), 10_000, "GetVersion parity");
        if (value.isError) throw new Error(`${mode} GetVersion returned an error`);
        if (mode === "native-readonly" && (await transport.sample()).processes !== 1) throw new Error("Native GetVersion unexpectedly acquired a compatibility descendant");
        if (!baselineVersion) baselineVersion = value;
        return { mode, getVersion: assertVersion(value, baselineVersion, `${mode} GetVersion result`) };
      }, bun, sampler);
      parity.push(checked);
    }
    if (bun) {
      try {
        if (!existsSync(bun)) throw new Error("Requested Bun executable is unavailable; no installation attempted");
        report.optionalBun.stage = "discovery-parity";
        const discovery = await session(root, "bun-baseline", offline, temporary, sampleMs, async ({ inventory }) => ({ mode: "bun-baseline", inventory: assertBytes(inventory, baselineInventory, "Bun tool inventory") }), bun, sampler);
        report.optionalBun.stage = "get-version-parity";
        const version = await session(root, "bun-baseline", fake.url, temporary, sampleMs, async ({ client, inventory }) => {
          assertBytes(inventory, baselineInventory, "Bun connected inventory");
          const value = await deadline(client.callTool({ name: "obs-get-version", arguments: {} }), 10_000, "Bun GetVersion parity");
          if (value.isError) throw new Error("Bun GetVersion returned an error");
          return { mode: "bun-baseline", getVersion: assertVersion(value, baselineVersion, "Bun GetVersion result") };
        }, bun, sampler);
        parity.push(discovery, version);
        modes.push("bun-baseline");
        report.optionalBun.status = "parity-qualified";
        report.optionalBun.stage = "timing";
      } catch (error) {
        unsupportedBun(error, report.optionalBun.stage ?? "discovery-parity");
      }
    }
    report.parity = { verified: true, measuredModes: [...modes], cases: parity };
    for (const [condition, url] of [["disconnected-discovery", offline], ["fake-obs-connected-discovery", fake.url]]) {
      for (const mode of [...modes]) {
        const runs = [];
        try {
          for (let index = 0; index < coldRuns; index++) {
            runs.push(await session(root, mode, url, temporary, sampleMs, async ({ inventory, coldMs, transport }) => {
              assertBytes(inventory, baselineInventory, `${mode} cold inventory`);
              if (mode === "native-readonly" && (await transport.sample()).processes !== 1) throw new Error("Native discovery acquired a compatibility descendant");
              return { milliseconds: coldMs };
            }, bun, sampler));
          }
          report.cold.push({ condition, mode, milliseconds: distribution(runs.map((run) => run.milliseconds)), peakSampledRssBytes: distribution(runs.map((run) => run.memory.peakSampledRssBytes)), idleAggregateRssBytes: distribution(runs.map((run) => run.memory.idleAggregateRssBytes)), runs });
        } catch (error) {
          if (mode !== "bun-baseline") throw error;
          unsupportedBun(error, `cold:${condition}`);
        }
      }
    }
    for (const mode of [...modes]) {
      try {
        const measured = await session(root, mode, fake.url, temporary, sampleMs, async ({ client, inventory }) => {
          assertBytes(inventory, baselineInventory, `${mode} warm inventory`);
          assertVersion(await client.callTool({ name: "obs-get-version", arguments: {} }), baselineVersion, `${mode} warmup`);
          const milliseconds = [];
          for (let n = 0; n < warmRequests; n++) {
            const start = performance.now();
            const value = await deadline(client.callTool({ name: "obs-get-version", arguments: {} }), 10_000, "warm GetVersion");
            milliseconds.push(performance.now() - start);
            assertVersion(value, baselineVersion, `${mode} warm result`);
          }
          return { mode, milliseconds: distribution(milliseconds), samplesMs: milliseconds };
        }, bun, sampler);
        report.warm.push(measured);
      } catch (error) {
        if (mode !== "bun-baseline") throw error;
        unsupportedBun(error, "warm:GetVersion");
      }
    }
    if (workflows) {
      report.method.workflows = {requestsPerMode:warmRequests, scope:"Full read-only MCP call against synthetic OBS, including native actor connection/capability discovery and joined cleanup before reply; Node may retain its connected session", normalization:"Only independently validated wall timestamps, nonnegative duration and physical connection generations differ; reports, errors, ordered arrays, evidence and proof hashes are retained"};
      report.parity.workflowVerified = false;
      const calls = [
        {name:"obs-plan-scene-template",arguments:{sceneCollection:"Benchmark collection",sceneName:"Review",template:{version:1,name:"Benchmark",canvas:{width:1920,height:1080},roles:[{role:"slides",inputKinds:["screen_capture"],transform:{positionX:12},enabled:true,locked:false,index:0}]},bindings:[{role:"slides",sourceName:"Application",sourceUuid:"application-source-uuid",containerUuid:"review-scene-uuid",sceneItemId:7}]},status:"resolved"},
        {name:"obs-audit-scene",arguments:{sceneCollection:"Benchmark collection",sceneName:"Review",includeScreenshot:false},status:"pass"},
      ];
      const normalizeReport = value => {
        const copy = structuredClone(value);
        for (const key of ["observedAt","completedAt"]) if (key in copy) {
          if (typeof copy[key]!=="string" || !/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(copy[key])) throw Error("Workflow returned invalid timestamp");
          copy[key]="2026-10-31T00:00:00.000Z";
        }
        if ("durationMs" in copy) { if (!Number.isSafeInteger(copy.durationMs)||copy.durationMs<0) throw Error("Workflow returned invalid duration"); copy.durationMs=0; }
        if ("generation" in copy) {if(!Number.isSafeInteger(copy.generation)||copy.generation<=0)throw Error("Workflow returned invalid generation");copy.generation=1;}
        return copy;
      };
      const normalized = (value, call) => {
        if(value.isError || value.structuredContent?.status!==call.status) throw Error(`${call.name} returned unexpected outcome`);
        return {...value,structuredContent:normalizeReport(value.structuredContent),content:value.content.map(block=>block.type==="text"?{...block,text:normalizeReport(JSON.parse(block.text))}:block)};
      };
      for (const {status,...call} of calls) {
        let baseline;
        for (const mode of [...modes]) {
          const checked = await session(root,mode,fake.url,temporary,sampleMs,async({client,transport})=>{
            const value = normalized(await client.callTool(call),{...call,status});
            baseline ??= value;
            const identity = assertBytes(value,baseline,`${mode} ${call.name} report`);
            if(mode==="native-readonly" && (await transport.sample()).processes!==1)throw Error("Native workflow acquired a JavaScript child");
            return {mode,tool:call.name,result:identity};
          },bun,sampler);
          report.workflowParity.push(checked);
        }
        for (const mode of [...modes]) {
          const measured = await session(root,mode,fake.url,temporary,sampleMs,async({client,transport})=>{
            assertBytes(normalized(await client.callTool(call),{...call,status}),baseline,`${mode} ${call.name} warmup`);
            const samplesMs=[];
            for(let n=0;n<warmRequests;n++) {
              const start=performance.now();
              const value=await deadline(client.callTool(call),15000,call.name);
              samplesMs.push(performance.now()-start);
              assertBytes(normalized(value,{...call,status}),baseline,`${mode} ${call.name} result`);
            }
            if(mode==="native-readonly" && (await transport.sample()).processes!==1)throw Error("Native workflow acquired a JavaScript child");
            return {mode,tool:call.name,milliseconds:distribution(samplesMs),samplesMs};
          },bun,sampler);
          report.workflows.push(measured);
        }
      }
      report.parity.workflowVerified=true;
    }
    if (report.optionalBun.status === "parity-qualified") Object.assign(report.optionalBun, { status: "measured", stage: "complete" });
    report.parity.measuredModes = [...modes];
    report.fakeObs = fake.stats();
    if(report.fakeObs.unexpectedEffects)throw Error("Read-only benchmark attempted an effect or batch");
    await fake.close(); fake = undefined;
    report.cleanup = { verified: true, fixtureListenerClosed: true, allSessionProcessGroupsChecked: true };
    report.completedAt = new Date().toISOString();
    return report;
  } catch (error) { report.error = error.message; throw error; }
  finally {
    if (fake) { try { await fake.close(); } catch (error) { report.cleanupError = error.message; } }
    rmSync(temporary, { recursive: true, force: true });
    mkdirSync(dirname(resolve(out)), { recursive: true });
    writeFileSync(resolve(out), `${JSON.stringify(report, null, 2)}\n`, { mode: 0o600 });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { values } = parseArgs({ options: { root: { type: "string" }, out: { type: "string" }, bun: { type: "string" }, "cold-runs": { type: "string" }, "warm-requests": { type: "string" }, "sample-ms": { type: "string" }, workflows: {type:"boolean",default:false} } });
    const report = await benchmarkNative({ root: values.root ? resolve(values.root) : defaultRoot, out: values.out, bun: values.bun, coldRuns: values["cold-runs"], warmRequests: values["warm-requests"], sampleMs: values["sample-ms"], workflows: values.workflows });
    console.log(JSON.stringify({ report: resolve(values.out), parity: report.parity.verified, optionalBun: report.optionalBun, coldCases: report.cold.length, warmCases: report.warm.length, workflowCases:report.workflows.length, workflowParity:report.parity.workflowVerified??null, cleanup: report.cleanup }));
  } catch (error) { console.error(`Native benchmark failed: ${error.message}`); process.exitCode = 1; }
}
