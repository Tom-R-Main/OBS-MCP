/* SPDX-License-Identifier: GPL-2.0-only */
// One-shot immutable predecessor counterexample. Product integration coverage is
// owned elsewhere; run explicitly with RECEIPT_POLICY_ARCHIVED_BINARY.
import { createHash } from "node:crypto";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { expect, it, vi } from "vitest";
import { FakeOBSServer, FakeOBSRequestError } from "../../support/fake-obs-server.js";
import { serveProduction } from "../../support/production-fixture.js";

const root = fileURLToPath(new URL("../../../", import.meta.url));
const archived = process.env.RECEIPT_POLICY_ARCHIVED_BINARY;
let sequence = 0;

async function run(native: boolean) {
  const fake = await FakeOBSServer.start({ platform: "macos", supportedImageFormats: ["png"] });
  const state = serveProduction(fake);
  state.preview = "Main";
  const directory = join(root, "native/target", `receipt-source-policy-${process.pid}-${sequence++}`);
  mkdirSync(directory);
  const recordingRoot = join(directory, "recordings");
  mkdirSync(recordingRoot);
  const clip = join(recordingRoot, "synthetic-no-media.mkv");
  let active = false;
  fake.respondWith("GetProfileParameter", data => ({ parameterValue: ({
    "Output/Mode": "Simple", "SimpleOutput/RecQuality": "Small",
    "SimpleOutput/RecEncoder": "apple_h264", "SimpleOutput/RecTracks": "1",
    "SimpleOutput/RecFormat2": "mkv",
  } as Record<string, string>)[`${data.parameterCategory}/${data.parameterName}`] ?? null }));
  fake.respondWith("GetRecordDirectory", () => { throw new FakeOBSRequestError(604, "fixture directory unavailable"); });
  fake.respondWith("GetStats", () => ({ availableDiskSpace: 50_000, renderSkippedFrames: 0, renderTotalFrames: 0 }));
  fake.respondWith("GetRecordStatus", () => ({ outputActive: active, outputPaused: false, outputBytes: 1_048_576 }));
  fake.respondWith("StartRecord", () => {
    active = true;
    fake.sendEvent("RecordStateChanged", { outputState: "OBS_WEBSOCKET_OUTPUT_STARTED", outputActive: true, outputPath: clip });
    return {};
  });
  fake.respondWith("StopRecord", () => {
    active = false;
    fake.sendEvent("RecordStateChanged", { outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED", outputActive: false, outputPath: clip });
    return { outputPath: clip };
  });
  fake.respondWith("SetInputMute", () => ({}));
  fake.respondWith("CreateRecordChapter", () => ({}));
  const client = new Client({ name: "receipt-source-policy", version: "1" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
  const transport = new StdioClientTransport({
    command: native ? archived! : process.execPath,
    args: native ? [] : [join(root, "build/index.js")], cwd: root,
    env: { ...getDefaultEnvironment(), OBS_WEBSOCKET_URL: fake.url,
      OBS_MCP_STATE_DIR: directory, OBS_MCP_NATIVE_STATEFUL: native ? "1" : "0",
      OBS_MCP_NATIVE_RECORDING_ROOT: recordingRoot, OBS_MCP_TOOLSETS: "all", OBS_MCP_TOOLS: "",
      OBS_MCP_COMPAT_NODE: join(directory, "no-node"), OBS_MCP_COMPAT_ENTRY: join(directory, "no-sdk"),
      OBS_MCP_READ_ONLY: "false", OBS_MCP_DYNAMIC_TOOLSETS: "false", OBS_MCP_CONFIRM_LIVE: "false", OBS_MCP_READ_OBS_CONFIG: "false" },
    stderr: "pipe",
  });
  transport.stderr?.on("data", () => undefined);
  const count = (request: string) => fake.history().filter(entry => entry.frame.op === 6 && entry.frame.d.requestType === request).length;
  try {
    await client.connect(transport);
    const started = await client.callTool({ name: "obs-take-start", arguments: { expectSilent: true } });
    expect(started.isError).not.toBe(true);
    expect(active).toBe(true);
    const tools = (await client.listTools()).tools.map(tool => tool.name);
    let setter: unknown;
    try { setter = await client.callTool({ name: "obs-set-input-mute", arguments: { inputName: "Application", inputMuted: true } }); }
    catch (error) { setter = { refused: error instanceof Error ? error.message : String(error) }; }
    const staged = await client.callTool({ name: "obs-stage-scene", arguments: { sceneCollection: state.collection, sceneName: state.sceneName } });
    expect(active).toBe(true);
    if (native) {
      expect(tools).not.toContain("obs-set-input-mute");
      expect(count("SetInputMute")).toBe(0);
      expect(staged.isError).toBe(true);
      expect(staged.content).toEqual(expect.arrayContaining([expect.objectContaining({ type: "text", text: expect.stringContaining("Busy") })]));
      expect(count("SetCurrentPreviewScene")).toBe(0);
    } else {
      expect(tools).toContain("obs-set-input-mute");
      expect(setter).toMatchObject({ content: [{ type: "text", text: "Successfully muted input: Application" }] });
      expect(count("SetInputMute")).toBe(1);
      expect(staged.isError).not.toBe(true);
      expect(staged.structuredContent).toMatchObject({ status: "staged", requestAccepted: true, renderedComposition: "unverified" });
      expect(count("SetCurrentPreviewScene")).toBe(1);
      const locked = await client.callTool({ name: "obs-set-current-profile", arguments: { profileName: "Other" } });
      expect(locked.isError).toBe(true);
      expect(count("SetCurrentProfile")).toBe(0);
    }
    const stopped = await client.callTool({ name: "obs-take-stop", arguments: {} });
    expect(stopped.structuredContent).toMatchObject({ stopConfirmed: true });
    expect(active).toBe(false);
    expect(count("StopRecord")).toBe(1);
    return { native, setter, stage: staged.structuredContent ?? staged, stopConfirmed: true,
      effects: { mute: count("SetInputMute"), stage: count("SetCurrentPreviewScene"), stop: count("StopRecord") } };
  } finally {
    try { await client.close(); await transport.close(); await vi.waitFor(() => expect(fake.openConnectionCount).toBe(0), { timeout: 4_000 }); }
    finally { await fake.close(); rmSync(directory, { recursive: true, force: true }); }
  }
}

it.runIf(Boolean(archived))("preserves actual SDK allowed setter/stage versus immutable83 refusal while Take owns recording", async () => {
  const sha256 = createHash("sha256").update(readFileSync(archived!)).digest("hex");
  expect(sha256).toBe("a8d75cf31a4e5352cd7bb529e955e381884c3fd00f1ed803ad154504e039c066");
  const source = await run(false);
  const predecessor = await run(true);
  const evidence = { archivedBinarySha256: sha256, source, predecessor };
  if (process.env.RECEIPT_POLICY_EVIDENCE) {
    writeFileSync(process.env.RECEIPT_POLICY_EVIDENCE, `${JSON.stringify(evidence, null, 2)}\n`, { flag: "wx", mode: 0o600 });
  }
  console.info(JSON.stringify(evidence));
}, 20_000);
