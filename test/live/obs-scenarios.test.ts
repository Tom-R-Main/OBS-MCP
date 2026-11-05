/* SPDX-License-Identifier: MIT */
/*
 * Use-case scenarios against a real OBS. Each one builds a setup through MCP
 * tools, records it, and judges the file with ffmpeg rather than trusting
 * OBS's own status. See "Scenario tests" in the README for the setup.
 */
import { mkdtempSync, realpathSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, type CallToolResult } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  GAME_TONE_HZ,
  MIC_TONE_HZ,
  SYNC_BEEP_HZ,
  generateScenarioFixtures,
  hasMediaTools,
  inspectRecording,
  type RecordingReport,
  type ScenarioFixtures,
} from "../support/scenario-media.js";

type JsonObject = Record<string, unknown>;

const enabled = process.env.OBS_MCP_LIVE_TEST === "1" && process.env.OBS_MCP_LIVE_SCENARIOS === "1";
const expectedCollection = process.env.OBS_MCP_LIVE_SCENE_COLLECTION;
const serverKind = process.env.OBS_MCP_LIVE_SERVER === "native" ? "native" : "oracle";
const projectRoot = fileURLToPath(new URL("../../", import.meta.url));

/** Present tones must be this loud, absent ones this quiet (dBFS). */
const PRESENT_DB = -20;
const ABSENT_DB = -50;
/** Editors can fix a constant offset, but more than a few frames reads as out of sync. */
const MAX_BASELINE_OFFSET_S = 0.1;

let client: Client | undefined;
let workDir: string;
let stateDir: string | undefined;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function textContent(result: CallToolResult): string {
  return result.content
    .filter((block): block is { type: "text"; text: string } => isObject(block) && block.type === "text" && typeof block.text === "string")
    .map(({ text }) => text)
    .join("\n");
}

async function call(toolName: string, args: JsonObject = {}): Promise<JsonObject> {
  if (!client) throw new Error("Scenario MCP client is not connected");
  const result = await client.callTool({ name: toolName, arguments: args }) as CallToolResult;
  if (result.isError) throw new Error(`${toolName} failed: ${textContent(result)}`);
  return isObject(result.structuredContent) ? result.structuredContent : { text: textContent(result) };
}

/** Which audio tracks the active profile's recording writes, from the same settings OBS reads. */
async function recordedTracks(): Promise<number[]> {
  const parameter = async (parameterCategory: string, parameterName: string) => {
    const { parameterValue } = await call("obs-get-profile-parameter", { parameterCategory, parameterName });
    return typeof parameterValue === "string" ? parameterValue : null;
  };
  let mask: number;
  if (await parameter("Output", "Mode") === "Advanced") {
    const ffmpeg = await parameter("AdvOut", "RecType") === "FFmpeg";
    mask = Number(await parameter("AdvOut", ffmpeg ? "FFAudioMixes" : "RecTracks") ?? "1");
  } else {
    const quality = await parameter("SimpleOutput", "RecQuality");
    const format = await parameter("SimpleOutput", "RecFormat2");
    mask = quality === "Stream" || format === "flv" ? 1 : Number(await parameter("SimpleOutput", "RecTracks") ?? "1");
  }
  return [1, 2, 3, 4, 5, 6].filter((track) => (mask & (1 << (track - 1))) !== 0);
}

const tracks = (...on: number[]) => Object.fromEntries([1, 2, 3, 4, 5, 6].map((track) => [String(track), on.includes(track)]));

async function recordFor(seconds: number): Promise<string> {
  await call("obs-start-record");
  let outputPath: string | undefined;
  try {
    await call("obs-sleep", { sleepMillis: seconds * 1000 });
  } finally {
    const stopped = await call("obs-stop-record");
    const message = String(stopped.message ?? stopped.text ?? "");
    outputPath = typeof stopped.outputPath === "string" ? stopped.outputPath : /saved to: (.+)$/m.exec(message)?.[1];
  }
  if (!outputPath) throw new Error("obs-stop-record did not report the recording path");
  return outputPath;
}

function summarize(report: RecordingReport): string {
  return JSON.stringify({
    format: report.formatName,
    duration: report.duration,
    tracks: report.tracks.map(({ levels }) => Object.fromEntries(Object.entries(levels).map(([hz, db]) => [hz, Math.round(db)]))),
    sync: report.sync,
    flashes: report.flashes,
    beeps: report.beeps,
  });
}

describe.skipIf(!enabled)(`OBS use-case scenarios (${serverKind} server)`, () => {
  beforeAll(async () => {
    if (!hasMediaTools) throw new Error("Scenario tests need ffmpeg and ffprobe on PATH");
    if (!expectedCollection) throw new Error("OBS_MCP_LIVE_SCENE_COLLECTION is required for scenario tests");
    workDir = mkdtempSync(join(tmpdir(), "obs-mcp-scenarios-"));

    const env: Record<string, string> = {
      ...getDefaultEnvironment(),
      OBS_WEBSOCKET_URL: process.env.OBS_WEBSOCKET_URL ?? "ws://localhost:4455",
      ...(process.env.OBS_WEBSOCKET_PASSWORD ? { OBS_WEBSOCKET_PASSWORD: process.env.OBS_WEBSOCKET_PASSWORD } : {}),
      ...(process.env.OBS_MCP_READ_OBS_CONFIG ? { OBS_MCP_READ_OBS_CONFIG: process.env.OBS_MCP_READ_OBS_CONFIG } : {}),
    };
    let command = process.execPath;
    let args = [join(projectRoot, "build/index.js")];
    if (serverKind === "native") {
      // Advanced writes durable operation receipts; keep them out of any real state namespace.
      // The service refuses a path with a symlinked ancestor, such as macOS's /var.
      stateDir = realpathSync(mkdtempSync(join(tmpdir(), "obs-mcp-scenario-state-")));
      command = process.env.OBS_MCP_LIVE_NATIVE_BIN ?? join(projectRoot, "native/target/release/obs-mcp-native");
      args = [];
      Object.assign(env, { OBS_MCP_NATIVE_PROFILE: "advanced", OBS_MCP_STATE_DIR: stateDir });
    }

    client = new Client({ name: "obs-mcp-scenario-test", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
    const transport = new StdioClientTransport({ command, args, cwd: projectRoot, env, stderr: "pipe" });
    transport.stderr?.on("data", () => undefined);
    await client.connect(transport);
    // A real request: the native service connects to OBS on first use, not at startup.
    await vi.waitFor(async () => {
      expect(await call("obs-get-version")).toEqual(expect.objectContaining({ obsVersion: expect.any(String) }));
    }, { timeout: 20_000, interval: 500 });

    const collection = await call("obs-get-scene-collection-list");
    expect(collection.currentSceneCollectionName, "Select the disposable scenario collection in OBS first").toBe(expectedCollection);
    const [stream, record] = [await call("obs-get-stream-status"), await call("obs-get-record-status")];
    expect(stream.outputActive, "Streaming must be stopped").toBe(false);
    expect(record.outputActive, "Recording must be stopped").toBe(false);
  });

  afterAll(async () => {
    await client?.close();
    client = undefined;
    if (workDir) rmSync(workDir, { recursive: true, force: true });
    if (stateDir) rmSync(stateDir, { recursive: true, force: true });
  });

  /*
   * Use case 2: a vlogger records the mic and the game on separate tracks for
   * an editor, and lines the tracks up with the picture afterwards. Two tones
   * stand in for the mic and the game; a flash-and-beep clip measures sync.
   *
   * Not covered yet: correcting sync with an audio sync offset. On OBS
   * 32.2.2, changing SetInputAudioSyncOffset on a playing media source did
   * not move the recorded audio by the requested amount (a change from zero
   * is ignored by source_output_audio_data() in libobs/obs-source.c; other
   * changes gave inconsistent shifts). It needs its own investigation.
   */
  describe("multi-track recording for editing", () => {
    const run = `${process.pid}_${Date.now()}`;
    const sceneName = `__obs_mcp_scenario_tracks_${run}`;
    const names = {
      sync: `__scenario_sync_${run}`,
      mic: `__scenario_mic_${run}`,
      game: `__scenario_game_${run}`,
    };
    const muted: string[] = [];
    const created: string[] = [];
    const scenes: string[] = [];
    let originalScene: unknown;
    let originalDirectory: unknown;

    beforeAll(async () => {
      expect(
        await recordedTracks(),
        "Set the scenario profile to record audio tracks 1, 2 and 3 (Settings > Output > Recording), then restart OBS",
      ).toEqual(expect.arrayContaining([1, 2, 3]));
      const fixtures: ScenarioFixtures = generateScenarioFixtures(workDir, { seconds: 10 });
      ({ currentProgramSceneName: originalScene } = await call("obs-get-current-scene"));
      ({ recordDirectory: originalDirectory } = await call("obs-get-record-directory"));

      // Silence every other audio source so only the fixtures reach the tracks.
      const { inputs } = await call("obs-get-input-list");
      for (const input of Array.isArray(inputs) ? inputs.filter(isObject) : []) {
        const inputName = String(input.inputName);
        const state = await call("obs-get-input-mute", { inputName }).catch(() => undefined);
        if (state?.inputMuted === false) {
          await call("obs-set-input-mute", { inputName, inputMuted: true });
          muted.push(inputName);
        }
      }

      await call("obs-set-record-directory", { recordDirectory: mkdtempSync(join(workDir, "recordings-")) });
      await call("obs-create-scene", { sceneName });
      scenes.push(sceneName);
      const sources = [
        { inputName: names.sync, file: fixtures.syncClip, on: [1] },
        { inputName: names.mic, file: fixtures.micTone, on: [2] },
        { inputName: names.game, file: fixtures.gameTone, on: [3] },
      ];
      for (const { inputName, file, on } of sources) {
        await call("obs-create-input", {
          sceneName,
          inputName,
          inputKind: "ffmpeg_source",
          inputSettings: { is_local_file: true, local_file: file, looping: true, restart_on_activate: true, close_when_inactive: false },
          sceneItemEnabled: true,
        });
        created.push(inputName);
        await call("obs-set-input-audio-tracks", { inputName, inputAudioTracks: tracks(...on) });
      }
      await call("obs-set-current-scene", { sceneName });
      await call("obs-sleep", { sleepMillis: 1_500 });
    }, 120_000);

    afterAll(async () => {
      if (!client) return;
      const failures: string[] = [];
      const attempt = async (step: string, run: () => Promise<unknown>) => {
        try { await run(); } catch (error) { failures.push(`${step}: ${error instanceof Error ? error.message : String(error)}`); }
      };
      const record = await call("obs-get-record-status").catch(() => undefined);
      if (record?.outputActive === true) await attempt("stop recording", () => call("obs-stop-record"));
      if (typeof originalScene === "string") await attempt("restore scene", () => call("obs-set-current-scene", { sceneName: originalScene }));
      for (const inputName of created) await attempt(`remove ${inputName}`, () => call("obs-remove-input", { inputName }));
      for (const scene of scenes) await attempt(`remove ${scene}`, () => call("obs-remove-scene", { sceneName: scene }));
      for (const inputName of muted) await attempt(`unmute ${inputName}`, () => call("obs-set-input-mute", { inputName, inputMuted: false }));
      if (typeof originalDirectory === "string") {
        await attempt("restore record directory", () => call("obs-set-record-directory", { recordDirectory: originalDirectory }));
      }
      if (failures.length > 0) throw new Error(`Scenario cleanup failed:\n${failures.join("\n")}`);
    }, 60_000);

    it("keeps each source on its own track, in sync with the picture", async () => {
      const report = inspectRecording(await recordFor(6));
      const evidence = summarize(report);
      expect(report.tracks.length, evidence).toBeGreaterThanOrEqual(3);
      const [syncTrack, micTrack, gameTrack] = report.tracks;
      expect(micTrack!.levels[MIC_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(micTrack!.levels[GAME_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(micTrack!.levels[SYNC_BEEP_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(gameTrack!.levels[GAME_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(gameTrack!.levels[MIC_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(gameTrack!.levels[SYNC_BEEP_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(syncTrack!.levels[MIC_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(syncTrack!.levels[GAME_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(report.sync?.pairs ?? 0, evidence).toBeGreaterThanOrEqual(3);
      expect(Math.abs(report.sync!.medianOffset), evidence).toBeLessThanOrEqual(MAX_BASELINE_OFFSET_S);
      expect(report.sync!.worstOffset, evidence).toBeLessThanOrEqual(MAX_BASELINE_OFFSET_S);
    }, 60_000);
  });
});
