/* SPDX-License-Identifier: MIT */
/*
 * Six users and their goals, each run end to end against a real OBS through
 * the shipped native service in its default Production profile. Every check
 * reads what came out of OBS (a recording, a replay, what an RTMP server
 * received, a screenshot) rather than what OBS reported. See docs/personas.md
 * for the personas and "Persona tests" in the README for the setup.
 */
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { mkdtempSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Client, type CallToolResult } from "@modelcontextprotocol/client";
import { StdioClientTransport, getDefaultEnvironment } from "@modelcontextprotocol/client/stdio";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import {
  PERSONA_COLORS,
  chapterTitles,
  colorNear,
  generatePersonaFixtures,
  regionColor,
  type PersonaFixtures,
} from "../support/persona-media.js";
import { GAME_TONE_HZ, MIC_TONE_HZ, hasMediaTools, inspectRecording } from "../support/scenario-media.js";

type JsonObject = Record<string, unknown>;
type Result = JsonObject & { images: string[] };

const enabled = process.env.OBS_MCP_LIVE_TEST === "1" && process.env.OBS_MCP_LIVE_PERSONAS === "1";
const collection = process.env.OBS_MCP_LIVE_SCENE_COLLECTION ?? "";
const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const nativeBin = process.env.OBS_MCP_LIVE_NATIVE_BIN ?? join(projectRoot, "native/target/release/obs-mcp-native");
const hasTkinter = !spawnSync("python3", ["-c", "import tkinter"]).error && spawnSync("python3", ["-c", "import tkinter"]).status === 0;
/** Sam's journey creates a macOS `screen_capture` input; X11 and PipeWire capture are not covered yet. */
const canRunProductDemo = process.platform === "darwin" && hasTkinter;
/** Reading frames from the OBS Virtual Camera device needs camera permission for the test runner. */
const readVirtualCamera = process.env.OBS_MCP_LIVE_VCAM_FRAMES === "1";

/** Present tones must be this loud, absent ones this quiet (dBFS). */
const PRESENT_DB = -20;
const ABSENT_DB = -50;
/** OBS output frames are 1280×720 for a 1920×1080 canvas in the scenario profile; regions are given in canvas pixels. */
let outputScale = 1;

let fixtures: PersonaFixtures;
let workDir: string;

const isObject = (value: unknown): value is JsonObject => typeof value === "object" && value !== null && !Array.isArray(value);
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
const tracks = (...on: number[]) => Object.fromEntries([1, 2, 3, 4, 5, 6].map((track) => [String(track), on.includes(track)]));
const canvasRegion = (x: number, y: number, width: number, height: number) => ({
  x: Math.round(x * outputScale), y: Math.round(y * outputScale), width: Math.round(width * outputScale), height: Math.round(height * outputScale),
});
const privateDir = (prefix: string) => realpathSync(mkdtempSync(join(workDir, prefix)));

/** One MCP session against the native service, configured the way a persona would install it. */
class Session {
  private constructor(private readonly client: Client, private readonly stateDir: string) {}

  static async open({ recordingRoot }: { recordingRoot?: string } = {}): Promise<Session> {
    // The service refuses a state path with a symlinked ancestor, such as macOS's /var.
    const stateDir = realpathSync(mkdtempSync(join(tmpdir(), "obs-mcp-persona-state-")));
    const env: Record<string, string> = {
      ...getDefaultEnvironment(),
      OBS_WEBSOCKET_URL: process.env.OBS_WEBSOCKET_URL ?? "ws://localhost:4455",
      OBS_MCP_NATIVE_PROFILE: "production",
      OBS_MCP_STATE_DIR: stateDir,
      ...(process.env.OBS_WEBSOCKET_PASSWORD ? { OBS_WEBSOCKET_PASSWORD: process.env.OBS_WEBSOCKET_PASSWORD } : {}),
      ...(process.env.OBS_MCP_READ_OBS_CONFIG ? { OBS_MCP_READ_OBS_CONFIG: process.env.OBS_MCP_READ_OBS_CONFIG } : {}),
      ...(recordingRoot ? { OBS_MCP_NATIVE_RECORDING_ROOT: recordingRoot } : {}),
    };
    const client = new Client({ name: "obs-mcp-persona-test", version: "1.0.0" }, { versionNegotiation: { mode: { pin: "2026-07-28" } } });
    const transport = new StdioClientTransport({ command: nativeBin, args: [], cwd: projectRoot, env, stderr: "pipe" });
    transport.stderr?.on("data", () => undefined);
    await client.connect(transport);
    const session = new Session(client, stateDir);
    // The native service connects to OBS on first use, not at startup.
    await vi.waitFor(async () => {
      expect(await session.call("obs-get-version")).toEqual(expect.objectContaining({ obsVersion: expect.any(String) }));
    }, { timeout: 20_000, interval: 500 });
    return session;
  }

  async tools(): Promise<string[]> {
    return (await this.client.listTools()).tools.map(({ name }) => name);
  }

  async attempt(name: string, args: JsonObject = {}): Promise<{ ok: boolean; text: string; result: Result }> {
    const response = await this.client.callTool({ name, arguments: args }) as CallToolResult;
    const text = response.content.flatMap((block) => (block.type === "text" ? [block.text] : [])).join("\n");
    const images = response.content.flatMap((block) => (block.type === "image" ? [block.data] : []));
    const structured = isObject(response.structuredContent) ? response.structuredContent : { text };
    return { ok: !response.isError, text, result: { ...structured, images } };
  }

  async call(name: string, args: JsonObject = {}): Promise<Result> {
    const { ok, text, result } = await this.attempt(name, args);
    if (!ok) throw new Error(`${name} failed: ${text}`);
    return result;
  }

  async close(): Promise<void> {
    await this.client.close();
    rmSync(this.stateDir, { recursive: true, force: true });
  }
}

/** Writes the first image of a result to disk so ffmpeg can read its pixels. */
function saveImage(result: Result, name: string): string {
  const file = join(workDir, `${name}.png`);
  writeFileSync(file, Buffer.from(result.images[0] ?? "", "base64"));
  return file;
}

/** Each output's status tool and the tool that stops it. */
const OUTPUTS = [
  ["obs-get-stream-status", "obs-stop-stream"],
  ["obs-get-record-status", "obs-take-stop"],
  ["obs-get-replay-buffer-status", "obs-stop-replay-buffer"],
  ["obs-get-virtual-cam-status", "obs-stop-virtual-cam"],
] as const;

/**
 * Stops a test-owned child process and waits for it to exit: SIGTERM, then
 * SIGKILL after `graceMs`. Throws if it is still running after both.
 */
async function stopChild(child: ChildProcess | undefined, graceMs = 3_000): Promise<void> {
  if (!child || child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once("exit", () => resolve()));
  const within = (ms: number) => Promise.race([exited.then(() => true), sleep(ms).then(() => false)]);
  child.kill("SIGTERM");
  if (await within(graceMs)) return;
  child.kill("SIGKILL");
  if (!(await within(graceMs))) throw new Error(`Child process ${child.pid} did not exit after SIGKILL`);
}

/**
 * Records what each persona changes and puts it back afterwards: outputs,
 * program scene, Studio Mode, recording directory, and its scenes and inputs.
 */
class Footprint {
  private readonly scenes = new Set<string>();
  private readonly inputs = new Set<string>();
  private programScene?: string;
  private recordDirectory?: string;
  /** Outputs already running when the persona began; cleanup leaves them alone. */
  private readonly preexisting = new Set<string>();

  constructor(private readonly session: Session) {}

  async begin(): Promise<void> {
    ({ currentProgramSceneName: this.programScene } = await this.session.call("obs-get-current-scene") as { currentProgramSceneName?: string });
    ({ recordDirectory: this.recordDirectory } = await this.session.call("obs-get-record-directory") as { recordDirectory?: string });
    for (const [status] of OUTPUTS) {
      const { ok, result } = await this.session.attempt(status);
      if (ok && result.outputActive === true) this.preexisting.add(status);
    }
  }

  /** Scenes and inputs the persona is about to create; leftovers from an interrupted run are removed first. */
  async own({ scenes = [], inputs = [] }: { scenes?: string[]; inputs?: string[] }): Promise<void> {
    for (const scene of scenes) {
      this.scenes.add(scene);
      await this.session.attempt("obs-remove-scene", { sceneName: scene });
    }
    for (const input of inputs) {
      this.inputs.add(input);
      await this.session.attempt("obs-remove-input", { inputName: input });
    }
  }

  async restore(): Promise<void> {
    const failures: string[] = [];
    const step = async (what: string, name: string, args: JsonObject = {}) => {
      const { ok, text } = await this.session.attempt(name, args);
      if (!ok) failures.push(`${what}: ${text}`);
    };
    for (const [status, stop] of OUTPUTS) {
      if (this.preexisting.has(status)) continue;
      const { ok, result } = await this.session.attempt(status);
      if (!ok || result.outputActive !== true) continue;
      await step(`stop (${status})`, stop);
      // Stop requests return before OBS finishes; settings restored next depend on it.
      try { await waitInactive(this.session, status); } catch (error) { failures.push(`${status} still active: ${error instanceof Error ? error.message : String(error)}`); }
    }
    await step("leave Studio Mode", "obs-set-studio-mode", { studioModeEnabled: false });
    if (this.programScene && !this.scenes.has(this.programScene)) await step("restore program scene", "obs-set-current-scene", { sceneName: this.programScene });
    for (const scene of this.scenes) await this.session.attempt("obs-remove-scene", { sceneName: scene });
    for (const input of this.inputs) await this.session.attempt("obs-remove-input", { inputName: input });
    if (this.recordDirectory) await step("restore record directory", "obs-set-record-directory", { recordDirectory: this.recordDirectory });
    if (failures.length > 0) throw new Error(`Persona cleanup failed:\n${failures.join("\n")}`);
  }
}

/** Waits for an output to report inactive. Stop requests return before OBS finishes stopping. */
async function waitInactive(session: Session, statusTool: string): Promise<void> {
  await vi.waitFor(async () => expect((await session.call(statusTool)).outputActive).toBe(false), { timeout: 15_000, interval: 250 });
}

/** A paced timeline: waits until `seconds` after `start`. */
function clock(): (seconds: number) => Promise<void> {
  const start = Date.now();
  return async (seconds) => { await sleep(Math.max(0, seconds * 1000 - (Date.now() - start))); };
}

const media = (file: string) => ({ is_local_file: true, local_file: file, looping: true, close_when_inactive: false });
const pip = (x: number, y: number, width: number, height: number) => ({ positionX: x, positionY: y, boundsType: "OBS_BOUNDS_SCALE_INNER", boundsWidth: width, boundsHeight: height });

describe.skipIf(!enabled)("personas reach their goals on a real OBS (native Production)", () => {
  beforeAll(async () => {
    if (!hasMediaTools) throw new Error("Persona tests need ffmpeg and ffprobe on PATH");
    if (!collection) throw new Error("OBS_MCP_LIVE_SCENE_COLLECTION is required for persona tests");
    workDir = realpathSync(mkdtempSync(join(tmpdir(), "obs-mcp-personas-")));
    fixtures = generatePersonaFixtures(privateDir("media-"));

    const session = await Session.open();
    try {
      const { currentSceneCollectionName } = await session.call("obs-get-scene-collection-list");
      expect(currentSceneCollectionName, "Select the disposable persona collection in OBS first").toBe(collection);
      const parameter = async (parameterCategory: string, parameterName: string) =>
        (await session.call("obs-get-profile-parameter", { parameterCategory, parameterName })).parameterValue;
      expect(await parameter("Output", "Mode"), "Persona tests expect Simple output mode").toBe("Simple");
      expect(Number(await parameter("SimpleOutput", "RecTracks")) & 0b111, "Record audio tracks 1, 2 and 3, then restart OBS").toBe(0b111);
      expect(await parameter("SimpleOutput", "RecFormat2"), "Chapters need the Hybrid MP4 recording format").toBe("hybrid_mp4");
      expect(await parameter("SimpleOutput", "RecRB"), "Enable the replay buffer, then restart OBS").toBe("true");
      // Cleanup stops outputs the personas start, so none may be running beforehand.
      for (const [status] of OUTPUTS) {
        expect((await session.call(status)).outputActive, `${status}: stop every output first`).toBe(false);
      }
      const video = await session.call("obs-get-video-settings");
      expect([video.baseWidth, video.baseHeight], "Persona layouts assume a 1920×1080 canvas").toEqual([1920, 1080]);
      outputScale = Number(video.outputWidth) / 1920;
    } finally {
      await session.close();
    }
  }, 120_000);

  afterAll(() => {
    if (workDir) rmSync(workDir, { recursive: true, force: true });
  });

  /*
   * Maya streams games to Twitch. She wants "Starting soon", "Live" and "BRB"
   * scenes with her webcam and an alert overlay, to go live, pop an alert,
   * step away and come back, and end the stream cleanly.
   */
  describe("Maya, a streamer with overlays", () => {
    let session: Session;
    let footprint: Footprint;
    let originalService: Result | undefined;
    let sink: ChildProcess | undefined;

    beforeAll(async () => {
      // No recording root: Production offers the stream tools only without one.
      session = await Session.open();
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({
        scenes: ["Maya · Starting Soon", "Maya · Live", "Maya · BRB"],
        inputs: ["Maya Starting BG", "Maya Starting Title", "Maya Gameplay", "Maya Webcam", "Maya Follower Alert", "Maya BRB Card"],
      });
      originalService = await session.call("obs-get-stream-service-settings");
    }, 60_000);

    afterAll(async () => {
      const failures: unknown[] = [];
      const run = async (task: () => Promise<unknown>) => { try { await task(); } catch (error) { failures.push(error); } };
      // restore() waits for the stream to stop; OBS refuses service changes while streaming.
      await run(() => footprint?.restore());
      // OBS refuses empty service settings, so an unconfigured service cannot be put back.
      if (session && originalService && isObject(originalService.streamServiceSettings) && Object.keys(originalService.streamServiceSettings).length > 0) {
        await run(() => session.call("obs-set-stream-service-settings", {
          streamServiceType: originalService!.streamServiceType,
          streamServiceSettings: originalService!.streamServiceSettings,
        }));
      }
      await run(() => stopChild(sink));
      await run(() => session?.close());
      if (failures.length > 0) throw new AggregateError(failures, "Maya cleanup failed");
    }, 60_000);

    it("builds her scenes, goes live, pops an alert, takes a break, and ends the stream", async () => {
      expect(await session.tools()).toEqual(expect.arrayContaining(["obs-start-stream", "obs-stop-stream"]));
      const scenes = [
        { sceneName: "Maya · Starting Soon", sources: [
          { name: "Maya Starting BG", kind: "color_source_v3", settings: { color: 0xff0080ff, width: 1920, height: 1080 } },
          { name: "Maya Starting Title", kind: "text_ft2_source_v2", settings: { text: "Starting soon", font: { face: "Helvetica", size: 96, style: "Bold" } }, transform: { positionX: 560, positionY: 480 } },
        ] },
        { sceneName: "Maya · Live", sources: [
          { name: "Maya Gameplay", kind: "ffmpeg_source", settings: media(fixtures.game), fit: "canvas" },
          { name: "Maya Webcam", kind: "ffmpeg_source", settings: media(fixtures.camera), transform: pip(40, 760, 480, 270) },
          { name: "Maya Follower Alert", kind: "image_source", settings: { file: fixtures.alert }, visible: false, transform: { positionX: 660, positionY: 60 } },
        ] },
        { sceneName: "Maya · BRB", sources: [{ name: "Maya BRB Card", kind: "color_source_v3", settings: { color: 0xff802060, width: 1920, height: 1080 } }] },
      ];
      for (const scene of scenes) {
        const applied = await session.call("obs-apply-scene", { apply: true, order: true, ...scene });
        expect(applied.converged, JSON.stringify(applied)).toBe(true);
      }
      await session.call("obs-set-current-scene", { sceneName: "Maya · Starting Soon" });

      const port = 19_350 + Math.floor(Math.random() * 500);
      const received = join(workDir, "maya-received.flv");
      sink = spawn("ffmpeg", ["-v", "error", "-y", "-listen", "1", "-i", `rtmp://127.0.0.1:${port}/live/maya`, "-c", "copy", received], { stdio: "ignore" });
      await sleep(1_000);
      await session.call("obs-set-stream-service-settings", {
        streamServiceType: "rtmp_custom",
        streamServiceSettings: { server: `rtmp://127.0.0.1:${port}/live`, key: "maya", use_auth: false },
      });

      await session.call("obs-start-stream");
      const at = clock();
      await at(3); await session.call("obs-set-current-scene", { sceneName: "Maya · Live" });
      const { sceneItemId } = await session.call("obs-get-scene-item-id", { sceneName: "Maya · Live", sourceName: "Maya Follower Alert" });
      await at(6); await session.call("obs-set-scene-item-enabled", { sceneName: "Maya · Live", sceneItemId, enabled: true });
      await at(9); await session.call("obs-set-scene-item-enabled", { sceneName: "Maya · Live", sceneItemId, enabled: false });
      await at(11); await session.call("obs-set-current-scene", { sceneName: "Maya · BRB" });
      await at(14); await session.call("obs-set-current-scene", { sceneName: "Maya · Live" });
      await at(16);
      const status = await session.call("obs-get-stream-status");
      expect(status.outputSkippedFrames, JSON.stringify(status)).toBe(0);
      await session.call("obs-stop-stream");
      await waitInactive(session, "obs-get-stream-status");
      await vi.waitFor(() => expect(sink?.exitCode ?? sink?.signalCode).not.toBeNull(), { timeout: 10_000 });

      // What the ingest received, moment by moment. The stream starts at 0 s in the file.
      const alert = canvasRegion(735, 85, 450, 150);
      const center = canvasRegion(600, 300, 1200, 600);
      const look = (seconds: number) => ({ alert: regionColor(received, seconds, alert), center: regionColor(received, seconds, center) });
      const moments = { starting: look(1.5), live: look(4.5), alert: look(7.5), afterAlert: look(10.2), brb: look(12.7), back: look(15.2) };
      const evidence = JSON.stringify(moments);
      expect(colorNear(moments.starting.center, [255, 128, 0]), evidence).toBe(true);
      expect(colorNear(moments.live.center, PERSONA_COLORS.game), evidence).toBe(true);
      expect(colorNear(moments.live.alert, PERSONA_COLORS.alert), evidence).toBe(false);
      expect(colorNear(moments.alert.alert, PERSONA_COLORS.alert), evidence).toBe(true);
      expect(colorNear(moments.afterAlert.alert, PERSONA_COLORS.alert), evidence).toBe(false);
      expect(colorNear(moments.brb.center, [96, 32, 128]), evidence).toBe(true);
      expect(colorNear(moments.back.center, PERSONA_COLORS.game), evidence).toBe(true);
    }, 120_000);
  });

  /*
   * Dev makes YouTube tutorials. He wants his screen with his camera in the
   * corner, his voice and desktop audio on separate tracks for DaVinci
   * Resolve, chapters for each segment, and a quick look at the take.
   */
  describe("Dev, a vlogger editing in Resolve", () => {
    let session: Session;
    let footprint: Footprint;
    let recordingRoot: string;

    beforeAll(async () => {
      recordingRoot = privateDir("dev-recordings-");
      session = await Session.open({ recordingRoot });
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({ scenes: ["Dev · Screen + Cam"], inputs: ["Dev Desktop", "Dev Camera"] });
    }, 60_000);

    afterAll(async () => {
      try { await footprint?.restore(); } finally { await session?.close(); }
    }, 60_000);

    it("records a chaptered take with his voice and desktop on their own tracks, camera in the corner", async () => {
      await session.call("obs-set-record-directory", { recordDirectory: recordingRoot });
      const applied = await session.call("obs-apply-scene", { apply: true, order: true, sceneName: "Dev · Screen + Cam", sources: [
        { name: "Dev Desktop", kind: "ffmpeg_source", settings: media(fixtures.game), fit: "canvas", audioTracks: [1, 3] },
        { name: "Dev Camera", kind: "ffmpeg_source", settings: media(fixtures.camera), audioTracks: [1, 2], transform: pip(1440, 810, 480, 270) },
      ] });
      expect(applied.converged, JSON.stringify(applied)).toBe(true);
      await session.call("obs-set-current-scene", { sceneName: "Dev · Screen + Cam" });
      await sleep(1_000);
      expect((await session.call("obs-preflight")).ready).toBe(true);

      await session.call("obs-take-start", { chapter: "Intro", maxDurationSeconds: 120 });
      const at = clock();
      await at(4); await session.call("obs-take-mark", { name: "Demo" });
      await at(8); await session.call("obs-take-mark", { name: "Outro" });
      await at(11);
      const stop = await session.call("obs-take-stop");
      expect(stop.problems, JSON.stringify(stop)).toEqual([]);
      const file = String(stop.outputPath);

      expect(chapterTitles(file).map(({ title }) => title)).toEqual(["Intro", "Demo", "Outro"]);
      const report = inspectRecording(file);
      const levels = report.tracks.map(({ levels }) => levels);
      const evidence = JSON.stringify(levels);
      expect(levels.length, evidence).toBeGreaterThanOrEqual(3);
      expect(levels[0]![MIC_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[0]![GAME_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[1]![MIC_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[1]![GAME_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(levels[2]![GAME_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[2]![MIC_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      // Regions avoid the band each clip's moving box travels through.
      expect(colorNear(regionColor(file, 5, canvasRegion(1500, 820, 300, 70)), PERSONA_COLORS.camera)).toBe(true);
      expect(colorNear(regionColor(file, 5, canvasRegion(100, 100, 1200, 600)), PERSONA_COLORS.game)).toBe(true);

      const sheet = await session.call("obs-contact-sheet", { path: file });
      expect(sheet.images).toHaveLength(1);
      // OBS marks all three tracks default; trimming must still accept the take.
      const plan = await session.call("obs-trim-take", { path: file });
      expect(plan.durationSeconds).toBeGreaterThan(10);
    }, 180_000);
  });

  /*
   * Priya consults over video calls. She wants her camera with a branded
   * lower third as her meeting camera, to check it before joining, to step
   * away behind a "back soon" card, and to turn it off when the call ends.
   */
  describe("Priya, a consultant on video calls", () => {
    let session: Session;
    let footprint: Footprint;

    beforeAll(async () => {
      session = await Session.open();
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({
        scenes: ["Priya · Meeting", "Priya · Be Right Back"],
        inputs: ["Priya Camera", "Priya Lower Third Bar", "Priya Lower Third Name", "Priya BRB Card", "Priya BRB Text"],
      });
    }, 60_000);

    afterAll(async () => {
      try { await footprint?.restore(); } finally { await session?.close(); }
    }, 60_000);

    it("checks her meeting camera, joins with the virtual camera, steps away, and leaves", async () => {
      for (const scene of [
        { sceneName: "Priya · Meeting", sources: [
          { name: "Priya Camera", kind: "ffmpeg_source", settings: media(fixtures.camera), fit: "canvas" },
          { name: "Priya Lower Third Bar", kind: "color_source_v3", settings: { color: 0xff808000, width: 1920, height: 160 }, transform: { positionX: 0, positionY: 880 } },
          { name: "Priya Lower Third Name", kind: "text_ft2_source_v2", settings: { text: "Priya Shah · Acme Consulting", font: { face: "Helvetica", size: 64, style: "Bold" } }, transform: { positionX: 60, positionY: 920 } },
        ] },
        { sceneName: "Priya · Be Right Back", sources: [
          { name: "Priya BRB Card", kind: "color_source_v3", settings: { color: 0xff303030, width: 1920, height: 1080 } },
          { name: "Priya BRB Text", kind: "text_ft2_source_v2", settings: { text: "Back in 2 minutes", font: { face: "Helvetica", size: 96, style: "Bold" } }, transform: { positionX: 520, positionY: 480 } },
        ] },
      ]) {
        expect((await session.call("obs-apply-scene", { apply: true, order: true, ...scene })).converged).toBe(true);
      }
      await session.call("obs-set-current-scene", { sceneName: "Priya · Meeting" });
      await sleep(1_000);

      // Pre-meeting check: required items present and the picture looks right.
      const audit = await session.call("obs-audit-scene", {
        sceneCollection: collection, sceneName: "Priya · Meeting", includeScreenshot: true,
        requiredItems: [{ sourceName: "Priya Camera" }, { sourceName: "Priya Lower Third Bar" }, { sourceName: "Priya Lower Third Name" }],
      });
      const checks = (Array.isArray(audit.checks) ? audit.checks : []).filter(isObject);
      expect(checks.filter(({ status }) => status === "fail"), JSON.stringify(checks)).toEqual([]);
      const preview = saveImage(audit, "priya-preview");
      expect(colorNear(regionColor(preview, 0, { x: 400, y: 50, width: 40, height: 40 }), PERSONA_COLORS.camera)).toBe(true);

      await session.call("obs-start-virtual-cam");
      expect((await session.call("obs-get-virtual-cam-status")).outputActive).toBe(true);
      let device: ChildProcess | undefined;
      const deviceFile = join(workDir, "priya-virtual-camera.mkv");
      if (readVirtualCamera) {
        device = spawn("ffmpeg", ["-v", "error", "-y", "-f", "avfoundation", "-framerate", "60", "-video_size", "1920x1080", "-pixel_format", "uyvy422",
          "-i", "OBS Virtual Camera", "-t", "9", "-vf", "scale=480:270", "-c:v", "ffv1", deviceFile], { stdio: "ignore" });
      }
      try {
        const at = clock();
        await at(3); await session.call("obs-set-current-scene", { sceneName: "Priya · Be Right Back" });
        const away = await session.call("obs-get-source-screenshot", { sourceName: "Priya · Be Right Back", imageFormat: "png", imageWidth: 480 });
        expect(colorNear(regionColor(saveImage(away, "priya-away"), 0, { x: 20, y: 20, width: 40, height: 40 }), [48, 48, 48])).toBe(true);
        await at(6); await session.call("obs-set-current-scene", { sceneName: "Priya · Meeting" });
        await at(9);
        if (device) {
          // `-t 9` bounds media time, not wall time: a camera-permission prompt stalls the reader.
          await vi.waitFor(() => expect(device.exitCode ?? device.signalCode).not.toBeNull(), { timeout: 15_000 });
          expect(device.exitCode, "the virtual camera reader failed").toBe(0);
          const scale = 480 / 1920;
          const corner = { x: Math.round(400 * scale), y: 10, width: 20, height: 20 };
          expect(colorNear(regionColor(deviceFile, 1.5, corner), PERSONA_COLORS.camera)).toBe(true);
          expect(colorNear(regionColor(deviceFile, 4.5, corner), [48, 48, 48])).toBe(true);
          expect(colorNear(regionColor(deviceFile, 7.5, corner), PERSONA_COLORS.camera)).toBe(true);
        }
      } finally {
        await stopChild(device);
      }
      await session.call("obs-stop-virtual-cam");
      await waitInactive(session, "obs-get-virtual-cam-status");
    }, 120_000);
  });

  /*
   * Kai plays ranked matches. He wants the game full screen with a facecam,
   * his voice and the game on separate tracks, the replay buffer running, and
   * to save the last moments after a great play without dropping frames.
   */
  describe("Kai, a competitive gamer", () => {
    let session: Session;
    let footprint: Footprint;
    let recordingRoot: string;

    beforeAll(async () => {
      recordingRoot = privateDir("kai-recordings-");
      session = await Session.open({ recordingRoot });
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({ scenes: ["Kai · Ranked"], inputs: ["Kai Game Capture", "Kai Facecam"] });
    }, 60_000);

    afterAll(async () => {
      try { await footprint?.restore(); } finally { await session?.close(); }
    }, 60_000);

    it("saves a verified replay of the last moments with game and voice on their own tracks", async () => {
      await session.call("obs-set-record-directory", { recordDirectory: recordingRoot });
      expect((await session.call("obs-apply-scene", { apply: true, order: true, sceneName: "Kai · Ranked", sources: [
        { name: "Kai Game Capture", kind: "ffmpeg_source", settings: media(fixtures.game), fit: "canvas", audioTracks: [1, 3] },
        { name: "Kai Facecam", kind: "ffmpeg_source", settings: media(fixtures.camera), audioTracks: [1, 2], transform: pip(1500, 40, 380, 214) },
      ] })).converged).toBe(true);
      await session.call("obs-set-current-scene", { sceneName: "Kai · Ranked" });
      const before = await session.call("obs-get-stats");

      await session.call("obs-start-replay-buffer");
      await sleep(20_000);
      const saved = await session.call("obs-save-replay-verified");
      const after = await session.call("obs-get-stats");
      await session.call("obs-stop-replay-buffer");
      await waitInactive(session, "obs-get-replay-buffer-status");

      expect(saved.status, JSON.stringify(saved)).toBe("verified");
      const file = isObject(saved.file) ? saved.file : {};
      expect(Number(file.durationSeconds)).toBeGreaterThan(5);
      const path = String(file.path);
      expect(path.startsWith(recordingRoot)).toBe(true);
      const levels = inspectRecording(path).tracks.map(({ levels }) => levels);
      const evidence = JSON.stringify(levels);
      expect(levels[1]![MIC_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[1]![GAME_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(levels[2]![GAME_TONE_HZ], evidence).toBeGreaterThan(PRESENT_DB);
      expect(levels[2]![MIC_TONE_HZ], evidence).toBeLessThan(ABSENT_DB);
      expect(colorNear(regionColor(path, 3, canvasRegion(1520, 50, 340, 60)), PERSONA_COLORS.camera)).toBe(true);
      expect(colorNear(regionColor(path, 3, canvasRegion(100, 300, 1300, 700)), PERSONA_COLORS.game)).toBe(true);
      expect(Number(after.outputSkippedFrames) - Number(before.outputSkippedFrames)).toBe(0);
    }, 120_000);
  });

  /*
   * Dr. Lin teaches online. She wants slide, slide-with-presenter, and
   * presenter layouts, to preview each change before her audience sees it,
   * to record the lecture with a chapter per section, and to cut the silent
   * wait for questions before uploading.
   */
  describe("Dr. Lin, a lecturer switching layouts", () => {
    let session: Session;
    let footprint: Footprint;
    let recordingRoot: string;

    beforeAll(async () => {
      recordingRoot = privateDir("lin-recordings-");
      session = await Session.open({ recordingRoot });
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({ scenes: ["Lin · Slides + Presenter", "Lin · Slides", "Lin · Presenter"], inputs: ["Lin Slide Deck", "Lin Presenter"] });
    }, 60_000);

    afterAll(async () => {
      try { await footprint?.restore(); } finally { await session?.close(); }
    }, 60_000);

    it("previews each layout before taking it, records chapters, and trims the silent wait", async () => {
      await session.call("obs-set-record-directory", { recordDirectory: recordingRoot });
      const deck = { name: "Lin Slide Deck", kind: "image_source", settings: { file: fixtures.slides[0] }, fit: "canvas" };
      const presenter = { name: "Lin Presenter", kind: "ffmpeg_source", settings: media(fixtures.camera) };
      await session.call("obs-apply-scene", { apply: true, order: true, sceneName: "Lin · Slides + Presenter", sources: [deck, { ...presenter, transform: pip(1420, 780, 480, 270) }] });
      await session.call("obs-apply-scene", { apply: true, sceneName: "Lin · Slides", sources: [{ name: "Lin Slide Deck" }] });
      await session.call("obs-apply-scene", { apply: true, sceneName: "Lin · Presenter", sources: [{ name: "Lin Presenter", fit: "canvas" }] });
      await session.call("obs-set-studio-mode", { studioModeEnabled: true });

      const show = async (sceneName: string) => {
        const staged = await session.call("obs-stage-scene", { sceneCollection: collection, sceneName });
        expect(staged.intendedSceneObservedInPreview, JSON.stringify(staged)).toBe(true);
        expect(staged.images).toHaveLength(1);
        const taken = await session.call("obs-take-scene", { stageId: staged.stageId });
        expect(taken.intendedSceneObservedOnProgram, JSON.stringify(taken)).toBe(true);
      };
      const slide = (index: 0 | 1 | 2) => session.call("obs-set-input-settings", { inputName: "Lin Slide Deck", inputSettings: { file: fixtures.slides[index] } });

      await show("Lin · Slides + Presenter");
      await sleep(1_500);
      await session.call("obs-take-start", { chapter: "Introduction", maxDurationSeconds: 180 });
      const at = clock();
      await at(5); await show("Lin · Slides"); await slide(1); await session.call("obs-take-mark", { name: "Key concepts" });
      await at(10); await show("Lin · Presenter"); await session.call("obs-take-mark", { name: "Worked example" });
      await at(15); await show("Lin · Slides"); await slide(2); await session.call("obs-take-mark", { name: "Q&A" });
      await session.call("obs-set-input-mute", { inputName: "Lin Presenter", inputMuted: true }); // waiting for questions
      await at(26); await session.call("obs-set-input-mute", { inputName: "Lin Presenter", inputMuted: false });
      await at(29);
      const stop = await session.call("obs-take-stop");
      expect(stop.problems, JSON.stringify(stop)).toEqual([]);
      const file = String(stop.outputPath);

      expect(chapterTitles(file).map(({ title }) => title)).toEqual(["Introduction", "Key concepts", "Worked example", "Q&A"]);
      const main = canvasRegion(300, 60, 600, 300);
      const corner = canvasRegion(1440, 790, 440, 80);
      expect(colorNear(regionColor(file, 2.5, main), PERSONA_COLORS.slides[0])).toBe(true);
      expect(colorNear(regionColor(file, 2.5, corner), PERSONA_COLORS.camera)).toBe(true);
      expect(colorNear(regionColor(file, 7.5, corner), PERSONA_COLORS.slides[1])).toBe(true);
      expect(colorNear(regionColor(file, 12.5, main), PERSONA_COLORS.camera)).toBe(true);
      expect(colorNear(regionColor(file, 20, main), PERSONA_COLORS.slides[2])).toBe(true);

      const trimmed = await session.call("obs-trim-take", { path: file, apply: true });
      expect(trimmed.committed, JSON.stringify(trimmed)).toBe(true);
      expect(Number(trimmed.outputDurationSeconds)).toBeLessThan(Number(trimmed.durationSeconds) - 5);
      expect(trimmed.outputChapters).toBe(4);
      const output = String(trimmed.outputPath);
      expect(inspectRecording(output).tracks).toHaveLength(3);
      expect((await session.call("obs-contact-sheet", { path: output })).images).toHaveLength(1);
    }, 180_000);
  });

  /*
   * Sam is a founder recording a product demo. She wants only her app's
   * window in the shot and no stray audio, a chapter per feature, the
   * loading and idle time cut out, and a contact sheet to check the edit.
   */
  describe.skipIf(!canRunProductDemo)("Sam, a founder recording a product demo", () => {
    let session: Session;
    let footprint: Footprint;
    let recordingRoot: string;
    let app: ChildProcess | undefined;
    const title = `Acme Dashboard ${process.pid}`;
    const go = () => join(workDir, "demo-go");

    beforeAll(async () => {
      recordingRoot = privateDir("sam-recordings-");
      session = await Session.open({ recordingRoot });
      footprint = new Footprint(session);
      await footprint.begin();
      await footprint.own({ scenes: ["Sam · Product Demo"], inputs: ["Sam App Window"] });
      app = spawn("python3", [join(projectRoot, "test/support/demo-app.py"), title, go(), "120"], { stdio: "ignore" });
      await sleep(4_000); // let the window appear and settle before discovery
    }, 60_000);

    afterAll(async () => {
      try {
        await stopChild(app);
      } finally {
        try { await footprint?.restore(); } finally { await session?.close(); }
      }
    }, 60_000);

    it("captures only the app window, records a chaptered demo, and trims the dead time", async () => {
      await session.call("obs-set-record-directory", { recordDirectory: recordingRoot });
      await session.call("obs-create-scene", { sceneName: "Sam · Product Demo" });
      await session.call("obs-set-current-scene", { sceneName: "Sam · Product Demo" });
      // obs-capture-window discovers windows through an existing capture; a fresh collection has none.
      await session.call("obs-create-input", { sceneName: "Sam · Product Demo", inputName: "Sam App Window", inputKind: "screen_capture", inputSettings: { type: 1 } });
      let captured: { ok: boolean; text: string; result: Result } | undefined;
      for (let tries = 0; tries < 3 && !captured?.ok; tries += 1) {
        // A window that changes during discovery is refused; the tool asks for a rerun.
        if (tries > 0) await sleep(1_500);
        captured = await session.attempt("obs-capture-window", { sceneName: "Sam · Product Demo", inputName: "Sam App Window", window: title, includeScreenshot: true });
      }
      expect(captured?.ok, captured?.text).toBe(true);
      expect(String(captured!.result.message)).toContain(title);
      const prepared = await session.call("obs-prepare-demo", { sceneCollection: collection, sceneName: "Sam · Product Demo", inputName: "Sam App Window" });
      expect(prepared.status, JSON.stringify(prepared)).toBe("already-prepared");
      expect((await session.call("obs-preflight", { expectSilent: true })).ready).toBe(true);

      await session.call("obs-take-start", { chapter: "Sign in", expectSilent: true, maxDurationSeconds: 120 });
      writeFileSync(go(), "");
      const at = clock();
      await at(11); await session.call("obs-take-mark", { name: "Dashboard" });
      await at(23); await session.call("obs-take-mark", { name: "Export report" });
      await at(30);
      const stop = await session.call("obs-take-stop");
      expect(stop.problems, JSON.stringify(stop)).toEqual([]);
      const take = isObject(stop.take) ? stop.take : {};
      expect(take.blanks, "A black capture usually means OBS lacks Screen Recording permission").toEqual([]);
      const file = String(stop.outputPath);
      expect(chapterTitles(file).map(({ title: name }) => name)).toEqual(["Sign in", "Dashboard", "Export report"]);

      const trimmed = await session.call("obs-trim-take", { path: file, apply: true });
      expect(trimmed.committed, JSON.stringify(trimmed)).toBe(true);
      expect(Number(trimmed.outputDurationSeconds)).toBeLessThan(Number(trimmed.durationSeconds) - 5);
      expect(trimmed.outputChapters).toBe(3);
      const sheet = await session.call("obs-contact-sheet", { path: String(trimmed.outputPath) });
      expect(sheet.images).toHaveLength(1);
      const exported = (Array.isArray(trimmed.chapters) ? trimmed.chapters.filter(isObject) : []).find(({ name }) => name === "Export report");
      const exportedFrame = regionColor(String(trimmed.outputPath), Number(exported?.atSeconds ?? 0) + 2, canvasRegion(700, 850, 500, 100));
      expect(colorNear(exportedFrame, [46, 204, 113]), JSON.stringify(exportedFrame)).toBe(true);
    }, 180_000);
  });
});
