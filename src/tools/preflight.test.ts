/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError, OBS_OP } from "../../test/support/fake-obs-server.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import type { PreflightCheck } from "./preflight.js";
import { healthyObsState, servePreflightState, type FakeObsState } from "../../test/support/fake-obs-state.js";

let harness: McpHarness;
let recordDirectory: string;
let state: FakeObsState;

function wireFakeObs(): void {
  servePreflightState(harness.fakeObs, () => state);
  harness.fakeObs.respondWith("SetProfileParameter", () => ({}));
}

async function preflight(args: Record<string, unknown> = {}) {
  const result = await harness.call("obs-preflight", args);
  expect(result.isError).toBeFalsy();
  const { ready, checks } = result.structuredContent as { ready: boolean; checks: PreflightCheck[] };
  const byId = (id: string) => checks.find((check) => check.id === id);
  return { result, ready, checks, byId };
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  recordDirectory = mkdtempSync(join(tmpdir(), "obs-mcp-preflight-"));
  state = healthyObsState(recordDirectory);
  harness = await startMcpHarness({ obsStudioVersion: "32.2.2" });
  wireFakeObs();
});

afterEach(async () => {
  await harness.close();
  rmSync(recordDirectory, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("obs-preflight", () => {
  it("reports ready with no failures or warnings for a healthy setup", async () => {
    const { ready, checks, result } = await preflight();

    expect(ready).toBe(true);
    expect(checks.filter(({ status }) => status === "fail" || status === "warn")).toEqual([]);
    expect(resultText(result)).toMatch(/^Ready to record/);
  });

  it("fails when recording is already active", async () => {
    state.recording = true;
    const { ready, byId } = await preflight();

    expect(ready).toBe(false);
    expect(byId("record-idle")?.status).toBe("fail");
  });

  it("fails when the recording directory does not exist", async () => {
    state.recordDirectory = join(recordDirectory, "missing");
    const { ready, byId } = await preflight();

    expect(ready).toBe(false);
    expect(byId("record-directory")).toMatchObject({ status: "fail" });
  });

  it("fails below the free disk space minimum", async () => {
    state.availableDiskSpace = 500;
    const { ready, byId } = await preflight({ minFreeDiskMb: 1024 });

    expect(ready).toBe(false);
    expect(byId("disk-space")?.message).toContain("500 MB free");
  });

  it("fails for an encoder that does not exist on the OBS platform", async () => {
    state.profile["SimpleOutput/RecEncoder"] = "nvenc";
    const { ready, byId } = await preflight();

    expect(ready).toBe(false);
    expect(byId("encoder")?.message).toContain("not available on macOS");
  });

  it("follows the streaming encoder when simple mode records at stream quality", async () => {
    state.profile["SimpleOutput/RecQuality"] = "Stream";
    state.profile["SimpleOutput/StreamEncoder"] = "apple_h264";
    state.profile["SimpleOutput/RecEncoder"] = "nvenc";
    const { byId } = await preflight();

    expect(byId("encoder")).toMatchObject({ status: "pass", message: expect.stringContaining("apple_h264") });
  });

  it("fails after output settings were changed until OBS reconnects", async () => {
    await harness.call("obs-set-profile-parameter", {
      parameterCategory: "SimpleOutput",
      parameterName: "RecEncoder",
      parameterValue: "apple_h264",
    });
    const { ready, byId } = await preflight();

    expect(ready).toBe(false);
    expect(byId("output-settings")?.message).toContain("Restart OBS");
  });

  it("warns about an unmuted input on a recorded track, and fails when silence is expected", async () => {
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: true } };

    const warned = await preflight();
    const failed = await preflight({ expectSilent: true });

    expect(warned.ready).toBe(true);
    expect(warned.byId("audio")).toMatchObject({ status: "warn", message: expect.stringContaining("Chrome (track 1)") });
    expect(failed.ready).toBe(false);
    expect(failed.byId("audio")?.status).toBe("fail");
  });

  it("reads every input's audio state in one batch", async () => {
    state.inputs.push(
      { inputName: "Mic", inputKind: "coreaudio_input_capture", muted: false, tracks: { 1: true } },
      { inputName: "Camera", inputKind: "av_capture_input" },
    );

    const { byId } = await preflight();

    const batches = harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch);
    expect(batches).toHaveLength(1);
    expect(batches[0]?.frame.d.requests).toHaveLength(state.inputs.length * 2);
    expect(byId("audio")?.message).toContain("Mic (track 1)");
    expect(byId("audio")?.message).not.toContain("Camera");
  });

  it("ignores unmuted inputs whose recorded tracks are disabled", async () => {
    state.profile["Output/Mode"] = "Advanced";
    state.profile["AdvOut/RecEncoder"] = "com.apple.videotoolbox.videoencoder.ave.avc";
    state.profile["AdvOut/RecTracks"] = "2";
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: true, 2: false } };

    const { byId } = await preflight({ expectSilent: true });

    expect(byId("audio")?.status).toBe("pass");
  });

  it("warns about visible sources that render at 0×0", async () => {
    state.sceneItems[1] = { sceneItemId: 2, sourceName: "Chrome", sceneItemEnabled: true, sourceWidth: 0, sourceHeight: 0 };
    const { ready, byId } = await preflight();

    expect(ready).toBe(true);
    expect(byId("scene-items")).toMatchObject({ status: "warn", message: expect.stringContaining("Chrome") });
  });

  it("ignores hidden 0×0 sources", async () => {
    state.sceneItems[1] = { sceneItemId: 2, sourceName: "Chrome", sceneItemEnabled: false, sourceWidth: 0, sourceHeight: 0 };
    const { byId } = await preflight();

    expect(byId("scene-items")?.status).toBe("pass");
  });

  it("reports a check as unknown instead of failing when OBS rejects its request", async () => {
    harness.fakeObs.respondWith("GetStats", () => {
      throw new FakeOBSRequestError(500, "boom");
    });
    const { ready, byId } = await preflight();

    expect(ready).toBe(true);
    expect(byId("disk-space")).toMatchObject({ status: "unknown", message: expect.stringContaining("boom") });
  });

  it("fails with a single connection check when OBS is unreachable", async () => {
    await harness.fakeObs.close();
    const { ready, checks } = await preflight();

    expect(ready).toBe(false);
    expect(checks).toEqual([expect.objectContaining({ id: "connection", status: "fail" })]);
  });
});
