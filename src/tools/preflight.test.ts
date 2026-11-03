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
  it("reports unknown audio instead of silence when a mute read fails unexpectedly", async () => {
    harness.fakeObs.respondWith("GetInputMute", () => { throw new FakeOBSRequestError(600, "Input disappeared"); });
    expect((await preflight({ expectSilent: true })).byId("audio")?.status).toBe("unknown");
  });
  it("describes configured routes without measuring signal or declaring monitor-only audio silent", async () => {
    state.inputs[0] = { inputName: "Mic", inputKind: "coreaudio_input_capture", muted: false, tracks: { 1: true } };
    for (const monitorType of ["OBS_MONITORING_TYPE_NONE", "OBS_MONITORING_TYPE_MONITOR_ONLY", "OBS_MONITORING_TYPE_MONITOR_AND_OUTPUT", null, "INVALID"]) {
      harness.fakeObs.respondWith("GetInputAudioMonitorType", () => ({ monitorType }));
      const result = await preflight({ expectSilent: true });
      expect(result.ready).toBe(false);
      expect(result.byId("audio")).toMatchObject({status: "fail", message: expect.stringContaining("Configured unmuted audio routes")});
      expect(result.byId("audio")?.message).toContain("Signal levels and monitoring/loopback paths were not measured");
      expect(result.byId("audio")?.message).not.toContain("Recorded audio comes from");
    }
    expect(harness.fakeObs.history().flatMap(({frame}) => frame.op === OBS_OP.RequestBatch && Array.isArray(frame.d.requests) ? frame.d.requests.map((request: {requestType: string}) => request.requestType) : frame.op === OBS_OP.Request ? [frame.d.requestType] : [])).not.toContain("GetInputAudioMonitorType");
  });
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

  describe("inputs OBS does not mix", () => {
    // Live OBS 32.2.2: inputs reported inactive (hidden, in a scene that is not
    // on program, or only in the Studio Mode preview) were absent from the
    // recording; active ones, including nested scenes, were recorded.
    function serveActivity(active: Record<string, boolean | Error>, special: Record<string, string | null> | Error = { desktop1: null, mic1: null }): void {
      harness.fakeObs.respondWith("GetSourceActive", (data) => {
        const value = active[String(data.sourceName)];
        if (value instanceof Error || value === undefined) throw new FakeOBSRequestError(600, "activity unavailable");
        return { videoActive: value, videoShowing: value };
      });
      harness.fakeObs.respondWith("GetSpecialInputs", () => {
        if (special instanceof Error) throw new FakeOBSRequestError(600, special.message);
        return special;
      });
    }
    beforeEach(() => {
      state.inputs.push(
        { inputName: "Live Tone", inputKind: "ffmpeg_source", muted: false, tracks: { 1: true } },
        { inputName: "Other Scene Tone", inputKind: "ffmpeg_source", muted: false, tracks: { 1: true } },
      );
    });

    it("leaves out inactive inputs and says so, while still failing on active ones", async () => {
      serveActivity({ "Live Tone": true, "Other Scene Tone": false });
      const { byId } = await preflight({ expectSilent: true });
      expect(byId("audio")).toMatchObject({ status: "fail", message: expect.stringContaining("Live Tone (track 1)") });
      expect(byId("audio")?.message).not.toContain("Other Scene Tone (track");
      expect(byId("audio")?.message).toContain("OBS reports them inactive");
      expect(byId("audio")?.message).toContain("switching scenes during the take can make them audible: Other Scene Tone.");
    });

    it("passes a silent take when every unmuted routed input is inactive", async () => {
      serveActivity({ "Live Tone": false, "Other Scene Tone": false });
      const { byId } = await preflight({ expectSilent: true });
      expect(byId("audio")).toMatchObject({ status: "pass", message: expect.stringContaining("Signal levels and monitoring/loopback paths were not measured.") });
      expect(byId("audio")?.message).toContain("Live Tone, Other Scene Tone.");
    });

    it("keeps counting global audio inputs even when OBS reports them inactive", async () => {
      serveActivity({ "Live Tone": false, "Other Scene Tone": false }, { desktop1: "Live Tone", mic1: null });
      const { byId } = await preflight({ expectSilent: true });
      expect(byId("audio")).toMatchObject({ status: "fail", message: expect.stringContaining("Live Tone (track 1)") });
    });

    it("counts an input whose activity cannot be read", async () => {
      serveActivity({ "Live Tone": false, "Other Scene Tone": new Error("gone") });
      const { byId } = await preflight({ expectSilent: true });
      expect(byId("audio")).toMatchObject({ status: "fail", message: expect.stringContaining("Other Scene Tone (track 1)") });
    });

    it("counts every input when OBS cannot say which inputs are global", async () => {
      serveActivity({ "Live Tone": false, "Other Scene Tone": false }, new Error("special inputs unavailable"));
      const { byId } = await preflight({ expectSilent: true });
      expect(byId("audio")?.status).toBe("fail");
      expect(byId("audio")?.message).toContain("Live Tone (track 1); Other Scene Tone (track 1)");
      expect(byId("audio")?.message).not.toContain("inactive");
    });
  });

  it("follows the tracks Simple mode records, not just track 1", async () => {
    state.profile["SimpleOutput/RecTracks"] = "3";
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: false, 2: true } };

    const { byId } = await preflight({ expectSilent: true });

    expect(byId("audio")).toMatchObject({ status: "fail", message: expect.stringContaining("Chrome (track 2)") });
  });

  it("records only track 1 in Simple mode at stream quality", async () => {
    state.profile["SimpleOutput/RecQuality"] = "Stream";
    state.profile["SimpleOutput/StreamEncoder"] = "apple_h264";
    state.profile["SimpleOutput/RecTracks"] = "3";
    state.inputs[0] = { inputName: "Chrome", inputKind: "screen_capture", muted: false, tracks: { 1: false, 2: true } };

    const { byId } = await preflight({ expectSilent: true });

    expect(byId("audio")?.status).toBe("pass");
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

  it("ignores audio-only sources, which are always 0×0", async () => {
    state.inputs.push({ inputName: "Mic", inputKind: "coreaudio_input_capture", muted: true, tracks: { 1: true } });
    state.sceneItems.push({ sceneItemId: 3, sourceName: "Mic", sceneItemEnabled: true, sourceWidth: 0, sourceHeight: 0 });

    const { byId } = await preflight();

    expect(byId("scene-items")?.status).toBe("pass");
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
