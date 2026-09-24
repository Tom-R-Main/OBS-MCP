/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { existsSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EventSubscription } from "../client.js";
import { FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { healthyObsState, servePreflightState, type FakeObsState } from "../../test/support/fake-obs-state.js";
import { resultText, startMcpHarness, type HarnessOptions, type McpHarness } from "../../test/support/mcp-harness.js";
import { currentTake } from "./takes.js";

let harness: McpHarness;
let recordDirectory: string;
let state: FakeObsState;
let clipPath: string;

function sent(requestType: string): number {
  return harness.fakeObs.history().filter(({ frame }) => frame.d.requestType === requestType).length;
}

async function startHarness(options: HarnessOptions = {}): Promise<void> {
  harness = await startMcpHarness({ platform: "macos", ...options });
  servePreflightState(harness.fakeObs, () => state);
  const { fakeObs } = harness;
  fakeObs.respondWith("StartRecord", () => {
    setTimeout(() => fakeObs.sendEvent("RecordStateChanged", {
      outputActive: true,
      outputState: "OBS_WEBSOCKET_OUTPUT_STARTED",
      outputPath: clipPath,
    }), 5);
    return {};
  });
  fakeObs.respondWith("StopRecord", () => ({ outputPath: clipPath }));
  fakeObs.respondWith("CreateRecordChapter", () => ({}));
  fakeObs.respondWith("SetCurrentProfile", () => ({}));
  fakeObs.respondWith("SetProfileParameter", () => ({}));
  fakeObs.respondWith("GetStats", () => ({ availableDiskSpace: 50_000, renderSkippedFrames: 0, renderTotalFrames: 0 }));
}

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  recordDirectory = mkdtempSync(join(tmpdir(), "obs-mcp-takes-"));
  clipPath = join(recordDirectory, "take.mp4");
  state = healthyObsState(recordDirectory);
});

afterEach(async () => {
  const take = currentTake(harness.obsClient);
  if (take) await harness.call("obs-take-stop", { confirm: true });
  await harness.close();
  rmSync(recordDirectory, { recursive: true, force: true });
  vi.restoreAllMocks();
});

describe("recording takes", () => {
  it("starts, marks, reports status, and stops with a take log", async () => {
    await startHarness();

    const start = await harness.call("obs-take-start", { expectSilent: true, chapter: "Intro" });
    expect(start.isError).toBeFalsy();
    expect(resultText(start)).toMatch(/Take \w+ is recording \(profile and output changes locked\)/);
    await vi.waitFor(() => expect(harness.fakeObs.eventSubscriptions()).toBe(EventSubscription.All | EventSubscription.InputVolumeMeters));

    const mark = await harness.call("obs-take-mark", { name: "Step 2" });
    const status = await harness.call("obs-take-status");
    const stop = await harness.call("obs-take-stop");

    expect(resultText(mark)).toMatch(/Marked "Step 2" at [\d.]+s/);
    expect(resultText(status)).toMatch(/^Take \w+: [\d.]+s/);
    expect(stop.isError).toBeFalsy();
    expect(resultText(stop)).toContain(`to ${clipPath}`);
    expect(resultText(stop)).toMatch(/Chapters: [\d.]+s Intro, [\d.]+s Step 2/);
    expect(existsSync(join(recordDirectory, "take.take.json"))).toBe(true);
    expect(sent("CreateRecordChapter")).toBe(2);
    expect(currentTake(harness.obsClient)).toBeUndefined();
    await vi.waitFor(() => expect(harness.fakeObs.eventSubscriptions()).toBe(EventSubscription.All));
  });

  it("refuses a second take while one records", async () => {
    await startHarness();
    await harness.call("obs-take-start");

    const second = await harness.call("obs-take-start");

    expect(second.isError).toBe(true);
    expect(resultText(second)).toContain("is already recording");
    expect(sent("StartRecord")).toBe(1);
  });

  it("does not start when preflight fails", async () => {
    await startHarness();
    state.recordDirectory = join(recordDirectory, "missing");

    const start = await harness.call("obs-take-start");

    expect(start.isError).toBe(true);
    expect(resultText(start)).toContain("record-directory");
    expect(sent("StartRecord")).toBe(0);
  });

  it("locks profile and output changes during a take, but never stopping", async () => {
    await startHarness();
    await harness.call("obs-take-start");

    const profile = await harness.call("obs-set-current-profile", { profileName: "Streaming" });
    const batch = await harness.call("obs-batch", {
      requests: [{ requestType: "GetStats" }, { requestType: "SetProfileParameter", requestData: { parameterCategory: "Output", parameterName: "Mode", parameterValue: "Advanced" } }],
    });
    const readOnly = await harness.call("obs-batch", { requests: [{ requestType: "GetStats" }] });

    expect(resultText(profile)).toMatch(/is recording, and changing this now would switch profiles under it/);
    expect(resultText(batch)).toContain("would send SetProfileParameter");
    expect(readOnly.isError).toBeFalsy();
    expect(sent("SetCurrentProfile")).toBe(0);
    expect(sent("SetProfileParameter")).toBe(0);

    expect((await harness.call("obs-stop-record")).isError).toBeFalsy();
    await harness.call("obs-take-stop");
    expect((await harness.call("obs-set-current-profile", { profileName: "Streaming" })).isError).toBeFalsy();
  });

  it("leaves changes unlocked with lock: false", async () => {
    await startHarness();
    await harness.call("obs-take-start", { lock: false });

    expect((await harness.call("obs-set-current-profile", { profileName: "Streaming" })).isError).toBeFalsy();
  });

  it("reports the file and the unrequested stop when OBS stopped on its own", async () => {
    await startHarness();
    harness.fakeObs.respondWith("StopRecord", () => {
      throw new FakeOBSRequestError(501, "The output is not running");
    });
    await harness.call("obs-take-start");

    harness.fakeObs.sendEvent("RecordStateChanged", {
      outputActive: false,
      outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED",
      outputPath: clipPath,
    });
    await vi.waitFor(() => expect(currentTake(harness.obsClient)?.monitor.summary().warnings).toHaveLength(1));
    const stop = await harness.call("obs-take-stop");

    expect(stop.isError).toBe(true);
    expect(resultText(stop)).toContain(`to ${clipPath}`);
    expect(resultText(stop)).toContain("The recording stopped before it was asked to");
    expect(currentTake(harness.obsClient)).toBeUndefined();
  });

  it("answers mark, status, and stop with an error when no take is recording", async () => {
    await startHarness();

    for (const [tool, args] of [["obs-take-mark", { name: "x" }], ["obs-take-status", {}], ["obs-take-stop", {}]] as const) {
      const result = await harness.call(tool, args);
      expect(result.isError).toBe(true);
      expect(resultText(result)).toContain("No take is recording");
    }
  });

  it("asks before stopping a take when live confirmation is on", async () => {
    await startHarness({ confirmLive: true, onElicit: () => ({ action: "decline" }) });
    await harness.call("obs-take-start");

    const stop = await harness.call("obs-take-stop");

    expect(resultText(stop)).toContain("Cancelled");
    expect(sent("StopRecord")).toBe(0);
    expect(currentTake(harness.obsClient)).toBeDefined();
  });
});
