/* SPDX-License-Identifier: GPL-2.0-only */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { FakeOBSServer, FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";
import { confirmOutputStopped, REPLAY_BUFFER_STOP, STREAM_STOP } from "./output-start.js";

const BUDGET_MS = 1_000;
let fake: FakeOBSServer;
let client: OBSWebSocketClient;

const requests = () => fake.history().filter(({ frame }) => frame.op === 6 && frame.d.requestType !== "GetVersion").map(({ frame }) => String(frame.d.requestType));

describe("stop confirmation", () => {
  beforeEach(async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    fake = await FakeOBSServer.start({ availableRequests: ["StopStream", "GetStreamStatus", "StopReplayBuffer", "GetReplayBufferStatus"] });
    client = new OBSWebSocketClient(fake.url);
    await client.connect();
  });
  afterEach(async () => {
    await client.disconnect();
    await fake.close();
    vi.restoreAllMocks();
  });

  it("confirms only when a status read sent after the stop reports the output inactive", async () => {
    let reads = 0;
    fake.respondWith("StopStream", () => ({}));
    fake.respondWith("GetStreamStatus", () => ({ outputActive: ++reads < 3 }));
    await client.sendRequest("StopStream");
    expect(await confirmOutputStopped(client, STREAM_STOP, Date.now(), BUDGET_MS)).toBeUndefined();
    expect(requests()).toEqual(["StopStream", "GetStreamStatus", "GetStreamStatus", "GetStreamStatus"]);
  });

  it("ignores a stale STOPPED event while status still reports the output active", async () => {
    fake.respondWith("GetStreamStatus", () => ({ outputActive: true }));
    fake.sendEvent("StreamStateChanged", { outputActive: false, outputState: "OBS_WEBSOCKET_OUTPUT_STOPPED" });
    const started = Date.now();
    const result = await confirmOutputStopped(client, STREAM_STOP, started, BUDGET_MS);
    expect(result).toContain("did not observe streaming stop within 1s");
    expect(result).toContain("A stream delay keeps the stream live");
    expect(result).toContain("This is not evidence that it is still running");
    expect(result).toContain("do not resend the stop automatically");
    // Polling stops with 500 ms left rather than send a read that could outlive the deadline.
    expect(Date.now() - started).toBeGreaterThanOrEqual(BUDGET_MS - 520);
    expect(Date.now() - started).toBeLessThan(BUDGET_MS);
  });

  it("never counts a status without outputActive:false as stopped", async () => {
    fake.respondWith("GetReplayBufferStatus", () => ({ outputActive: "no" }));
    const result = await confirmOutputStopped(client, REPLAY_BUFFER_STOP, Date.now(), BUDGET_MS);
    expect(result).toContain("did not observe the replay buffer stop");
    expect(result).not.toContain("stream delay");
  });

  it("reports the outcome as unknown when the status read fails", async () => {
    fake.respondWith("GetStreamStatus", () => { throw new FakeOBSRequestError(500, "status unavailable"); });
    const result = await confirmOutputStopped(client, STREAM_STOP, Date.now(), BUDGET_MS);
    expect(result).toContain("could not be read afterwards on the same connection, so whether streaming stopped is unknown");
    expect(result).toContain("not this request's outcome");
  });

  it("does not read status on a new connection after a drop", async () => {
    fake.queueDisconnect("GetStreamStatus");
    const result = await confirmOutputStopped(client, STREAM_STOP, Date.now(), BUDGET_MS);
    expect(result).toContain("is unknown");
    expect(fake.connectionCount).toBe(1);
  });

  it("returns unconfirmed without any read once the budget has passed", async () => {
    fake.respondWith("GetStreamStatus", () => ({ outputActive: false }));
    const result = await confirmOutputStopped(client, STREAM_STOP, Date.now() - BUDGET_MS, BUDGET_MS);
    expect(result).toContain("did not observe streaming stop");
    expect(requests()).not.toContain("GetStreamStatus");
  });
});

describe("stop tools", () => {
  let harness: McpHarness;
  beforeEach(async () => {
    vi.spyOn(console, "error").mockImplementation(() => undefined);
    harness = await startMcpHarness();
  });
  afterEach(async () => {
    await harness.close();
    vi.restoreAllMocks();
  });

  it("keeps the success message once OBS reports the stream inactive", async () => {
    let active = true;
    harness.fakeObs.respondWith("StopStream", () => { setTimeout(() => { active = false; }, 250); return {}; });
    harness.fakeObs.respondWith("GetStreamStatus", () => ({ outputActive: active }));
    const result = await harness.call("obs-stop-stream");
    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toBe("Successfully stopped streaming");
    const types = harness.fakeObs.history().filter(({ frame }) => frame.op === 6).map(({ frame }) => frame.d.requestType);
    expect(types.indexOf("GetStreamStatus")).toBeGreaterThan(types.indexOf("StopStream"));
  });

  it("keeps OBS's refusal and reads no status when the stop is rejected", async () => {
    harness.fakeObs.respondWith("StopStream", () => { throw new FakeOBSRequestError(501, "Output not running"); });
    harness.fakeObs.respondWith("GetStreamStatus", () => ({ outputActive: false }));
    const result = await harness.call("obs-stop-stream");
    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/^Stop Stream failed: .*501/);
    expect(harness.fakeObs.history().some(({ frame }) => frame.d?.requestType === "GetStreamStatus")).toBe(false);
  });
});
