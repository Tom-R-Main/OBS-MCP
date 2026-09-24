/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError, OBS_OP } from "../../test/support/fake-obs-server.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness();
  const { fakeObs } = harness;
  fakeObs.respondWith("SetSceneItemEnabled", ({ sceneItemId }) => {
    if (sceneItemId === 99) throw new FakeOBSRequestError(600, "No scene item 99");
    return {};
  });
  fakeObs.respondWith("GetCurrentProgramScene", () => ({ sceneName: "Demo" }));
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

function batchFrames() {
  return harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch);
}

describe("obs-batch", () => {
  it("sends the requests as one batch and reports each result", async () => {
    const result = await harness.call("obs-batch", {
      executionType: "serial-frame",
      requests: [
        { requestType: "SetSceneItemEnabled", requestData: { sceneName: "Demo", sceneItemId: 1, sceneItemEnabled: false } },
        { requestType: "SetSceneItemEnabled", requestData: { sceneName: "Demo", sceneItemId: 2, sceneItemEnabled: true } },
        { requestType: "GetCurrentProgramScene" },
      ],
    });

    expect(result.isError).toBeFalsy();
    expect(batchFrames()).toHaveLength(1);
    expect(batchFrames()[0]?.frame.d.executionType).toBe(1);
    expect(result.structuredContent).toMatchObject({ succeeded: 3, failed: 0, skipped: 0 });
    expect((result.structuredContent as { results: { responseData: unknown }[] }).results[2]?.responseData)
      .toEqual({ sceneName: "Demo" });
  });

  it("reports failures and the requests skipped after them", async () => {
    const result = await harness.call("obs-batch", {
      haltOnFailure: true,
      requests: [
        { requestType: "SetSceneItemEnabled", requestData: { sceneName: "Demo", sceneItemId: 99, sceneItemEnabled: true } },
        { requestType: "GetCurrentProgramScene" },
      ],
    });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("#0 SetSceneItemEnabled failed with code 600: No scene item 99");
    expect(resultText(result)).toContain("1 request(s) skipped");
    expect(result.structuredContent).toMatchObject({ succeeded: 0, failed: 1, skipped: 1 });
  });

  it("pauses serial batches with Sleep", async () => {
    const started = Date.now();

    const result = await harness.call("obs-batch", {
      requests: [{ requestType: "Sleep", requestData: { sleepMillis: 120 } }, { requestType: "GetCurrentProgramScene" }],
    });

    expect(result.isError).toBeFalsy();
    expect(Date.now() - started).toBeGreaterThanOrEqual(110);
  });

  it("refuses unknown requests and the parallel mode without contacting OBS", async () => {
    const unknown = await harness.call("obs-batch", { requests: [{ requestType: "LaunchRocket" }] });
    // obs-websocket 5.7 deadlocks on concurrent parallel batches, so the mode is not offered.
    const parallel = await harness.call("obs-batch", {
      executionType: "parallel",
      requests: [{ requestType: "GetCurrentProgramScene" }],
    });

    expect(resultText(unknown)).toContain("Unknown OBS WebSocket request: LaunchRocket");
    expect(parallel.isError).toBe(true);
    expect(batchFrames()).toHaveLength(0);
  });
});
