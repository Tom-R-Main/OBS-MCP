/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OBS_OP } from "../../test/support/fake-obs-server.js";
import { demoSceneState, serveSceneState, type FakeSceneInput, type FakeSceneItem } from "../../test/support/fake-obs-scene.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;
let inputs: Record<string, FakeSceneInput>;
let items: FakeSceneItem[];

function sent(requestType: string): Record<string, unknown>[] {
  return harness.fakeObs.history()
    .filter(({ frame }) => frame.d.requestType === requestType)
    .map(({ frame }) => frame.d.requestData as Record<string, unknown>);
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  const state = demoSceneState();
  inputs = state.inputs;
  items = state.scenes.Demo!;
  harness = await startMcpHarness();
  serveSceneState(harness.fakeObs, state);
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

function breakEverything(): void {
  inputs.Chrome!.settings = { type: 2, application: "com.google.Chrome" };
  inputs.Chrome!.muted = false;
  inputs.Chrome!.volumeMul = 0.5;
  inputs.Chrome!.tracks = { 1: true, 2: false };
  items[1]!.transform = { ...items[1]!.transform, positionX: 0, cropTop: 0 };
  items[0]!.enabled = false;
  items[0]!.locked = false;
  items.reverse();
}

describe("obs-snapshot and obs-restore", () => {
  it("saves the program scene and its inputs, and undoes later changes in one batch", async () => {
    const snapshot = await harness.call("obs-snapshot", { label: "before capture" });
    expect(resultText(snapshot)).toMatch(/Saved snapshot \w+ "before capture" .*: 2 input\(s\), 2 item\(s\) in Demo/);
    const original = structuredClone({ inputs, items });

    breakEverything();
    const preview = await harness.call("obs-restore", { dryRun: true });
    expect(sent("SetInputSettings")).toEqual([]);
    expect(resultText(preview)).toContain("- input Chrome: settings");
    expect(resultText(preview)).toContain("- Demo › Background (#1): show");
    expect(resultText(preview)).toContain("order (to 0)");

    const restored = await harness.call("obs-restore");

    expect(restored.isError).toBeFalsy();
    expect(resultText(restored)).toMatch(/Restored snapshot \w+: 9 of 9 change\(s\)/);
    expect({ inputs, items }).toEqual(original);
    // Snapshot, dry run, and restore each read items and inputs in one batch apiece; the restore adds one for its changes.
    expect(harness.fakeObs.history().filter(({ frame }) => frame.op === OBS_OP.RequestBatch)).toHaveLength(7);
    expect(sent("SetInputSettings")[0]).toMatchObject({ overlay: false, inputSettings: { type: 1, window: 31875 } });
    expect(sent("SetSceneItemTransform")[0]?.sceneItemTransform).not.toHaveProperty("sourceWidth");
  });

  it("says when nothing differs", async () => {
    await harness.call("obs-snapshot");

    const result = await harness.call("obs-restore");

    expect(resultText(result)).toMatch(/OBS already matches snapshot/);
  });

  it("reports removed and added items instead of recreating or removing them", async () => {
    await harness.call("obs-snapshot");
    items.splice(0, 1);
    items.push({ sceneItemId: 3, sourceName: "Webcam", enabled: true, locked: false, transform: {} });

    const result = await harness.call("obs-restore", { dryRun: true });

    expect(resultText(result)).toContain("Not restored: Demo › Background (#1) was removed");
    expect(resultText(result)).toContain("Not restored: Demo › Webcam (#3) was added since; it is left in place");
  });

  it("saves only the named inputs and scenes, lists snapshots, and picks one by id", async () => {
    const first = await harness.call("obs-snapshot", { inputs: ["Chrome"] });
    await harness.call("obs-snapshot", { label: "second" });
    const firstId = (first.structuredContent as { snapshotId: string }).snapshotId;
    expect(first.structuredContent).toMatchObject({ inputs: ["Chrome"], items: 0 });

    const list = await harness.call("obs-snapshot", { list: true });
    inputs.Chrome!.muted = false;
    const restored = await harness.call("obs-restore", { snapshotId: firstId });

    expect(resultText(list).split("\n")).toHaveLength(2);
    expect(resultText(restored)).toContain("input Chrome: mute");
    expect(inputs.Chrome!.muted).toBe(true);
    expect(resultText(await harness.call("obs-restore", { snapshotId: "nope" }))).toContain("No snapshot nope");
  });

  it("refuses to restore before any snapshot", async () => {
    const result = await harness.call("obs-restore");

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("take one with obs-snapshot");
  });
});
