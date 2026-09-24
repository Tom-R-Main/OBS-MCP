/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError, OBS_OP } from "../../test/support/fake-obs-server.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

type Input = { kind: string; settings: Record<string, unknown>; muted?: boolean; volume?: number; tracks?: Record<string, boolean> };
type Item = { sceneItemId: number; sourceName: string; enabled: boolean; locked: boolean; transform: Record<string, unknown> };

let harness: McpHarness;
let inputs: Record<string, Input>;
let items: Item[];

function initialState(): void {
  inputs = {
    Chrome: { kind: "screen_capture", settings: { type: 1, window: 31875 }, muted: true, volume: 1, tracks: { 1: false, 2: false } },
    Background: { kind: "color_source_v3", settings: { color: 4278190080 } },
  };
  items = [
    { sceneItemId: 1, sourceName: "Background", enabled: true, locked: true, transform: { positionX: 0, positionY: 0, scaleX: 1, scaleY: 1, sourceWidth: 1920 } },
    { sceneItemId: 2, sourceName: "Chrome", enabled: true, locked: false, transform: { positionX: 204, positionY: 65, scaleX: 1, scaleY: 1, cropTop: 40, sourceWidth: 1512 } },
  ];
}

function input(data: Record<string, unknown>): Input {
  const found = inputs[String(data.inputName)];
  if (!found) throw new FakeOBSRequestError(600, "No source was found");
  return found;
}

function item(data: Record<string, unknown>): Item {
  const found = items.find(({ sceneItemId }) => sceneItemId === data.sceneItemId);
  if (!found || data.sceneName !== "Demo") throw new FakeOBSRequestError(600, "No scene item was found");
  return found;
}

function audio(data: Record<string, unknown>): Input {
  const found = input(data);
  if (found.muted === undefined) throw new FakeOBSRequestError(604, "The specified input does not support audio");
  return found;
}

function sent(requestType: string): Record<string, unknown>[] {
  return harness.fakeObs.history()
    .filter(({ frame }) => frame.d.requestType === requestType)
    .map(({ frame }) => frame.d.requestData as Record<string, unknown>);
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  initialState();
  harness = await startMcpHarness();
  const { fakeObs } = harness;
  fakeObs.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: "Demo" }));
  fakeObs.respondWith("GetInputList", () => ({ inputs: Object.entries(inputs).map(([inputName, { kind }]) => ({ inputName, inputKind: kind })) }));
  fakeObs.respondWith("GetSceneItemList", ({ sceneName }) => {
    if (sceneName !== "Demo") throw new FakeOBSRequestError(600, "No source was found");
    return {
      sceneItems: items.map((entry, index) => ({
        sceneItemId: entry.sceneItemId,
        sourceName: entry.sourceName,
        sceneItemIndex: index,
        sceneItemEnabled: entry.enabled,
        sceneItemLocked: entry.locked,
        sceneItemTransform: { ...entry.transform },
      })),
    };
  });
  fakeObs.respondWith("GetInputSettings", (data) => ({ inputKind: input(data).kind, inputSettings: { ...input(data).settings } }));
  fakeObs.respondWith("GetInputMute", (data) => ({ inputMuted: audio(data).muted }));
  fakeObs.respondWith("GetInputVolume", (data) => ({ inputVolumeMul: audio(data).volume, inputVolumeDb: 0 }));
  fakeObs.respondWith("GetInputAudioTracks", (data) => ({ inputAudioTracks: { ...audio(data).tracks } }));
  fakeObs.respondWith("SetInputSettings", (data) => {
    const target = input(data);
    target.settings = data.overlay === false ? { ...(data.inputSettings as object) } : { ...target.settings, ...(data.inputSettings as object) };
    return {};
  });
  fakeObs.respondWith("SetInputMute", (data) => {
    audio(data).muted = data.inputMuted as boolean;
    return {};
  });
  fakeObs.respondWith("SetInputVolume", (data) => {
    audio(data).volume = data.inputVolumeMul as number;
    return {};
  });
  fakeObs.respondWith("SetInputAudioTracks", (data) => {
    audio(data).tracks = data.inputAudioTracks as Record<string, boolean>;
    return {};
  });
  fakeObs.respondWith("SetSceneItemTransform", (data) => {
    const target = item(data);
    target.transform = { ...target.transform, ...(data.sceneItemTransform as object) };
    return {};
  });
  fakeObs.respondWith("SetSceneItemEnabled", (data) => {
    item(data).enabled = data.sceneItemEnabled as boolean;
    return {};
  });
  fakeObs.respondWith("SetSceneItemLocked", (data) => {
    item(data).locked = data.sceneItemLocked as boolean;
    return {};
  });
  fakeObs.respondWith("SetSceneItemIndex", (data) => {
    const target = item(data);
    items.splice(items.indexOf(target), 1);
    items.splice(Number(data.sceneItemIndex), 0, target);
    return {};
  });
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

function breakEverything(): void {
  inputs.Chrome!.settings = { type: 2, application: "com.google.Chrome" };
  inputs.Chrome!.muted = false;
  inputs.Chrome!.volume = 0.5;
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
