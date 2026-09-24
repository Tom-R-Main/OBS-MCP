/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { readdirSync, readFileSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";
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

  it("keeps snapshots on disk, so a new server can restore them", async () => {
    const taken = await harness.call("obs-snapshot", { label: "before restart" });
    const snapshotId = (taken.structuredContent as { snapshotId: string }).snapshotId;
    const port = Number(new URL(harness.fakeObs.url).port);
    await harness.close();

    // A new server process, reaching the same OBS at the same address.
    const state = demoSceneState();
    state.inputs.Chrome!.muted = false;
    inputs = state.inputs;
    items = state.scenes.Demo!;
    harness = await startMcpHarness({ port });
    serveSceneState(harness.fakeObs, state);

    const restored = await harness.call("obs-restore", { snapshotId });
    expect(resultText(restored)).toContain("input Chrome: mute");
    expect(state.inputs.Chrome!.muted).toBe(true);
    expect(statSync(join(process.env.OBS_MCP_STATE_DIR!, "snapshots.json")).mode & 0o777).toBe(0o600);
  });

  it("lists only snapshots of the OBS instance this server connects to", async () => {
    await harness.call("obs-snapshot", { label: "this OBS" });
    const file = join(process.env.OBS_MCP_STATE_DIR!, "snapshots.json");
    const saved = JSON.parse(readFileSync(file, "utf8")) as { obsUrl: string }[];
    writeFileSync(file, JSON.stringify([...saved, { ...saved[0], id: "elsewhere", obsUrl: "ws://studio-pc:4455" }]));

    const list = await harness.call("obs-snapshot", { list: true });

    expect(resultText(list)).toContain('"this OBS"');
    expect(resultText(list)).not.toContain("elsewhere");
  });

  it("restores the saved order however the items were rearranged", async () => {
    const extra = ["Slides", "Webcam"].map((sourceName, index) => ({ sceneItemId: 3 + index, sourceName, enabled: true, locked: false, transform: {} }));
    items.push(...extra);
    inputs.Slides = { kind: "image_source", settings: {} };
    inputs.Webcam = { kind: "image_source", settings: {} };
    const saved = items.map(({ sceneItemId }) => sceneItemId);
    await harness.call("obs-snapshot");

    const permutations = (list: number[]): number[][] => list.length <= 1 ? [list]
      : list.flatMap((head, index) => permutations([...list.slice(0, index), ...list.slice(index + 1)]).map((rest) => [head, ...rest]));
    for (const order of permutations(saved)) {
      const byId = new Map(items.map((item) => [item.sceneItemId, item]));
      items.splice(0, items.length, ...order.map((id) => byId.get(id)!));
      await harness.call("obs-restore");
      expect(items.map(({ sceneItemId }) => sceneItemId), `from ${order.join(",")}`).toEqual(saved);
    }
  });

  it("keeps an item added since in its place while restoring the others' order", async () => {
    await harness.call("obs-snapshot");
    items.reverse();
    items.unshift({ sceneItemId: 9, sourceName: "Webcam", enabled: true, locked: false, transform: {} });

    await harness.call("obs-restore");

    expect(items.map(({ sourceName }) => sourceName)).toEqual(["Webcam", "Background", "Chrome"]);
  });

  it("restores the scenes that still exist when another was deleted", async () => {
    await harness.call("obs-snapshot", { scenes: ["Demo"], inputs: ["Chrome"] });
    const file = join(process.env.OBS_MCP_STATE_DIR!, "snapshots.json");
    const snapshots = JSON.parse(readFileSync(file, "utf8")) as { items: { sceneName: string }[] }[];
    snapshots[0]!.items.push({ ...snapshots[0]!.items[0]!, sceneName: "Deleted Scene" });
    writeFileSync(file, JSON.stringify(snapshots));
    items[1]!.enabled = false;

    const result = await harness.call("obs-restore");

    expect(resultText(result)).toContain("scene Deleted Scene no longer exists");
    expect(items[1]!.enabled).toBe(true);
  });

  it("moves an unreadable snapshot file aside instead of overwriting it", async () => {
    const directory = process.env.OBS_MCP_STATE_DIR!;
    writeFileSync(join(directory, "snapshots.json"), "{ not json");

    await harness.call("obs-snapshot");

    expect(readdirSync(directory).some((name) => name.startsWith("snapshots.json.unreadable-"))).toBe(true);
    expect(JSON.parse(readFileSync(join(directory, "snapshots.json"), "utf8"))).toHaveLength(1);
  });

  it("does not let automatic snapshots push out ones a user took", async () => {
    await harness.call("obs-snapshot", { label: "mine" });
    for (let index = 0; index < 12; index += 1) {
      await harness.call("obs-apply-scene", { sceneName: "Demo", sources: [{ name: "Chrome", volumeDb: -index - 1 }], apply: true });
    }

    const list = resultText(await harness.call("obs-snapshot", { list: true }));

    expect(list).toContain('"mine"');
    expect(list.split("\n").filter((line) => line.includes("before obs-apply-scene"))).toHaveLength(10);
  });

  it("restores a half-pixel move, which OBS keeps", async () => {
    await harness.call("obs-snapshot");
    items[1]!.transform = { ...items[1]!.transform, positionX: 204.5 };

    const result = await harness.call("obs-restore");

    expect(resultText(result)).toContain("Chrome (#2): transform");
    expect(items[1]!.transform.positionX).toBe(204);
  });

  it("refuses to restore before any snapshot", async () => {
    const result = await harness.call("obs-restore");

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("take one with obs-snapshot");
  });
});
