/* SPDX-License-Identifier: GPL-2.0-only */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { FakeOBSRequestError } from "../../test/support/fake-obs-server.js";
import { SCENE_AUDIT_READS, serveSceneAuditFixture } from "../../test/support/scene-audit-fixture.js";
import { startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;
let state: ReturnType<typeof serveSceneAuditFixture>;
const selection = { sceneCollection: "Audit collection", sceneName: "Review" };
type Report = { status: string; renderedContent: string; items: Record<string, unknown>[]; checks: { id: string; status: string; message: string }[]; requests: number; limits: { maxRequests: number } };
async function audit(extra: Record<string, unknown> = {}) {
  const result = await harness.call("obs-audit-scene", { ...selection, ...extra });
  expect(result.isError).toBeFalsy();
  return { result, report: result.structuredContent as Report };
}
beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness();
  state = serveSceneAuditFixture(harness.fakeObs);
});
afterEach(async () => { await harness.close(); vi.restoreAllMocks(); });

describe("scene structural audit through MCP", () => {
  it("reports a nonvacuous off-program scene pass with stable occurrence identity and only reads", async () => {
    const listeners = harness.obsClient.listenerCount("event");
    const { report } = await audit({ requiredItems: [{ sourceName: "Application" }] });
    expect(report).toMatchObject({ status: "pass", renderedContent: "unverified", items: [{ sourceName: "Application", sourceUuid: "application-source-uuid", containerUuid: "review-scene-uuid", sceneItemId: 7, effectiveEnabled: true }] });
    expect(report.checks.filter((check) => check.status === "pass").length).toBeGreaterThanOrEqual(5);
    expect(harness.fakeObs.history().filter(({ frame }) => frame.d.requestType).every(({ frame }) => SCENE_AUDIT_READS.has(String(frame.d.requestType)))).toBe(true);
    expect(harness.obsClient.listenerCount("event")).toBe(listeners);
  });
  it("fails missing required items only after complete membership observations", async () => {
    const { report } = await audit({ requiredItems: [{ sourceName: "Missing" }] });
    expect(report.status).toBe("fail");
    expect(report.checks).toContainEqual(expect.objectContaining({ id: "required-item", status: "fail" }));
  });
  it("fails a hidden required item", async () => {
    state.items[0]!.sceneItemEnabled = false;
    expect((await audit({ requiredItems: [{ sourceName: "Application" }] })).report.status).toBe("fail");
  });
  it("keeps repeated occurrences distinct and requires an unambiguous selector", async () => {
    state.items.push({ ...structuredClone(state.items[0]!), sceneItemId: 9 });
    const ambiguous = await audit({ requiredItems: [{ sourceName: "Application" }] });
    expect(ambiguous.report).toMatchObject({ status: "unknown", items: [{ sceneItemId: 7 }, { sceneItemId: 9 }] });
    expect((await audit({ requiredItems: [{ sourceName: "Application", sceneItemId: 9, containerUuid: state.sceneUuid }] })).report.status).toBe("pass");
  });
  it.each([{}, { scenes: null }, { scenes: [{}] }, { scenes: [{ sceneName: "Review" }] }])("rejects incomplete scene inventory %j conservatively", async (response) => {
    harness.fakeObs.respondWith("GetSceneList", () => response);
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each([{}, { sceneItems: null }, { sceneItems: [{}] }])("does not treat incomplete item list %j as an empty pass", async (response) => {
    harness.fakeObs.respondWith("GetSceneItemList", () => response);
    expect((await audit({ requiredItems: [{ sourceName: "Missing" }] })).report.status).toBe("unknown");
  });
  it("fails a completely observed empty scene", async () => {
    state.items.length = 0;
    expect((await audit()).report.status).toBe("fail");
  });
  it.each(["sourceUuid", "sceneItemId", "isGroup"])("does not infer missing item identity %s", async (field) => {
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: state.items.map((item) => { const copy = { ...item } as Record<string, unknown>; delete copy[field]; return copy; }) }));
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each(["sourceWidth", "scaleX", "cropLeft", "alignment", "boundsType"])("marks missing transform %s unknown", async (field) => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => {
      const transform = { ...state.items[0]!.sceneItemTransform } as Record<string, unknown>;
      delete transform[field];
      return { sceneItemTransform: transform };
    });
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each(["sourceWidth", "sourceHeight", "scaleX", "scaleY"])("fails zero visual %s", async (field) => {
    Object.assign(state.items[0]!.sceneItemTransform, { [field]: 0 });
    expect((await audit()).report.status).toBe("fail");
  });
  it("handles signed scale, crop, and alignment without an off-canvas claim", async () => {
    Object.assign(state.items[0]!.sceneItemTransform, { scaleX: -1, scaleY: -2, cropLeft: 24, cropTop: 12, alignment: 10, positionX: -9000 });
    expect((await audit()).report.status).toBe("pass");
  });
  it("fails fully cropped visual sources", async () => {
    state.items[0]!.sceneItemTransform.cropLeft = 1280;
    expect((await audit()).report.status).toBe("fail");
  });
  it.each([{ rotation: 20 }, { boundsType: "OBS_BOUNDS_SCALE_INNER" }])("marks unsupported geometry %j unknown", async (transform) => {
    Object.assign(state.items[0]!.sceneItemTransform, transform);
    expect((await audit()).report.status).toBe("unknown");
  });
  it("exempts observed audio-only source kinds from zero dimensions", async () => {
    Object.assign(state.items[0]!.sceneItemTransform, { sourceWidth: 0, sourceHeight: 0 });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: "coreaudio_input_capture" }] }));
    expect((await audit()).report.status).toBe("pass");
  });
  it("does not promote a zero-size .ogg source to verified audio-only content", async () => {
    Object.assign(state.items[0]!.sceneItemTransform, { sourceWidth: 0, sourceHeight: 0 });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: "ffmpeg_source" }] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputSettings: { is_local_file: true, local_file: "video.ogg" } }));
    const { report } = await audit();
    expect(report.status).toBe("unknown");
    expect(report.checks).toContainEqual(expect.objectContaining({ id: "source-dimensions", status: "unknown" }));
  });
  it.each(["sourceWidth", "sourceHeight"].flatMap((field) => [0.5, Number.MAX_VALUE, 4294967296].map((dimension) => ({ field, dimension }))))("does not accept malformed source pixel dimensions $field=$dimension", async ({ field, dimension }) => {
    Object.assign(state.items[0]!.sceneItemTransform, { [field]: dimension });
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each(["ffmpeg_source", "vlc_source"])("leaves zero-size %s unknown despite apparent audio filenames", async (inputKind) => {
    Object.assign(state.items[0]!.sceneItemTransform, { sourceWidth: 0, sourceHeight: 0 });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind }] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputSettings: { local_file: "track.wav", is_local_file: true } }));
    expect((await audit()).report.status).toBe("unknown");
    expect(harness.fakeObs.history().some(({ frame }) => frame.d.requestType === "GetInputSettings")).toBe(false);
  });
  it("does not exempt network media from remembered audio filenames", async () => {
    Object.assign(state.items[0]!.sceneItemTransform, { sourceWidth: 0, sourceHeight: 0 });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: "ffmpeg_source" }] }));
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputSettings: { local_file: "track.wav", is_local_file: false } }));
    expect((await audit()).report.status).toBe("unknown");
  });
  it("does not read media paths to infer audio-only dimensions", async () => {
    Object.assign(state.items[0]!.sceneItemTransform, { sourceWidth: 0, sourceHeight: 0 });
    harness.fakeObs.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: "ffmpeg_source" }] }));
    let reads = 0;
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputSettings: { local_file: ++reads > 1 ? "video.mp4" : "track.wav", is_local_file: true } }));
    expect((await audit()).report.status).toBe("unknown");
    expect(reads).toBe(0);
  });
  it("projects transforms onto bounded known fields", async () => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => ({ sceneItemTransform: { ...state.items[0]!.sceneItemTransform, untrustedExtra: "not evidence" } }));
    const { report } = await audit();
    expect(report.status).toBe("pass");
    expect(JSON.stringify(report)).not.toContain("untrustedExtra");
  });
  it("rejects oversized response evidence conservatively", async () => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => ({ sceneItemTransform: { ...state.items[0]!.sceneItemTransform, untrustedExtra: "x".repeat(256 * 1024) } }));
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each(["GetSceneItemTransform", "GetInputList", "GetSceneItemList"])("converts failed %s to unknown and releases listeners", async (type) => {
    const before = harness.obsClient.listenerCount("event");
    harness.fakeObs.respondWith(type, () => { throw new FakeOBSRequestError(500, "unavailable"); });
    expect((await audit()).report.status).toBe("unknown");
    expect(harness.obsClient.listenerCount("event")).toBe(before);
  });
  it("invalidates every conclusion when an observed item identity changes", async () => {
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: state.items.map((item) => ({ ...item, sourceUuid: ++reads > 1 ? "replacement" : item.sourceUuid })) }));
    const { report } = await audit();
    expect(report.status).toBe("unknown");
    expect(report.checks.every((check) => check.status === "unknown")).toBe(true);
  });
  it("invalidates changed transform dimensions on the final reread", async () => {
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneItemTransform", () => ({ sceneItemTransform: { ...state.items[0]!.sceneItemTransform, sourceWidth: ++reads > 1 ? 0 : 1280 } }));
    expect((await audit()).report.status).toBe("unknown");
  });
  it("invalidates a changed collection", async () => {
    let reads = 0;
    harness.fakeObs.respondWith("GetSceneCollectionList", () => ({ currentSceneCollectionName: ++reads > 1 ? "Other" : state.collection }));
    expect((await audit()).report.status).toBe("unknown");
  });
  it.each(["SceneItemEnableStateChanged", "CurrentSceneCollectionChanging"])("invalidates a %s event during the sample", async (event) => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => {
      harness.fakeObs.sendEvent(event, { sceneUuid: state.sceneUuid, sceneItemId: 7 });
      return { sceneItemTransform: state.items[0]!.sceneItemTransform };
    });
    expect((await audit()).report.status).toBe("unknown");
  });
  it("invalidates reconnect identification even if identities remain equal", async () => {
    harness.fakeObs.respondWith("GetSceneItemTransform", () => {
      harness.obsClient.emit("identified");
      return { sceneItemTransform: state.items[0]!.sceneItemTransform };
    });
    expect((await audit()).report.status).toBe("unknown");
  });
  it("returns unknown on disconnect and detaches observation listeners", async () => {
    const before = harness.obsClient.listenerCount("disconnected");
    harness.fakeObs.queueDisconnect("GetSceneItemTransform");
    expect((await audit()).report.status).toBe("unknown");
    expect(harness.obsClient.listenerCount("disconnected")).toBe(before);
  });
  it("bounds a stalled request without scheduling the rest of the sample", async () => {
    harness.fakeObs.queueSuccess("GetSceneItemList", { sceneItems: state.items }, 2500);
    expect((await audit()).report.status).toBe("unknown");
    expect(harness.fakeObs.history().filter(({ frame }) => frame.d.requestType === "GetSceneItemTransform")).toHaveLength(0);
  });
  it("bounds large item inventories", async () => {
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: Array.from({ length: 129 }, (_, sceneItemId) => ({ ...state.items[0], sceneItemId })) }));
    const { report } = await audit();
    expect(report.status).toBe("unknown");
    expect(report.requests).toBeLessThan(report.limits.maxRequests);
  });
  it("traverses nested scenes but leaves composed geometry unknown and preserves disabled ancestry", async () => {
    const child = { ...structuredClone(state.items[0]!), sceneItemId: 2 };
    const parent = { ...structuredClone(state.items[0]!), sourceName: "Child", sourceUuid: "child-scene", sceneItemEnabled: false };
    harness.fakeObs.respondWith("GetSceneList", () => ({ scenes: [{ sceneName: state.sceneName, sceneUuid: state.sceneUuid }, { sceneName: "Child", sceneUuid: "child-scene" }] }));
    harness.fakeObs.respondWith("GetSceneItemList", ({ sceneUuid }) => ({ sceneItems: sceneUuid === "child-scene" ? [child] : [parent] }));
    harness.fakeObs.respondWith("GetSceneItemTransform", () => ({ sceneItemTransform: child.sceneItemTransform }));
    const { report } = await audit({ requiredItems: [{ sourceName: "Application" }] });
    expect(report.status).toBe("fail");
    expect(report.items).toHaveLength(2);
    expect(report.items[1]).toMatchObject({ effectiveEnabled: false, containerUuid: "child-scene", path: [{ sceneItemId: 7 }, { sceneItemId: 2 }] });
    expect(report.checks).toContainEqual(expect.objectContaining({ id: "geometry", status: "unknown" }));
  });
  it("detects scene cycles without erasing repeated membership identities", async () => {
    const recursive = { ...structuredClone(state.items[0]!), sourceName: state.sceneName, sourceUuid: state.sceneUuid };
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [recursive] }));
    const { report } = await audit({ requiredItems: [{ sourceName: "Missing" }] });
    expect(report.status).toBe("unknown");
    expect(report.checks).toContainEqual(expect.objectContaining({ id: "required-item", status: "unknown" }));
  });
  it("bounds acyclic nested scene depth", async () => {
    harness.fakeObs.respondWith("GetSceneList", () => ({ scenes: [{ sceneName: state.sceneName, sceneUuid: state.sceneUuid }, ...Array.from({ length: 12 }, (_, i) => ({ sceneName: `Scene ${i}`, sceneUuid: `scene-${i}` }))] }));
    harness.fakeObs.respondWith("GetSceneItemList", ({ sceneUuid }) => {
      const next = sceneUuid === state.sceneUuid ? 0 : Number(String(sceneUuid).slice(6)) + 1;
      return { sceneItems: [{ ...state.items[0], sourceName: `Scene ${next}`, sourceUuid: `scene-${next}` }] };
    });
    const { report } = await audit();
    expect(report.status).toBe("unknown");
    expect(report.items.length).toBeLessThanOrEqual(9);
  });
  it("bounds the aggregate report for large repeated nested occurrence paths", async () => {
    const groups = Array.from({ length: 8 }, (_, i) => ({ name: `${i}`.padEnd(256, "g"), uuid: `${i}`.padEnd(256, "u") }));
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ ...state.items[0], sourceName: groups[0]!.name, sourceUuid: groups[0]!.uuid, isGroup: true }] }));
    harness.fakeObs.respondWith("GetGroupSceneItemList", ({ sceneUuid }) => {
      const index = groups.findIndex((group) => group.uuid === sceneUuid);
      return { sceneItems: index === 7
        ? Array.from({ length: 80 }, (_, sceneItemId) => ({ ...state.items[0], sceneItemId }))
        : [{ ...state.items[0], sourceName: groups[index + 1]!.name, sourceUuid: groups[index + 1]!.uuid, isGroup: true }] };
    });
    harness.fakeObs.respondWith("GetSceneItemTransform", () => ({ sceneItemTransform: state.items[0]!.sceneItemTransform }));
    const { report } = await audit();
    expect(report).toMatchObject({ status: "unknown", items: [], checks: [{ id: "report-size", status: "unknown" }] });
    expect(Buffer.byteLength(JSON.stringify(report))).toBeLessThan(512 * 1024);
  });
  it("traverses group membership through the group read endpoint", async () => {
    harness.fakeObs.respondWith("GetSceneItemList", () => ({ sceneItems: [{ ...state.items[0], sourceName: "Group", sourceUuid: "group-id", isGroup: true }] }));
    harness.fakeObs.respondWith("GetGroupSceneItemList", () => ({ sceneItems: state.items }));
    const { report } = await audit();
    expect(report.status).toBe("unknown");
    expect(report.items).toHaveLength(2);
    expect(report.items[1]).toMatchObject({ containerUuid: "group-id", sourceUuid: "application-source-uuid" });
  });
  it("includes only a bounded optional image, leaving rendered content unverified", async () => {
    const { result, report } = await audit({ includeScreenshot: true });
    expect(report).toMatchObject({ status: "pass", renderedContent: "unverified" });
    expect(result.content.filter((block) => block.type === "image")).toHaveLength(1);
    expect(JSON.stringify(report)).not.toContain("base64");
  });
  it("leaves invalid screenshot observations unknown", async () => {
    harness.fakeObs.respondWith("GetSourceScreenshot", () => ({ imageData: "bad" }));
    const { result, report } = await audit({ includeScreenshot: true });
    expect(report.status).toBe("unknown");
    expect(result.content.filter((block) => block.type === "image")).toHaveLength(0);
  });
});
