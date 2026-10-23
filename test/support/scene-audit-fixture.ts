/* SPDX-License-Identifier: GPL-2.0-only */
import { FakeOBSRequestError, type FakeOBSServer } from "./fake-obs-server.js";

/** A fully observed flat scene. It is deliberately independent of demo preparation. */
export function serveSceneAuditFixture(fake: FakeOBSServer, canvasUuid?: string) {
  const checkCanvas = (data: Record<string, unknown>) => {
    if (data.canvasUuid !== canvasUuid) throw new FakeOBSRequestError(600, "Canvas does not match selected fixture");
  };
  const state = {
    collection: "Audit collection",
    sceneName: "Review",
    sceneUuid: canvasUuid ? `${canvasUuid}-scene-uuid` : "review-scene-uuid",
    items: [{
      sceneItemId: 7,
      sceneItemIndex: 0,
      sourceName: "Application",
      sourceUuid: "application-source-uuid",
      inputKind: "screen_capture",
      isGroup: false,
      sceneItemEnabled: true,
      sceneItemLocked: false,
      sceneItemTransform: {
        sourceWidth: 1280, sourceHeight: 720, width: 1280, height: 720,
        positionX: 0, positionY: 0, scaleX: 1, scaleY: 1,
        rotation: 0, alignment: 5, boundsType: "OBS_BOUNDS_NONE",
        boundsWidth: 0, boundsHeight: 0, boundsAlignment: 0,
        cropLeft: 0, cropRight: 0, cropTop: 0, cropBottom: 0,
      },
    }],
  };
  fake.respondWith("GetSceneCollectionList", () => ({ currentSceneCollectionName: state.collection, sceneCollections: [{ sceneCollectionName: state.collection }] }));
  fake.respondWith("GetSceneList", (data) => { checkCanvas(data); return { currentProgramSceneName: "Other program scene", scenes: [{ sceneName: state.sceneName, sceneUuid: state.sceneUuid }] }; });
  fake.respondWith("GetSceneItemList", (data) => {
    checkCanvas(data);
    const { sceneName, sceneUuid } = data;
    if (sceneName !== state.sceneName && sceneUuid !== state.sceneUuid) throw new FakeOBSRequestError(600, "Scene does not exist");
    return { sceneItems: structuredClone(state.items) };
  });
  fake.respondWith("GetSceneItemTransform", (data) => {
    checkCanvas(data);
    const { sceneItemId } = data;
    const item = state.items.find((candidate) => candidate.sceneItemId === sceneItemId);
    if (!item) throw new FakeOBSRequestError(600, "Item does not exist");
    return { sceneItemTransform: structuredClone(item.sceneItemTransform) };
  });
  fake.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: "screen_capture", unversionedInputKind: "screen_capture" }] }));
  fake.respondWith("GetInputKindList", () => ({ inputKinds: ["screen_capture"] }));
  fake.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 1, window: 42 } }));
  fake.respondWith("GetSourceActive", () => ({ videoActive: false, videoShowing: false }));
  fake.respondWith("GetVideoSettings", () => ({ baseWidth: 1920, baseHeight: 1080, outputWidth: 1920, outputHeight: 1080, fpsNumerator: 30, fpsDenominator: 1 }));
  fake.respondWith("GetSourceScreenshot", (data) => { checkCanvas(data); return { imageData: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==" }; });
  return state;
}

/** Exact query allowlist: unexpected requests fail even if their names look read-only. */
export const SCENE_AUDIT_READS = new Set([
  "GetVersion", "GetSceneCollectionList", "GetSceneList", "GetSceneItemList",
  "GetGroupSceneItemList", "GetSceneItemTransform", "GetInputList", "GetSourceScreenshot",
]);
