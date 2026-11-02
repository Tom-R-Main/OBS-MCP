/* SPDX-License-Identifier: GPL-2.0-only */
import type { FakeOBSServer } from "./fake-obs-server.js";
import { serveSceneAuditFixture } from "./scene-audit-fixture.js";
export function serveProduction(fake: FakeOBSServer) {
  const base = serveSceneAuditFixture(fake);
  const state = { ...base, studio: true, program: "Main", preview: "Review", setting: 1, kind: "screen_capture" };
  const mainUuid = "main-scene-uuid";
  const sceneUuid = (name: string) => name === state.sceneName ? state.sceneUuid : mainUuid;
  fake.respondWith("GetSceneCollectionList", () => ({ currentSceneCollectionName: state.collection }));
  fake.respondWith("GetSceneList", () => ({ currentProgramSceneName: state.program, currentProgramSceneUuid: sceneUuid(state.program), scenes: [{ sceneName: "Main", sceneUuid: mainUuid }, { sceneName: state.sceneName, sceneUuid: state.sceneUuid }] }));
  fake.respondWith("GetSceneItemList", () => ({ sceneItems: structuredClone(state.items) }));
  fake.respondWith("GetInputList", () => ({ inputs: [{ inputName: "Application", inputUuid: "application-source-uuid", inputKind: state.kind }] }));
  fake.respondWith("GetInputSettings", () => ({ inputKind: state.kind, inputSettings: { type: 1, window: 42, setting: state.setting } }));
  fake.respondWith("GetSourceFilterList", () => ({ filters: [] }));
  fake.respondWith("GetInputMute", () => ({ inputMuted: true }));
  fake.respondWith("GetInputVolume", () => ({ inputVolumeMul: 1 }));
  fake.respondWith("GetInputAudioTracks", () => ({ inputAudioTracks: { 1: false } }));
  fake.respondWith("GetStudioModeEnabled", () => ({ studioModeEnabled: state.studio }));
  fake.respondWith("GetCurrentPreviewScene", () => ({ currentPreviewSceneName: state.preview, currentPreviewSceneUuid: sceneUuid(state.preview) }));
  fake.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: state.program, currentProgramSceneUuid: sceneUuid(state.program) }));
  fake.respondWith("SetCurrentPreviewScene", (data) => { state.preview = data.sceneUuid === mainUuid ? "Main" : state.sceneName; return {}; });
  fake.respondWith("TriggerStudioModeTransition", () => { [state.program, state.preview] = [state.preview, state.program]; return {}; });
  for (const type of ["GetRecordStatus", "GetStreamStatus", "GetReplayBufferStatus", "GetVirtualCamStatus"]) fake.respondWith(type, () => ({ outputActive: false }));
  fake.respondWith("GetOutputList", () => ({ outputs: [] }));
  return state;
}
