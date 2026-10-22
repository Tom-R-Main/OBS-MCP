import type { FakeOBSServer } from "./fake-obs-server.js";
import { demoSceneState, serveSceneState } from "./fake-obs-scene.js";

/** Realistic identity-bearing OBS state behind the external WebSocket boundary. */
export function serveDemoFixture(fake: FakeOBSServer) {
  const state = demoSceneState();
  state.inputs.Chrome!.muted = false;
  Object.assign(state.scenes.Demo![1]!.transform, { boundsWidth: 0, boundsHeight: 0, boundsAlignment: 0 });
  serveSceneState(fake, state);
  fake.respondWith("GetVersion", () => ({ platform: "macos", obsVersion: "32.2.2", obsWebSocketVersion: "5.6.3" }));
  fake.respondWith("GetStreamStatus", () => ({ outputActive: false }));
  fake.respondWith("GetRecordStatus", () => ({ outputActive: false }));
  fake.respondWith("GetVirtualCamStatus", () => ({ outputActive: false }));
  fake.respondWith("GetReplayBufferStatus", () => ({ outputActive: false }));
  fake.respondWith("GetOutputList", () => ({ outputs: [{ outputName: "plugin-output", outputActive: false }] }));
  fake.respondWith("GetOutputStatus", () => ({ outputActive: false, outputReconnecting: false }));
  fake.respondWith("GetSceneCollectionList", () => ({ currentSceneCollectionName: "Disposable" }));
  fake.respondWith("GetSceneList", () => ({ currentProgramSceneName: state.program, scenes: Object.keys(state.scenes).map((sceneName) => ({ sceneName, sceneUuid: `${sceneName}-uuid` })) }));
  fake.respondWith("GetInputList", () => ({ inputs: Object.entries(state.inputs).map(([inputName, input]) => ({ inputName, inputKind: input.kind, inputUuid: `${inputName}-uuid` })) }));
  fake.respondWith("GetSceneItemList", ({ sceneName }) => ({ sceneItems: state.scenes[String(sceneName)]!.map((item, sceneItemIndex) => ({
    ...item, sceneItemIndex, sourceUuid: `${item.sourceName}-uuid`, isGroup: false,
    sceneItemEnabled: item.enabled, sceneItemLocked: item.locked,
    sceneItemTransform: { ...item.transform, sourceWidth: 1512, sourceHeight: 949 },
  })) }));
  return state;
}
