/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { FakeOBSRequestError, type FakeOBSServer } from "./fake-obs-server.js";

type JsonObject = Record<string, unknown>;

export type FakeSceneInput = {
  kind: string;
  settings: JsonObject;
  /** Absent for inputs without audio. */
  muted?: boolean;
  volumeMul?: number;
  tracks?: Record<string, boolean>;
};

export type FakeSceneItem = {
  sceneItemId: number;
  sourceName: string;
  enabled: boolean;
  locked: boolean;
  transform: JsonObject;
};

/** Scenes, inputs, and items behind a fake OBS, changed by the requests that change them in OBS. */
export type FakeSceneState = {
  program: string;
  video: { baseWidth: number; baseHeight: number };
  inputs: Record<string, FakeSceneInput>;
  /** Items bottom to top, as sceneItemIndex counts them. */
  scenes: Record<string, FakeSceneItem[]>;
  nextItemId: number;
};

const AUDIO_KINDS = new Set(["screen_capture", "coreaudio_input_capture", "ffmpeg_source", "browser_source"]);
/** What each kind renders at, so transforms carry a source size. */
const SOURCE_SIZE: Record<string, [number, number]> = {
  screen_capture: [1512, 949],
  color_source_v3: [1920, 1080],
  image_source: [800, 600],
};

export function newInput(kind: string, settings: JsonObject = {}): FakeSceneInput {
  return AUDIO_KINDS.has(kind)
    ? { kind, settings, muted: false, volumeMul: 1, tracks: { 1: true, 2: false, 3: false, 4: false, 5: false, 6: false } }
    : { kind, settings };
}

/** The Demo scene from the Siftable recording: a black background under a Chrome window capture. */
export function demoSceneState(): FakeSceneState {
  return {
    program: "Demo",
    video: { baseWidth: 1920, baseHeight: 1080 },
    inputs: {
      Background: newInput("color_source_v3", { color: 4278190080 }),
      Chrome: { ...newInput("screen_capture", { type: 1, window: 31875 }), muted: true, tracks: { 1: false, 2: false, 3: false, 4: false, 5: false, 6: false } },
    },
    scenes: {
      Demo: [
        { sceneItemId: 1, sourceName: "Background", enabled: true, locked: true, transform: { positionX: 0, positionY: 0, scaleX: 1, scaleY: 1, rotation: 0, alignment: 5, boundsType: "OBS_BOUNDS_NONE", cropTop: 0 } },
        { sceneItemId: 2, sourceName: "Chrome", enabled: true, locked: false, transform: { positionX: 204, positionY: 65, scaleX: 1, scaleY: 1, rotation: 0, alignment: 5, boundsType: "OBS_BOUNDS_NONE", cropTop: 40 } },
      ],
    },
    nextItemId: 3,
  };
}

/** Answers scene, input, and scene item requests from state that tests can inspect and change. */
export function serveSceneState(fakeObs: FakeOBSServer, state: FakeSceneState): void {
  const scene = (data: JsonObject): FakeSceneItem[] => {
    const items = state.scenes[String(data.sceneName)];
    if (!items) throw new FakeOBSRequestError(600, `No scene named ${String(data.sceneName)}`);
    return items;
  };
  const input = (data: JsonObject): FakeSceneInput => {
    const found = state.inputs[String(data.inputName)];
    if (!found) throw new FakeOBSRequestError(600, "No source was found");
    return found;
  };
  const audio = (data: JsonObject): FakeSceneInput => {
    const found = input(data);
    if (found.muted === undefined) throw new FakeOBSRequestError(604, "The specified input does not support audio");
    return found;
  };
  const item = (data: JsonObject): FakeSceneItem => {
    const found = scene(data).find(({ sceneItemId }) => sceneItemId === data.sceneItemId);
    if (!found) throw new FakeOBSRequestError(600, "No scene item was found");
    return found;
  };
  const addItem = (sceneName: string, sourceName: string, enabled: unknown): number => {
    const sceneItemId = state.nextItemId++;
    scene({ sceneName }).push({
      sceneItemId,
      sourceName,
      enabled: enabled !== false,
      locked: false,
      transform: { positionX: 0, positionY: 0, scaleX: 1, scaleY: 1, rotation: 0, alignment: 5, boundsType: "OBS_BOUNDS_NONE" },
    });
    return sceneItemId;
  };
  const done = (change: () => void) => () => {
    change();
    return {};
  };

  fakeObs.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: state.program, sceneName: state.program }));
  fakeObs.respondWith("GetVideoSettings", () => ({ ...state.video, outputWidth: state.video.baseWidth, outputHeight: state.video.baseHeight, fpsNumerator: 30, fpsDenominator: 1 }));
  fakeObs.respondWith("GetSceneList", () => ({
    currentProgramSceneName: state.program,
    scenes: Object.keys(state.scenes).map((sceneName, sceneIndex) => ({ sceneName, sceneIndex })),
  }));
  fakeObs.respondWith("CreateScene", ({ sceneName }) => {
    if (state.scenes[String(sceneName)]) throw new FakeOBSRequestError(601, "A source already exists by that name");
    state.scenes[String(sceneName)] = [];
    return {};
  });
  fakeObs.respondWith("GetInputList", () => ({
    inputs: Object.entries(state.inputs).map(([inputName, { kind }]) => ({ inputName, inputKind: kind })),
  }));
  fakeObs.respondWith("CreateInput", (data) => {
    const inputName = String(data.inputName);
    if (state.inputs[inputName] || state.scenes[inputName]) throw new FakeOBSRequestError(601, "A source already exists by that name");
    scene(data);
    state.inputs[inputName] = newInput(String(data.inputKind), { ...(data.inputSettings as JsonObject | undefined) });
    return { sceneItemId: addItem(String(data.sceneName), inputName, data.sceneItemEnabled) };
  });
  fakeObs.respondWith("CreateSceneItem", (data) => {
    if (!state.inputs[String(data.sourceName)]) throw new FakeOBSRequestError(600, "No source was found");
    return { sceneItemId: addItem(String(data.sceneName), String(data.sourceName), data.sceneItemEnabled) };
  });
  fakeObs.respondWith("RemoveSceneItem", (data) => {
    const items = scene(data);
    items.splice(items.indexOf(item(data)), 1);
    return {};
  });
  fakeObs.respondWith("GetSceneItemList", (data) => ({
    sceneItems: scene(data).map((entry, sceneItemIndex) => {
      const kind = state.inputs[entry.sourceName]?.kind ?? "";
      const [sourceWidth, sourceHeight] = SOURCE_SIZE[kind] ?? [0, 0];
      return {
        sceneItemId: entry.sceneItemId,
        sourceName: entry.sourceName,
        inputKind: kind,
        sceneItemIndex,
        sceneItemEnabled: entry.enabled,
        sceneItemLocked: entry.locked,
        sceneItemTransform: { ...entry.transform, sourceWidth, sourceHeight },
      };
    }),
  }));
  fakeObs.respondWith("GetSceneItemId", (data) => {
    const found = scene(data).find(({ sourceName }) => sourceName === data.sourceName);
    if (!found) throw new FakeOBSRequestError(600, "No scene item was found");
    return { sceneItemId: found.sceneItemId };
  });
  fakeObs.respondWith("GetInputSettings", (data) => ({ inputKind: input(data).kind, inputSettings: { ...input(data).settings } }));
  fakeObs.respondWith("SetInputSettings", (data) => {
    const target = input(data);
    const settings = data.inputSettings as JsonObject;
    target.settings = data.overlay === false ? { ...settings } : { ...target.settings, ...settings };
    return {};
  });
  fakeObs.respondWith("GetInputMute", (data) => ({ inputMuted: audio(data).muted }));
  fakeObs.respondWith("SetInputMute", (data) => done(() => { audio(data).muted = data.inputMuted as boolean; })());
  fakeObs.respondWith("GetInputVolume", (data) => {
    const mul = audio(data).volumeMul ?? 1;
    return { inputVolumeMul: mul, inputVolumeDb: mul > 0 ? 20 * Math.log10(mul) : -100 };
  });
  fakeObs.respondWith("SetInputVolume", (data) => done(() => {
    audio(data).volumeMul = typeof data.inputVolumeMul === "number" ? data.inputVolumeMul : 10 ** (Number(data.inputVolumeDb) / 20);
  })());
  fakeObs.respondWith("GetInputAudioTracks", (data) => ({ inputAudioTracks: { ...audio(data).tracks } }));
  fakeObs.respondWith("SetInputAudioTracks", (data) => done(() => {
    const target = audio(data);
    target.tracks = { ...target.tracks, ...(data.inputAudioTracks as Record<string, boolean>) };
  })());
  fakeObs.respondWith("SetSceneItemTransform", (data) => done(() => {
    const target = item(data);
    target.transform = { ...target.transform, ...(data.sceneItemTransform as JsonObject) };
  })());
  fakeObs.respondWith("SetSceneItemEnabled", (data) => done(() => { item(data).enabled = data.sceneItemEnabled as boolean; })());
  fakeObs.respondWith("SetSceneItemLocked", (data) => done(() => { item(data).locked = data.sceneItemLocked as boolean; })());
  fakeObs.respondWith("SetSceneItemIndex", (data) => done(() => {
    const items = scene(data);
    const target = item(data);
    items.splice(items.indexOf(target), 1);
    items.splice(Number(data.sceneItemIndex), 0, target);
  })());
}
