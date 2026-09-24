/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { FakeOBSRequestError, type FakeOBSServer } from "./fake-obs-server.js";

export type FakeInput = {
  inputName: string;
  inputKind: string;
  muted?: boolean; // undefined means the input has no audio
  tracks?: Record<string, boolean>;
};

export type FakeSceneItem = {
  sceneItemId: number;
  sourceName: string;
  sceneItemEnabled: boolean;
  sourceWidth: number;
  sourceHeight: number;
};

/** OBS state behind a fake server; tests mutate it to create one problem at a time. */
export type FakeObsState = {
  recording: boolean;
  recordDirectory: string;
  availableDiskSpace: number;
  profile: Record<string, string>;
  inputs: FakeInput[];
  sceneItems: FakeSceneItem[];
};

/** A recording setup that obs-preflight passes: silent, sized, apple_h264. */
export function healthyObsState(recordDirectory: string): FakeObsState {
  return {
    recording: false,
    recordDirectory,
    availableDiskSpace: 50_000,
    profile: {
      "Output/Mode": "Simple",
      "SimpleOutput/RecQuality": "Small",
      "SimpleOutput/RecEncoder": "apple_h264",
    },
    inputs: [
      { inputName: "Chrome", inputKind: "screen_capture", muted: true, tracks: { 1: false } },
      { inputName: "Background", inputKind: "color_source_v3" },
    ],
    sceneItems: [
      { sceneItemId: 1, sourceName: "Background", sceneItemEnabled: true, sourceWidth: 1920, sourceHeight: 1080 },
      { sceneItemId: 2, sourceName: "Chrome", sceneItemEnabled: true, sourceWidth: 1512, sourceHeight: 949 },
    ],
  };
}

/** Answers the requests obs-preflight makes from the state returned by getState. */
export function servePreflightState(fakeObs: FakeOBSServer, getState: () => FakeObsState): void {
  const findInput = (requestData: Record<string, unknown>): FakeInput => {
    const input = getState().inputs.find(({ inputName }) => inputName === requestData.inputName);
    if (!input) throw new FakeOBSRequestError(600, "No source was found");
    return input;
  };
  fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: getState().recording }));
  fakeObs.respondWith("GetRecordDirectory", () => ({ recordDirectory: getState().recordDirectory }));
  fakeObs.respondWith("GetStats", () => ({ availableDiskSpace: getState().availableDiskSpace }));
  fakeObs.respondWith("GetProfileParameter", ({ parameterCategory, parameterName }) => ({
    parameterValue: getState().profile[`${String(parameterCategory)}/${String(parameterName)}`] ?? null,
    defaultParameterValue: null,
  }));
  fakeObs.respondWith("GetInputList", () => ({
    inputs: getState().inputs.map(({ inputName, inputKind }) => ({ inputName, inputKind })),
  }));
  fakeObs.respondWith("GetInputMute", (data) => {
    const input = findInput(data);
    if (input.muted === undefined) throw new FakeOBSRequestError(604, "The specified input does not support audio");
    return { inputMuted: input.muted };
  });
  fakeObs.respondWith("GetInputAudioTracks", (data) => ({ inputAudioTracks: findInput(data).tracks ?? {} }));
  fakeObs.respondWith("GetCurrentProgramScene", () => ({ currentProgramSceneName: "Demo" }));
  fakeObs.respondWith("GetSceneItemList", () => ({
    sceneItems: getState().sceneItems.map(({ sourceWidth, sourceHeight, ...item }) => ({
      ...item,
      inputKind: getState().inputs.find(({ inputName }) => inputName === item.sourceName)?.inputKind,
      sceneItemTransform: { sourceWidth, sourceHeight },
    })),
  }));
}

/**
 * Answers StartRecord and StopRecord the way OBS does: each returns at once,
 * and the RecordStateChanged event that confirms it follows shortly after.
 */
export function serveRecordOutput(fakeObs: FakeOBSServer, outputPath: () => string): void {
  const later = (outputState: string, outputActive: boolean) => setTimeout(() => {
    try {
      fakeObs.sendEvent("RecordStateChanged", { outputActive, outputState, outputPath: outputPath() });
    } catch {
      // The connection closed first.
    }
  }, 5);
  fakeObs.respondWith("StartRecord", () => {
    later("OBS_WEBSOCKET_OUTPUT_STARTED", true);
    return {};
  });
  fakeObs.respondWith("StopRecord", () => {
    later("OBS_WEBSOCKET_OUTPUT_STOPPED", false);
    return { outputPath: outputPath() };
  });
}
