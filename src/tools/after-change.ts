/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult } from "@modelcontextprotocol/server";
import type { OBSWebSocketClient } from "../client.js";
import { screenshotResult } from "./screenshot.js";

type JsonObject = Record<string, unknown>;

export type InputAppearance = {
  sceneName: string;
  sceneItemId: number;
  sceneItemEnabled: boolean;
  sourceWidth: number;
  sourceHeight: number;
};

export type InputState = {
  inputSettings: JsonObject;
  appearances: InputAppearance[];
  warnings: string[];
};

// Capture sources (ScreenCaptureKit, window capture) report 0×0 until their
// first frame arrives, so a zero size is re-read briefly before warning.
const SIZE_SETTLE_ATTEMPTS = 4;
const SIZE_SETTLE_DELAY_MS = 250;
const SCREENSHOT_WIDTH = 960;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}

async function programSceneAppearances(
  client: OBSWebSocketClient,
  inputName: string,
): Promise<InputAppearance[]> {
  const program: unknown = await client.sendRequest("GetCurrentProgramScene");
  const sceneName = isObject(program) ? program.currentProgramSceneName : undefined;
  if (typeof sceneName !== "string") return [];
  const list: unknown = await client.sendRequest("GetSceneItemList", { sceneName });
  const items = isObject(list) && Array.isArray(list.sceneItems) ? list.sceneItems.filter(isObject) : [];

  return items
    .filter((item) => item.sourceName === inputName && typeof item.sceneItemId === "number")
    .map((item) => {
      const transform = isObject(item.sceneItemTransform) ? item.sceneItemTransform : {};
      return {
        sceneName,
        sceneItemId: item.sceneItemId as number,
        sceneItemEnabled: item.sceneItemEnabled !== false,
        sourceWidth: typeof transform.sourceWidth === "number" ? transform.sourceWidth : 0,
        sourceHeight: typeof transform.sourceHeight === "number" ? transform.sourceHeight : 0,
      };
    });
}

function isBlank(appearance: InputAppearance): boolean {
  return appearance.sceneItemEnabled && (appearance.sourceWidth === 0 || appearance.sourceHeight === 0);
}

/**
 * Reads an input back after a change: its settings, its size wherever it
 * appears in the program scene, and warnings for problems the change caused.
 */
export async function readInputState(
  client: OBSWebSocketClient,
  inputName: string,
  settleDelayMs = SIZE_SETTLE_DELAY_MS,
): Promise<InputState> {
  const warnings: string[] = [];
  let inputSettings: JsonObject = {};
  try {
    const response: unknown = await client.sendRequest("GetInputSettings", { inputName });
    if (isObject(response) && isObject(response.inputSettings)) inputSettings = response.inputSettings;
  } catch (error) {
    warnings.push(`Could not read the input back: ${errorMessage(error)}`);
  }

  let appearances: InputAppearance[] = [];
  try {
    for (let attempt = 0; attempt < SIZE_SETTLE_ATTEMPTS; attempt += 1) {
      appearances = await programSceneAppearances(client, inputName);
      if (!appearances.some(isBlank)) break;
      if (attempt < SIZE_SETTLE_ATTEMPTS - 1) await new Promise((resolve) => setTimeout(resolve, settleDelayMs));
    }
  } catch (error) {
    warnings.push(`Could not read the program scene: ${errorMessage(error)}`);
  }

  if (appearances.some(isBlank)) {
    warnings.push(
      `${inputName} renders at 0×0 in the program scene. A capture source usually needs a display, `
        + "window, or application selected (macOS screen_capture application capture also needs "
        + "display_uuid) and screen recording permission for OBS",
    );
  }
  return { inputSettings, appearances, warnings };
}

/** Appends a bounded screenshot of the input, or a note explaining why there is none. */
export async function appendInputScreenshot(
  client: OBSWebSocketClient,
  inputName: string,
  result: CallToolResult,
): Promise<CallToolResult> {
  try {
    const response: unknown = await client.sendRequest("GetSourceScreenshot", {
      sourceName: inputName,
      imageFormat: "jpeg",
      imageWidth: SCREENSHOT_WIDTH,
    });
    const screenshot = screenshotResult(response);
    return { ...result, content: [...result.content, ...screenshot.content] };
  } catch (error) {
    return {
      ...result,
      content: [...result.content, { type: "text", text: `No screenshot: ${errorMessage(error)}` }],
    };
  }
}

export function inputStateResult(message: string, state: InputState): CallToolResult {
  const sizes = state.appearances.map(({ sceneName, sceneItemId, sourceWidth, sourceHeight }) => (
    `${sceneName} item ${sceneItemId}: ${sourceWidth}×${sourceHeight}`
  ));
  const lines = [
    message,
    sizes.length > 0 ? `Size in program scene: ${sizes.join("; ")}` : "Not in the program scene",
    ...state.warnings.map((warning) => `Warning: ${warning}`),
  ];
  return {
    content: [{ type: "text", text: lines.join("\n") }],
    structuredContent: { message, ...state },
  };
}
