import type { McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import {
  DESTRUCTIVE_WRITE_TOOL,
  IDEMPOTENT_WRITE_TOOL,
  NON_IDEMPOTENT_WRITE_TOOL,
  READ_ONLY_TOOL,
  registerObsRequestTool,
} from "./request-tool.js";

const inputSelector = {
  inputName: z.string().optional().describe("Name of the input"),
  inputUuid: z.string().optional().describe("UUID of the input"),
};

const sceneSelector = {
  canvasUuid: z.string().optional().describe("UUID of the canvas containing the scene"),
  sceneName: z.string().optional().describe("Name of the scene"),
  sceneUuid: z.string().optional().describe("UUID of the scene"),
};

const sceneItemSelector = {
  ...sceneSelector,
  sceneItemId: z.number().int().nonnegative().describe("Numeric ID of the scene item"),
};

const deinterlaceFieldOrder = z.enum([
  "OBS_DEINTERLACE_FIELD_ORDER_TOP",
  "OBS_DEINTERLACE_FIELD_ORDER_BOTTOM",
]);

const deinterlaceMode = z.enum([
  "OBS_DEINTERLACE_MODE_DISABLE",
  "OBS_DEINTERLACE_MODE_DISCARD",
  "OBS_DEINTERLACE_MODE_RETRO",
  "OBS_DEINTERLACE_MODE_BLEND",
  "OBS_DEINTERLACE_MODE_BLEND_2X",
  "OBS_DEINTERLACE_MODE_LINEAR",
  "OBS_DEINTERLACE_MODE_LINEAR_2X",
  "OBS_DEINTERLACE_MODE_YADIF",
  "OBS_DEINTERLACE_MODE_YADIF_2X",
]);

const sceneItemBlendMode = z.enum([
  "OBS_BLEND_NORMAL",
  "OBS_BLEND_ADDITIVE",
  "OBS_BLEND_SUBTRACT",
  "OBS_BLEND_SCREEN",
  "OBS_BLEND_MULTIPLY",
  "OBS_BLEND_LIGHTEN",
  "OBS_BLEND_DARKEN",
]);

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  registerObsRequestTool(server, client, {
    name: "obs-get-canvas-list",
    title: "Get Canvas List",
    description: "Get all canvases available in OBS",
    requestType: "GetCanvasList",
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-input-audio-tracks",
    title: "Get Input Audio Tracks",
    description: "Get the enabled audio tracks for an input",
    requestType: "GetInputAudioTracks",
    inputSchema: z.object(inputSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-input-audio-tracks",
    title: "Set Input Audio Tracks",
    description: "Set the enabled audio tracks for an input",
    requestType: "SetInputAudioTracks",
    inputSchema: z.object({
      ...inputSelector,
      inputAudioTracks: z.record(
        z.enum(["1", "2", "3", "4", "5", "6"]),
        z.boolean(),
      ).describe("Enable state for OBS audio tracks 1 through 6"),
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-input-deinterlace-field-order",
    title: "Get Input Deinterlace Field Order",
    description: "Get the deinterlace field order of an asynchronous input",
    requestType: "GetInputDeinterlaceFieldOrder",
    inputSchema: z.object(inputSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-input-deinterlace-field-order",
    title: "Set Input Deinterlace Field Order",
    description: "Set the deinterlace field order of an asynchronous input",
    requestType: "SetInputDeinterlaceFieldOrder",
    inputSchema: z.object({
      ...inputSelector,
      inputDeinterlaceFieldOrder: deinterlaceFieldOrder,
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-input-deinterlace-mode",
    title: "Get Input Deinterlace Mode",
    description: "Get the deinterlace mode of an asynchronous input",
    requestType: "GetInputDeinterlaceMode",
    inputSchema: z.object(inputSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-input-deinterlace-mode",
    title: "Set Input Deinterlace Mode",
    description: "Set the deinterlace mode of an asynchronous input",
    requestType: "SetInputDeinterlaceMode",
    inputSchema: z.object({
      ...inputSelector,
      inputDeinterlaceMode: deinterlaceMode,
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-input-property-list-items",
    title: "Get Input Property List Items",
    description: "Get the selectable items for a dynamic input list property",
    requestType: "GetInputPropertiesListPropertyItems",
    inputSchema: z.object({
      ...inputSelector,
      propertyName: z.string().describe("Name of the list property"),
    }),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-press-input-property-button",
    title: "Press Input Property Button",
    description: "Press a button exposed by an input property",
    requestType: "PressInputPropertiesButton",
    inputSchema: z.object({
      ...inputSelector,
      propertyName: z.string().describe("Name of the button property"),
    }),
    annotations: DESTRUCTIVE_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-duplicate-scene-item",
    title: "Duplicate Scene Item",
    description: "Duplicate a scene item while preserving its transform and crop",
    requestType: "DuplicateSceneItem",
    inputSchema: z.object({
      ...sceneItemSelector,
      destinationSceneName: z.string().optional().describe("Destination scene name; defaults to the source scene"),
      destinationSceneUuid: z.string().optional().describe("Destination scene UUID; defaults to the source scene"),
    }),
    annotations: NON_IDEMPOTENT_WRITE_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-group-scene-items",
    title: "Get Group Scene Items",
    description: "Get the scene items contained in an OBS group",
    requestType: "GetGroupSceneItemList",
    inputSchema: z.object(sceneSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-enabled",
    title: "Get Scene Item Enabled",
    description: "Get whether a scene item is enabled",
    requestType: "GetSceneItemEnabled",
    inputSchema: z.object(sceneItemSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-index",
    title: "Get Scene Item Index",
    description: "Get the index position of a scene item",
    requestType: "GetSceneItemIndex",
    inputSchema: z.object(sceneItemSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-scene-item-index",
    title: "Set Scene Item Index",
    description: "Set the index position of a scene item",
    requestType: "SetSceneItemIndex",
    inputSchema: z.object({
      ...sceneItemSelector,
      sceneItemIndex: z.number().int().nonnegative().describe("New scene item index"),
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-locked",
    title: "Get Scene Item Locked",
    description: "Get whether a scene item is locked",
    requestType: "GetSceneItemLocked",
    inputSchema: z.object(sceneItemSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-scene-item-locked",
    title: "Set Scene Item Locked",
    description: "Set whether a scene item is locked",
    requestType: "SetSceneItemLocked",
    inputSchema: z.object({
      ...sceneItemSelector,
      sceneItemLocked: z.boolean().describe("New lock state"),
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-blend-mode",
    title: "Get Scene Item Blend Mode",
    description: "Get the blend mode of a scene item",
    requestType: "GetSceneItemBlendMode",
    inputSchema: z.object(sceneItemSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-scene-item-blend-mode",
    title: "Set Scene Item Blend Mode",
    description: "Set the blend mode of a scene item",
    requestType: "SetSceneItemBlendMode",
    inputSchema: z.object({
      ...sceneItemSelector,
      sceneItemBlendMode,
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-item-source",
    title: "Get Scene Item Source",
    description: "Get the source associated with a scene item",
    requestType: "GetSceneItemSource",
    inputSchema: z.object(sceneItemSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-group-list",
    title: "Get Group List",
    description: "Get all OBS groups",
    requestType: "GetGroupList",
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-scene-name",
    title: "Set Scene Name",
    description: "Rename an OBS scene",
    requestType: "SetSceneName",
    inputSchema: z.object({
      ...sceneSelector,
      newSceneName: z.string().min(1).describe("New scene name"),
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-scene-transition-override",
    title: "Get Scene Transition Override",
    description: "Get the transition override configured for a scene",
    requestType: "GetSceneSceneTransitionOverride",
    inputSchema: z.object(sceneSelector),
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-scene-transition-override",
    title: "Set Scene Transition Override",
    description: "Set or clear the transition override for a scene",
    requestType: "SetSceneSceneTransitionOverride",
    inputSchema: z.object({
      ...sceneSelector,
      transitionName: z.string().nullable().optional().describe("Transition name, or null to clear it"),
      transitionDuration: z.number().int().min(50).max(20000).nullable().optional()
        .describe("Override duration in milliseconds, or null to clear it"),
    }),
    annotations: IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-transition-kind-list",
    title: "Get Transition Kind List",
    description: "Get all available OBS transition kinds",
    requestType: "GetTransitionKindList",
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-get-transition-cursor",
    title: "Get Transition Cursor",
    description: "Get the current scene transition cursor position",
    requestType: "GetCurrentSceneTransitionCursor",
    annotations: READ_ONLY_TOOL,
  });

  registerObsRequestTool(server, client, {
    name: "obs-set-tbar-position",
    title: "Set T-Bar Position",
    description: "Set and optionally release the OBS Studio Mode T-Bar position",
    requestType: "SetTBarPosition",
    inputSchema: z.object({
      position: z.number().min(0).max(1).describe("T-Bar position between 0 and 1"),
      release: z.boolean().optional().default(true).describe("Release the T-Bar after setting the position"),
    }),
    annotations: NON_IDEMPOTENT_WRITE_TOOL,
    responseMode: "success",
  });
}
