/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { ResourceTemplate, type McpServer } from "@modelcontextprotocol/server";
import type { OBSWebSocketClient } from "../client.js";
import { screenshotResult } from "./screenshot.js";
import { activeTake, takeUpdates } from "./take-monitor.js";

type JsonObject = Record<string, unknown>;

export const STATUS_URI = "obs://status";
export const SCENES_URI = "obs://scenes";
export const TAKE_URI = "obs://take/current";
export const sceneItemsUri = (sceneName: string) => `obs://scene/${encodeURIComponent(sceneName)}/items`;

const SCREENSHOT_WIDTH = 960;

// OBS events that change what each resource returns.
const STATUS_EVENTS = [
  "RecordStateChanged",
  "StreamStateChanged",
  "ReplayBufferStateChanged",
  "VirtualcamStateChanged",
  "CurrentProgramSceneChanged",
  "CurrentProfileChanged",
  "CurrentSceneCollectionChanged",
];
const SCENE_LIST_EVENTS = [
  "SceneCreated",
  "SceneRemoved",
  "SceneNameChanged",
  "SceneListChanged",
  "CurrentProgramSceneChanged",
  "CurrentPreviewSceneChanged",
  "CurrentSceneCollectionChanged",
];
const SCENE_ITEM_EVENTS = [
  "SceneItemCreated",
  "SceneItemRemoved",
  "SceneItemListReindexed",
  "SceneItemEnableStateChanged",
  "SceneItemLockStateChanged",
];

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function json(uri: string, value: unknown) {
  // Silent audio levels are -Infinity, which JSON cannot carry.
  const text = JSON.stringify(value, (_key, item: unknown) => (item === -Infinity ? null : item), 2);
  return { contents: [{ uri, mimeType: "application/json", text }] };
}

async function sceneNames(client: OBSWebSocketClient): Promise<string[]> {
  const response: unknown = await client.sendRequest("GetSceneList");
  const scenes = isObject(response) && Array.isArray(response.scenes) ? response.scenes : [];
  return scenes.filter(isObject).map(({ sceneName }) => sceneName).filter((name): name is string => typeof name === "string");
}

/**
 * Exposes OBS state as resources and forwards OBS events as resource-updated
 * notifications. Every resource duplicates a read-only tool, because few
 * clients read resources or honor subscriptions.
 */
export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerResource(
    "status",
    STATUS_URI,
    {
      title: "OBS status",
      description: "Connection, recording, streaming, and current scene; updates when any of them change",
      mimeType: "application/json",
    },
    async (uri) => {
      const [record, stream, scene] = await Promise.all([
        client.sendRequest("GetRecordStatus"),
        client.sendRequest("GetStreamStatus"),
        client.sendRequest("GetCurrentProgramScene"),
      ]);
      return json(uri.href, {
        connection: client.getConnectionStatus(),
        record,
        stream,
        currentProgramSceneName: isObject(scene) ? scene.currentProgramSceneName : null,
      });
    },
  );

  server.registerResource(
    "take",
    TAKE_URI,
    {
      title: "Current take",
      description: "While obs-record-clip or a recording session runs: audio levels on recorded tracks, skipped "
        + "frames, black or unchanging picture, chapters, and warnings; updates when a warning is raised",
      mimeType: "application/json",
    },
    async (uri) => json(uri.href, activeTake(client)?.summary() ?? { running: false }),
  );

  server.registerResource(
    "scenes",
    SCENES_URI,
    {
      title: "OBS scenes",
      description: "Scene list with the program and preview scenes; updates when scenes change",
      mimeType: "application/json",
    },
    async (uri) => json(uri.href, await client.sendRequest("GetSceneList")),
  );

  server.registerResource(
    "scene-items",
    new ResourceTemplate("obs://scene/{sceneName}/items", {
      list: async () => ({
        resources: (await sceneNames(client)).map((sceneName) => ({
          uri: sceneItemsUri(sceneName),
          name: `${sceneName} items`,
          mimeType: "application/json",
        })),
      }),
      complete: {
        sceneName: async (value) => (await sceneNames(client)).filter((name) => name.startsWith(value)),
      },
    }),
    {
      title: "Scene items",
      description: "Items in a scene with their transforms; updates when items are added, removed, shown, or hidden",
      mimeType: "application/json",
    },
    async (uri, { sceneName }) => json(
      uri.href,
      await client.sendRequest("GetSceneItemList", { sceneName: decodeURIComponent(String(sceneName)) }),
    ),
  );

  server.registerResource(
    "scene-screenshot",
    new ResourceTemplate("obs://scene/{sceneName}/screenshot", { list: undefined }),
    {
      title: "Scene screenshot",
      description: `A ${SCREENSHOT_WIDTH}px-wide JPEG of a scene as OBS renders it`,
      mimeType: "image/jpeg",
    },
    async (uri, { sceneName }) => {
      const response: unknown = await client.sendRequest("GetSourceScreenshot", {
        sourceName: decodeURIComponent(String(sceneName)),
        imageFormat: "jpeg",
        imageWidth: SCREENSHOT_WIDTH,
      });
      const image = screenshotResult(response).content.find((block) => block.type === "image");
      if (!image || image.type !== "image") throw new Error("OBS returned no screenshot");
      return { contents: [{ uri: uri.href, mimeType: image.mimeType, blob: image.data }] };
    },
  );

  forwardEvents(server, client);
}

/**
 * 2025-era clients subscribe per URI; 2026-07-28 clients choose URIs on their
 * subscriptions/listen stream, which the SDK filters. A 2025 session has
 * client capabilities from initialize, which is how the two are told apart.
 */
function forwardEvents(server: McpServer, client: OBSWebSocketClient): void {
  const subscriptions = new Set<string>();
  const lowLevel = server.server;
  lowLevel.registerCapabilities({ resources: { subscribe: true, listChanged: true } });
  lowLevel.setRequestHandler("resources/subscribe", async (request) => {
    subscriptions.add(request.params.uri);
    return {};
  });
  lowLevel.setRequestHandler("resources/unsubscribe", async (request) => {
    subscriptions.delete(request.params.uri);
    return {};
  });

  const notify = (uri: string) => {
    const legacySession = lowLevel.getClientCapabilities() !== undefined;
    if (legacySession && !subscriptions.has(uri)) return;
    void lowLevel.sendResourceUpdated({ uri }).catch(() => undefined);
  };

  const listeners: [string, (eventData: unknown) => void][] = [
    ...STATUS_EVENTS.map((event): [string, () => void] => [event, () => notify(STATUS_URI)]),
    ...SCENE_LIST_EVENTS.map((event): [string, () => void] => [event, () => notify(SCENES_URI)]),
    ...SCENE_ITEM_EVENTS.map((event): [string, (eventData: unknown) => void] => [event, (eventData) => {
      if (isObject(eventData) && typeof eventData.sceneName === "string") notify(sceneItemsUri(eventData.sceneName));
    }]),
    ...["SceneCreated", "SceneRemoved", "SceneNameChanged"].map((event): [string, () => void] => [
      event,
      () => void lowLevel.sendResourceListChanged().catch(() => undefined),
    ]),
  ];
  for (const [event, listener] of listeners) client.on(event, listener);
  const onTakeUpdate = () => notify(TAKE_URI);
  takeUpdates.on("update", onTakeUpdate);

  // serveStdio builds a server per session; drop this session's listeners with it.
  const previousOnClose = lowLevel.onclose;
  lowLevel.onclose = () => {
    for (const [event, listener] of listeners) client.off(event, listener);
    takeUpdates.off("update", onTakeUpdate);
    previousOnClose?.();
  };
}
