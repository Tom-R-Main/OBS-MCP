/* SPDX-License-Identifier: GPL-2.0-only */
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { rendersNoVideo } from "./audio-only.js";
import { READ_ONLY_TOOL } from "./request-tool.js";
import { screenshotResult } from "./screenshot.js";

type Obj = Record<string, unknown>;
type Status = "pass" | "fail" | "unknown";
type Identity = { sourceName: string; sourceUuid: string; containerUuid: string; sceneItemId: number };
type Item = Identity & { path: Identity[]; enabled: boolean | null; effectiveEnabled: boolean | null; transform?: Obj };
type Check = { id: string; status: Status; message: string; identity?: Identity; evidence?: Obj };
const name = z.string().min(1).max(256);
const selector = z.object({
  sourceName: name, sourceUuid: name.optional(),
  containerUuid: name.optional(), sceneItemId: z.number().int().nonnegative().optional(),
});
const inputSchema = z.object({
  sceneCollection: name, sceneName: name,
  requiredItems: z.array(selector).max(64).default([]), includeScreenshot: z.boolean().default(false),
});
type Options = z.infer<typeof inputSchema>;
const LIMITS = { maxItems: 128, maxDepth: 8, maxRequests: 512, maxDurationMs: 12_000, requestTimeoutMs: 2_000 };
const NUMERIC_FIELDS = ["positionX", "positionY", "rotation", "scaleX", "scaleY", "sourceWidth", "sourceHeight", "cropLeft", "cropRight", "cropTop", "cropBottom", "alignment", "boundsAlignment", "boundsWidth", "boundsHeight"];
const statusSchema = z.enum(["pass", "fail", "unknown"]);
const identitySchema = z.object({ sourceName: name, sourceUuid: name, containerUuid: name, sceneItemId: z.number().int().nonnegative() });
const outputSchema = z.object({
  version: z.literal(1), scope: z.literal("structural"), status: statusSchema,
  target: z.object({ sceneCollection: name, sceneName: name, sceneUuid: name.optional() }),
  observedAt: z.string(), completedAt: z.string(), durationMs: z.number().nonnegative(), requests: z.number().int().nonnegative(),
  limits: z.object({ maxItems: z.number(), maxDepth: z.number(), maxRequests: z.number(), maxDurationMs: z.number(), requestTimeoutMs: z.number() }),
  items: z.array(identitySchema.extend({ path: z.array(identitySchema).max(LIMITS.maxDepth + 1), enabled: z.boolean().nullable(), effectiveEnabled: z.boolean().nullable(), transform: z.record(z.string(), z.union([z.number(), z.string()])).optional() })).max(LIMITS.maxItems),
  checks: z.array(z.object({ id: z.string(), status: statusSchema, message: z.string().max(1024), identity: identitySchema.optional(), evidence: selector.optional() })).max(1024),
  renderedContent: z.literal("unverified"), limitations: z.array(z.string()),
});
const object = (value: unknown): value is Obj => typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === "string" && value.length > 0 && value.length <= 256;
const finite = (value: unknown): value is number => typeof value === "number" && Number.isFinite(value);
const message = (error: unknown): string => error instanceof Error ? error.message : String(error);
function objects(value: unknown): Obj[] {
  if (!Array.isArray(value) || !value.every(object)) throw new Error("OBS returned an incomplete list");
  return value;
}
function signature(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) => object(item) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
}
function membership(items: Obj[]): unknown {
  return items.map(({ sourceName, sourceUuid, sceneItemId, sceneItemEnabled, isGroup, inputKind }) =>
    ({ sourceName, sourceUuid, sceneItemId, sceneItemEnabled, isGroup, inputKind }));
}
function boundedTransform(value: unknown): Obj | undefined {
  if (!object(value)) return;
  return Object.fromEntries([...NUMERIC_FIELDS, "boundsType"].flatMap((key) => finite(value[key]) || (key === "boundsType" && text(value[key])) ? [[key, value[key]]] : []));
}

/** One bounded, sequential sample. In-flight requests may finish after a timeout;
 * no further reads are scheduled once its deadline or consistency guard trips. */
async function audit(client: OBSWebSocketClient, options: Options): Promise<CallToolResult> {
  const started = Date.now();
  const observedAt = new Date(started).toISOString();
  const checks: Check[] = [];
  const items: Item[] = [];
  const target: Obj = { sceneCollection: options.sceneCollection, sceneName: options.sceneName };
  let requests = 0;
  let invalidated = false;
  let complete = true;
  let closed = false;
  let identified = client.getConnectionStatus().identified;
  let screenshot: CallToolResult | undefined;
  const add = (id: string, status: Status, message: string, identity?: Identity, evidence?: Obj) => checks.push({ id, status, message: message.slice(0, 900), ...(identity ? { identity: { sourceName: identity.sourceName, sourceUuid: identity.sourceUuid, containerUuid: identity.containerUuid, sceneItemId: identity.sceneItemId } } : {}), ...(evidence ? { evidence } : {}) });
  const invalidate = () => { invalidated = true; };
  const onIdentified = () => { if (identified) invalidate(); identified = true; };
  // Transform events require a separate subscription. Full transform rereads
  // below provide a second observation without changing subscriptions.
  const onEvent = (type: string) => {
    if (/^(CurrentSceneCollection|SceneCollection|SceneCreated|SceneRemoved|SceneNameChanged|SceneItem|InputCreated|InputRemoved|InputNameChanged|InputSettingsChanged|VideoSettingsChanged)/.test(type)) invalidate();
  };
  client.on("disconnected", invalidate);
  client.on("identified", onIdentified);
  client.on("event", onEvent);
  const request = async (type: string, data?: Obj): Promise<Obj> => {
    if (closed || invalidated) throw new Error("OBS state changed while sampling");
    const remaining = LIMITS.maxDurationMs - (Date.now() - started);
    if (remaining <= 0 || requests >= LIMITS.maxRequests) throw new Error("Scene audit request or time limit reached");
    requests++;
    const timeout = Math.min(remaining, LIMITS.requestTimeoutMs);
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const response: unknown = await Promise.race([
        client.sendRequest<unknown>(type, data, timeout),
        new Promise<never>((_resolve, reject) => { timer = setTimeout(() => reject(new Error(`Scene audit timed out reading ${type}`)), timeout); }),
      ]);
      if (invalidated || Date.now() - started >= LIMITS.maxDurationMs) throw new Error("OBS state changed or scene audit deadline elapsed while sampling");
      if (!object(response)) throw new Error(`OBS returned incomplete ${type} state`);
      if (type !== "GetSourceScreenshot" && JSON.stringify(response).length > 256 * 1024) throw new Error(`OBS returned oversized ${type} evidence`);
      return response;
    } finally { if (timer) clearTimeout(timer); }
  };
  const containers: { uuid: string; group: boolean; before: string }[] = [];
  const transforms: { item: Item; before: string }[] = [];
  const mediaSettings: { uuid: string; before: string }[] = [];
  try {
    const collection = await request("GetSceneCollectionList");
    if (collection.currentSceneCollectionName !== options.sceneCollection) throw new Error("The selected scene collection is not current or was not reported");
    add("collection", "pass", "Selected scene collection is current");
    const scenes = objects((await request("GetSceneList")).scenes);
    if (scenes.length > 256 || scenes.some((scene) => !text(scene.sceneName) || !text(scene.sceneUuid))
      || new Set(scenes.map((scene) => scene.sceneUuid)).size !== scenes.length) throw new Error("Scene inventory is incomplete, ambiguous, or exceeds the limit");
    const selected = scenes.filter((scene) => scene.sceneName === options.sceneName);
    if (selected.length !== 1) throw new Error("Selected scene is missing or ambiguous");
    const sceneUuid = selected[0]!.sceneUuid as string;
    target.sceneUuid = sceneUuid;
    const inputs = objects((await request("GetInputList")).inputs);
    if (inputs.length > 2048 || inputs.some((input) => !text(input.inputName) || !text(input.inputUuid) || !text(input.inputKind))
      || new Set(inputs.map((input) => input.inputUuid)).size !== inputs.length) throw new Error("Input inventory is incomplete or ambiguous");
    const scenesByUuid = new Map(scenes.map((scene) => [scene.sceneUuid, scene]));
    const walk = async (uuid: string, group: boolean, path: Identity[], ancestors: Set<string>, ancestorEnabled: boolean | null): Promise<void> => {
      if (path.length > LIMITS.maxDepth || ancestors.has(uuid)) { complete = false; add("membership", "unknown", "Nested scene cycle or depth limit reached"); return; }
      const type = group ? "GetGroupSceneItemList" : "GetSceneItemList";
      const members = objects((await request(type, { sceneUuid: uuid })).sceneItems);
      if (members.length > LIMITS.maxItems || new Set(members.map((item) => item.sceneItemId)).size !== members.length) throw new Error("Scene item inventory is ambiguous or exceeds the item limit");
      containers.push({ uuid, group, before: signature(membership(members)) });
      for (const member of members) {
        if (items.length >= LIMITS.maxItems) throw new Error("Scene audit item limit reached");
        if (!text(member.sourceName) || !text(member.sourceUuid) || !Number.isSafeInteger(member.sceneItemId) || Number(member.sceneItemId) < 0 || typeof member.isGroup !== "boolean") throw new Error("Scene item identity or membership is incomplete");
        const identity: Identity = { sourceName: member.sourceName, sourceUuid: member.sourceUuid, containerUuid: uuid, sceneItemId: member.sceneItemId as number };
        const enabled = typeof member.sceneItemEnabled === "boolean" ? member.sceneItemEnabled : null;
        const effectiveEnabled = enabled === false || ancestorEnabled === false ? false : enabled === null || ancestorEnabled === null ? null : true;
        const item: Item = { ...identity, path: [...path, identity], enabled, effectiveEnabled };
        items.push(item);
        if (enabled === null) add("item-enabled", "unknown", "OBS did not report item enabled state", identity);
        const response = await request("GetSceneItemTransform", { sceneUuid: uuid, sceneItemId: identity.sceneItemId });
        const transform = boundedTransform(response.sceneItemTransform);
        if (!object(transform)) { add("transform", "unknown", "OBS did not report an item transform", identity); }
        else {
          item.transform = transform;
          transforms.push({ item, before: signature(transform) });
          const valid = NUMERIC_FIELDS.every((key) => finite(transform[key]))
            && ["sourceWidth", "sourceHeight", "boundsWidth", "boundsHeight", "cropLeft", "cropRight", "cropTop", "cropBottom"].every((key) => Number(transform[key]) >= 0)
            && ["cropLeft", "cropRight", "cropTop", "cropBottom"].every((key) => Number.isInteger(transform[key]))
            && [0, 1, 2, 4, 5, 6, 8, 9, 10].includes(Number(transform.alignment))
            && [0, 1, 2, 4, 5, 6, 8, 9, 10].includes(Number(transform.boundsAlignment))
            && text(transform.boundsType);
          if (!valid) add("transform", "unknown", "Item transform has missing, malformed, or nonfinite fields", identity);
          else {
            add("transform", "pass", "Required transform fields are well formed", identity);
            const source = inputs.filter((input) => input.inputUuid === member.sourceUuid && input.inputName === member.sourceName);
            const nested = scenesByUuid.get(member.sourceUuid);
            if (!member.isGroup && !nested && source.length !== 1) add("source-identity", "unknown", "Item source could not be resolved uniquely in the input inventory", identity);
            else {
              let audioOnly = source.length === 1 && rendersNoVideo(source[0]!.inputKind);
              if (!audioOnly && source.length === 1 && ["ffmpeg_source", "vlc_source"].includes(String(source[0]!.inputKind))
                && (transform.sourceWidth === 0 || transform.sourceHeight === 0)) {
                const settings = await request("GetInputSettings", { inputUuid: member.sourceUuid });
                if (!object(settings.inputSettings)) add("source-dimensions", "unknown", "Media source settings unavailable; audio-only status unknown", identity);
                else {
                  mediaSettings.push({ uuid: member.sourceUuid, before: signature(settings.inputSettings) });
                  // A remembered local_file is not evidence while FFmpeg uses
                  // a network URL or VLC uses a playlist instead.
                  audioOnly = source[0]!.inputKind === "ffmpeg_source" && settings.inputSettings.is_local_file === true
                    && rendersNoVideo(source[0]!.inputKind, settings.inputSettings);
                }
                if (!object(settings.inputSettings)) continue;
              }
              const noPicture = Number(transform.sourceWidth) === 0 || Number(transform.sourceHeight) === 0
                || Number(transform.cropLeft) + Number(transform.cropRight) >= Number(transform.sourceWidth)
                || Number(transform.cropTop) + Number(transform.cropBottom) >= Number(transform.sourceHeight)
                || transform.scaleX === 0 || transform.scaleY === 0;
              add("source-dimensions", audioOnly || !noPicture ? "pass" : "fail", audioOnly ? "Source is audio-only by kind or media settings" : noPicture ? "Visual source has zero source size, zero scale, or is fully cropped" : "Visual source has nonzero dimensions after crop and scale", identity);
            }
            if (path.length > 0 || member.isGroup || nested || transform.rotation !== 0 || transform.boundsType !== "OBS_BOUNDS_NONE") {
              add("geometry", "unknown", "Nested, rotated, or bounded geometry is outside this structural audit", identity);
            }
          }
        }
        if (member.isGroup || scenesByUuid.has(member.sourceUuid)) {
          const childScene = scenesByUuid.get(member.sourceUuid);
          if (childScene && childScene.sceneName !== member.sourceName) { complete = false; add("membership", "unknown", "Nested scene name and UUID disagree", identity); }
          else await walk(member.sourceUuid, member.isGroup, item.path, new Set([...ancestors, uuid]), effectiveEnabled);
        }
      }
    };
    await walk(sceneUuid, false, [], new Set(), true);
    add("membership", complete ? "pass" : "unknown", complete ? `Observed ${items.length} distinct scene item occurrences` : "Scene membership observation is incomplete");
    if (items.length === 0) add("scene-content", "fail", "Selected scene has no scene items");
    for (const required of options.requiredItems) {
      const matches = items.filter((item) => item.sourceName === required.sourceName
        && (required.sourceUuid === undefined || item.sourceUuid === required.sourceUuid)
        && (required.containerUuid === undefined || item.containerUuid === required.containerUuid)
        && (required.sceneItemId === undefined || item.sceneItemId === required.sceneItemId));
      if (matches.length !== 1) add("required-item", matches.length === 0 && complete ? "fail" : "unknown", matches.length === 0 ? `Required item ${required.sourceName} was not found` : `Required item ${required.sourceName} matches multiple occurrences`, undefined, required);
      else add("required-item", matches[0]!.effectiveEnabled === null ? "unknown" : matches[0]!.effectiveEnabled ? "pass" : "fail", matches[0]!.effectiveEnabled === null ? "Required item or ancestor enabled state is unavailable" : matches[0]!.effectiveEnabled ? "Required item and its ancestors are enabled" : "Required item or an ancestor is disabled", matches[0]);
    }
    if (options.includeScreenshot) {
      try { screenshot = screenshotResult(await request("GetSourceScreenshot", { sourceUuid: sceneUuid, imageFormat: "png", imageWidth: 960 })); add("screenshot", "pass", "Bounded scene screenshot included for separate visual inspection"); }
      catch (error) { add("screenshot", "unknown", `Screenshot unavailable: ${message(error)}`); }
    }
    // Recheck observed values, identities, and membership; no atomic snapshot is claimed.
    for (const container of containers) {
      const members = objects((await request(container.group ? "GetGroupSceneItemList" : "GetSceneItemList", { sceneUuid: container.uuid })).sceneItems);
      if (signature(membership(members)) !== container.before) invalidate();
    }
    for (const { item, before } of transforms) {
      const now = await request("GetSceneItemTransform", { sceneUuid: item.containerUuid, sceneItemId: item.sceneItemId });
      if (signature(boundedTransform(now.sceneItemTransform)) !== before) invalidate();
    }
    for (const { uuid, before } of mediaSettings) {
      if (signature((await request("GetInputSettings", { inputUuid: uuid })).inputSettings) !== before) invalidate();
    }
    if (signature(objects((await request("GetSceneList")).scenes)) !== signature(scenes)
      || signature(objects((await request("GetInputList")).inputs)) !== signature(inputs)
      || (await request("GetSceneCollectionList")).currentSceneCollectionName !== options.sceneCollection) invalidate();
    if (!client.getConnectionStatus().identified) invalidate();
    if (invalidated) throw new Error("OBS state changed while sampling");
    add("consistency", "pass", "Repeated observations agree; this is not an atomic snapshot");
  } catch (error) {
    complete = false;
    add("observation", "unknown", message(error));
  } finally {
    closed = true;
    client.off("disconnected", invalidate);
    client.off("identified", onIdentified);
    client.off("event", onEvent);
  }
  if (invalidated) {
    for (const check of checks) { check.status = "unknown"; check.message = `Invalidated observation: ${check.message}`; }
    screenshot = undefined;
    add("consistency", "unknown", "OBS changed during the sample; all observed conclusions require a fresh audit");
  }
  const status: Status = invalidated || !complete ? "unknown" : checks.some((check) => check.status === "fail") ? "fail" : checks.some((check) => check.status === "unknown") ? "unknown" : "pass";
  const report = {
    version: 1, scope: "structural", status, target, observedAt, completedAt: new Date().toISOString(), durationMs: Date.now() - started,
    requests, limits: LIMITS, items, checks, renderedContent: "unverified",
    limitations: ["Structural results do not establish recording readiness or rendered content correctness.", "No off-canvas, occlusion, legibility, capture-target, or composed-geometry verification is performed.", "OBS reads are sequential observations, not an atomic snapshot; unobserved transient changes remain possible."],
  };
  // Text and structuredContent both carry the report. Reserve room under the
  // transport frame limit for two copies and the bounded screenshot's base64.
  if (Buffer.byteLength(JSON.stringify(report), "utf8") > 512 * 1024) {
    report.status = "unknown";
    report.items = [];
    report.checks = [{ id: "report-size", status: "unknown", message: "Scene audit evidence exceeded the 512 KiB report limit; item and check evidence was omitted" }];
    screenshot = undefined;
  }
  return { content: [{ type: "text", text: JSON.stringify(report) }, ...(screenshot?.content.filter((block) => block.type === "image") ?? [])], structuredContent: report };
}

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool("obs-audit-scene", {
    title: "Audit Scene Structure",
    description: "Read a specifically selected scene and report structural pass, fail, or unknown checks with item occurrence identities. Optional screenshot supports separate visual review. A structural pass does not establish correct rendered content or recording readiness. Does not alter OBS.",
    inputSchema, outputSchema, annotations: READ_ONLY_TOOL,
  }, (options) => audit(client, options));
}
