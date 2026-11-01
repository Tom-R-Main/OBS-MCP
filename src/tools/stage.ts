/* SPDX-License-Identifier: GPL-2.0-only */
import { normalizeSceneItem } from "./scene-identity.js";
import crypto from "node:crypto";
import type { CallToolResult, McpServer } from "@modelcontextprotocol/server";
import { z } from "zod";
import { OBSRequestError, type OBSWebSocketClient } from "../client.js";
import { targetSchema } from "./scene-template.js";
import { screenshotResult } from "./screenshot.js";
import { prepareOperation, updateOperation, OperationReceiptError, type OperationReceipt } from "./operation-receipts.js";
import { Observation, ObservationError, canvas, digest, failure, flatScene, identity, list, obj, signature, type Obj, type Target } from "./observation.js";

const TTL = 10 * 60_000;
type Staged = { target: Target; generation: number; expiresAt: number; subject: string; program: string; sceneUuid: string; sharedSources: string[] };
const staged = new WeakMap<OBSWebSocketClient, Map<string, Staged>>();
const busy = new WeakSet<OBSWebSocketClient>();
const annotations = { readOnlyHint: false, destructiveHint: true, idempotentHint: false, openWorldHint: false };
function result(report: Obj, image?: CallToolResult, error = false): CallToolResult {
  return { content: [{ type: "text", text: JSON.stringify(report) }, ...(image?.content.filter((block) => block.type === "image") ?? [])], structuredContent: report, ...(error ? { isError: true } : {}) };
}
function tokens(client: OBSWebSocketClient): Map<string, Staged> {
  let map = staged.get(client);
  if (!map) { map = new Map(); staged.set(client, map); }
  for (const [id, entry] of map) if (entry.expiresAt <= Date.now()) map.delete(id);
  return map;
}

/** Scene dependencies include nested membership, input settings, and filters.
 * Settings are hashed internally, never returned (browser URLs may contain secrets). */
async function subject(sample: Observation, target: Target) {
  const root = await flatScene(sample, target);
  const sources = new Set<string>();
  const visited = new Set<string>();
  const parts: unknown[] = [];
  let count = 0;
  const sceneIds = new Set(list(root.scenes.scenes).map((s) => s.sceneUuid));
  const walk = async (uuid: string, group: boolean, depth: number): Promise<void> => {
    if (depth > 8 || visited.has(uuid)) throw new ObservationError("evidence_unavailable", "Nested scene cycle, reuse or depth limit prevents staging");
    visited.add(uuid);
    const items = list((await sample.read(group ? "GetGroupSceneItemList" : "GetSceneItemList", { ...canvas(target), sceneUuid: uuid })).sceneItems, 128).map(normalizeSceneItem);
    if (new Set(items.map((item) => item.sceneItemId)).size !== items.length) throw new ObservationError("evidence_unavailable", "Duplicate occurrence identity");
    parts.push({ uuid, items });
    parts.push(await sample.read("GetSourceFilterList", { sourceUuid: uuid }));
    for (const item of items) {
      if (++count > 128) throw new ObservationError("observation_limit", "Staging dependency limit reached");
      const sourceUuid = identity(item.sourceUuid);
      identity(item.sourceName);
      if (!Number.isSafeInteger(item.sceneItemId) || typeof item.sceneItemEnabled !== "boolean" || typeof item.isGroup !== "boolean") throw new ObservationError("evidence_unavailable", "Incomplete occurrence state");
      parts.push(await sample.read("GetSceneItemTransform", { ...canvas(target), sceneUuid: uuid, sceneItemId: item.sceneItemId }));
      if (item.isGroup || sceneIds.has(sourceUuid)) await walk(sourceUuid, item.isGroup === true, depth + 1);
      else if (!sources.has(sourceUuid)) {
        const input = root.inputs.filter((input) => input.inputUuid === sourceUuid && input.inputName === item.sourceName);
        if (input.length !== 1) throw new ObservationError("target_changed", "Source inventory and scene identity disagree");
        sources.add(sourceUuid);
        parts.push(input[0], await sample.read("GetInputSettings", { inputUuid: sourceUuid }), await sample.read("GetSourceFilterList", { sourceUuid }));
        for (const type of ["GetInputMute", "GetInputVolume", "GetInputAudioTracks"]) {
          try { parts.push({ type, sourceUuid, state: await sample.read(type, { inputUuid: sourceUuid }) }); }
          catch (error) {
            sample.check();
            if (!(error instanceof OBSRequestError) || error.code !== 604) throw error;
            parts.push({ type, sourceUuid, capability: "unsupported" });
          }
        }
      }
    }
  };
  await walk(root.selected.sceneUuid, false, 0);
  return { sceneUuid: root.selected.sceneUuid, fingerprint: digest({ collection: root.collection, scene: root.selected, video: root.video, parts }), sources };
}
async function inspect(client: OBSWebSocketClient, target: Target, signal?: AbortSignal) {
  const sample = new Observation(client, signal);
  try {
    const studio = await sample.read("GetStudioModeEnabled");
    if (studio.studioModeEnabled !== true) throw new ObservationError("capability_unavailable", "Enable Studio Mode explicitly before staging; this workflow never toggles it");
    const before = await flatScene(sample, target);
    const programName = identity(before.scenes.currentProgramSceneName);
    const preview = await sample.read("GetCurrentPreviewScene", canvas(target));
    const previewName = identity(preview.currentPreviewSceneName);
    const sceneList = list(before.scenes.scenes);
    const programScene = sceneList.filter((s) => s.sceneName === programName && (before.scenes.currentProgramSceneUuid === undefined || s.sceneUuid === before.scenes.currentProgramSceneUuid));
    const previewScene = sceneList.filter((s) => s.sceneName === previewName && (preview.currentPreviewSceneUuid === undefined || s.sceneUuid === preview.currentPreviewSceneUuid));
    if (programScene.length !== 1 || previewScene.length !== 1) throw new ObservationError("evidence_unavailable", "Program/preview identities are missing or ambiguous");
    const intended = await subject(sample, target);
    const program = await subject(sample, { ...target, sceneName: programName });
    const after = await flatScene(sample, target);
    if (signature(before) !== signature(after) || signature(preview) !== signature(await sample.read("GetCurrentPreviewScene", canvas(target)))) throw new ObservationError("observation_invalidated", "Program or preview changed during inspection");
    if ((await sample.read("GetStudioModeEnabled")).studioModeEnabled !== true) throw new ObservationError("observation_invalidated", "Studio Mode changed during inspection");
    return { intended, program, preview: { sceneName: previewName, sceneUuid: identity(previewScene[0]!.sceneUuid) }, generation: sample.generation };
  } finally { sample.close(); }
}
async function exclusive(client: OBSWebSocketClient, run: () => Promise<CallToolResult>) {
  if (busy.has(client)) return result({ status: "blocked", reason: "operation_in_progress", nextCheck: "Wait for the current stage/take receipt" }, undefined, true);
  busy.add(client);
  try { return await run(); } finally { busy.delete(client); }
}
export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool("obs-stage-scene", {
    title: "Stage Scene in Preview",
    description: "Stage an exact existing scene in Studio Mode preview, include its screenshot, and issue a short-lived one-use stageId. Rechecks dependencies and program before/after staging. Does not edit inputs, filters, audio, or layout; shared inputs remain shared. Inspect the image before obs-take-scene.",
    inputSchema: targetSchema, annotations,
  }, (target, ctx) => exclusive(client, async () => {
    let dispatched = false;
    let accepted: boolean | null = null;
    let receipt: OperationReceipt | undefined;
    try {
      await client.connect();
      tokens(client).clear();
      const before = await inspect(client, target, ctx.mcpReq.signal);
      ctx.mcpReq.signal?.throwIfAborted();
      const stageId = crypto.randomUUID();
      receipt = prepareOperation(client, "stage-scene", { sceneCollection: target.sceneCollection, sceneUuid: before.intended.sceneUuid, stageId });
      if (before.preview.sceneUuid !== before.intended.sceneUuid) {
        receipt = updateOperation(receipt, { phase: "dispatching", requestDispatched: null });
        dispatched = true;
        await client.sendRequestForGeneration(before.generation, "SetCurrentPreviewScene", { ...canvas(target), sceneUuid: before.intended.sceneUuid });
        accepted = true;
        receipt = updateOperation(receipt, { phase: "accepted", requestDispatched: true, requestAccepted: true });
      }
      const image = screenshotResult(await client.sendRequest("GetSourceScreenshot", { ...canvas(target), sourceUuid: before.intended.sceneUuid, imageFormat: "png", imageWidth: 960 }));
      const after = await inspect(client, target, ctx.mcpReq.signal);
      if (before.generation !== after.generation || before.intended.fingerprint !== after.intended.fingerprint || before.program.fingerprint !== after.program.fingerprint || after.preview.sceneUuid !== before.intended.sceneUuid) throw new ObservationError("target_changed", "Subject or program changed while staging; inspect current preview before staging again");
      const sharedSources = [...after.intended.sources].filter((id) => after.program.sources.has(id));
      const entry: Staged = { target, generation: after.generation, expiresAt: Date.now() + TTL, subject: after.intended.fingerprint, program: after.program.fingerprint, sceneUuid: after.intended.sceneUuid, sharedSources };
      receipt = updateOperation(receipt, { phase: "observed", postconditionObserved: true });
      tokens(client).set(stageId, entry);
      return result({ status: "staged", operationId: receipt.operationId, stageId, expiresAt: new Date(entry.expiresAt).toISOString(), target: { ...target, sceneUuid: entry.sceneUuid }, generation: entry.generation, preview: after.preview, programSceneUuid: after.program.sceneUuid, sharedSources, requestDispatched: dispatched, requestAccepted: accepted, intendedSceneObservedInPreview: true, renderedComposition: "unverified", nextCheck: "Inspect the preview image, then call obs-take-scene with this stageId", limitations: ["Changing a shared input, filter or audio setting can also affect program.", "Live pixels can change without a structural edit; a screenshot is an observation, not a content lock."] }, image);
    } catch (error) {
      if (receipt) { try { receipt = updateOperation(receipt, { phase: dispatched ? "unknown" : "not-dispatched", requestAccepted: accepted }); } catch { /* The last durable intent remains authoritative. */ } }
      return result({ status: dispatched ? "unknown" : "blocked", ...(receipt ? { operationId: receipt.operationId } : {}), requestDispatched: dispatched, requestAccepted: accepted, ...failure(error), ...(error instanceof OperationReceiptError ? { reason: error.reason } : {}), nextCheck: "Read obs-observe and inspect preview before staging again" }, undefined, true);
    }
  }));
  server.registerTool("obs-take-scene", {
    title: "Take Staged Scene to Program",
    description: "Consume a stageId once. Recheck the staged subject, program, preview, connection and Studio Mode; dispatch one transition; separately report request acceptance and whether the intended scene is observed on program. Never blindly retry uncertain outcomes. Rendered content still requires inspection.",
    inputSchema: z.object({ stageId: z.uuid() }), annotations,
  }, ({ stageId }, ctx) => exclusive(client, async () => {
    const entry = tokens(client).get(stageId);
    // Consume before asynchronous work: failures require a fresh stage and inspection.
    tokens(client).delete(stageId);
    let dispatched = false;
    let accepted: boolean | null = null;
    let receipt: OperationReceipt | undefined;
    try {
      if (!entry) throw new ObservationError("stage_unavailable", "Stage expired, was already consumed, or belongs to another server; stage again");
      if (!client.isConnected() || client.getConnectionStatus().generation !== entry.generation) throw new ObservationError("observation_invalidated", "Connection changed since staging");
      const before = await inspect(client, entry.target, ctx.mcpReq.signal);
      if (entry.expiresAt <= Date.now() || before.generation !== entry.generation || before.intended.fingerprint !== entry.subject || before.program.fingerprint !== entry.program || before.preview.sceneUuid !== entry.sceneUuid) throw new ObservationError("target_changed", "Program, preview or staged dependencies changed; stage and inspect again");
      ctx.mcpReq.signal?.throwIfAborted();
      receipt = prepareOperation(client, "take-scene", { sceneCollection: entry.target.sceneCollection, sceneUuid: entry.sceneUuid, stageId });
      receipt = updateOperation(receipt, { phase: "dispatching", requestDispatched: null });
      dispatched = true;
      await client.sendRequestForGeneration(entry.generation, "TriggerStudioModeTransition", canvas(entry.target));
      accepted = true;
      receipt = updateOperation(receipt, { phase: "accepted", requestDispatched: true, requestAccepted: true });
      const deadline = Date.now() + 5_000;
      while (Date.now() < deadline) {
        ctx.mcpReq.signal?.throwIfAborted();
        if (!client.isConnected() || client.getConnectionStatus().generation !== entry.generation) throw new ObservationError("observation_invalidated", "Connection changed after transition dispatch");
        const collection = obj(await client.sendRequest("GetSceneCollectionList", undefined, 1000));
        const program = obj(await client.sendRequest("GetCurrentProgramScene", canvas(entry.target), 1000));
        if (collection.currentSceneCollectionName !== entry.target.sceneCollection) throw new ObservationError("target_changed", "Collection changed after transition dispatch");
        if (program.currentProgramSceneUuid === entry.sceneUuid && program.currentProgramSceneName === entry.target.sceneName) {
          const collectionAfter = obj(await client.sendRequest("GetSceneCollectionList", undefined, 1000));
          if (collectionAfter.currentSceneCollectionName !== entry.target.sceneCollection) throw new ObservationError("target_changed", "Collection changed during program verification");
          if (!client.isConnected() || client.getConnectionStatus().generation !== entry.generation) throw new ObservationError("observation_invalidated", "Connection changed during verification");
          receipt = updateOperation(receipt, { phase: "observed", postconditionObserved: true });
          return result({ status: "verified", operationId: receipt.operationId, requestDispatched: true, requestAccepted: true, intendedSceneObservedOnProgram: true, target: { ...entry.target, sceneUuid: entry.sceneUuid }, verifiedAt: new Date().toISOString(), renderedComposition: "unverified", nextCheck: "Inspect the program screenshot to assess the rendered composition" });
        }
        await new Promise((resolve) => setTimeout(resolve, 100));
      }
      throw new ObservationError("verification_timeout", "Intended scene was not observed on program within five seconds");
    } catch (error) {
      if (receipt) { try { receipt = updateOperation(receipt, { phase: dispatched ? "unknown" : "not-dispatched", requestAccepted: accepted }); } catch { /* Retain durable uncertainty when completion cannot be persisted. */ } }
      return result({ status: dispatched ? "unknown" : "blocked", ...(receipt ? { operationId: receipt.operationId } : {}), requestDispatched: dispatched, requestAccepted: accepted, intendedSceneObservedOnProgram: null, renderedComposition: "unverified", ...failure(error), ...(error instanceof OperationReceiptError ? { reason: error.reason } : {}), nextCheck: dispatched ? "Read obs-observe and inspect program before any further transition; do not replay this stageId" : "Stage and inspect again" }, undefined, true);
    }
  }));
}
