/* SPDX-License-Identifier: GPL-2.0-only */
import type { McpServer, CallToolResult } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { READ_ONLY_TOOL } from "./request-tool.js";
import { activeLease } from "./lease.js";
import { activeTake } from "./take-monitor.js";
import { currentTake } from "./takes.js";
import { recoverySummary } from "./demo.js";
import { operationSummary } from "./operation-receipts.js";
import { Observation, ObservationError, failure, identity, list, signature, type Obj } from "./observation.js";

/** The tool and resource share this implementation, including its consistency envelope. */
export async function observe(client: OBSWebSocketClient, canvasUuid?: string, signal?: AbortSignal): Promise<Obj> {
  const started = Date.now();
  let sample: Observation | undefined;
  try {
    await client.connect();
    sample = new Observation(client, signal);
    if (canvasUuid !== undefined) throw new ObservationError("capability_unavailable", "obs-observe currently requires the default canvas; the video-settings contract is not canvas-scoped. Use obs-audit-scene for a named canvas structural audit");
    const scope = canvasUuid === undefined ? {} : { canvasUuid };
    const missing: Obj[] = [];
    const optional = async (type: string, data?: Obj): Promise<Obj | null> => {
      try { return await sample!.read(type, data); }
      catch (error) {
        sample!.check();
        if (error instanceof ObservationError && error.reason === "observation_limit") throw error;
        missing.push({ request: type, ...failure(error) });
        return null;
      }
    };
    const local = () => {
      const take = activeTake(client)?.summary();
      const owned = currentTake(client);
      return { control: activeLease(client) ?? null, activeTake: take ? { id: owned?.id ?? null, running: take.running, startedAt: take.startedAt, expectSilent: take.expectSilent, locked: owned?.locked ?? null, maxDurationSeconds: owned?.maxDurationSeconds ?? null, ownershipLost: owned?.ownershipLost ?? null } : null, recovery: recoverySummary(client), operationReceipts: operationSummary(client) };
    };
    const beforeLocal = local();
    const read = async () => {
      const collection = identity((await sample!.read("GetSceneCollectionList")).currentSceneCollectionName);
      const scenes = await sample!.read("GetSceneList", scope);
      const inventory = list(scenes.scenes).map((s) => ({ sceneName: identity(s.sceneName), sceneUuid: identity(s.sceneUuid) }));
      const resolve = (name: unknown, uuid: unknown) => {
        const matches = inventory.filter((s) => s.sceneName === name && (uuid === undefined || s.sceneUuid === uuid));
        if (matches.length !== 1) throw new ObservationError("evidence_unavailable", "Scene identity is missing or ambiguous");
        return matches[0]!;
      };
      const program = resolve(scenes.currentProgramSceneName, scenes.currentProgramSceneUuid);
      const studio = await optional("GetStudioModeEnabled");
      let preview = null;
      if (studio?.studioModeEnabled === true) {
        const response = await optional("GetCurrentPreviewScene", scope);
        if (response) preview = resolve(response.currentPreviewSceneName, response.currentPreviewSceneUuid);
      } else if (studio?.studioModeEnabled !== false) missing.push({ request: "GetStudioModeEnabled", reason: "evidence_unavailable", nextCheck: "Inspect Studio Mode" });
      const video = await sample!.read("GetVideoSettings", scope);
      if (![video.baseWidth, video.baseHeight].every((v) => Number.isSafeInteger(v) && Number(v) > 0)) throw new ObservationError("evidence_unavailable", "Canvas dimensions are unavailable");
      const outputs: Obj = {};
      for (const [name, type] of [["record", "GetRecordStatus"], ["stream", "GetStreamStatus"], ["replayBuffer", "GetReplayBufferStatus"], ["virtualCamera", "GetVirtualCamStatus"]]) {
        const response = await optional(type!);
        outputs[name!] = response && typeof response.outputActive === "boolean"
          ? { outputActive: response.outputActive, ...(typeof response.outputPaused === "boolean" ? { outputPaused: response.outputPaused } : {}), ...(typeof response.outputReconnecting === "boolean" ? { outputReconnecting: response.outputReconnecting } : {}) }
          : null;
        if (response && typeof response.outputActive !== "boolean") missing.push({ request: type, reason: "evidence_unavailable", nextCheck: `Inspect ${type}` });
      }
      const other = await optional("GetOutputList");
      const namedOutputs: Obj[] = [];
      if (other) for (const entry of list(other.outputs, 32)) {
        const outputName = identity(entry.outputName);
        const response = await optional("GetOutputStatus", { outputName });
        namedOutputs.push({ outputName, outputActive: typeof response?.outputActive === "boolean" ? response.outputActive : null, outputReconnecting: typeof response?.outputReconnecting === "boolean" ? response.outputReconnecting : null });
        if (typeof response?.outputActive !== "boolean" || typeof response?.outputReconnecting !== "boolean") missing.push({ request: "GetOutputStatus", outputName, reason: "evidence_unavailable", nextCheck: "Inspect this output before changing production" });
      }
      return { collection, canvas: { selection: "default", ...scope, baseWidth: video.baseWidth, baseHeight: video.baseHeight }, program, preview, studioModeEnabled: studio?.studioModeEnabled ?? null, outputs, namedOutputs };
    };
    const before = await read();
    const after = await read();
    const afterLocal = local();
    if (signature(before) !== signature(after) || signature(beforeLocal) !== signature(afterLocal)) throw new ObservationError("observation_invalidated", "Relevant production state changed within the observation interval");
    sample.check();
    if (afterLocal.recovery.status === "unknown") missing.push({ component: "recovery", reason: "evidence_unavailable", message: afterLocal.recovery.reason, nextCheck: "Inspect local recovery journals before preparing more changes" });
    const uniqueMissing = [...new Map(missing.map((item) => [signature(item), item])).values()];
    const record = after.outputs.record as Obj | null;
    const summary = `Recording is ${record === null ? "unknown" : record.outputActive ? "active" : "idle"}. “${after.program.sceneName}” is on program; ${after.preview ? `“${after.preview.sceneName}” is in preview` : "no preview was observed"}.`;
    return {
      version: 1, status: uniqueMissing.length ? "incomplete" : "observed", summary,
      connection: client.getConnectionStatus(), generation: sample.generation,
      ...after, ...afterLocal,
      // Preserve the status resource's original fields for older consumers.
      record: after.outputs.record, stream: after.outputs.stream, currentProgramSceneName: after.program.sceneName,
      observedAt: sample.observedAt, completedAt: new Date().toISOString(), durationMs: Date.now() - started, requests: sample.requests,
      missingEvidence: uniqueMissing,
      availableActions: ["obs-audit-scene", "obs-plan-scene-template", ...(after.studioModeEnabled === true ? ["obs-stage-scene"] : [])],
      limitations: ["Repeated observations agree; OBS does not provide an atomic snapshot.", "Rendered composition and recording readiness require separate checks.", "Control lease describes cooperating MCP clients; the OBS operator can still edit."],
    };
  } catch (error) {
    return { version: 1, status: "unknown", ...failure(error), connection: client.getConnectionStatus(), operationReceipts: operationSummary(client), observedAt: new Date(started).toISOString(), completedAt: new Date().toISOString(), durationMs: Date.now() - started, requests: sample?.requests ?? 0, availableActions: ["obs-observe"] };
  } finally { sample?.close(); }
}
export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool("obs-observe", {
    title: "Observe Production",
    description: "Bounded read-only production observation: exact collection/canvas/program/preview identities, outputs, control, take and recovery state. Rejects inconsistent reads and reports missing evidence with next checks. Same contract as obs://status; no rendered-content or readiness claim.",
    inputSchema: z.object({ canvasUuid: z.string().min(1).max(256).optional() }), annotations: READ_ONLY_TOOL,
  }, async ({ canvasUuid }, ctx): Promise<CallToolResult> => {
    const report = await observe(client, canvasUuid, ctx.mcpReq.signal);
    return { content: [{ type: "text", text: JSON.stringify(report) }], structuredContent: report };
  });
}
