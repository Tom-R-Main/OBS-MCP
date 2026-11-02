/* SPDX-License-Identifier: GPL-2.0-only */
import type { McpServer, CallToolResult } from "@modelcontextprotocol/server";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { READ_ONLY_TOOL } from "./request-tool.js";
import { sameTransformValue } from "./snapshots.js";
import { Observation, ObservationError, canvas, failure, flatScene, object, signature, type Obj } from "./observation.js";

const name = z.string().min(1).max(256);
export const targetSchema = z.object({ sceneCollection: name, sceneName: name, canvasUuid: name.optional() });
const transform = z.object({
  positionX: z.number().finite(), positionY: z.number().finite(), scaleX: z.number().finite(), scaleY: z.number().finite(),
  rotation: z.number().finite(), alignment: z.union([z.literal(0), z.literal(1), z.literal(2), z.literal(4), z.literal(5), z.literal(6), z.literal(8), z.literal(9), z.literal(10)]),
  boundsType: z.enum(["OBS_BOUNDS_NONE", "OBS_BOUNDS_STRETCH", "OBS_BOUNDS_SCALE_INNER", "OBS_BOUNDS_SCALE_OUTER", "OBS_BOUNDS_SCALE_TO_WIDTH", "OBS_BOUNDS_SCALE_TO_HEIGHT", "OBS_BOUNDS_MAX_ONLY"]),
  boundsWidth: z.number().finite().nonnegative(), boundsHeight: z.number().finite().nonnegative(),
  boundsAlignment: z.number().int().nonnegative().max(10),
  cropLeft: z.number().int().nonnegative(), cropRight: z.number().int().nonnegative(), cropTop: z.number().int().nonnegative(), cropBottom: z.number().int().nonnegative(),
}).partial().strict();
const schema = targetSchema.extend({
  template: z.object({
    version: z.literal(1), name,
    canvas: z.object({ width: z.number().int().positive(), height: z.number().int().positive() }),
    roles: z.array(z.object({ role: name, inputKinds: z.array(name).min(1).max(32), transform, enabled: z.boolean(), locked: z.boolean(), index: z.number().int().nonnegative() })).min(1).max(64),
  }),
  bindings: z.array(z.object({ role: name, sourceName: name, sourceUuid: name, containerUuid: name, sceneItemId: z.number().int().nonnegative() })).max(64),
});

export function initialize(server: McpServer, client: OBSWebSocketClient): void {
  server.registerTool("obs-plan-scene-template", {
    title: "Plan Portable Scene Template",
    description: "Resolve portable logical roles against exact local collection/canvas/source/occurrence bindings. Flat layouts and literal transforms only. Returns proposed changes and explicit blockers for missing/duplicate bindings, renamed/replaced sources, incompatible kinds, nested scenes, and wrong canvas. Never sends mutation requests.",
    inputSchema: schema, annotations: READ_ONLY_TOOL,
  }, async (args, ctx): Promise<CallToolResult> => {
    let sample: Observation | undefined;
    try {
      await client.connect();
      sample = new Observation(client, ctx.mcpReq.signal);
      const before = await flatScene(sample, args);
      const blockers: Obj[] = [];
      const changes: Obj[] = [];
      const resolved: Obj[] = [];
      const block = (reason: string, message: string, role?: string) => blockers.push({ reason, message, ...(role ? { role } : {}), nextCheck: "Inspect obs-get-scene-items and provide an exact binding" });
      if (before.video.baseWidth !== args.template.canvas.width || before.video.baseHeight !== args.template.canvas.height) block("canvas_mismatch", "Template canvas dimensions differ from the selected canvas");
      const roleNames = args.template.roles.map((role) => role.role);
      if (new Set(roleNames).size !== roleNames.length) block("duplicate_role", "Template role names must be unique");
      const indices = args.template.roles.map((role) => role.index);
      if (new Set(indices).size !== indices.length || indices.some((index) => index >= before.items.length)) block("invalid_order", "Role indices must be unique and within this scene's item count");
      if (args.bindings.some((b) => !roleNames.includes(b.role))) block("unknown_role", "Binding names a role absent from the template");
      const occurrences = args.bindings.map((b) => `${b.containerUuid}:${b.sceneItemId}`);
      if (new Set(occurrences).size !== occurrences.length) block("duplicate_occurrence", "An occurrence cannot fill more than one role");
      const sceneIds = new Set((before.scenes.scenes as Obj[]).map((s) => s.sceneUuid));
      if (before.items.some((item) => item.isGroup || sceneIds.has(item.sourceUuid))) block("nested_layout", "Flat layouts cannot bind groups or nested scenes");
      for (const role of args.template.roles) {
        const bindings = args.bindings.filter((b) => b.role === role.role);
        if (bindings.length !== 1) { block(bindings.length ? "duplicate_binding" : "missing_binding", `Provide exactly one binding for ${role.role}`, role.role); continue; }
        const binding = bindings[0]!;
        if (binding.containerUuid !== before.selected.sceneUuid) { block("container_mismatch", "Binding container differs from selected scene", role.role); continue; }
        const matches = before.items.filter((item) => item.sceneItemId === binding.sceneItemId);
        const item = matches[0];
        if (!item || item.sourceUuid !== binding.sourceUuid) { block("target_replaced", "Bound occurrence or source UUID was removed/replaced", role.role); continue; }
        if (item.sourceName !== binding.sourceName) { block("target_renamed", `Source is now named ${String(item.sourceName)}; confirm a fresh binding`, role.role); continue; }
        const inputs = before.inputs.filter((input) => input.inputUuid === binding.sourceUuid && input.inputName === binding.sourceName);
        if (inputs.length !== 1 || !role.inputKinds.includes(String(inputs[0]!.inputKind))) { block("incompatible_kind", "Bound source kind is missing, ambiguous or incompatible", role.role); continue; }
        const currentTransform = (await sample.read("GetSceneItemTransform", { ...canvas(args), sceneUuid: before.selected.sceneUuid, sceneItemId: binding.sceneItemId })).sceneItemTransform;
        if (!object(currentTransform) || typeof item.sceneItemEnabled !== "boolean" || typeof item.sceneItemLocked !== "boolean" || !Number.isSafeInteger(item.sceneItemIndex)) { block("evidence_unavailable", "Transform, order, enabled or lock state is unavailable", role.role); continue; }
        resolved.push({ ...binding, inputKind: inputs[0]!.inputKind });
        for (const [field, desired] of Object.entries({ sceneItemEnabled: role.enabled, sceneItemLocked: role.locked, sceneItemIndex: role.index })) {
          if (item[field] !== desired) changes.push({ role: role.role, binding, field, before: item[field], after: desired });
        }
        for (const [field, desired] of Object.entries(role.transform)) {
          if (!(field in currentTransform)) block("evidence_unavailable", `Transform ${field} is unavailable`, role.role);
          else if (!sameTransformValue(field, currentTransform[field], desired)) changes.push({ role: role.role, binding, field: `transform.${field}`, before: currentTransform[field], after: desired });
        }
        const reread = (await sample.read("GetSceneItemTransform", { ...canvas(args), sceneUuid: before.selected.sceneUuid, sceneItemId: binding.sceneItemId })).sceneItemTransform;
        if (signature(currentTransform) !== signature(reread)) throw new ObservationError("observation_invalidated", "Transform changed during resolution");
      }
      const after = await flatScene(sample, args);
      if (signature(before) !== signature(after)) throw new ObservationError("observation_invalidated", "Selected inventory changed during resolution");
      sample.check();
      const report = { version: 1, status: blockers.length ? "blocked" : "resolved", applied: false, target: { sceneCollection: args.sceneCollection, sceneName: args.sceneName, ...canvas(args), sceneUuid: before.selected.sceneUuid }, generation: sample.generation, observedAt: sample.observedAt, completedAt: new Date().toISOString(), resolved, changes, blockers, renderedContent: "unverified", nextCheck: blockers.length ? "Resolve blockers and plan again" : "Inspect proposed changes; this tool does not apply layouts" };
      return { content: [{ type: "text", text: JSON.stringify(report) }], structuredContent: report };
    } catch (error) {
      const report = { version: 1, status: "unknown", applied: false, changes: [], blockers: [failure(error)] };
      return { content: [{ type: "text", text: JSON.stringify(report) }], structuredContent: report };
    } finally { sample?.close(); }
  });
}
