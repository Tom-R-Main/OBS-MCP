/* SPDX-License-Identifier: GPL-2.0-only */
/** OBS 5.x emits isGroup:null for input sources. Its explicit source type and
 * input kind establish that this is an input, while null on a scene remains unknown. */
export function normalizeSceneItem(item: Record<string, unknown>): Record<string, unknown> {
  return item.isGroup === null && item.sourceType === "OBS_SOURCE_TYPE_INPUT" && typeof item.inputKind === "string" && item.inputKind.length > 0
    ? { ...item, isGroup: false } : item;
}
