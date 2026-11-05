/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: MIT
 */

/** Number of tools the server registers with no tool filtering configured. */
export const TOOL_COUNT = 179;

/** Declared union across opt-in feature configurations; not one host inventory. */
export const NATIVE_STATEFUL_DECLARED_TOOL_COUNT = 179;

/** Production all-host inventory: union179 minus fourteen advanced/Stream names.
 * Stream callbacks may trigger OBS automatic recording and remain separate from
 * immutable Take/media ownership. Independent of runtime catalog metadata. */
export const NATIVE_STATEFUL_TOOL_COUNT = 165;

/** Deferred Source names with all host adapters enabled; independent of metadata. */
export const NATIVE_STATEFUL_DEFERRED_TOOL_NAMES = [
  "obs-press-input-property-button",
  "obs-start-stream",
  "obs-stop-stream",
  "obs-toggle-stream",
  "obs-call-vendor-request",
  "obs-trigger-hotkey-by-name",
  "obs-trigger-hotkey-by-key-sequence",
  "obs-toggle-output",
  "obs-start-output",
  "obs-stop-output",
  "obs-toggle-record",
  "obs-start-record",
  "obs-call-request",
  "obs-batch"
] as const;

/** Advanced profile excludes sixteen guarded workflows from the native union. */
export const NATIVE_ADVANCED_TOOL_COUNT = 163;
