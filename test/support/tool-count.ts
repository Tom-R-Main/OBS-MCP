/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */

/** Number of tools the server registers with no tool filtering configured. */
export const TOOL_COUNT = 178;

/** Declared union across opt-in feature configurations; not one host inventory. */
export const NATIVE_STATEFUL_DECLARED_TOOL_COUNT = 166;

/** Qualified all-host-adapters plan: union166 minus the three Stream controls.
 * Stream callbacks may trigger OBS automatic recording and remain separate from
 * immutable Take/media ownership. Independent of runtime catalog metadata. */
export const NATIVE_STATEFUL_TOOL_COUNT = 163;

/** Deferred Source names with all host adapters enabled; independent of metadata. */
export const NATIVE_STATEFUL_DEFERRED_TOOL_NAMES = [
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
  "obs-batch",
  "obs-capture-window",
  "obs-apply-scene"
] as const;
