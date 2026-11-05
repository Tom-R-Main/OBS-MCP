/* SPDX-License-Identifier: MIT */
import { appendFileSync, chmodSync, existsSync, mkdirSync, mkdtempSync, realpathSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { afterAll, expect } from 'vitest';
import { NATIVE_STATEFUL_DEFERRED_TOOL_NAMES } from './tool-count.js';

const owned: { directory: string; nodeMarker: string; mediaMarker: string }[] = [];
/** Explicit approved namespace plus a real trap for any forbidden JS handoff.
 * Fake media executables bind discovery capabilities only; these tests must not
 * invoke them. Source/SDK oracles retain their real development Node runtime. */
export function nativeDefaultFixture(root: string, allAdapters = false): Record<string, string> {
  const parent = join(root, 'native/target');
  mkdirSync(parent, { recursive: true });
  const directory = realpathSync(mkdtempSync(join(parent, 'default-test-')));
  const nodeMarker = join(directory, 'node-invoked');
  const mediaMarker = join(directory, 'media-invoked');
  const node = join(directory, 'node-trap');
  writeFileSync(node, '#!/bin/sh\nprintf invoked >> "$OBS_MCP_TEST_NODE_TRAP"\nexit 93\n');
  chmodSync(node, 0o755);
  owned.push({ directory, nodeMarker, mediaMarker });
  const env: Record<string, string> = {
    OBS_MCP_STATE_DIR: directory, OBS_MCP_COMPAT_NODE: node,
    OBS_MCP_COMPAT_ENTRY: join(directory, 'missing-sdk.js'), OBS_MCP_TEST_NODE_TRAP: nodeMarker,
    OBS_MCP_READ_OBS_CONFIG: 'false', OBS_MCP_NATIVE_RECORDING_ROOT: '',
  };
  if (allAdapters) {
    const recording = join(directory, 'recordings'); mkdirSync(recording, { mode: 0o700 });
    const media = join(directory, 'media-trap');
    writeFileSync(media, '#!/bin/sh\nprintf invoked >> "$OBS_MCP_TEST_MEDIA_TRAP"\nexit 94\n'); chmodSync(media, 0o755);
    Object.assign(env, { OBS_MCP_NATIVE_RECORDING_ROOT: recording, OBS_MCP_NATIVE_FFMPEG: media, OBS_MCP_NATIVE_FFPROBE: media, OBS_MCP_TEST_MEDIA_TRAP: mediaMarker });
  }
  return env;
}
/** Independent reviewed Production boundary, never inferred from native assets.
 * All-adapter callers retain Source order and full descriptors after projection. */
export function productionSourceTools<T extends { name: string }>(source: readonly T[]): T[] {
  const excluded = new Set<string>(NATIVE_STATEFUL_DEFERRED_TOOL_NAMES);
  return source.filter(tool => !excluded.has(tool.name));
}
afterAll(() => {
  const violations: string[] = [];
  for (const fixture of owned.splice(0)) {
    try {
      if (existsSync(fixture.nodeMarker)) violations.push('native product attempted a JS compatibility launch');
      if (existsSync(fixture.mediaMarker)) violations.push('discovery/read fixture unexpectedly executed media');
    } finally { rmSync(fixture.directory, { recursive: true, force: true }); }
  }
  expect(violations).toEqual([]);
});

/** Optional fixture-only Source receipt for an explicitly selected local path. */
export function recordDefaultOracle(kind: string, legacy: boolean, input: unknown, source: unknown, native?: unknown): void {
  if (process.env.NATIVE_DEFAULT_ORACLE_EVIDENCE) appendFileSync(process.env.NATIVE_DEFAULT_ORACLE_EVIDENCE, JSON.stringify({ kind, legacy, input, source, native }) + '\n');
}
