/* SPDX-License-Identifier: GPL-2.0-only */
import { expect } from 'vitest';
import { TOOL_COUNT, NATIVE_STATEFUL_TOOL_COUNT, NATIVE_STATEFUL_DEFERRED_TOOL_NAMES } from './tool-count.js';

/** Expected inventory comes from the Source registry and reviewed migration
 * plan, independently of the native catalog. Use only with all host adapters. */
export function expectNativeInventory(
  actual: readonly { name: string }[],
  source: readonly { name: string }[],
): void {
  const deferred = new Set<string>(NATIVE_STATEFUL_DEFERRED_TOOL_NAMES);
  expect(deferred.size).toBe(TOOL_COUNT - NATIVE_STATEFUL_TOOL_COUNT);
  expect(source).toHaveLength(TOOL_COUNT);
  expect(actual).toHaveLength(NATIVE_STATEFUL_TOOL_COUNT);
  expect(actual.map(tool => tool.name).sort()).toEqual(
    source.filter(tool => !deferred.has(tool.name)).map(tool => tool.name).sort(),
  );
  for (const tool of actual) {
    expect(tool).toEqual(source.find(expected => expected.name === tool.name));
  }
}
