#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../', import.meta.url));
const nativeRoot = fileURLToPath(new URL('../native/', import.meta.url));
const localCache = fileURLToPath(new URL('../native/.cargo-home', import.meta.url));
const env = { ...process.env, RUSTUP_AUTO_INSTALL: '0', ...(existsSync(localCache) ? { CARGO_HOME: localCache } : {}) };
const run = (cmd, args, environment = process.env, cwd = root) => {
  const result = spawnSync(cmd, args, {cwd,env:environment,stdio:'inherit'});
  if (result.error) throw result.error;
  if (result.status !== 0) process.exit(result.status ?? 1);
};
run(process.execPath, ['scripts/export-native-contract.mjs']);
run(process.execPath, ['scripts/export-native-prompts.mjs']);
// rustup selects the nearest toolchain from cwd, not --manifest-path.
run('cargo', ['build','--locked','--offline','--release'], env, nativeRoot);
