#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
import { spawnSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../', import.meta.url));
const nativeRoot = fileURLToPath(new URL('../native/', import.meta.url));
const cache = fileURLToPath(new URL('../native/.cargo-home', import.meta.url));
const env = {...process.env, RUSTUP_AUTO_INSTALL:'0', ...(existsSync(cache)?{CARGO_HOME:cache}:{})};
const commands = [
  ['node',['scripts/check-native-licenses.mjs']],
  ['npm',['run','native:build']],
  ['cargo',['fmt','--all','--','--check']],
  ['cargo',['clippy','--locked','--offline','--all-targets','--','-D','warnings']],
  ['cargo',['test','--locked','--offline']],
  ['npm',['run','typecheck:test']],
  ['npm',['run','native:test']],
  ['node',['scripts/check-native-migration.mjs']],
];
for (const [command,args] of commands) {
  const result=spawnSync(command,args,{cwd:command==='cargo'?nativeRoot:root,env,stdio:'inherit'});
  if(result.error)throw result.error;
  if(result.status!==0)process.exit(result.status??1);
}
