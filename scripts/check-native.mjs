#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
import { spawnSync } from 'node:child_process';
import { prepareNativeTestBinaries } from './native-probes.mjs';
import { existsSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
const root = fileURLToPath(new URL('../', import.meta.url));
const nativeRoot = fileURLToPath(new URL('../native/', import.meta.url));
const cache = fileURLToPath(new URL('../native/.cargo-home', import.meta.url));
const env = {...process.env, RUSTUP_AUTO_INSTALL:'0', ...(existsSync(cache)?{CARGO_HOME:cache}:{})};
const commands = [
  ['node',['scripts/check-native-licenses.mjs']],
  // Development Source oracle preparation is separate from native compilation.
  ['npm',['run','build']],
  ['npm',['run','native:build']],
  ['cargo',['fmt','--all','--','--check']],
  ['cargo',['clippy','--locked','--offline','--all-targets','--','-D','warnings']],
  ['cargo',['test','--locked','--offline']],
  ['cargo',['clippy','--locked','--offline','--all-targets','--all-features','--','-D','warnings']],
  ['cargo',['test','--locked','--offline','--all-features','--lib','--bins']],
  ['cargo',['test','--locked','--offline','--features','resolver-fixture','--test','resolver_owner_contract']],
  // Path dependencies are not workspace members: root tests do not execute
  // their unit controls. Select only the applicable platform TLS backend.
  ...['crates/resolver/Cargo.toml','crates/platform-tls/Cargo.toml'].flatMap(manifest => {
    const features = manifest.includes('platform-tls')
      ? ['--features',process.platform === 'linux' ? 'gnutls-probe' : 'plain-fixture']
      : ['--all-features'];
    return [
      ['cargo',['fmt','--manifest-path',manifest,'--','--check']],
      ['cargo',['clippy','--manifest-path',manifest,'--locked','--offline','--all-targets',...features,'--','-D','warnings']],
      ['cargo',['test','--manifest-path',manifest,'--locked','--offline',...features]],
    ];
  }),
  ['npm',['run','typecheck:test']],
  ['npm',['run','native:test']],
  ['node',['scripts/check-native-migration.mjs']],
];
let binaries;
try {
  for (const [command,args] of commands) {
    const publicTests=command==='npm' && args[1]==='native:test';
    if(publicTests){
      binaries=prepareNativeTestBinaries(nativeRoot,env);
      // Development gate evidence contains hashes only, never configuration.
      console.log(JSON.stringify({nativePublicBinaries:binaries.hashes}));
    }
    const result=spawnSync(command,args,{cwd:command==='cargo'?nativeRoot:root,env:publicTests?binaries.env:env,stdio:'inherit'});
    if(result.error)throw result.error;
    if(result.status!==0)throw Object.assign(new Error('Native check command failed'),{exitCode:result.status??1});
  }
}catch(error){
  console.error(error.message);
  process.exitCode=error.exitCode??1;
}finally{
  binaries?.close();
}
