#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// Optional development driver. Direct Cargo builds require no Node installation.
import {spawnSync} from 'node:child_process';
import {existsSync,lstatSync,readFileSync,readdirSync,utimesSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
const root=fileURLToPath(new URL('../native/',import.meta.url));
// Checked-in native assets are production inputs. Source exporters are only
// explicit development oracles, never a dependency of compiling this binary.
for(const file of['assets/tools.json','assets/prompts.json'])JSON.parse(readFileSync(new URL('../native/'+file,import.meta.url),'utf8'));
if(process.env.CARGO_BUILD_TARGET)throw new Error('Host-native builds only; cross-target packages require separate qualification');
// Shared target pools can otherwise reuse another source root's later mtimes.
// Refresh mutable compile inputs without changing bytes or cleaning any cache.
const refresh=path=>{const stat=lstatSync(path);if(stat.isSymbolicLink())throw new Error('Native build inputs must not be symbolic links');if(stat.isDirectory()){for(const name of readdirSync(path))if(!['target','.cargo-home','.git','node_modules','dist','__pycache__','.venv'].includes(name))refresh(path+'/'+name);}else if(stat.isFile())utimesSync(path,new Date(),new Date());};
for(const directory of['src','assets','platform','crates'])if(existsSync(root+directory))refresh(root+directory);
for(const file of['Cargo.toml','Cargo.lock','rust-toolchain.toml'])refresh(root+file);
refresh(fileURLToPath(new URL('../docs/protocol.json',import.meta.url)));
const cache=root+'.cargo-home';const env={...process.env,RUSTUP_AUTO_INSTALL:'0',...(existsSync(cache)?{CARGO_HOME:cache}:{})};
const result=spawnSync('cargo',['build','--locked','--offline','--release','--bin','obs-mcp-native'],{cwd:root,env,stdio:'inherit'});
if(result.error)throw result.error;if(result.status!==0)process.exit(result.status??1);
