#!/usr/bin/env node
// SPDX-License-Identifier: MIT
// Optional development driver. Direct Cargo builds require no Node installation.
import {spawnSync} from 'node:child_process';
import {createHash} from 'node:crypto';
import {resolve,join,isAbsolute} from 'node:path';
import {nativeBuildFeatures,nativeBuildSource,nativeArtifactOwner,nativeArtifactCleanup} from './native-bundle.mjs';
import {existsSync,lstatSync,readFileSync,readdirSync,utimesSync,writeFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
const root=fileURLToPath(new URL('../native/',import.meta.url));
const project=resolve(root,'..'),features=nativeBuildFeatures(process.env.OBS_MCP_NATIVE_BUILD_FEATURES);
const options=process.argv.slice(2),packagePin=options.length===2&&options[0]==='--package-artifact'?options[1]:undefined;
if(options.length&&!packagePin)throw new Error('Native build options are selected only through OBS_MCP_NATIVE_BUILD_FEATURES');
if(packagePin){const info=lstatSync(packagePin);if(!isAbsolute(packagePin)||resolve(packagePin)!==packagePin||!packagePin.startsWith(join(project,'native/target/obs-native-artifact-'))||!info.isDirectory()||info.isSymbolicLink()||(info.mode&0o077)!==0||readdirSync(packagePin).length)throw new Error('Package artifact pin must be an empty private caller-owned directory');}
if(features.length&&!['darwin','linux'].includes(process.platform))throw new Error('Native TLS evaluation builds require a macOS/Linux host');
// Checked-in native assets are production inputs. Source exporters are only
// explicit development oracles, never a dependency of compiling this binary.
for(const file of['assets/tools.json','assets/prompts.json'])JSON.parse(readFileSync(new URL('../native/'+file,import.meta.url),'utf8'));
if(process.env.CARGO_BUILD_TARGET)throw new Error('Host-native builds only; cross-target packages require separate qualification');
// Shared target pools can otherwise reuse another source root's later mtimes.
// Refresh mutable compile inputs without changing bytes or cleaning any cache.
const refresh=path=>{const stat=lstatSync(path);if(stat.isSymbolicLink())throw new Error('Native build inputs must not be symbolic links');if(stat.isDirectory()){for(const name of readdirSync(path))if(!['target','.cargo-home','.git','node_modules','dist','__pycache__','.venv'].includes(name))refresh(path+'/'+name);}else if(stat.isFile())utimesSync(path,new Date(),new Date());};
for(const directory of['src','assets','platform','crates'])if(existsSync(root+directory))refresh(root+directory);
for(const file of['Cargo.toml','Cargo.lock','rust-toolchain.toml'])refresh(root+file);
if(existsSync(root+'build.rs'))refresh(root+'build.rs');
refresh(fileURLToPath(new URL('../docs/protocol.json',import.meta.url)));
const cache=root+'.cargo-home';const env={...process.env,RUSTUP_AUTO_INSTALL:'0',...(existsSync(cache)?{CARGO_HOME:cache}:{})};
const isolated=Boolean(packagePin||features.length),before=isolated?nativeBuildSource(project):undefined;
const compiler=isolated?nativeArtifactOwner(project,'obs-native-build-'):undefined;
let pin,retained=false,compilerCleaned=false;
try{
 if(compiler){env.CARGO_TARGET_DIR=join(compiler.path,'target');env.CARGO_BUILD_BUILD_DIR=join(compiler.path,'build');}
 const args=['build','--locked','--offline','--release','--bin','obs-mcp-native',...(features.length?['--features',features[0]]:[]),...(isolated?['--target-dir',env.CARGO_TARGET_DIR,'--message-format=json']:[])];
 const result=spawnSync('cargo',args,{cwd:root,env,...(isolated?{encoding:'utf8',stdio:['ignore','pipe','inherit'],maxBuffer:64*1024*1024}:{stdio:'inherit'})});
 if(result.error)throw result.error;if(result.status!==0){if(!isolated)process.exit(result.status??1);throw new Error(`Native Cargo build failed (${result.status??1})`);}
 if(isolated){
  let messages;try{messages=result.stdout.split(/\r?\n/).filter(line=>line.trim()).map(line=>JSON.parse(line));}catch{throw new Error('Cargo did not return valid feature-build artifact JSON');}
  const artifacts=messages.filter(row=>row.reason==='compiler-artifact'&&row.manifest_path===join(project,'native/Cargo.toml')&&row.target?.name==='obs-mcp-native'&&JSON.stringify(row.target.kind)===JSON.stringify(['bin'])&&row.target.src_path===join(project,'native/src/main.rs')&&row.profile?.test===false&&typeof row.executable==='string');
  if(artifacts.length!==1||JSON.stringify(artifacts[0].features)!==JSON.stringify(features))throw new Error('Expected exactly one matching reviewed feature-build executable artifact');
  const artifact=artifacts[0],expected=join(compiler.path,'target/release/obs-mcp-native');
  if(artifact.executable!==expected)throw new Error('Feature-build artifact must use the selected host release output; cross-target configuration is unavailable');
  const info=lstatSync(artifact.executable);
  if(!isAbsolute(artifact.executable)||!info.isFile()||info.isSymbolicLink()||(info.mode&0o111)===0)throw new Error('Feature-build executable must be a regular executable file');
  if(nativeBuildSource(project).sha256!==before.sha256)throw new Error('Native sources changed during feature build');
  const bytes=readFileSync(artifact.executable);
  pin=packagePin?undefined:nativeArtifactOwner(project);
  const executable=join(packagePin??pin.path,'obs-mcp-native');
  writeFileSync(executable,bytes,{flag:'wx',mode:0o500});
  const receipt={schemaVersion:1,host:{platform:process.platform,arch:process.arch},features,source:before,artifact:{manifest:artifact.manifest_path,source:artifact.target.src_path,executable,sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length}};
  // Compiler custody ends only after the immutable copy has been completed.
  nativeArtifactCleanup(compiler);compilerCleaned=true;
  console.log(JSON.stringify(receipt));retained=true;
 }
}finally{
 if(compiler&&!compilerCleaned)nativeArtifactCleanup(compiler);
 if(pin&&!retained)nativeArtifactCleanup(pin);
}
