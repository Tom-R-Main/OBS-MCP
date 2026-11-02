#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// Node/MCPB CLI are development archive tools only, never extension payload.
import {execFileSync} from 'node:child_process';
import {randomUUID} from 'node:crypto';
import {createRequire} from 'node:module';
import {cpSync,lstatSync,mkdirSync,mkdtempSync,renameSync,rmSync,writeFileSync} from 'node:fs';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {checkDependencies} from './check-dependencies.mjs';
import {nativeBundleFiles,nativeManifest} from './native-bundle.mjs';
const require=createRequire(import.meta.url),defaultRoot=fileURLToPath(new URL('../',import.meta.url));
export async function pack({root=defaultRoot,nativeBundle=true,offline=false,output=resolve(root,'dist/obs-studio-native.mcpb'),keepStaging=false}={}){
 if(!nativeBundle)throw new Error('The product package is native-only; Source is retained as development oracle');
 if(!['darwin','linux'].includes(process.platform))throw new Error('Native MCPB packaging is qualified only for macOS/Linux build hosts');
 if(process.env.CARGO_BUILD_TARGET)throw new Error('Host-native packaging only; cross-target artifacts require separate qualification');
 checkDependencies({root});
 // Qualification gates run before creating a staging owner or changing output.
 for(const script of['check-native-licenses.mjs','build-native.mjs','check-native-migration.mjs'])execFileSync(process.execPath,['scripts/'+script],{cwd:root,stdio:'inherit'});
 const manifest=nativeManifest(root),files=nativeBundleFiles(root),binary=resolve(root,'native',process.env.CARGO_TARGET_DIR||'target','release/obs-mcp-native'),info=lstatSync(binary);
 if(!info.isFile()||info.isSymbolicLink()||(info.mode&0o111)===0)throw new Error('Native bundle requires the freshly checked executable');
 const staging=mkdtempSync(resolve(root,'.pack-native-stage-'));let archive;
 try{
  for(const file of files){mkdirSync(dirname(resolve(staging,file)),{recursive:true});cpSync(resolve(root,file),resolve(staging,file));}
  cpSync(binary,resolve(staging,'native/obs-mcp-native'));
  writeFileSync(resolve(staging,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
  // Retain buildable corresponding Rust source/configuration that default MCPB
  // ignore rules otherwise hide. There is no npm install or Source build.
  writeFileSync(resolve(staging,'.mcpbignore'),'!native/\n!native/**\n!docs/\n!docs/**\n!licenses/\n!licenses/**\n!.agents/\n!.agents/**\n');
  mkdirSync(dirname(output),{recursive:true});archive=resolve(dirname(output),`.${randomUUID()}-obs-native.mcpb`);
  const cli=resolve(dirname(require.resolve('@anthropic-ai/mcpb')),'cli/cli.js');execFileSync(process.execPath,[cli,'pack','.',archive],{cwd:staging,stdio:'inherit'});
  renameSync(archive,output);archive=undefined;return{output,staging};
 }finally{if(archive)rmSync(archive,{force:true});if(!keepStaging)rmSync(staging,{recursive:true,force:true});}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url))await pack({offline:process.argv.includes('--offline')});
