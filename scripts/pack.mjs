#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// Node/MCPB CLI are development archive tools only, never extension payload.
import {execFileSync} from 'node:child_process';
import {createHash,randomUUID} from 'node:crypto';
import {createRequire} from 'node:module';
import {cpSync,lstatSync,mkdirSync,mkdtempSync,renameSync,rmSync,writeFileSync,readFileSync} from 'node:fs';
import {dirname,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {checkDependencies} from './check-dependencies.mjs';
import {nativeBundleFiles,nativeManifest,nativeBuildFeatures,nativeBuildSource} from './native-bundle.mjs';
const require=createRequire(import.meta.url),defaultRoot=fileURLToPath(new URL('../',import.meta.url));
export async function pack({root=defaultRoot,nativeBundle=true,offline=false,output,keepStaging=false,nativeFeatures}={}){
 const inherited=process.env.OBS_MCP_NATIVE_BUILD_FEATURES;
 if(nativeFeatures!==undefined&&inherited!==undefined&&nativeFeatures!==inherited)throw new Error('Native package feature selection disagrees with build configuration');
 const features=nativeBuildFeatures(nativeFeatures===undefined?inherited:nativeFeatures),selected=features.join(',');
 output??=resolve(root,features.length?'dist/obs-studio-native-tls-evaluation.mcpb':'dist/obs-studio-native.mcpb');
 if(features.length&&resolve(output)===resolve(root,'dist/obs-studio-native.mcpb')||!features.length&&resolve(output)===resolve(root,'dist/obs-studio-native-tls-evaluation.mcpb'))throw new Error('Native package output is reserved for a different feature selection');
 if(!nativeBundle)throw new Error('The product package is native-only; Source is retained as development oracle');
 if(!['darwin','linux'].includes(process.platform))throw new Error('Native MCPB packaging is qualified only for macOS/Linux build hosts');
 if(process.env.CARGO_BUILD_TARGET)throw new Error('Host-native packaging only; cross-target artifacts require separate qualification');
 checkDependencies({root});
 // Qualification gates run before creating a staging owner or changing output.
 let receipt;
 const env={...process.env,OBS_MCP_NATIVE_BUILD_FEATURES:selected};
 for(const script of['check-native-licenses.mjs','build-native.mjs','check-native-migration.mjs']){
  const capture=features.length&&script==='build-native.mjs';
  const result=execFileSync(process.execPath,['scripts/'+script],{cwd:root,env,...(capture?{encoding:'utf8',stdio:['ignore','pipe','inherit'],maxBuffer:64*1024*1024}:{stdio:'inherit'})});
  if(capture){try{receipt=JSON.parse(result);}catch{throw new Error('Native feature build did not return a fresh identity receipt');}}
 }
 const expected=resolve(root,'native',process.env.CARGO_TARGET_DIR||'target','release/obs-mcp-native'),source=features.length?nativeBuildSource(root):undefined;
 if(features.length&&(receipt?.schemaVersion!==1||receipt.host?.platform!==process.platform||receipt.host?.arch!==process.arch||JSON.stringify(receipt.features)!==JSON.stringify(features)||receipt.artifact?.manifest!==resolve(root,'native/Cargo.toml')||receipt.artifact?.source!==resolve(root,'native/src/main.rs')||receipt.artifact?.executable!==expected||receipt.source?.sha256!==source.sha256||JSON.stringify(receipt.source.files)!==JSON.stringify(source.files)))throw new Error('Native feature build receipt does not match this host, source, executable and feature selection');
 const manifest=nativeManifest(root,process.platform,process.arch,selected),files=nativeBundleFiles(root),binary=features.length?receipt.artifact.executable:expected,info=lstatSync(binary);
 if(!info.isFile()||info.isSymbolicLink()||(info.mode&0o111)===0)throw new Error('Native bundle requires the freshly checked executable');
 const binaryBytes=readFileSync(binary),binaryHash=createHash('sha256').update(binaryBytes).digest('hex');
 if(features.length&&(binaryHash!==receipt.artifact.sha256||binaryBytes.length!==receipt.artifact.bytes))throw new Error('Native feature-build executable bytes changed before staging');
 const staging=mkdtempSync(resolve(root,'.pack-native-stage-'));let archive;
 try{
  for(const file of files){mkdirSync(dirname(resolve(staging,file)),{recursive:true});cpSync(resolve(root,file),resolve(staging,file));}
  writeFileSync(resolve(staging,'native/obs-mcp-native'),binaryBytes,{mode:info.mode&0o777});
  if(features.length){
   if(nativeBuildSource(staging).sha256!==receipt.source.sha256)throw new Error('Staged native source does not match feature-build identity');
   writeFileSync(resolve(staging,'native/build.json'),JSON.stringify({schemaVersion:1,host:receipt.host,features,source:receipt.source,executable:{path:'native/obs-mcp-native',sha256:binaryHash,bytes:binaryBytes.length},qualification:'evaluation-only; artifact inclusion does not establish production TLS acceptance'},null,2)+'\n');
  }
  writeFileSync(resolve(staging,'manifest.json'),JSON.stringify(manifest,null,2)+'\n');
  // Retain buildable corresponding Rust source/configuration that default MCPB
  // ignore rules otherwise hide. There is no npm install or Source build.
  writeFileSync(resolve(staging,'.mcpbignore'),'!native/\n!native/**\n!docs/\n!docs/**\n!licenses/\n!licenses/**\n!.agents/\n!.agents/**\n');
  mkdirSync(dirname(output),{recursive:true});archive=resolve(dirname(output),`.${randomUUID()}-obs-native.mcpb`);
  const cli=resolve(dirname(require.resolve('@anthropic-ai/mcpb')),'cli/cli.js');execFileSync(process.execPath,[cli,'pack','.',archive],{cwd:staging,stdio:'inherit'});
  renameSync(archive,output);archive=undefined;return{output,staging};
 }finally{if(archive)rmSync(archive,{force:true});if(!keepStaging)rmSync(staging,{recursive:true,force:true});}
}
if(process.argv[1]&&resolve(process.argv[1])===fileURLToPath(import.meta.url)){if(process.argv.slice(2).some(arg=>arg!=='--offline'))throw new Error('Unsupported native package options; select reviewed features through OBS_MCP_NATIVE_BUILD_FEATURES');await pack({offline:process.argv.includes('--offline')});}
