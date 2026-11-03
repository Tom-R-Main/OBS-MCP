/* SPDX-License-Identifier: GPL-2.0-only */
import {createHash} from 'node:crypto';
import {chmodSync,copyFileSync,existsSync,lstatSync,mkdirSync,mkdtempSync,readFileSync,readdirSync,renameSync,rmSync,writeFileSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath,pathToFileURL} from 'node:url';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {pack} from '../../scripts/pack.mjs';
import {nativeBuildFeatures,nativeBuildSource,nativeBundleFiles,nativeManifest,nativeArtifactOwner,nativeArtifactCleanup} from '../../scripts/native-bundle.mjs';
const source=fileURLToPath(new URL('../../',import.meta.url)),roots:string[]=[];
afterEach(()=>{vi.unstubAllEnvs();for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
function fixture(mode='valid'){
 const parent=join(source,'native/target');mkdirSync(parent,{recursive:true});const root=mkdtempSync(join(parent,'tls-package-'));roots.push(root);
 for(const file of [...nativeBundleFiles(source),'manifest.json','package.json','package-lock.json','scripts/build-native.mjs','scripts/native-bundle.mjs']){const destination=join(root,file);mkdirSync(dirname(destination),{recursive:true});copyFileSync(join(source,file),destination);}
 for(const directory of ['bin','dist','native/.cargo-home','native/target/release'])mkdirSync(join(root,directory),{recursive:true});
 const binary=join(root,'native/target/release/obs-mcp-native');writeFileSync(binary,'fixture TLS executable bytes');chmodSync(binary,0o755);
 const plain=join(root,'dist/obs-studio-native.mcpb');writeFileSync(plain,'preceding qualified plain artifact');
 const trace=join(root,'trace.jsonl');for(const name of ['check-native-licenses.mjs','check-native-migration.mjs'])writeFileSync(join(root,'scripts',name),`import {appendFileSync,writeFileSync} from 'node:fs';appendFileSync(${JSON.stringify(trace)},${JSON.stringify(name+'\n')});${name==='check-native-migration.mjs'?`writeFileSync(${JSON.stringify(binary)},'opposite feature shared bytes after Cargo completion');`:''}`);
 const cargo=join(root,'bin/cargo');writeFileSync(cargo,'#!/usr/bin/env node\n'+`import {appendFileSync,writeFileSync,mkdirSync} from 'node:fs';import {join,dirname} from 'node:path';const args=process.argv.slice(2),executable=process.env.CARGO_TARGET_DIR?join(process.env.CARGO_TARGET_DIR,'release/obs-mcp-native'):${JSON.stringify(binary)};mkdirSync(dirname(executable),{recursive:true});writeFileSync(executable,'fixture selected private bytes',{mode:0o755});if(executable!==${JSON.stringify(binary)})writeFileSync(${JSON.stringify(binary)},'opposite feature shared bytes');appendFileSync(${JSON.stringify(trace)},'cargo '+JSON.stringify({args,target:process.env.CARGO_TARGET_DIR,buildDir:process.env.CARGO_BUILD_BUILD_DIR})+'\\n');console.log(JSON.stringify({reason:'compiler-artifact',manifest_path:${JSON.stringify(join(root,'native/Cargo.toml'))},target:{name:'obs-mcp-native',kind:['bin'],src_path:${JSON.stringify(join(root,'native/src/main.rs'))}},profile:{test:false},features:process.env.OBS_MCP_NATIVE_BUILD_FEATURES?[process.env.OBS_MCP_NATIVE_BUILD_FEATURES]:[],executable}));`);chmodSync(cargo,0o755);
 if(mode!=='valid')writeFileSync(join(root,'scripts/build-native.mjs'),`import {readFileSync,writeFileSync} from 'node:fs';import {join} from 'node:path';import {createHash} from 'node:crypto';import {nativeBuildSource} from './native-bundle.mjs';const bytes=readFileSync(${JSON.stringify(binary)}),pin=join(process.argv[3],'obs-mcp-native');writeFileSync(pin,bytes,{mode:0o500});const receipt={schemaVersion:1,host:{platform:process.platform,arch:process.arch},features:['platform-tls-evaluation'],source:nativeBuildSource(${JSON.stringify(root)}),artifact:{manifest:${JSON.stringify(join(root,'native/Cargo.toml'))},source:${JSON.stringify(join(root,'native/src/main.rs'))},executable:pin,sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length}};${mode==='feature'?'receipt.features=[];':mode==='host'?"receipt.host.arch='wrong-host';":mode==='source'?"receipt.source.sha256='wrong-source';":mode==='hash'?"receipt.artifact.sha256='wrong-hash';":"receipt.artifact.executable='/unrelated/executable';"}console.log(JSON.stringify(receipt));`);
 // Execute the replacement after spawnSync has returned from Cargo, before
 // build-native can read or hash any executable: the original review race.
 const preload=join(root,'after-cargo.mjs');writeFileSync(preload,`import child from 'node:child_process';import {syncBuiltinESMExports} from 'node:module';import {writeFileSync,appendFileSync} from 'node:fs';const run=child.spawnSync;child.spawnSync=function(...args){const result=run(...args);if(args[0]==='cargo'){writeFileSync(${JSON.stringify(binary)},'opposite feature immediately after Cargo returned');appendFileSync(${JSON.stringify(trace)},'post-cargo replacement\\n');}return result;};syncBuiltinESMExports();`);vi.stubEnv('NODE_OPTIONS','--import='+pathToFileURL(preload).href);
 vi.stubEnv('PATH',`${join(root,'bin')}:${process.env.PATH??''}`);vi.stubEnv('CARGO_TARGET_DIR','');vi.stubEnv('CARGO_BUILD_TARGET','');vi.stubEnv('OBS_MCP_NATIVE_BUILD_FEATURES','platform-tls-evaluation');
 return{root,binary,plain,trace};
}
describe.skipIf(!['darwin','linux'].includes(process.platform))('feature-preserving native packaging',()=>{
 it('accepts only absent/empty/default or the exact reviewed TLS feature',()=>{
  expect(nativeBuildFeatures()).toEqual([]);expect(nativeBuildFeatures('')).toEqual([]);expect(nativeBuildFeatures('platform-tls-evaluation')).toEqual(['platform-tls-evaluation']);
  for(const invalid of ['resolver-fixture','all','platform-tls-evaluation,resolver-fixture','platform-tls-evaluation plain-fixture',' platform-tls-evaluation','platform-tls-evaluation '])expect(()=>nativeBuildFeatures(invalid)).toThrow(/Only the reviewed/);
 });
 it('includes an optional build script in both source identity and corresponding source',()=>{
  const{root}=fixture();const before=nativeBuildSource(root);writeFileSync(join(root,'native/build.rs'),'fn main() {}');expect(nativeBundleFiles(root)).toContain('native/build.rs');expect(nativeBuildSource(root).files).toHaveProperty('native/build.rs');expect(nativeBuildSource(root).sha256).not.toBe(before.sha256);
 });
 it('retains a changed cleanup owner rather than deleting its replacement',()=>{
  const{root}=fixture(),owner=nativeArtifactOwner(root);renameSync(owner.path,owner.path+'-original');mkdirSync(owner.path);writeFileSync(join(owner.path,'keep'),'replacement');expect(()=>nativeArtifactCleanup(owner)).toThrow(/owner changed/);expect(readFileSync(join(owner.path,'keep'),'utf8')).toBe('replacement');
 });
 it('pins default package bytes independently of shared TLS output and inherited build dirs',async()=>{
  const{root,binary,trace}=fixture();vi.stubEnv('OBS_MCP_NATIVE_BUILD_FEATURES','');vi.stubEnv('CARGO_TARGET_DIR',join(root,'shared-target'));vi.stubEnv('CARGO_BUILD_BUILD_DIR',join(root,'shared-build'));
  const result=await pack({root,keepStaging:true});expect(readFileSync(join(result.staging,'native/obs-mcp-native'),'utf8')).toBe('fixture selected private bytes');expect(readFileSync(binary,'utf8')).toBe('opposite feature shared bytes after Cargo completion');expect(existsSync(join(root,'shared-target'))).toBe(false);expect(existsSync(join(root,'shared-build'))).toBe(false);expect(existsSync(join(result.staging,'native/build.json'))).toBe(false);expect(readFileSync(trace,'utf8')).toContain('post-cargo replacement\ncheck-native-migration.mjs');const cargo=JSON.parse(readFileSync(trace,'utf8').split('\n').find(line=>line.startsWith('cargo '))!.slice(6));expect(cargo.args).toContain('--target-dir');expect(cargo.target).toMatch(/obs-native-build-[^/]+\/target$/);expect(cargo.buildDir).toBe(join(dirname(cargo.target),'build'));expect(cargo.target).not.toBe(join(root,'shared-target'));expect(cargo.buildDir).not.toBe(join(root,'shared-build'));expect(readdirSync(join(root,'native/target')).filter(name=>/^obs-native-(?:artifact|build)-/.test(name))).toEqual([]);
 });
 it('advertises the optional CA file only on the matching evaluation manifest',()=>{
  const plain=nativeManifest(source),tls=nativeManifest(source,process.platform,process.arch,'platform-tls-evaluation');
  expect(plain.user_config).not.toHaveProperty('obs_mcp_tls_ca_file');expect(plain.server.mcp_config.env).not.toHaveProperty('OBS_MCP_TLS_CA_FILE');
  expect(tls.user_config.obs_mcp_tls_ca_file).toMatchObject({type:'file',required:false});expect(tls.server.mcp_config.env.OBS_MCP_TLS_CA_FILE).toBe('${user_config.obs_mcp_tls_ca_file}');expect(tls.long_description).toContain('not production TLS acceptance');expect(tls.tools).toEqual(plain.tools);
 });
 it.each(['feature','host','source','hash','executable'])('refuses a mismatched %s identity before staging or publication',async mode=>{
  const{root,plain}=fixture(mode);await expect(pack({root})).rejects.toThrow(/receipt does not match|bytes changed/);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');expect(existsSync(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'))).toBe(false);expect(readdirSync(root).some(name=>name.startsWith('.pack-native-stage-'))).toBe(false);expect(readdirSync(join(root,'native/target')).filter(name=>/^obs-native-(?:artifact|build)-/.test(name))).toEqual([]);
 });
 it('cleans only its private owners when a post-build gate fails',async()=>{
  const{root,binary}=fixture();writeFileSync(join(root,'scripts/check-native-migration.mjs'),"throw new Error('post-build fixture refusal')");await expect(pack({root})).rejects.toThrow();expect(readFileSync(binary,'utf8')).toBe('opposite feature immediately after Cargo returned');expect(readdirSync(join(root,'native/target')).filter(name=>/^obs-native-(?:artifact|build)-/.test(name))).toEqual([]);expect(existsSync(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'))).toBe(false);
 });
 it('refuses an artifact pin directory replacement and retains the ambiguous replacement',async()=>{
  const{root}=fixture();writeFileSync(join(root,'scripts/check-native-migration.mjs'),`import {readdirSync,renameSync,mkdirSync,writeFileSync} from 'node:fs';import {join} from 'node:path';const parent=${JSON.stringify(join(root,'native/target'))},name=readdirSync(parent).find(name=>name.startsWith('obs-native-artifact-')),path=join(parent,name);renameSync(path,path+'-original');mkdirSync(path);writeFileSync(join(path,'keep'),'replacement');`);await expect(pack({root})).rejects.toThrow(/owner changed/);const replacement=readdirSync(join(root,'native/target')).find(name=>name.startsWith('obs-native-artifact-')&&!name.endsWith('-original'))!;expect(readFileSync(join(root,'native/target',replacement,'keep'),'utf8')).toBe('replacement');expect(existsSync(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'))).toBe(false);
 });
 it('rejects conflicting feature options and cross-target use before gates',async()=>{
  const{root,plain,trace}=fixture();await expect(pack({root,nativeFeatures:''})).rejects.toThrow(/disagrees/);expect(existsSync(trace)).toBe(false);
  vi.stubEnv('CARGO_BUILD_TARGET','unqualified-target');await expect(pack({root})).rejects.toThrow(/Host-native/);expect(existsSync(trace)).toBe(false);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
 });
 it('refuses TLS output over the reserved plain artifact before gates',async()=>{
  const{root,plain,trace}=fixture();await expect(pack({root,output:plain})).rejects.toThrow(/reserved/);expect(existsSync(trace)).toBe(false);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
 });
 it('packages the exact selected feature bytes and source identity without overwriting the plain artifact',async()=>{
  const{root,binary,plain,trace}=fixture();const result=await pack({root,keepStaging:true});
  expect(readFileSync(trace,'utf8')).toContain('post-cargo replacement\ncheck-native-migration.mjs');expect(result.output).toBe(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'));expect(existsSync(result.output)).toBe(true);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
  const staged=readFileSync(join(result.staging,'native/obs-mcp-native')),identity=JSON.parse(readFileSync(join(result.staging,'native/build.json'),'utf8'));
  expect(lstatSync(join(result.staging,'native/obs-mcp-native')).mode&0o777).toBe(0o500);expect(staged.toString()).toBe('fixture selected private bytes');expect(readFileSync(binary,'utf8')).toBe('opposite feature shared bytes after Cargo completion');expect(identity.features).toEqual(['platform-tls-evaluation']);expect(identity.host).toEqual({platform:process.platform,arch:process.arch});expect(identity.executable).toEqual({path:'native/obs-mcp-native',sha256:sha(staged),bytes:staged.length});expect(identity.source).toEqual(nativeBuildSource(result.staging));expect(identity.qualification).toContain('evaluation-only');
  const manifest=JSON.parse(readFileSync(join(result.staging,'manifest.json'),'utf8'));expect(manifest.server.type).toBe('binary');expect(manifest.user_config.obs_mcp_tls_ca_file.required).toBe(false);expect(manifest.compatibility.runtimes).toBeUndefined();expect(existsSync(join(result.staging,'node_modules'))).toBe(false);expect(existsSync(join(result.staging,'package.json'))).toBe(false);expect(JSON.stringify(identity)).not.toContain(resolve(root));expect(readdirSync(join(root,'native/target')).filter(name=>/^obs-native-(?:artifact|build)-/.test(name))).toEqual([]);
 });
});
