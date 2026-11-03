/* SPDX-License-Identifier: GPL-2.0-only */
import {createHash} from 'node:crypto';
import {chmodSync,copyFileSync,existsSync,mkdirSync,mkdtempSync,readFileSync,readdirSync,rmSync,writeFileSync} from 'node:fs';
import {dirname,join,resolve} from 'node:path';
import {fileURLToPath} from 'node:url';
import {afterEach,describe,expect,it,vi} from 'vitest';
import {pack} from '../../scripts/pack.mjs';
import {nativeBuildFeatures,nativeBuildSource,nativeBundleFiles,nativeManifest} from '../../scripts/native-bundle.mjs';
const source=fileURLToPath(new URL('../../',import.meta.url)),roots:string[]=[];
afterEach(()=>{vi.unstubAllEnvs();for(const root of roots.splice(0))rmSync(root,{recursive:true,force:true});});
const sha=(bytes:Buffer)=>createHash('sha256').update(bytes).digest('hex');
function fixture(mode='valid'){
 const parent=join(source,'native/target');mkdirSync(parent,{recursive:true});const root=mkdtempSync(join(parent,'tls-package-'));roots.push(root);
 for(const file of [...nativeBundleFiles(source),'manifest.json','package.json','package-lock.json','scripts/build-native.mjs','scripts/native-bundle.mjs']){const destination=join(root,file);mkdirSync(dirname(destination),{recursive:true});copyFileSync(join(source,file),destination);}
 for(const directory of ['bin','dist','native/.cargo-home','native/target/release'])mkdirSync(join(root,directory),{recursive:true});
 const binary=join(root,'native/target/release/obs-mcp-native');writeFileSync(binary,'fixture TLS executable bytes');chmodSync(binary,0o755);
 const plain=join(root,'dist/obs-studio-native.mcpb');writeFileSync(plain,'preceding qualified plain artifact');
 const trace=join(root,'trace.jsonl');for(const name of ['check-native-licenses.mjs','check-native-migration.mjs'])writeFileSync(join(root,'scripts',name),`import {appendFileSync} from 'node:fs';appendFileSync(${JSON.stringify(trace)},${JSON.stringify(name+'\n')});`);
 const cargo=join(root,'bin/cargo');writeFileSync(cargo,'#!/usr/bin/env node\n'+`import {appendFileSync} from 'node:fs';appendFileSync(${JSON.stringify(trace)},'cargo '+JSON.stringify(process.argv.slice(2))+'\\n');console.log(JSON.stringify({reason:'compiler-artifact',manifest_path:${JSON.stringify(join(root,'native/Cargo.toml'))},target:{name:'obs-mcp-native',kind:['bin'],src_path:${JSON.stringify(join(root,'native/src/main.rs'))}},profile:{test:false},features:['platform-tls-evaluation'],executable:${JSON.stringify(binary)}}));`);chmodSync(cargo,0o755);
 if(mode!=='valid')writeFileSync(join(root,'scripts/build-native.mjs'),`import {readFileSync} from 'node:fs';import {createHash} from 'node:crypto';import {nativeBuildSource} from './native-bundle.mjs';const bytes=readFileSync(${JSON.stringify(binary)});const receipt={schemaVersion:1,host:{platform:process.platform,arch:process.arch},features:['platform-tls-evaluation'],source:nativeBuildSource(${JSON.stringify(root)}),artifact:{manifest:${JSON.stringify(join(root,'native/Cargo.toml'))},source:${JSON.stringify(join(root,'native/src/main.rs'))},executable:${JSON.stringify(binary)},sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length}};${mode==='feature'?'receipt.features=[];':mode==='host'?"receipt.host.arch='wrong-host';":mode==='source'?"receipt.source.sha256='wrong-source';":mode==='hash'?"receipt.artifact.sha256='wrong-hash';":"receipt.artifact.executable='/unrelated/executable';"}console.log(JSON.stringify(receipt));`);
 vi.stubEnv('PATH',`${join(root,'bin')}:${process.env.PATH??''}`);vi.stubEnv('CARGO_TARGET_DIR','');vi.stubEnv('CARGO_BUILD_TARGET','');vi.stubEnv('OBS_MCP_NATIVE_BUILD_FEATURES','platform-tls-evaluation');
 return{root,binary,plain,trace};
}
describe.skipIf(!['darwin','linux'].includes(process.platform))('feature-preserving native packaging',()=>{
 it('accepts only absent/empty/default or the exact reviewed TLS feature',()=>{
  expect(nativeBuildFeatures()).toEqual([]);expect(nativeBuildFeatures('')).toEqual([]);expect(nativeBuildFeatures('platform-tls-evaluation')).toEqual(['platform-tls-evaluation']);
  for(const invalid of ['resolver-fixture','all','platform-tls-evaluation,resolver-fixture','platform-tls-evaluation plain-fixture',' platform-tls-evaluation','platform-tls-evaluation '])expect(()=>nativeBuildFeatures(invalid)).toThrow(/Only the reviewed/);
 });
 it('advertises the optional CA file only on the matching evaluation manifest',()=>{
  const plain=nativeManifest(source),tls=nativeManifest(source,process.platform,process.arch,'platform-tls-evaluation');
  expect(plain.user_config).not.toHaveProperty('obs_mcp_tls_ca_file');expect(plain.server.mcp_config.env).not.toHaveProperty('OBS_MCP_TLS_CA_FILE');
  expect(tls.user_config.obs_mcp_tls_ca_file).toMatchObject({type:'file',required:false});expect(tls.server.mcp_config.env.OBS_MCP_TLS_CA_FILE).toBe('${user_config.obs_mcp_tls_ca_file}');expect(tls.long_description).toContain('not production TLS acceptance');expect(tls.tools).toEqual(plain.tools);
 });
 it.each(['feature','host','source','hash','executable'])('refuses a mismatched %s identity before staging or publication',async mode=>{
  const{root,plain}=fixture(mode);await expect(pack({root})).rejects.toThrow(/receipt does not match|bytes changed/);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');expect(existsSync(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'))).toBe(false);expect(readdirSync(root).some(name=>name.startsWith('.pack-native-stage-'))).toBe(false);
 });
 it('rejects conflicting feature options and cross-target use before gates',async()=>{
  const{root,plain,trace}=fixture();await expect(pack({root,nativeFeatures:''})).rejects.toThrow(/disagrees/);expect(existsSync(trace)).toBe(false);
  vi.stubEnv('CARGO_BUILD_TARGET','unqualified-target');await expect(pack({root})).rejects.toThrow(/Host-native/);expect(existsSync(trace)).toBe(false);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
 });
 it('refuses TLS output over the reserved plain artifact before gates',async()=>{
  const{root,plain,trace}=fixture();await expect(pack({root,output:plain})).rejects.toThrow(/reserved/);expect(existsSync(trace)).toBe(false);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
 });
 it('packages the exact selected feature bytes and source identity without overwriting the plain artifact',async()=>{
  const{root,binary,plain}=fixture();const result=await pack({root,keepStaging:true});
  expect(result.output).toBe(join(root,'dist/obs-studio-native-tls-evaluation.mcpb'));expect(existsSync(result.output)).toBe(true);expect(readFileSync(plain,'utf8')).toBe('preceding qualified plain artifact');
  const staged=readFileSync(join(result.staging,'native/obs-mcp-native')),identity=JSON.parse(readFileSync(join(result.staging,'native/build.json'),'utf8'));
  expect(staged).toEqual(readFileSync(binary));expect(identity.features).toEqual(['platform-tls-evaluation']);expect(identity.host).toEqual({platform:process.platform,arch:process.arch});expect(identity.executable).toEqual({path:'native/obs-mcp-native',sha256:sha(staged),bytes:staged.length});expect(identity.source).toEqual(nativeBuildSource(result.staging));expect(identity.qualification).toContain('evaluation-only');
  const manifest=JSON.parse(readFileSync(join(result.staging,'manifest.json'),'utf8'));expect(manifest.server.type).toBe('binary');expect(manifest.user_config.obs_mcp_tls_ca_file.required).toBe(false);expect(manifest.compatibility.runtimes).toBeUndefined();expect(existsSync(join(result.staging,'node_modules'))).toBe(false);expect(existsSync(join(result.staging,'package.json'))).toBe(false);expect(JSON.stringify(identity)).not.toContain(resolve(root));
 });
});
