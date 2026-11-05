/* SPDX-License-Identifier: MIT */
import {readFileSync} from 'node:fs';
import {fileURLToPath} from 'node:url';
import {describe,expect,it} from 'vitest';
import {nativeBundleFiles,nativeManifest} from '../../scripts/native-bundle.mjs';
const root=fileURLToPath(new URL('../../',import.meta.url));
describe('native-only distribution plan',()=>{
 it('requires explicit state onboarding, immutable profile selection and optional recording/media paths without a Node runtime',()=>{
  const manifest=nativeManifest(root,'linux','x64');expect(manifest.server).toMatchObject({type:'binary',entry_point:'native/obs-mcp-native',mcp_config:{command:'${__dirname}/native/obs-mcp-native',args:[]}});expect(manifest.compatibility).toEqual({platforms:['linux']});expect(manifest.server.mcp_config.env).not.toHaveProperty('OBS_MCP_COMPAT_ENTRY');expect(manifest.server.mcp_config.env).not.toHaveProperty('OBS_MCP_NATIVE_STATEFUL');expect(manifest.user_config.obs_mcp_state_dir).toMatchObject({type:'directory',required:true});expect(manifest.user_config.obs_mcp_state_dir).not.toHaveProperty('default');expect(manifest.user_config.obs_mcp_native_profile?.default).toBe('production');for(const name of['obs_mcp_recording_root','obs_mcp_ffmpeg','obs_mcp_ffprobe'])expect(manifest.user_config[name]?.required).toBe(false);expect(manifest.tools).toHaveLength(179);expect(manifest.long_description).toContain('union');expect(()=>nativeManifest(root,'win32','x64')).toThrow(/macOS\/Linux/);
 });
 it('retains corresponding Rust inputs and notices while excluding JavaScript SDK/runtime payload and compiler caches',()=>{
  const files=nativeBundleFiles(root);for(const path of['LICENSE','NOTICE.md','licenses/native/INVENTORY.txt','licenses/native/HTTP-INVENTORY.txt','licenses/native/registry/hyper-1.11.1/LICENSE.txt','licenses/native/objc2-bindings/ATTRIBUTION.txt','licenses/native/objc2-bindings/MIT-GRANT.txt','native/Cargo.lock','native/rust-toolchain.toml','native/src/main.rs','native/src/stdio_input.rs','native/assets/tools.json','native/assets/prompts.json','docs/protocol.json','docs/native-distribution.md'])expect(files).toContain(path);expect(files.some(path=>/^(?:src|build|server|node_modules)\//.test(path)||/\.(?:[cm]?js|[cm]?ts|node)$/.test(path))).toBe(false);expect(files).not.toContain('package.json');expect(files.some(path=>path.includes('/target/')||path.includes('/.cargo-home/'))).toBe(false);
 });
 it('build entry consumes checked native assets and never invokes Source exporters or a TypeScript build',()=>{
  const build=readFileSync(new URL('../../scripts/build-native.mjs',import.meta.url),'utf8');expect(build).not.toMatch(/export-native-(?:contract|prompts)|npm.*build/);const main=readFileSync(new URL('../../native/src/main.rs',import.meta.url),'utf8');expect(main).not.toMatch(/^mod compat;/m);expect(main).not.toContain('compat::');expect(main).toContain('native_service::run().await');
 });
});
it('declares native-default service ownership and keeps Source solely as a hashed development oracle',()=>{
 const contract=JSON.parse(readFileSync(new URL('../../native/assets/tools.json',import.meta.url),'utf8'));
 const ledger=JSON.parse(readFileSync(new URL('../../docs/native-migration-ledger.json',import.meta.url),'utf8'));
 expect(contract.service.activation).toBe('native-default');expect(Object.values(ledger.toolRoutes).filter(owner=>owner==='native-service')).toHaveLength(117);expect(Object.values(ledger.toolRoutes)).not.toContain('compat');expect(Object.values(ledger.sourceOwnership).every((entry:any)=>entry.owner==='dev-oracle')).toBe(true);expect(ledger.protocolRoutes['other-methods']).not.toMatch(/compat|handoff/);
});

it('keeps the private development Node floor and CI matrix aligned with pinned Vitest5 rather than runtime history',()=>{
 const project=JSON.parse(readFileSync(new URL('../../package.json',import.meta.url),'utf8'));
 const vitest=JSON.parse(readFileSync(new URL('../../node_modules/vitest/package.json',import.meta.url),'utf8'));
 const workflow=readFileSync(new URL('../../.github/workflows/ci.yml',import.meta.url),'utf8');
 expect(project.engines.node).toBe(vitest.engines.node);expect(workflow).toContain('node: [22.12.0, 24.x, 26.x]');expect(workflow).not.toContain('20.19.0');expect(workflow).toContain('cargo fetch --locked --manifest-path native/Cargo.toml');
});
