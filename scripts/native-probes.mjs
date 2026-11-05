// SPDX-License-Identifier: MIT
// Development-only executable custody for native public tests. Never a runtime dependency.
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { chmodSync, constants, copyFileSync, lstatSync, mkdirSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { isAbsolute, join, resolve } from 'node:path';

function buildArtifact(nativeRoot, env, test) {
  const args = test
    ? ['test','--locked','--offline','--bin','obs-mcp-native','--no-run','--message-format=json']
    : ['build','--locked','--offline','--release','--bin','obs-mcp-native','--message-format=json'];
  const result = spawnSync('cargo', args, {cwd:nativeRoot, env, encoding:'utf8', stdio:['ignore','pipe','inherit'], maxBuffer:64*1024*1024});
  if(result.error)throw result.error;
  if(result.status!==0)throw Object.assign(new Error('Native executable preparation failed'), {exitCode:result.status??1});
  let messages;
  try { messages=result.stdout.split(/\r?\n/).filter(line=>line.trim()).map(line=>JSON.parse(line)); }
  catch { throw new Error('Cargo did not return valid artifact JSON'); }
  const artifacts = messages.filter(row =>
    row.reason==='compiler-artifact' && row.manifest_path===join(nativeRoot,'Cargo.toml') &&
    row.target?.name==='obs-mcp-native' && row.target?.kind?.length===1 && row.target.kind[0]==='bin' &&
    row.target.src_path===join(nativeRoot,'src/main.rs') && row.profile?.test===test && typeof row.executable==='string');
  if(artifacts.length!==1)throw new Error('Expected exactly one native compiler executable artifact');
  const path=artifacts[0].executable;
  const stat=isAbsolute(path)?lstatSync(path):undefined;
  if(!stat?.isFile() || stat.isSymbolicLink())throw new Error('Native compiler executable must be a regular file');
  return path;
}
const digest=path=>createHash('sha256').update(readFileSync(path)).digest('hex');

export function prepareNativeTestBinaries(nativeRoot, env) {
  nativeRoot=resolve(nativeRoot);
  const parent=join(nativeRoot,'target');mkdirSync(parent,{recursive:true});
  const directory=mkdtempSync(join(parent,'obs-native-check-'));
  const close=()=>rmSync(directory,{recursive:true,force:true});
  try {
    const pin=(source,name)=>{
      const before=digest(source),destination=join(directory,name+(process.platform==='win32'?'.exe':''));
      copyFileSync(source,destination,constants.COPYFILE_EXCL);
      if(digest(source)!==before || digest(destination)!==before)throw new Error('Native executable changed during snapshot');
      chmodSync(destination,0o500);
      return destination;
    };
    const native=pin(buildArtifact(nativeRoot,env,false),'native-release');
    const test=pin(buildArtifact(nativeRoot,env,true),'native-probe');
    return {env:{...env,NATIVE_TEST_BINARY:native,HTTP_NATIVE_ENTRY_BINARY:native,NATIVE_TEST_COMPAT_ENTRY:resolve(nativeRoot,'../build/index.js'),HTTP_BRIDGE_PROBE_BINARY:test,HTTP_TCP_PROBE_BINARY:test},hashes:{nativeSha256:digest(native),probeSha256:digest(test)},close};
  }catch(error){close();throw error;}
}
