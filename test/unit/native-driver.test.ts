/* SPDX-License-Identifier: GPL-2.0-only */
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const source = process.env.NATIVE_DRIVER_SOURCE_ROOT ?? fileURLToPath(new URL("../../", import.meta.url));
const fixtures: string[] = [];
afterEach(() => { for (const root of fixtures.splice(0)) rmSync(root, { recursive: true, force: true }); });
type Row = { command: string; cwd: string; args: string[]; autoInstall: string; cache: string; native?: string; entry?: string; compat?: string; bridge?: string; tcp?: string; nativeBytes?: string; probeBytes?: string };
function fixture(script: string, mode = "valid") {
  const parent=join(source,"native/target");mkdirSync(parent,{recursive:true});
  const root = realpathSync(mkdtempSync(join(parent, "obs-native-driver-"))); fixtures.push(root);
  for (const path of ["scripts", "bin", "native/.cargo-home", "native/src", "native/assets", "docs", "build"]) mkdirSync(join(root, path), { recursive: true });
  copyFileSync(join(source, "scripts", script), join(root, "scripts", script));
  if(existsSync(join(source,"scripts/native-probes.mjs")))copyFileSync(join(source,"scripts/native-probes.mjs"),join(root,"scripts/native-probes.mjs"));
  writeFileSync(join(root, "native/rust-toolchain.toml"), '[toolchain]\nchannel="1.95.0"\n');
  for (const helper of ["export-native-contract.mjs", "export-native-prompts.mjs", "check-native-licenses.mjs", "check-native-migration.mjs"]) writeFileSync(join(root, "scripts", helper), "// Separately qualified gates.\n");
  for(const file of["Cargo.toml","Cargo.lock"])writeFileSync(join(root,"native",file),"# bounded fixture input\n");
  for(const file of["tools.json","prompts.json"])writeFileSync(join(root,"native/assets",file),"{}\n");
  writeFileSync(join(root,"native/src/main.rs"),"fn main(){}\n");writeFileSync(join(root,"docs/protocol.json"),"{}\n");
  writeFileSync(join(root,"build/index.js"),"// Fresh development Source oracle.\n");
  const release=join(root,"default-release"),probe=join(root,"bin-test-executable");
  writeFileSync(release,"fresh default release");writeFileSync(probe,"fresh bin test");
  const trace = join(root, "trace.jsonl");
  for (const command of ["cargo", "npm"]) {
    const path = join(root, "bin", command);
    writeFileSync(path, '#!/usr/bin/env node\n' + `import {appendFileSync,readFileSync,writeFileSync,mkdirSync,unlinkSync} from 'node:fs';\nimport {basename} from 'node:path';\nconst args=process.argv.slice(2),command=basename(process.argv[1]);\nconst row={command,cwd:process.cwd(),args,autoInstall:process.env.RUSTUP_AUTO_INSTALL,cache:process.env.CARGO_HOME,native:process.env.NATIVE_TEST_BINARY,entry:process.env.HTTP_NATIVE_ENTRY_BINARY,compat:process.env.NATIVE_TEST_COMPAT_ENTRY,bridge:process.env.HTTP_BRIDGE_PROBE_BINARY,tcp:process.env.HTTP_TCP_PROBE_BINARY};\nif(command==='npm'&&args.includes('native:test')){writeFileSync(${JSON.stringify(release)},'shared release replaced');writeFileSync(${JSON.stringify(probe)},'shared probe replaced');row.nativeBytes=readFileSync(row.native,'utf8');row.probeBytes=readFileSync(row.bridge,'utf8');}\nappendFileSync(${JSON.stringify(trace)},JSON.stringify(row)+'\\n');\nif(command==='cargo'&&args.includes('--message-format=json')){const test=args[0]==='test';const executable=test?${JSON.stringify(probe)}:${JSON.stringify(release)};const artifact={reason:'compiler-artifact',manifest_path:${JSON.stringify(join(root,"native/Cargo.toml"))},target:{name:'obs-mcp-native',kind:['bin'],src_path:${JSON.stringify(join(root,"native/src/main.rs"))}},profile:{test},executable};console.log(JSON.stringify({reason:'compiler-artifact',target:{name:'unrelated',kind:['lib']},executable:null}));if(${JSON.stringify(mode)}==='invalid-json'&&test)console.log('not cargo json');else if(${JSON.stringify(mode)}!=='missing'||!test){if(${JSON.stringify(mode)}==='nonregular'&&test){unlinkSync(executable);mkdirSync(executable);}console.log(JSON.stringify(artifact));if(${JSON.stringify(mode)}==='ambiguous'&&test)console.log(JSON.stringify(artifact));}console.log(JSON.stringify({reason:'build-finished',success:true}));}\nif(command==='npm'&&args.includes('native:test')&&${JSON.stringify(mode)}==='public-fails')process.exitCode=7;\n`);
    chmodSync(path, 0o755);
  }
  const result = spawnSync(process.execPath, [join(root, "scripts", script)], { cwd: root, env: { ...process.env, PATH: `${join(root, "bin")}:${process.env.PATH ?? ""}`, NATIVE_TEST_BINARY:"stale-native",HTTP_NATIVE_ENTRY_BINARY:"stale-entry",NATIVE_TEST_COMPAT_ENTRY:"stale-source",HTTP_BRIDGE_PROBE_BINARY:"stale-bridge",HTTP_TCP_PROBE_BINARY:"stale-tcp" }, encoding: "utf8" });
  const commands=existsSync(trace)?readFileSync(trace,"utf8").trim().split("\n").map(line=>JSON.parse(line) as Row):[];
  return {root,result,commands,release,probe};
}

// POSIX process shims do not qualify Windows CreateProcess semantics.
describe.skipIf(process.platform === "win32")("native driver toolchain selection", () => {
  it.each(["build-native.mjs", "check-native.mjs"])("runs %s Cargo commands from the pinned native directory", script => {
    const {root,result,commands}=fixture(script);
    expect(result.status, result.stderr).toBe(0);
    const cargo = commands.filter(row => row.command === "cargo");
    if (script === "build-native.mjs") expect(cargo).toHaveLength(1);
    else {
      for (const kind of ["clippy", "test"]) expect(cargo.some(row => row.args[0] === kind && row.args.includes("--all-features") && !row.args.includes("--manifest-path")), `root ${kind} must exercise optional native owners`).toBe(true);
      expect(cargo.some(row => row.args[0] === "test" && row.args.includes("resolver-fixture") && row.args.includes("resolver_owner_contract")), "actual owned helper processes need a separate feature-enabled contract gate").toBe(true);
      for (const manifest of ["crates/resolver/Cargo.toml", "crates/platform-tls/Cargo.toml"]) for (const kind of ["fmt", "clippy", "test"]) {
        const matching = cargo.filter(row => row.args[0] === kind && row.args.includes(manifest));
        expect(matching, `${manifest} ${kind} must run separately from root`).toHaveLength(1);
        if (manifest.includes("platform-tls") && kind !== "fmt") {expect(matching[0]!.args).toContain(process.platform === "linux" ? "gnutls-probe" : "plain-fixture");expect(matching[0]!.args).not.toContain("--all-features");}
      }
      for(const kind of ["build","test"]){const row=cargo.find(row=>row.args[0]===kind&&row.args.includes('--message-format=json'));expect(row,`select exact ${kind} compiler artifact`).toBeDefined();expect(row!.args).toEqual(kind==='test'?['test','--locked','--offline','--bin','obs-mcp-native','--no-run','--message-format=json']:['build','--locked','--offline','--release','--bin','obs-mcp-native','--message-format=json']);}
      const publicTest=commands.find(row=>row.command==='npm'&&row.args.includes('native:test'))!;
      expect(publicTest.nativeBytes).toBe('fresh default release');expect(publicTest.probeBytes).toBe('fresh bin test');
      expect(publicTest.compat).toBe(join(root,'build/index.js'));expect(publicTest.entry).toBe(publicTest.native);expect(publicTest.bridge).toBe(publicTest.tcp);expect(publicTest.native).not.toBe(publicTest.bridge);
      for(const path of [publicTest.native!,publicTest.bridge!]){expect(path.startsWith(join(root,'native/target/obs-native-check-'))).toBe(true);expect(existsSync(path)).toBe(false);expect(existsSync(dirname(path))).toBe(false);}
    }
    for (const row of cargo) {expect(row.cwd).toBe(join(root, "native"));expect(row.autoInstall).toBe("0");expect(row.cache).toBe(join(root, "native/.cargo-home"));}
    for (const row of commands.filter(row => row.command === "npm")) expect(row.cwd).toBe(root);
  });
  it.each(['missing','ambiguous','nonregular','invalid-json'])('refuses %s probe artifact before public tests',mode=>{
    const {root,result,commands}=fixture('check-native.mjs',mode);expect(result.status).not.toBe(0);expect(result.stderr).toContain(mode==='invalid-json'?'Cargo did not return valid artifact JSON':mode==='nonregular'?'must be a regular file':'Expected exactly one native compiler executable artifact');expect(commands.some(row=>row.command==='npm'&&row.args.includes('native:test'))).toBe(false);expect(existsSync(join(root,'native/target'))?readdirSync(join(root,'native/target')).filter(name=>name.startsWith('obs-native-check-')):[]).toEqual([]);
  });
  it('cleans owned snapshots after failed public tests and preserves the exit status',()=>{
    const {result,commands}=fixture('check-native.mjs','public-fails');expect(result.status).toBe(7);const row=commands.find(row=>row.command==='npm'&&row.args.includes('native:test'))!;expect(row).toBeDefined();expect(existsSync(row.native!)).toBe(false);expect(existsSync(row.bridge!)).toBe(false);expect(existsSync(dirname(row.native!))).toBe(false);
  });
});
