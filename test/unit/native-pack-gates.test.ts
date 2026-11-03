/* SPDX-License-Identifier: GPL-2.0-only */
import { chmodSync, copyFileSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
// Public pack entry point is exercised with real gate subprocesses, before any
// dependency installation, output replacement or staging cleanup can occur.
import { pack } from "../../scripts/pack.mjs";
const roots: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
describe.skipIf(!["darwin", "linux"].includes(process.platform))("native package admission", () => {
  it.each(["license", "migration"])("refuses %s failure before changing staging or replacing an artifact", async failure => {
    const parent=fileURLToPath(new URL("../../native/target/",import.meta.url));mkdirSync(parent,{recursive:true});
    const root = mkdtempSync(join(parent, "obs-native-pack-gate-")); roots.push(root);
    for (const dir of ["scripts", "bin", "dist", ".pack-staging"]) mkdirSync(join(root, dir));
    const metadata = { name: "fixture", version: "1.0.0" };
    writeFileSync(join(root, "package.json"), JSON.stringify(metadata));
    writeFileSync(join(root, "package-lock.json"), JSON.stringify({ ...metadata, lockfileVersion: 3, packages: { "": metadata } }));
    const output = join(root, "dist/obs-studio-native.mcpb"); writeFileSync(output, "preceding artifact");
    const marker = join(root, ".pack-staging/previous-owner"); writeFileSync(marker, "preserved");
    const trace = join(root, "trace.jsonl");
    const script = (name: string, exit: number) => `import {appendFileSync} from 'node:fs';appendFileSync(${JSON.stringify(trace)},${JSON.stringify(name + "\n")});process.exit(${exit});`;
    writeFileSync(join(root, "scripts/check-native-licenses.mjs"), script("license", failure === "license" ? 9 : 0));
    writeFileSync(join(root, "scripts/check-native-migration.mjs"), script("migration", 9));
    // A successful build returns a real pin receipt, so this control reaches
    // the intended migration failure even if receipt validation moves earlier.
    mkdirSync(join(root,"native/src"),{recursive:true});mkdirSync(join(root,"docs"));
    for(const [path,bytes] of Object.entries({"native/Cargo.toml":"[package]\nname='fixture'\nversion='1.0.0'\n","native/Cargo.lock":"version = 4\n","native/rust-toolchain.toml":"[toolchain]\nchannel='1.95.0'\n","native/src/main.rs":"fn main() {}\n","docs/protocol.json":"{}"}))writeFileSync(join(root,path),bytes);
    copyFileSync(fileURLToPath(new URL("../../scripts/native-bundle.mjs",import.meta.url)),join(root,"scripts/native-bundle.mjs"));
    writeFileSync(join(root,"scripts/build-native.mjs"),`import {appendFileSync,writeFileSync} from 'node:fs';import {join} from 'node:path';import {createHash} from 'node:crypto';import {nativeBuildSource} from './native-bundle.mjs';appendFileSync(${JSON.stringify(trace)},'build\\n');const bytes=Buffer.from('fixture native executable'),executable=join(process.argv[3],'obs-mcp-native');writeFileSync(executable,bytes,{flag:'wx',mode:0o500});console.log(JSON.stringify({schemaVersion:1,host:{platform:process.platform,arch:process.arch},features:[],source:nativeBuildSource(${JSON.stringify(root)}),artifact:{manifest:${JSON.stringify(join(root,"native/Cargo.toml"))},source:${JSON.stringify(join(root,"native/src/main.rs"))},executable,sha256:createHash('sha256').update(bytes).digest('hex'),bytes:bytes.length}}));`);
    // Any npm invocation is a regression: packing cannot install a runtime.
    const npm=join(root,"bin/npm");writeFileSync(npm,"#!/usr/bin/env node\n"+script("unexpected npm",91));chmodSync(npm,0o755);
    vi.stubEnv("PATH", `${join(root, "bin")}:${process.env.PATH ?? ""}`);
    await expect(pack({ root, nativeBundle: true, output, offline: true })).rejects.toThrow(failure==="license"?"check-native-licenses.mjs":"check-native-migration.mjs");
    expect(readFileSync(trace, "utf8").trim().split("\n")).toEqual(failure === "license" ? ["license"] : ["license", "build", "migration"]);
    expect(readFileSync(marker, "utf8")).toBe("preserved"); expect(readFileSync(output, "utf8")).toBe("preceding artifact");
    expect(existsSync(join(root, "node_modules"))).toBe(false);
    expect(readdirSync(join(root,"native/target")).filter(name=>name.startsWith("obs-native-artifact-")||name.startsWith("obs-native-build-"))).toEqual([]);
  });
});
