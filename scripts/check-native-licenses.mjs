#!/usr/bin/env node
// SPDX-License-Identifier: GPL-2.0-only
// A reviewed-inventory gate, not a general SPDX parser or a legal opinion.
import { spawnSync } from "node:child_process";
import { existsSync, statSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const CRATES_IO = "registry+https://github.com/rust-lang/crates.io-index";
// Exact source/version/license tuples observed on 2026-10-31. No auto-adoption mode.
export const reviewedNativePackages = new Map([
  ["base64@0.22.1", "MIT OR Apache-2.0"],
  ["block-buffer@0.10.4", "MIT OR Apache-2.0"],
  ["bytes@1.12.1", "MIT"],
  ["cfg-if@1.0.5", "MIT OR Apache-2.0"],
  ["cpufeatures@0.2.17", "MIT OR Apache-2.0"],
  ["crypto-common@0.1.7", "MIT OR Apache-2.0"],
  ["data-encoding@2.11.1", "MIT"],
  ["digest@0.10.7", "MIT OR Apache-2.0"],
  ["errno@0.3.14", "MIT OR Apache-2.0"],
  ["futures-core@0.3.34", "MIT OR Apache-2.0"],
  ["futures-macro@0.3.34", "MIT OR Apache-2.0"],
  ["futures-sink@0.3.34", "MIT OR Apache-2.0"],
  ["futures-task@0.3.34", "MIT OR Apache-2.0"],
  ["futures-util@0.3.34", "MIT OR Apache-2.0"],
  ["generic-array@0.14.7", "MIT"],
  ["getrandom@0.3.4", "MIT OR Apache-2.0"],
  ["http@1.5.0", "MIT OR Apache-2.0"],
  ["httparse@1.10.1", "MIT OR Apache-2.0"],
  ["itoa@1.0.18", "MIT OR Apache-2.0"],
  ["libc@0.2.191", "MIT OR Apache-2.0"],
  ["log@0.4.34", "MIT OR Apache-2.0"],
  ["memchr@2.8.3", "Unlicense OR MIT"],
  ["mio@1.2.4", "MIT"],
  ["obs-mcp-native@1.1.1", "GPL-2.0-only"],
  ["pin-project-lite@0.2.17", "Apache-2.0 OR MIT"],
  ["ppv-lite86@0.2.21", "MIT OR Apache-2.0"],
  ["proc-macro2@1.0.107", "MIT OR Apache-2.0"],
  ["quote@1.0.47", "MIT OR Apache-2.0"],
  ["r-efi@5.3.0", "MIT OR Apache-2.0 OR LGPL-2.1-or-later"],
  ["rand@0.9.5", "MIT OR Apache-2.0"],
  ["rand_chacha@0.9.0", "MIT OR Apache-2.0"],
  ["rand_core@0.9.5", "MIT OR Apache-2.0"],
  ["serde@1.0.229", "MIT OR Apache-2.0"],
  ["serde_core@1.0.229", "MIT OR Apache-2.0"],
  ["serde_derive@1.0.229", "MIT OR Apache-2.0"],
  ["serde_json@1.0.154", "MIT OR Apache-2.0"],
  ["sha1@0.10.7", "MIT OR Apache-2.0"],
  ["sha2@0.10.9", "MIT OR Apache-2.0"],
  ["signal-hook-registry@1.4.8", "MIT OR Apache-2.0"],
  ["slab@0.4.12", "MIT"],
  ["socket2@0.6.5", "MIT OR Apache-2.0"],
  ["syn@2.0.119", "MIT OR Apache-2.0"],
  ["syn@3.0.9", "MIT OR Apache-2.0"],
  ["thiserror@2.0.21", "MIT OR Apache-2.0"],
  ["thiserror-impl@2.0.21", "MIT OR Apache-2.0"],
  ["tokio@1.54.1", "MIT"],
  ["tokio-macros@2.7.2", "MIT"],
  ["tokio-tungstenite@0.26.2", "MIT"],
  ["tungstenite@0.26.2", "MIT OR Apache-2.0"],
  ["typenum@1.20.1", "MIT OR Apache-2.0"],
  ["unicode-ident@1.0.27", "(MIT OR Apache-2.0) AND Unicode-3.0"],
  ["utf-8@0.7.6", "MIT OR Apache-2.0"],
  ["version_check@0.9.5", "MIT/Apache-2.0"],
  ["wasi@0.11.1+wasi-snapshot-preview1", "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT"],
  ["wasip2@1.0.4+wasi-0.2.12", "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT"],
  ["windows-link@0.2.1", "MIT OR Apache-2.0"],
  ["windows-sys@0.61.2", "MIT OR Apache-2.0"],
  ["wit-bindgen@0.57.1", "Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT"],
  ["zerocopy@0.8.63", "BSD-2-Clause OR Apache-2.0 OR MIT"],
  ["zerocopy-derive@0.8.63", "BSD-2-Clause OR Apache-2.0 OR MIT"],
  ["zmij@1.0.23", "MIT"],
]);

// These are explicit choices for expressions present in the reviewed inventory.
// An AND is never interpreted as an OR. Unknown expressions require review.
const choices = new Map([
  ["MIT", "MIT"],
  ["MIT OR Apache-2.0", "MIT"], ["Apache-2.0 OR MIT", "MIT"],
  ["Unlicense OR MIT", "MIT"],
  ["BSD-2-Clause OR Apache-2.0 OR MIT", "MIT"],
  ["MIT OR Apache-2.0 OR LGPL-2.1-or-later", "MIT"],
  ["Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT", "MIT"],
  ["(MIT OR Apache-2.0) AND Unicode-3.0", "MIT AND Unicode-3.0"],
  // Cargo's pre-SPDX slash expression is reviewed for this exact version only.
  ["MIT/Apache-2.0", "MIT"],
]);

function graphIds(metadata) {
  if (!metadata?.resolve?.root || !Array.isArray(metadata.resolve.nodes) || !Array.isArray(metadata.packages)) {
    throw new Error("Incomplete Cargo metadata: packages and resolved root graph are required");
  }
  const nodes = new Map(metadata.resolve.nodes.map(node => [node.id, node]));
  const packages = new Map(metadata.packages.map(pkg => [pkg.id, pkg]));
  if (nodes.size !== metadata.resolve.nodes.length || packages.size !== metadata.packages.length) throw new Error("Duplicate Cargo package or graph identity");
  const seen = new Set(), pending = [metadata.resolve.root];
  while (pending.length) {
    const id = pending.pop();
    if (seen.has(id)) continue;
    const node = nodes.get(id);
    if (!node || !packages.has(id) || !Array.isArray(node.dependencies)) throw new Error("Incomplete Cargo dependency graph");
    seen.add(id); pending.push(...node.dependencies);
  }
  return seen;
}

/** Input seams permit deterministic controls without changing the CLI's fixed review policy. */
export function auditNativeLicenses({ metadata, activeMetadata, target, reviewed = reviewedNativePackages,
  manifestExists = path => typeof path === "string" && existsSync(path) && statSync(path).isFile() }) {
  if (typeof target !== "string" || !target) throw new Error("An explicit target is required");
  const allIds = graphIds(metadata), activeIds = graphIds(activeMetadata);
  if (metadata.resolve.root !== activeMetadata.resolve.root) throw new Error("Target graph has a different root");
  if ([...activeIds].some(id => !allIds.has(id))) throw new Error("Active target contains a package absent from the all-target graph");
  const packages = [], blockers = [], observed = new Set();
  for (const pkg of metadata.packages) {
    const key = `${pkg.name}@${pkg.version}`;
    if (observed.has(key)) throw new Error("Duplicate Cargo name/version with different source identities");
    observed.add(key);
    if (!allIds.has(pkg.id)) throw new Error("Cargo metadata contains a package outside its resolved graph");
    if (!manifestExists(pkg.manifest_path)) throw new Error(`Missing crate manifest: ${key}`);
    const isRoot = pkg.id === metadata.resolve.root;
    const expected = reviewed.get(key), expression = pkg.license;
    let choice = null, reason = null;
    if (!reviewed.has(key)) reason = "unreviewed package/version";
    else if (typeof expression !== "string" || !expression) reason = "missing license metadata";
    else if (expression !== expected) reason = "license expression changed after review";
    else if ((isRoot && pkg.source !== null) || (!isRoot && pkg.source !== CRATES_IO)) reason = "unreviewed package source";
    else if (isRoot && pkg.name === "obs-mcp-native" && expression === "GPL-2.0-only") choice = expression;
    else if (expression === "GPL-2.0-only") reason = "project license is admitted only for the root package";
    else if (choices.has(expression)) choice = choices.get(expression);
    else reason = "no reviewed GPL-2.0-only-compatible choice; Apache-only/conjunctive/unknown terms fail closed";
    const entry = { package: key, expression: expression ?? null, choice, active: activeIds.has(pkg.id),
      scope: activeIds.has(pkg.id) ? "active-target" : "other-target", ...(reason ? { reason } : {}) };
    packages.push(entry);
    if (reason) blockers.push(entry);
  }
  for (const key of reviewed.keys()) if (!observed.has(key)) blockers.push({ package: key, reason: "reviewed package removed; inventory change requires review" });
  packages.sort((a, b) => a.package.localeCompare(b.package));
  return { schemaVersion: 1, passed: blockers.length === 0, target, scope: "all resolved targets, including build/proc-macro dependencies",
    packagesChecked: packages.length, activeTargetPackages: packages.filter(pkg => pkg.active).length,
    otherTargetPackages: packages.filter(pkg => !pkg.active).length, packages, blockers,
    limitations: ["Metadata/inventory review only; not a complete file-level, notice, patent, artifact, or legal audit", "Target filtering is a Cargo graph projection, not proof of a compiled or distributed artifact"] };
}

function run(command, args, cwd, env) {
  const result = spawnSync(command, args, { cwd, env, encoding: "utf8", maxBuffer: 16 * 1024 * 1024 });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed (${result.status ?? result.signal}); offline metadata/toolchain unavailable`);
  return result.stdout;
}

export function checkNativeLicenses(root = resolve(dirname(fileURLToPath(import.meta.url)), "..")) {
  const cwd = resolve(root, "native"), cache = resolve(cwd, ".cargo-home");
  const env = { ...process.env, RUSTUP_AUTO_INSTALL: "0", ...(existsSync(cache) ? { CARGO_HOME: cache } : {}) };
  // Cwd is native: rustup must select native/rust-toolchain.toml, not the global default.
  const version = run("rustc", ["--version", "--verbose"], cwd, env);
  const target = /^host: (\S+)$/m.exec(version)?.[1];
  if (!target) throw new Error("Pinned Rust toolchain did not report its host target");
  const args = ["metadata", "--locked", "--offline", "--format-version", "1"];
  const metadata = JSON.parse(run("cargo", args, cwd, env));
  const activeMetadata = JSON.parse(run("cargo", [...args, "--filter-platform", target], cwd, env));
  return auditNativeLicenses({ metadata, activeMetadata, target });
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    if (process.argv.length !== 2) throw new Error("Usage: node scripts/check-native-licenses.mjs (no adoption or override flags)");
    const report = checkNativeLicenses();
    console.log(JSON.stringify(report, null, 2));
    if (!report.passed) process.exitCode = 1;
  } catch (error) {
    console.error(`Native dependency gate failed: ${error.message}`); process.exitCode = 1;
  }
}
