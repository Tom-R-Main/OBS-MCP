/* SPDX-License-Identifier: MIT */
import { mkdirSync, mkdtempSync, writeFileSync, rmSync, unlinkSync, symlinkSync, renameSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, describe, expect, it } from "vitest";
// @ts-expect-error Build-time JavaScript gate intentionally has no production TS API.
import { auditNativeLicenses } from "../../scripts/check-native-licenses.mjs";

type Package = { id: string; name: string; version: string; license: string | null; source: string | null; manifest_path: string };
const manifest = fileURLToPath(import.meta.url);
const temporary = mkdtempSync(resolve(dirname(manifest), "..", ".native-license-controls-"));
afterAll(() => rmSync(temporary, {recursive: true, force: true}));
function createManifest(path: string) {
  mkdirSync(dirname(path), {recursive: true});
  writeFileSync(path, "[package]\nname = \"fixture\"\nversion = \"0.0.0\"\n");
}
const registry = "registry+https://github.com/rust-lang/crates.io-index";
function fixture(expression = "MIT OR Apache-2.0") {
  const rootPath = mkdtempSync(join(temporary, "project-"));
  const rootManifest = join(rootPath, "native", "Cargo.toml"); createManifest(rootManifest);
  const root: Package = { id: "root", name: "obs-mcp-native", version: "1.1.1", license: "MIT", source: null, manifest_path: rootManifest };
  const dep: Package = { id: "dep", name: "fixture-crate", version: "1.0.0", license: expression, source: registry, manifest_path: manifest };
  const metadata = { packages: [root, dep], resolve: { root: "root", nodes: [{ id: "root", dependencies: ["dep"] }, { id: "dep", dependencies: [] as string[] }] } };
  const reviewed = new Map([["obs-mcp-native@1.1.1", "MIT"], ["fixture-crate@1.0.0", expression]]);
  return { metadata, activeMetadata: structuredClone(metadata), target: "fixture-target", reviewed, root: rootPath };
}

describe("offline native dependency inventory gate", () => {
  it.each([
    ["MIT OR Apache-2.0", "MIT"], ["Apache-2.0 OR MIT", "MIT"],
    ["Unlicense OR MIT", "MIT"],
    ["(MIT OR Apache-2.0) AND Unicode-3.0", "MIT AND Unicode-3.0"],
    ["Apache-2.0 WITH LLVM-exception OR Apache-2.0 OR MIT", "MIT"],
  ])("makes an explicit branch choice for reviewed expression %s", (expression, choice) => {
    const result = auditNativeLicenses(fixture(expression));
    expect(result.passed).toBe(true);
    expect(result.packages.find((entry: { package: string }) => entry.package === "fixture-crate@1.0.0")).toMatchObject({ expression, choice });
  });

  it.each(["Apache-2.0", "Apache-2.0 AND ISC", "MIT AND Apache-2.0", "LicenseRef-not-reviewed", "MIT OR Unreviewed-License"])("fails closed for %s even when the version is inventoried", (expression) => {
    const result = auditNativeLicenses(fixture(expression));
    expect(result.passed).toBe(false);
    expect(result.blockers).toHaveLength(1);
    expect(result.blockers[0]).toMatchObject({ expression, choice: null, active: true });
  });

  it("rejects a new package/version even if its expression is otherwise allowed", () => {
    const input = fixture();
    input.reviewed.delete("fixture-crate@1.0.0");
    expect(auditNativeLicenses(input).blockers).toEqual([expect.objectContaining({ reason: "unreviewed package/version" })]);
  });

  it("rejects changed license terms rather than accepting their new SPDX label", () => {
    const input = fixture();
    input.metadata.packages[1]!.license = "MIT";
    input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("license expression changed after review");
  });

  it("does not interpret missing license metadata as permissive", () => {
    const input = fixture();
    input.metadata.packages[1]!.license = null;
    input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("missing license metadata");
  });

  it("does not admit a different source with the same reviewed name/version", () => {
    const input = fixture();
    input.metadata.packages[1]!.source = "git+https://example.invalid/replacement";
    input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("unreviewed package source");
  });

  it("fails when the crate source manifest is missing instead of trusting index metadata", () => {
    const input = fixture();
    input.metadata.packages[1]!.manifest_path = `${manifest}.missing-crate-manifest`;
    input.activeMetadata = structuredClone(input.metadata);
    expect(() => auditNativeLicenses(input)).toThrow("Missing crate manifest: fixture-crate@1.0.0");
  });

  it("checks target-only dependencies and reports that they are not active on this host", () => {
    const input = fixture("Apache-2.0 AND ISC");
    input.activeMetadata.packages.splice(1, 1);
    input.activeMetadata.resolve.nodes.splice(1, 1);
    input.activeMetadata.resolve.nodes[0]!.dependencies = [];
    const result = auditNativeLicenses(input);
    expect(result).toMatchObject({ passed: false, packagesChecked: 2, activeTargetPackages: 1, otherTargetPackages: 1 });
    expect(result.blockers[0]).toMatchObject({ active: false, scope: "outside-default-host-projection" });
  });

  it("requires review when a package disappears from the exact inventory", () => {
    const input = fixture();
    input.reviewed.set("removed@1.0.0", "MIT");
    expect(auditNativeLicenses(input).blockers[0]).toMatchObject({ package: "removed@1.0.0", reason: "reviewed package removed; inventory change requires review" });
  });

  it("does not pass an empty or incomplete graph", () => {
    const input = fixture();
    input.metadata.resolve.nodes.splice(1, 1);
    expect(() => auditNativeLicenses(input)).toThrow("Incomplete Cargo dependency graph");
  });

  it("rejects an active graph that names a different project root", () => {
    const input = fixture();
    input.activeMetadata.resolve.root = "dep";
    expect(() => auditNativeLicenses(input)).toThrow("different root");
  });
});

function ownedFixture() {
  const input = fixture();
  for (const [name, path] of [
    ["obs-mcp-resolver-qualification", "resolver"],
    ["obs-mcp-platform-tls-qualification", "platform-tls"],
  ]) {
    const manifestPath = join(input.root, "native", "crates", path!, "Cargo.toml");
    createManifest(manifestPath);
    const pkg: Package = {id: name!, name: name!, version: "0.0.0", license: "MIT", source: null, manifest_path: manifestPath};
    input.metadata.packages.push(pkg);
    input.metadata.resolve.nodes.push({id: pkg.id, dependencies: []});
    input.metadata.resolve.nodes[0]!.dependencies.push(pkg.id);
    input.reviewed.set(`${pkg.name}@${pkg.version}`, "MIT");
  }
  input.activeMetadata = structuredClone(input.metadata);
  return input;
}
describe("exact owned manifest identity and optional inventory", () => {
  it("admits only the root and two reviewed canonical private crates", () => {
    const input = ownedFixture();
    expect(auditNativeLicenses(input).passed).toBe(true);
  });
  it.each([2, 3])("rejects private package %i at a foreign existing manifest", (index) => {
    const input = ownedFixture(); const path = join(input.root, "foreign", "Cargo.toml"); createManifest(path);
    input.metadata.packages[index]!.manifest_path = path; input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason: expect.stringContaining("canonical manifest")}));
  });
  it.each([2, 3])("rejects registry source for reviewed private package %i", (index) => {
    const input = ownedFixture(); input.metadata.packages[index]!.source = registry; input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:"unreviewed package source"}));
  });
  it.each([2, 3])("rejects changed private version %i", (index) => {
    const input = ownedFixture(); input.metadata.packages[index]!.version = "0.0.1"; input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:"unreviewed package/version"}));
  });
  it.each([2, 3])("rejects changed private expression %i", (index) => {
    const input = ownedFixture(); input.metadata.packages[index]!.license = "GPL-2.0-or-later"; input.activeMetadata = structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:"license expression changed after review"}));
  });
  it("does not let a reviewed lookalike name turn any local dependency into an owned crate", () => {
    const input = ownedFixture(); const pkg = input.metadata.packages[2]!;
    input.reviewed.delete(`${pkg.name}@${pkg.version}`);pkg.name += "-copy"; input.reviewed.set(`${pkg.name}@${pkg.version}`, "MIT"); input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).passed).toBe(false);
  });
  it("does not admit a registry GPL dependency even with exact inventoried terms", () => {
    expect(auditNativeLicenses(fixture("GPL-2.0-only")).blockers).toContainEqual(expect.objectContaining({reason:expect.stringContaining("copyleft")}));
  });
  it("rejects a symlink manifest even when its target is a file", () => {
    const input = ownedFixture(); const pkg = input.metadata.packages[2]!; const target=join(input.root,"foreign-Cargo.toml");createManifest(target);
    unlinkSync(pkg.manifest_path);symlinkSync(target,pkg.manifest_path);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:expect.stringContaining("symlink")}));
  });
  it("rejects a symlink private directory under the canonical spelling", () => {
    const input = ownedFixture();const path=dirname(input.metadata.packages[2]!.manifest_path);const target=join(input.root,"foreign-resolver");renameSync(path,target);symlinkSync(target,path,"dir");
    expect(auditNativeLicenses(input).passed).toBe(false);
  });
  it("rejects a project-root symlink even with matching manifest spellings", () => {
    const input=ownedFixture();const original=input.root;const alias=join(temporary,"project-alias");symlinkSync(original,alias,"dir");
    for(const pkg of input.metadata.packages) if(pkg.source===null) pkg.manifest_path=pkg.manifest_path.replace(original,alias);
    input.root=alias;input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).passed).toBe(false);
  });
  it("does not let a canonical private crate become the graph root", () => {
    const input=ownedFixture();const pkg=input.metadata.packages[2]!;
    input.metadata.resolve.root=pkg.id;input.metadata.resolve.nodes[2]!.dependencies=["root"];
    input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:"owned package has an unreviewed graph role"}));
  });
  it("rejects an owned crate when its manifest disappears", () => {
    const input=ownedFixture();unlinkSync(input.metadata.packages[3]!.manifest_path);
    expect(()=>auditNativeLicenses(input)).toThrow("Missing crate manifest: obs-mcp-platform-tls-qualification@0.0.0");
  });
  it("requires review when an optional private package disappears from the full graph", () => {
    const input=ownedFixture();const pkg=input.metadata.packages.pop()!;input.metadata.resolve.nodes.pop();input.metadata.resolve.nodes[0]!.dependencies=input.metadata.resolve.nodes[0]!.dependencies.filter(id=>id!==pkg.id);input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({package:`${pkg.name}@${pkg.version}`,reason:expect.stringContaining("removed")}));
  });
  it("reports a full-feature package as disabled in the default host projection, not another OS", () => {
    const input=ownedFixture();const optional=input.activeMetadata.packages.pop()!;input.activeMetadata.resolve.nodes.pop();input.activeMetadata.resolve.nodes[0]!.dependencies=input.activeMetadata.resolve.nodes[0]!.dependencies.filter(id=>id!==optional.id);
    const result=auditNativeLicenses(input);expect(result.passed).toBe(true);
    expect(result.packages.find((p:{package:string})=>p.package===`${optional.name}@0.0.0`)).toMatchObject({active:false,scope:"outside-default-host-projection"});
    expect(result.activeProjection).toContain("default features");expect(result.scope).toContain("all-feature, all-target");
  });
  it("does not let active-host metadata alter a source identity behind the full inventory", () => {
    const input=ownedFixture();input.activeMetadata.packages[2]!.source=registry;
    expect(()=>auditNativeLicenses(input)).toThrow("projection changed a package identity");
  });
  it("binds root admission to its exact name, role and canonical manifest too", () => {
    const input=ownedFixture();const target=join(input.root,"foreign-root.toml");createManifest(target);input.metadata.packages[0]!.manifest_path=target;input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).passed).toBe(false);
    input.metadata.packages[0]!.manifest_path=join(input.root,"native","Cargo.toml");input.metadata.packages[0]!.name="root-lookalike";input.reviewed.set("root-lookalike@1.1.1","MIT");input.activeMetadata=structuredClone(input.metadata);
    expect(auditNativeLicenses(input).blockers).toContainEqual(expect.objectContaining({reason:"unreviewed root package identity"}));
  });
});
