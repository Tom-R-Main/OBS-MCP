/* SPDX-License-Identifier: GPL-2.0-only */
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";
// @ts-expect-error Build-time JavaScript gate intentionally has no production TS API.
import { auditNativeLicenses } from "../../scripts/check-native-licenses.mjs";

type Package = { id: string; name: string; version: string; license: string | null; source: string | null; manifest_path: string };
const manifest = fileURLToPath(import.meta.url);
const registry = "registry+https://github.com/rust-lang/crates.io-index";
function fixture(expression = "MIT OR Apache-2.0") {
  const root: Package = { id: "root", name: "obs-mcp-native", version: "1.1.1", license: "GPL-2.0-only", source: null, manifest_path: manifest };
  const dep: Package = { id: "dep", name: "fixture-crate", version: "1.0.0", license: expression, source: registry, manifest_path: manifest };
  const metadata = { packages: [root, dep], resolve: { root: "root", nodes: [{ id: "root", dependencies: ["dep"] }, { id: "dep", dependencies: [] as string[] }] } };
  const reviewed = new Map([["obs-mcp-native@1.1.1", "GPL-2.0-only"], ["fixture-crate@1.0.0", expression]]);
  return { metadata, activeMetadata: structuredClone(metadata), target: "fixture-target", reviewed };
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
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("license expression changed after review");
  });

  it("does not interpret missing license metadata as permissive", () => {
    const input = fixture();
    input.metadata.packages[1]!.license = null;
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("missing license metadata");
  });

  it("does not admit a different source with the same reviewed name/version", () => {
    const input = fixture();
    input.metadata.packages[1]!.source = "git+https://example.invalid/replacement";
    expect(auditNativeLicenses(input).blockers[0].reason).toBe("unreviewed package source");
  });

  it("fails when the crate source manifest is missing instead of trusting index metadata", () => {
    const input = fixture();
    input.metadata.packages[1]!.manifest_path = `${manifest}.missing-crate-manifest`;
    expect(() => auditNativeLicenses(input)).toThrow("Missing crate manifest: fixture-crate@1.0.0");
  });

  it("checks target-only dependencies and reports that they are not active on this host", () => {
    const input = fixture("Apache-2.0 AND ISC");
    input.activeMetadata.packages.splice(1, 1);
    input.activeMetadata.resolve.nodes.splice(1, 1);
    input.activeMetadata.resolve.nodes[0]!.dependencies = [];
    const result = auditNativeLicenses(input);
    expect(result).toMatchObject({ passed: false, packagesChecked: 2, activeTargetPackages: 1, otherTargetPackages: 1 });
    expect(result.blockers[0]).toMatchObject({ active: false, scope: "other-target" });
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
