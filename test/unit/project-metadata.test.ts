/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { readFile, readdir } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

type PackageMetadata = {
  version: string;
  engines: { node: string };
};

type ManifestMetadata = {
  version: string;
  long_description: string;
  compatibility: { runtimes: { node: string } };
};

type PackageLockMetadata = {
  version: string;
  packages: {
    "": {
      version: string;
      engines: { node: string };
    };
  };
};

const projectRoot = fileURLToPath(new URL("../../", import.meta.url));
const readProjectFile = (relativePath: string): Promise<string> =>
  readFile(path.join(projectRoot, relativePath), "utf8");

async function sourceFiles(directory: string): Promise<string[]> {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = await Promise.all(entries.map(async (entry) => {
    const absolutePath = path.join(directory, entry.name);
    if (entry.isDirectory()) return sourceFiles(absolutePath);
    return entry.isFile() && entry.name.endsWith(".ts") && !entry.name.endsWith(".test.ts")
      ? [absolutePath]
      : [];
  }));
  return files.flat();
}

describe("project metadata", () => {
  it("keeps package and manifest versions aligned", async () => {
    const packageMetadata = JSON.parse(await readProjectFile("package.json")) as PackageMetadata;
    const packageLock = JSON.parse(await readProjectFile("package-lock.json")) as PackageLockMetadata;
    const manifest = JSON.parse(await readProjectFile("manifest.json")) as ManifestMetadata;

    expect(manifest.version).toBe(packageMetadata.version);
    expect(packageLock.version).toBe(packageMetadata.version);
    expect(packageLock.packages[""].version).toBe(packageMetadata.version);
  });

  it("keeps the Node 20.19 floor aligned across published surfaces and CI", async () => {
    const packageMetadata = JSON.parse(await readProjectFile("package.json")) as PackageMetadata;
    const packageLock = JSON.parse(await readProjectFile("package-lock.json")) as PackageLockMetadata;
    const manifest = JSON.parse(await readProjectFile("manifest.json")) as ManifestMetadata;
    const readme = await readProjectFile("README.md");
    const workflow = await readProjectFile(".github/workflows/ci.yml");

    expect(packageMetadata.engines.node).toBe(">=20.19");
    expect(packageLock.packages[""].engines.node).toBe(packageMetadata.engines.node);
    expect(manifest.compatibility.runtimes.node).toBe(">=20.19.0");
    expect(manifest.long_description).toContain("Node.js 20.19 or newer");
    expect(readme).toContain("Node.js 20.19 or newer");
    expect(workflow).toMatch(/node:\s*\[20\.19\.0,\s*22\.x,\s*24\.x\]/);
  });
});

describe("stdio source safety", () => {
  it("does not write application diagnostics to stdout", async () => {
    const files = await sourceFiles(path.join(projectRoot, "src"));
    const violations: string[] = [];

    await Promise.all(files.map(async (file) => {
      const source = await readFile(file, "utf8");
      if (/console\.(?:log|info|debug)\s*\(|process\.stdout\.write\s*\(/.test(source)) {
        violations.push(path.relative(projectRoot, file));
      }
    }));

    expect(violations.sort()).toEqual([]);
  });
});
