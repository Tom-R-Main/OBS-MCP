/* SPDX-License-Identifier: GPL-2.0-only */
import { chmodSync, existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
// Public pack entry point is exercised with real gate subprocesses, before any
// dependency installation, output replacement or staging cleanup can occur.
import { pack } from "../../scripts/pack.mjs";
const roots: string[] = [];
afterEach(() => { vi.unstubAllEnvs(); for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });
describe.skipIf(!["darwin", "linux"].includes(process.platform))("native package admission", () => {
  it.each(["license", "migration"])("refuses %s failure before changing staging or replacing an artifact", async failure => {
    const root = mkdtempSync(join(tmpdir(), "obs-native-pack-gate-")); roots.push(root);
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
    const npm = join(root, "bin/npm"); writeFileSync(npm, "#!/usr/bin/env node\n" + script("build", 0)); chmodSync(npm, 0o755);
    vi.stubEnv("PATH", `${join(root, "bin")}:${process.env.PATH ?? ""}`);
    await expect(pack({ root, nativeBundle: true, output, offline: true })).rejects.toThrow();
    expect(readFileSync(trace, "utf8").trim().split("\n")).toEqual(failure === "license" ? ["license"] : ["license", "build", "migration"]);
    expect(readFileSync(marker, "utf8")).toBe("preserved"); expect(readFileSync(output, "utf8")).toBe("preceding artifact");
    expect(existsSync(join(root, "node_modules"))).toBe(false);
  });
});
