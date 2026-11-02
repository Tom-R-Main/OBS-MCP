/* SPDX-License-Identifier: GPL-2.0-only */
import { spawnSync } from "node:child_process";
import { chmodSync, copyFileSync, mkdirSync, mkdtempSync, readFileSync, realpathSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const source = process.env.NATIVE_DRIVER_SOURCE_ROOT ?? fileURLToPath(new URL("../../", import.meta.url));
const fixtures: string[] = [];
afterEach(() => { for (const root of fixtures.splice(0)) rmSync(root, { recursive: true, force: true }); });

// These process shims model POSIX Cargo/npm executables. Windows native
// qualification remains separate; they cannot model CreateProcess semantics.
describe.skipIf(process.platform === "win32")("native driver toolchain selection", () => {
  it.each(["build-native.mjs", "check-native.mjs"])("runs %s Cargo commands from the pinned native directory", script => {
    const root = realpathSync(mkdtempSync(join(tmpdir(), "obs-native-driver-"))); fixtures.push(root);
    for (const path of ["scripts", "bin", "native/.cargo-home"]) mkdirSync(join(root, path), { recursive: true });
    copyFileSync(join(source, "scripts", script), join(root, "scripts", script));
    writeFileSync(join(root, "native/rust-toolchain.toml"), '[toolchain]\nchannel="1.95.0"\n');
    for (const helper of ["export-native-contract.mjs", "export-native-prompts.mjs", "check-native-licenses.mjs", "check-native-migration.mjs"]) {
      writeFileSync(join(root, "scripts", helper), "// Separate gates have their own behavioral tests.\n");
    }
    const trace = join(root, "trace.jsonl");
    for (const command of ["cargo", "npm"]) {
      const path = join(root, "bin", command);
      writeFileSync(path, '#!/usr/bin/env node\n' + `import {appendFileSync} from 'node:fs';\nimport {basename} from 'node:path';\nappendFileSync(${JSON.stringify(trace)},JSON.stringify({command:basename(process.argv[1]),cwd:process.cwd(),args:process.argv.slice(2),autoInstall:process.env.RUSTUP_AUTO_INSTALL,cache:process.env.CARGO_HOME})+'\\n');\n`);
      chmodSync(path, 0o755);
    }
    const result = spawnSync(process.execPath, [join(root, "scripts", script)], {
      cwd: root, env: { ...process.env, PATH: `${join(root, "bin")}:${process.env.PATH ?? ""}` }, encoding: "utf8",
    });
    expect(result.status, result.stderr).toBe(0);
    const commands = readFileSync(trace, "utf8").trim().split("\n").map(line => JSON.parse(line) as { command: string; cwd: string; args: string[]; autoInstall: string; cache: string });
    const cargo = commands.filter(row => row.command === "cargo");
    expect(cargo).toHaveLength(script === "build-native.mjs" ? 1 : 3);
    for (const row of cargo) {
      expect(row.cwd).toBe(join(root, "native"));
      expect(row.autoInstall).toBe("0");
      expect(row.cache).toBe(join(root, "native/.cargo-home"));
    }
    for (const row of commands.filter(row => row.command === "npm")) expect(row.cwd).toBe(root);
  });
});
