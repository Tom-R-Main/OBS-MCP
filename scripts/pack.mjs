#!/usr/bin/env node
/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import {
  cpSync,
  mkdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { listTools } from "./list-tools.mjs";

const require = createRequire(import.meta.url);
const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");

function npmCommand() {
  return process.platform === "win32" ? "npm.cmd" : "npm";
}

function mcpbCliPath() {
  return resolve(dirname(require.resolve("@anthropic-ai/mcpb")), "cli/cli.js");
}

export async function pack({
  root = defaultRoot,
  output = resolve(root, "dist/obs-studio.mcpb"),
  keepStaging = false,
} = {}) {
  const staging = resolve(root, ".pack-staging");
  console.log("Preparing staging directory...");
  rmSync(staging, { recursive: true, force: true });
  mkdirSync(staging, { recursive: true });

  try {
    console.log("Building server...");
    execFileSync(npmCommand(), ["run", "build"], { cwd: root, stdio: "inherit" });

    for (const file of [
      "package.json",
      "package-lock.json",
      "icon.png",
      "LICENSE",
      "README.md",
      "NOTICE.md",
      "tsconfig.json",
    ]) {
      cpSync(resolve(root, file), resolve(staging, file), { force: true });
    }
    for (const directory of ["src", "scripts"]) {
      cpSync(resolve(root, directory), resolve(staging, directory), {
        recursive: true,
        force: true,
      });
    }
    // MCPB excludes build metadata by default. GPL source distributions need
    // the pinned dependency graph and compiler configuration to be rebuildable.
    writeFileSync(
      resolve(staging, ".mcpbignore"),
      "!package-lock.json\n!tsconfig.json\n",
    );
    mkdirSync(resolve(staging, "docs"), { recursive: true });
    cpSync(
      resolve(root, "docs/protocol.json"),
      resolve(staging, "docs/protocol.json"),
      { force: true },
    );

    console.log("Generating tools list from server...");
    const tools = await listTools(root);
    const { version } = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
    const manifest = JSON.parse(readFileSync(resolve(root, "manifest.json"), "utf8"));
    manifest.version = version;
    manifest.tools = tools;
    manifest.tools_generated = true;
    writeFileSync(
      resolve(staging, "manifest.json"),
      `${JSON.stringify(manifest, null, 2)}\n`,
    );
    console.log(`Found ${tools.length} tools.`);
    cpSync(resolve(root, "build"), resolve(staging, "server"), {
      recursive: true,
      force: true,
    });

    console.log("Installing production dependencies...");
    execFileSync(npmCommand(), ["ci", "--omit=dev"], {
      cwd: staging,
      stdio: "inherit",
    });

    console.log("Packing...");
    mkdirSync(dirname(output), { recursive: true });
    rmSync(output, { force: true });
    execFileSync(process.execPath, [mcpbCliPath(), "pack", ".", output], {
      cwd: staging,
      stdio: "inherit",
    });
    console.log(`Done: ${output}`);
    return { output, staging };
  } finally {
    if (!keepStaging) rmSync(staging, { recursive: true, force: true });
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await pack();
}
