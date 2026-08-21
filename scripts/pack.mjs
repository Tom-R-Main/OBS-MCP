#!/usr/bin/env node
/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { execFileSync } from "node:child_process";
import { cpSync, mkdirSync, rmSync, readFileSync, writeFileSync } from "node:fs";
import { listTools } from "./list-tools.mjs";
import { resolve, dirname } from "path";
import { fileURLToPath } from "url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const staging = resolve(root, ".pack-staging");
const output = resolve(root, "dist/obs-studio.mcpb");

console.log("Preparing staging directory...");
rmSync(staging, { recursive: true, force: true });
mkdirSync(staging, { recursive: true });

try {
  console.log("Building server...");
  execFileSync("npm", ["run", "build"], { cwd: root, stdio: "inherit" });

  // Include both the runnable server and its corresponding source.
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
  mkdirSync(resolve(staging, "docs"), { recursive: true });
  cpSync(resolve(root, "docs/protocol.json"), resolve(staging, "docs/protocol.json"), { force: true });

  // Generate the public MCP tool inventory and inject it into the manifest.
  console.log("Generating tools list from server...");
  const tools = await listTools(root);
  const { version } = JSON.parse(readFileSync(resolve(root, "package.json"), "utf8"));
  const manifest = JSON.parse(readFileSync(resolve(root, "manifest.json"), "utf8"));
  manifest.version = version;
  manifest.tools = tools;
  manifest.tools_generated = true;
  writeFileSync(resolve(staging, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(`Found ${tools.length} tools.`);
  cpSync(resolve(root, "build"), resolve(staging, "server"), { recursive: true, force: true });

  console.log("Installing production dependencies...");
  execFileSync("npm", ["ci", "--omit=dev"], { cwd: staging, stdio: "inherit" });

  console.log("Packing...");
  mkdirSync(dirname(output), { recursive: true });
  rmSync(output, { force: true });
  execFileSync(resolve(root, "node_modules/.bin/mcpb"), ["pack", ".", output], {
    cwd: staging,
    stdio: "inherit",
  });
} finally {
  rmSync(staging, { recursive: true, force: true });
}

console.log(`Done: ${output}`);
