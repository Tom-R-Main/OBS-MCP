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
import { checkDependencies } from "./check-dependencies.mjs";

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
  nativeBundle = false,
  offline = false,
  output = resolve(root, nativeBundle ? "dist/obs-studio-native.mcpb" : "dist/obs-studio.mcpb"),
  keepStaging = false,
} = {}) {
  checkDependencies({ root });
  if (nativeBundle) {
    if (!["darwin", "linux"].includes(process.platform)) throw new Error("Native MCPB packaging currently accepts only macOS/Linux build hosts");
    execFileSync(process.execPath, ["scripts/check-native-licenses.mjs"], { cwd: root, stdio: "inherit" });
    execFileSync(npmCommand(), ["run", "native:build"], { cwd: root, stdio: "inherit" });
    execFileSync(process.execPath, ["scripts/check-native-migration.mjs"], { cwd: root, stdio: "inherit" });
  }
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
      "!package-lock.json\n!tsconfig.json\n!.agents/\n!.agents/skills/\n!.agents/skills/obs-demo/\n!.agents/skills/obs-demo/SKILL.md\n",
    );
    mkdirSync(resolve(staging, "docs"), { recursive: true });
    cpSync(resolve(root, "docs/production-assistant.md"), resolve(staging, "docs/production-assistant.md"));
    cpSync(resolve(root, "docs/recipes"), resolve(staging, "docs/recipes"), { recursive: true });
    mkdirSync(resolve(staging, ".agents/skills"), { recursive: true });
    cpSync(resolve(root, ".agents/skills/obs-demo"), resolve(staging, ".agents/skills/obs-demo"), { recursive: true });
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
    if (nativeBundle) {
      manifest.name = "obs-mcp-native";
      manifest.display_name = "OBS MCP Native";
      manifest.description = "Rust discovery and read-only core with exclusive workflow compatibility runtime";
      manifest.long_description = `Native host build for ${process.platform}/${process.arch}. Rust serves discovery, offline prompts, resource inventories, eligible read-only requests, request description, scene planning and structural audit. Stateful workflows, HTTP, and unsupported protocol features use the included Node.js compatibility server. Node.js >=20.19 remains required for those workflows. This is a gradual migration with all tools retained; see docs/native-architecture.md.`;
      manifest.compatibility.platforms = [process.platform];
      manifest.server.type = "binary";
      manifest.server.entry_point = "native/obs-mcp-native";
      manifest.server.mcp_config.command = "${__dirname}/native/obs-mcp-native";
      manifest.server.mcp_config.args = [];
      manifest.server.mcp_config.env.OBS_MCP_COMPAT_ENTRY = "${__dirname}/server/index.js";
      mkdirSync(resolve(staging, "native"), { recursive: true });
      cpSync(resolve(root, "native/target/release/obs-mcp-native"), resolve(staging, "native/obs-mcp-native"));
      for (const file of ["Cargo.toml", "Cargo.lock", "rust-toolchain.toml"]) cpSync(resolve(root, "native", file), resolve(staging, "native", file));
      for (const directory of ["src", "assets"]) cpSync(resolve(root, "native", directory), resolve(staging, "native", directory), { recursive: true });
      for (const file of ["native-architecture.md", "native-dependencies.md", "full-rust-cutover.md", "native-migration-ledger.json"]) cpSync(resolve(root, "docs", file), resolve(staging, "docs", file));
    }
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
    execFileSync(npmCommand(), ["ci", "--omit=dev", "--ignore-scripts", "--no-audit", ...(offline ? ["--offline"] : [])], {
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
  await pack({ nativeBundle: process.argv.includes("--native"), offline: process.argv.includes("--offline") });
}
