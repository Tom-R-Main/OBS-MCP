/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { createRequire } from "node:module";

type PackageMetadata = {
  version?: unknown;
};

const packageMetadata: PackageMetadata = createRequire(import.meta.url)("../package.json");

if (typeof packageMetadata.version !== "string") {
  throw new Error("package.json must contain a string version");
}

export const PACKAGE_VERSION = packageMetadata.version;
