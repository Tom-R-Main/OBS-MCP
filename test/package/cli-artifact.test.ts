/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { constants } from "node:fs";
import { access } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { describe, expect, it } from "vitest";

const cliPath = fileURLToPath(new URL("../../build/index.js", import.meta.url));

describe("compiled CLI artifact", () => {
  it("exists and is executable after a build", async () => {
    await expect(access(cliPath, constants.F_OK | constants.X_OK)).resolves.toBeUndefined();
  });
});
