/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, describe, expect, it, vi } from "vitest";
import { logger } from "./logger.js";

afterEach(() => {
  vi.unstubAllEnvs();
  vi.restoreAllMocks();
});

describe("logger", () => {
  it("writes info and errors to stderr but hides debug output by default", () => {
    const stderr = vi.spyOn(console, "error").mockImplementation(() => undefined);
    const stdout = vi.spyOn(console, "log").mockImplementation(() => undefined);

    logger.debug("retrying");
    logger.log("connected");
    logger.error("failed");

    expect(stderr.mock.calls.map(([message]) => message)).toEqual([
      "[obs-mcp] connected",
      "[obs-mcp] error: failed",
    ]);
    expect(stdout).not.toHaveBeenCalled();
  });

  it("honours OBS_MCP_LOG_LEVEL", () => {
    const stderr = vi.spyOn(console, "error").mockImplementation(() => undefined);

    vi.stubEnv("OBS_MCP_LOG_LEVEL", "debug");
    logger.debug("retrying");
    vi.stubEnv("OBS_MCP_LOG_LEVEL", "silent");
    logger.error("failed");

    expect(stderr.mock.calls.map(([message]) => message)).toEqual(["[obs-mcp] debug: retrying"]);
  });
});
