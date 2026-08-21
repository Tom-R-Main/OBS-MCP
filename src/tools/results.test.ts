/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { describe, expect, it } from "vitest";
import { ensureStructuredContent } from "./results.js";
import { screenshotByteLimit, screenshotResult } from "./screenshot.js";

const transparentPng =
  "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=";

describe("MCP tool results", () => {
  it("preserves object JSON as structured content", () => {
    expect(ensureStructuredContent({
      content: [{ type: "text", text: '{"scene":"Program"}' }],
    }).structuredContent).toEqual({ scene: "Program" });
  });

  it("wraps human-readable success text without changing it", () => {
    const result = ensureStructuredContent({
      content: [{ type: "text", text: "Scene changed successfully" }],
    });
    expect(result.content).toEqual([{ type: "text", text: "Scene changed successfully" }]);
    expect(result.structuredContent).toEqual({ message: "Scene changed successfully" });
  });

  it("does not add structured content to tool errors", () => {
    expect(ensureStructuredContent({
      content: [{ type: "text", text: "OBS is unavailable" }],
      isError: true,
    }).structuredContent).toBeUndefined();
  });

  it("returns screenshot bytes as image content without duplicating them in text", () => {
    const result = screenshotResult(
      { imageData: `data:image/png;base64,${transparentPng}` },
      1_000,
    );
    const text = result.content.find((block) => block.type === "text");
    const image = result.content.find((block) => block.type === "image");

    expect(text).toEqual({ type: "text", text: "OBS screenshot (image/png, 68 bytes)" });
    expect(text && "text" in text ? text.text : "").not.toContain(transparentPng);
    expect(image).toEqual({ type: "image", mimeType: "image/png", data: transparentPng });
    expect(result.structuredContent).toEqual({ mimeType: "image/png", sizeBytes: 68 });
  });

  it("rejects malformed, unsupported, and oversized screenshot payloads", () => {
    expect(() => screenshotResult({ imageData: "not-a-data-url" }, 1_000))
      .toThrow("invalid or unsupported");
    expect(() => screenshotResult({ imageData: "data:image/gif;base64,R0lGODlh" }, 1_000))
      .toThrow("invalid or unsupported");
    expect(() => screenshotResult(
      { imageData: `data:image/png;base64,${transparentPng}` },
      67,
    )).toThrow("68 bytes; limit is 67 bytes");
  });

  it("validates the configurable screenshot byte budget", () => {
    expect(screenshotByteLimit(undefined)).toBe(4 * 1024 * 1024);
    expect(screenshotByteLimit("2048")).toBe(2048);
    expect(() => screenshotByteLimit("0")).toThrow("positive integer");
    expect(() => screenshotByteLimit(String(6 * 1024 * 1024 + 1))).toThrow("cannot exceed");
  });
});
