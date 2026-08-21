/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import type { CallToolResult } from "@modelcontextprotocol/server";

const DEFAULT_MAX_SCREENSHOT_BYTES = 4 * 1024 * 1024;
const HARD_MAX_SCREENSHOT_BYTES = 6 * 1024 * 1024;
const SUPPORTED_IMAGE_TYPES = new Set(["image/png", "image/jpeg", "image/webp"]);

type ScreenshotResponse = { imageData: string };

function isScreenshotResponse(value: unknown): value is ScreenshotResponse {
  return (
    typeof value === "object"
    && value !== null
    && "imageData" in value
    && typeof value.imageData === "string"
  );
}

export function screenshotByteLimit(value = process.env.OBS_MCP_MAX_SCREENSHOT_BYTES): number {
  if (value === undefined || value === "") return DEFAULT_MAX_SCREENSHOT_BYTES;
  if (!/^[1-9]\d*$/.test(value)) {
    throw new Error("OBS_MCP_MAX_SCREENSHOT_BYTES must be a positive integer");
  }

  const bytes = Number(value);
  if (!Number.isSafeInteger(bytes) || bytes > HARD_MAX_SCREENSHOT_BYTES) {
    throw new Error(
      `OBS_MCP_MAX_SCREENSHOT_BYTES cannot exceed ${HARD_MAX_SCREENSHOT_BYTES}`,
    );
  }
  return bytes;
}

export function screenshotResult(
  response: unknown,
  maxBytes = screenshotByteLimit(),
): CallToolResult {
  if (!isScreenshotResponse(response)) {
    throw new Error("OBS returned a screenshot response without imageData");
  }

  const match = /^data:(image\/(?:png|jpeg|webp));base64,([A-Za-z0-9+/]*={0,2})$/i.exec(
    response.imageData,
  );
  if (!match) {
    throw new Error("OBS returned an invalid or unsupported screenshot data URL");
  }

  const mimeType = match[1]?.toLowerCase();
  const data = match[2];
  if (!mimeType || !data || !SUPPORTED_IMAGE_TYPES.has(mimeType) || data.length % 4 !== 0) {
    throw new Error("OBS returned invalid screenshot image data");
  }

  const padding = data.endsWith("==") ? 2 : data.endsWith("=") ? 1 : 0;
  const sizeBytes = (data.length / 4) * 3 - padding;
  if (sizeBytes > maxBytes) {
    throw new Error(`OBS screenshot is ${sizeBytes} bytes; limit is ${maxBytes} bytes`);
  }

  return {
    content: [
      {
        type: "text",
        text: `OBS screenshot (${mimeType}, ${sizeBytes} bytes)`,
      },
      { type: "image", data, mimeType },
    ],
    structuredContent: { mimeType, sizeBytes },
  };
}
