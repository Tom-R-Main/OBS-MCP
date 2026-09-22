/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

let harness: McpHarness;

function requestTypes(): unknown[] {
  return harness.fakeObs.history().map(({ frame }) => frame.d.requestType).filter(Boolean);
}

beforeEach(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness({
    availableRequests: ["GetInputSettings", "GetInputPropertiesListPropertyItems"],
  });
});

afterEach(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

describe("obs-get-input-property-list-items", () => {
  it("refuses to list applications on a screen capture with no display", async () => {
    harness.fakeObs.respondWith("GetInputSettings", () => ({
      inputKind: "screen_capture",
      inputSettings: { type: 2 },
    }));

    const result = await harness.call("obs-get-input-property-list-items", {
      inputName: "Chrome",
      propertyName: "application",
    });

    expect(result.isError).toBe(true);
    expect(resultText(result)).toContain("display_uuid");
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
  });

  it("lists applications once a display is selected", async () => {
    harness.fakeObs.respondWith("GetInputSettings", () => ({
      inputKind: "screen_capture",
      inputSettings: { type: 2, display_uuid: "37D8832A" },
    }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({
      propertyItems: [{ itemName: "Google Chrome", itemValue: "com.google.Chrome", itemEnabled: true }],
    }));

    const result = await harness.call("obs-get-input-property-list-items", {
      inputName: "Chrome",
      propertyName: "application",
    });

    expect(result.isError).toBeFalsy();
    expect(resultText(result)).toContain("com.google.Chrome");
  });

  it("does not inspect settings for other list properties", async () => {
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [] }));

    const result = await harness.call("obs-get-input-property-list-items", {
      inputName: "Mic",
      propertyName: "device_id",
    });

    expect(result.isError).toBeFalsy();
    expect(requestTypes()).not.toContain("GetInputSettings");
  });
});
