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

const PROPERTY_ROUTES = ["obs-get-input-property-list-items", "obs-call-request", "obs-batch"] as const;
function propertyArgs(tool: string, propertyName: string) {
  const requestData = { inputName: "Capture", propertyName };
  return tool === "obs-call-request" ? { requestType: "GetInputPropertiesListPropertyItems", requestData }
    : tool === "obs-batch" ? { requests: [{ requestType: "GetInputPropertiesListPropertyItems", requestData }] } : requestData;
}

describe("ScreenCaptureKit property serialization crash guard", () => {
  it.each(PROPERTY_ROUTES.flatMap((tool) => ["display_uuid", "application"].flatMap((propertyName) => [false, true].map((configured) => ({ tool, propertyName, configured })))))
    ("withholds $propertyName through $tool even with configured display=$configured", async ({ tool, propertyName, configured }) => {
      harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "screen_capture", inputSettings: { type: 2, ...(configured ? { display_uuid: "37D8832A" } : {}) } }));
      harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [] }));
      const result = await harness.call(tool, propertyArgs(tool, propertyName));
      expect(result.isError).toBe(true);
      expect(resultText(result)).toMatch(/crash|safely|separately/);
      expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
    });

  it.each(PROPERTY_ROUTES.slice(0, 2))("allows the same string list on an unrelated source through %s", async (tool) => {
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "other_source", inputSettings: {} }));
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Valid", itemValue: "valid", itemEnabled: true }] }));
    const result = await harness.call(tool, propertyArgs(tool, "application"));
    expect(result.isError).toBeFalsy();
    expect(requestTypes()).toContain("GetInputPropertiesListPropertyItems");
  });

  it.each(PROPERTY_ROUTES)("allows safe integer window lists through %s", async (tool) => {
    harness.fakeObs.respondWith("GetInputPropertiesListPropertyItems", () => ({ propertyItems: [{ itemName: "Window", itemValue: 17, itemEnabled: true }] }));
    const result = await harness.call(tool, propertyArgs(tool, "window"));
    expect(result.isError).toBeFalsy();
    expect(requestTypes()).toContain("GetInputPropertiesListPropertyItems");
    expect(requestTypes()).not.toContain("GetInputSettings");
  });

  it.each(PROPERTY_ROUTES.slice(0, 2))("withholds string discovery if source kind cannot be established through %s", async (tool) => {
    harness.fakeObs.respondWith("GetInputSettings", () => ({}));
    const result = await harness.call(tool, propertyArgs(tool, "display_uuid"));
    expect(result.isError).toBe(true);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
  });

  it("refuses an unsafe batch before an earlier source mutation can run", async () => {
    const result = await harness.call("obs-batch", { requests: [
      { requestType: "SetInputSettings", requestData: { inputName: "Capture", inputSettings: { display_uuid: "chosen" } } },
      { requestType: "GetInputPropertiesListPropertyItems", requestData: { inputName: "Capture", propertyName: "display_uuid" } },
    ] });
    expect(result.isError).toBe(true);
    expect(requestTypes()).not.toContain("SetInputSettings");
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
  });
});

describe("XComposite enumeration crash guard", () => {
  it.each(["obs-get-input-property-list-items", "obs-call-request", "obs-batch"])("blocks unconfigured window enumeration through %s", async (tool) => {
    harness.fakeObs.respondWith("GetInputSettings", () => ({ inputKind: "xcomposite_input", inputSettings: { capture_window: "" } }));
    const data = { inputName: "Window", propertyName: "capture_window" };
    const args = tool === "obs-call-request" ? { requestType: "GetInputPropertiesListPropertyItems", requestData: data }
      : tool === "obs-batch" ? { requests: [{ requestType: "GetInputPropertiesListPropertyItems", requestData: data }] } : data;
    const result = await harness.call(tool, args);
    expect(result.isError).toBe(true);
    expect(requestTypes()).not.toContain("GetInputPropertiesListPropertyItems");
  });
});
