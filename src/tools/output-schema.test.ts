/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";
import { OBS_PROTOCOL_REQUESTS, type ObsProtocolField } from "../obs-protocol.js";
import { resultText, startMcpHarness, type McpHarness } from "../../test/support/mcp-harness.js";

type JsonSchema = {
  type?: string | string[];
  enum?: unknown[];
  properties?: Record<string, JsonSchema>;
  required?: string[];
  items?: JsonSchema;
  minimum?: number;
  anyOf?: JsonSchema[];
  additionalProperties?: JsonSchema | boolean;
};

function sampleValue(field: ObsProtocolField): unknown {
  switch (field.valueType) {
    case "String": return "sample";
    case "Number": return 1;
    case "Boolean": return true;
    case "Array<String>": return ["sample"];
    case "Array<Object>": return [{ sample: 1 }];
    case "Object": return { sample: 1 };
    default: return null;
  }
}

/** Builds valid tool arguments from a JSON Schema: required properties only. */
function sampleArgument(schema: JsonSchema): unknown {
  if (schema.enum) return schema.enum[0];
  if (schema.anyOf?.[0]) return sampleArgument(schema.anyOf[0]);
  const type = Array.isArray(schema.type) ? schema.type[0] : schema.type;
  switch (type) {
    case "string": return "sample";
    case "number":
    case "integer": return schema.minimum ?? 1;
    case "boolean": return true;
    case "array": return [];
    case "object": return Object.fromEntries(
      (schema.required ?? []).map((key) => [key, sampleArgument(
        schema.properties?.[key]
          ?? (typeof schema.additionalProperties === "object" ? schema.additionalProperties : {}),
      )]),
    );
    default: return "sample";
  }
}

let harness: McpHarness;

beforeAll(async () => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
  harness = await startMcpHarness();
  for (const request of OBS_PROTOCOL_REQUESTS) {
    if (request.requestType === "GetVersion") continue; // The fake answers with its advertised requests.
    const response = Object.fromEntries(
      request.responseFields.map((field) => [field.valueName, sampleValue(field)]),
    );
    harness.fakeObs.respondWith(request.requestType, () => response);
  }
  harness.fakeObs.respondWith("GetSourceScreenshot", () => ({
    imageData: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==",
  }));
});

afterAll(async () => {
  await harness.close();
  vi.restoreAllMocks();
});

describe("declared output schemas", () => {
  it("are declared by every single-request tool and accept protocol-shaped responses", async () => {
    const { tools } = await harness.mcpClient.listTools();
    const withSchema = tools.filter(({ outputSchema }) => outputSchema);
    expect(withSchema.length).toBeGreaterThanOrEqual(110);

    const failures: string[] = [];
    for (const tool of withSchema) {
      const args = sampleArgument(tool.inputSchema as JsonSchema) as Record<string, unknown>;
      try {
        const result = await harness.call(tool.name, args);
        if (result.isError) failures.push(`${tool.name}: ${resultText(result)}`);
      } catch (error) {
        failures.push(`${tool.name}: ${error instanceof Error ? error.message : String(error)}`);
      }
    }
    expect(failures).toEqual([]);
  });

  it("reports a response that contradicts the protocol as a tool error", async () => {
    harness.fakeObs.respondWith("GetRecordStatus", () => ({ outputActive: "yes" }));

    const result = await harness.call("obs-get-record-status");

    expect(result.isError).toBe(true);
    expect(resultText(result)).toMatch(/Output validation error.*outputActive/);
  });

  it("accepts null for fields the protocol does not document as nullable", async () => {
    harness.fakeObs.respondWith("GetSpecialInputs", () => ({ desktop1: "Desktop Audio", desktop2: null }));

    const result = await harness.call("obs-get-special-inputs");

    expect(result.isError).toBeFalsy();
  });

  it("accepts responses from older OBS versions that omit newer fields", async () => {
    harness.fakeObs.respondWith("GetSceneList", () => ({ scenes: [], currentProgramSceneName: "Main" }));

    const result = await harness.call("obs-get-scene-list");

    expect(result.isError).toBeFalsy();
  });
});
