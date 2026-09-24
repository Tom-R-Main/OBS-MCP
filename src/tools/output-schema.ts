/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { z } from "zod";
import { getObsProtocolRequest, type ObsProtocolField } from "../obs-protocol.js";

function fieldSchema(field: ObsProtocolField): z.ZodType {
  let schema: z.ZodType;
  switch (field.valueType) {
    case "String": schema = z.string(); break;
    case "Number": schema = z.number(); break;
    case "Boolean": schema = z.boolean(); break;
    case "Array<String>": schema = z.array(z.string()); break;
    case "Array<Object>": schema = z.array(z.record(z.string(), z.unknown())); break;
    case "Object": schema = z.record(z.string(), z.unknown()); break;
    default: return z.unknown().optional().describe(field.valueDescription);
  }
  // Nullable and optional: the protocol documents nulls only in prose and not
  // always (GetSpecialInputs returns null for unset devices), and older OBS
  // versions omit fields added later. A stricter schema would turn a working
  // call into an output validation error.
  return schema.nullable().optional().describe(field.valueDescription);
}

/**
 * The structuredContent shape of a tool that returns an OBS response as-is,
 * derived from the pinned protocol. Extra fields are allowed so newer OBS
 * versions that add response fields still validate.
 */
export function responseOutputSchema(requestType: string): z.ZodObject | undefined {
  const request = getObsProtocolRequest(requestType);
  if (!request) return undefined;
  const shape = Object.fromEntries(
    request.responseFields.map((field) => [field.valueName, fieldSchema(field)]),
  );
  return z.looseObject(shape);
}

/** The structuredContent shape of a tool that answers with a confirmation. */
export const MESSAGE_OUTPUT_SCHEMA = z.object({
  message: z.string().describe("Confirmation of the completed action"),
});
