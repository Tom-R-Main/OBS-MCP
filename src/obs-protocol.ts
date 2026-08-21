import { createRequire } from "node:module";

export type ObsProtocolField = {
  valueName: string;
  valueType: string;
  valueDescription: string;
  valueRestrictions?: string | null;
  valueOptional?: boolean;
  valueOptionalBehavior?: string | null;
};

export type ObsProtocolRequest = {
  requestType: string;
  description: string;
  category: string;
  deprecated: boolean;
  initialVersion: string;
  requestFields: ObsProtocolField[];
  responseFields: ObsProtocolField[];
};

type ObsProtocolDocument = {
  requests?: unknown;
};

const protocolDocument: ObsProtocolDocument = createRequire(import.meta.url)("../docs/protocol.json");

if (!Array.isArray(protocolDocument.requests)) {
  throw new Error("docs/protocol.json must contain a requests array");
}

function isProtocolRequest(value: unknown): value is ObsProtocolRequest {
  if (!value || typeof value !== "object") return false;
  const request = value as Record<string, unknown>;
  return typeof request.requestType === "string"
    && typeof request.description === "string"
    && typeof request.category === "string"
    && typeof request.deprecated === "boolean"
    && typeof request.initialVersion === "string"
    && Array.isArray(request.requestFields)
    && Array.isArray(request.responseFields);
}

export const OBS_PROTOCOL_REQUESTS = protocolDocument.requests.map((request) => {
  if (!isProtocolRequest(request)) {
    throw new Error("docs/protocol.json contains an invalid request definition");
  }
  return request;
});

const requestByType = new Map(
  OBS_PROTOCOL_REQUESTS.map((request) => [request.requestType, request]),
);

export function getObsProtocolRequest(requestType: string): ObsProtocolRequest | undefined {
  return requestByType.get(requestType);
}
