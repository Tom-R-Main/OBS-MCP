/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import { readFileSync, readdirSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { getObsProtocolRequest, OBS_PROTOCOL_REQUESTS } from "./obs-protocol.js";

describe("pinned OBS WebSocket protocol", () => {
  it("contains every unique request in the OBS 32 checkout", () => {
    const requestTypes = OBS_PROTOCOL_REQUESTS.map(({ requestType }) => requestType);

    expect(requestTypes).toHaveLength(147);
    expect(new Set(requestTypes).size).toBe(requestTypes.length);
    expect(requestTypes).toContain("GetCanvasList");
    expect(requestTypes).toContain("GetSceneTransitionList");
  });

  it("exposes exact request metadata for discovery", () => {
    expect(getObsProtocolRequest("GetCanvasList")).toMatchObject({
      category: "canvases",
      deprecated: false,
      initialVersion: "5.7.0",
      requestFields: [],
    });
    expect(getObsProtocolRequest("GetTransitionList")).toBeUndefined();
  });

  it("has an explicit tool wrapper for every official request type", () => {
    const toolDirectory = new URL("./tools/", import.meta.url);
    const wrappedRequests = new Set<string>();

    for (const file of readdirSync(toolDirectory)) {
      if (!file.endsWith(".ts") || file.endsWith(".test.ts")) continue;
      const source = readFileSync(new URL(file, toolDirectory), "utf8");

      for (const pattern of [
        /sendRequest\(\s*"([^"]+)"/g,
        /requestType:\s*"([^"]+)"/g,
      ]) {
        for (const match of source.matchAll(pattern)) {
          const requestType = match[1];
          if (requestType) wrappedRequests.add(requestType);
        }
      }
    }

    // OBS only honors Sleep inside a request batch; obs-sleep waits in the server instead.
    const batchOnlyRequests = new Set(["Sleep"]);
    const officialRequests = new Set(
      OBS_PROTOCOL_REQUESTS
        .map(({ requestType }) => requestType)
        .filter((requestType) => !batchOnlyRequests.has(requestType)),
    );
    expect([...wrappedRequests].filter((name) => !officialRequests.has(name))).toEqual([]);
    expect([...officialRequests].filter((name) => !wrappedRequests.has(name))).toEqual([]);
  });
});
