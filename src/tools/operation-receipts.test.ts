/* SPDX-License-Identifier: GPL-2.0-only */
import { readFileSync, readdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { OBSWebSocketClient } from "../client.js";
import { stateDirectory } from "../state-dir.js";
import { operationSummary, prepareOperation, updateOperation } from "./operation-receipts.js";

describe("durable operation receipts", () => {
  it("retains pre-dispatch uncertainty for a new client incarnation without claiming delivery", () => {
    const client = new OBSWebSocketClient("ws://127.0.0.1:4455");
    const receipt = prepareOperation(client, "take-scene", { sceneCollection: "fixture", sceneUuid: "scene-id" });
    updateOperation(receipt, { phase: "dispatching", requestDispatched: null });
    const restarted = new OBSWebSocketClient("ws://127.0.0.1:4455");
    expect(operationSummary(restarted)).toMatchObject({ status: "observed", total: 1, operations: [{ operationId: receipt.operationId, phase: "dispatching", status: "unknown", requestDispatched: null, requestAccepted: null, postconditionObserved: false, renderedComposition: "unverified" }] });
    expect(operationSummary(new OBSWebSocketClient("ws://127.0.0.1:4456"))).toMatchObject({ status: "observed", operations: [] });
  });

  it("does not persist raw endpoint credentials, collection names, payloads, or rendered claims", () => {
    const client = new OBSWebSocketClient("ws://private:password@127.0.0.1:4455/?token=secret");
    const receipt = prepareOperation(client, "stage-scene", { sceneCollection: "private show title", sceneUuid: "scene-id" });
    const dir = join(stateDirectory(), "operation-receipts");
    const text = readFileSync(join(dir, readdirSync(dir)[0]!), "utf8");
    for (const value of ["password", "private show title", "secret", "ws://"]) expect(text).not.toContain(value);
    expect(JSON.parse(text)).toMatchObject({ operationId: receipt.operationId, renderedComposition: "unverified", phase: "prepared", requestDispatched: false });
    expect(operationSummary(client).operations[0]).not.toHaveProperty("endpointHash");
  });

  it("reports unreadable or malformed receipts as unavailable instead of an empty history", () => {
    const client = new OBSWebSocketClient("ws://127.0.0.1:4455");
    const receipt = prepareOperation(client, "record-start", { takeId: "take-id" });
    writeFileSync(join(stateDirectory(), "operation-receipts", `${receipt.operationId}.json`), "{}");
    expect(operationSummary(client)).toMatchObject({ status: "unknown", reason: "receipt_unavailable" });
  });
});
