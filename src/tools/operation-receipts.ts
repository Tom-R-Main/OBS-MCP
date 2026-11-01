/* SPDX-License-Identifier: GPL-2.0-only */
import crypto from "node:crypto";
import { closeSync, fsyncSync, mkdirSync, openSync, readdirSync, readFileSync, renameSync, unlinkSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { z } from "zod";
import type { OBSWebSocketClient } from "../client.js";
import { stateDirectory } from "../state-dir.js";

const phaseSchema = z.enum(["prepared", "dispatching", "accepted", "observed", "not-dispatched", "unknown"]);
const receiptSchema = z.object({
  version: z.literal(1), operationId: z.uuid(),
  kind: z.enum(["stage-scene", "take-scene", "record-start", "record-stop"]),
  endpointHash: z.string().regex(/^[a-f0-9]{64}$/),
  collectionHash: z.string().regex(/^[a-f0-9]{64}$/).optional(),
  sceneUuid: z.string().min(1).max(256).optional(), stageId: z.uuid().optional(), takeId: z.string().min(1).max(256).optional(),
  generation: z.number().int().nonnegative(), createdAt: z.iso.datetime(), updatedAt: z.iso.datetime(),
  phase: phaseSchema, requestDispatched: z.boolean().nullable(), requestAccepted: z.boolean().nullable(),
  postconditionObserved: z.boolean(), renderedComposition: z.literal("unverified"),
}).strict();
export type OperationReceipt = z.infer<typeof receiptSchema>;
type Details = { sceneCollection?: string; sceneUuid?: string; stageId?: string; takeId?: string };
type Update = Pick<OperationReceipt, "phase"> & Partial<Pick<OperationReceipt, "requestDispatched" | "requestAccepted" | "postconditionObserved">>;
const hash = (value: string) => crypto.createHash("sha256").update(value).digest("hex");
const directory = () => join(stateDirectory(), "operation-receipts");

export class OperationReceiptError extends Error {
  readonly reason = "receipt_unavailable";
  constructor() { super("Operation receipt could not be persisted or inspected; inspect obs-observe before any further production change"); }
}

/** Durable before dispatch. Only identity metadata is retained: no endpoint URLs,
 * source settings, screenshots, request payloads, or exception text. */
function persist(receipt: OperationReceipt): OperationReceipt {
  const checked = receiptSchema.parse(receipt);
  let temporary: string | undefined;
  try {
    mkdirSync(directory(), { recursive: true, mode: 0o700 });
    const destination = join(directory(), `${checked.operationId}.json`);
    temporary = `${destination}.${process.pid}.${crypto.randomUUID()}.tmp`;
    const file = openSync(temporary, "wx", 0o600);
    try { writeFileSync(file, JSON.stringify(checked)); fsyncSync(file); } finally { closeSync(file); }
    renameSync(temporary, destination);
    temporary = undefined;
    for (const path of [directory(), stateDirectory()]) {
      const file = openSync(path, "r");
      try { fsyncSync(file); } finally { closeSync(file); }
    }
    return checked;
  } catch { throw new OperationReceiptError(); }
  finally { if (temporary) { try { unlinkSync(temporary); } catch { /* Only our uncommitted temporary file. */ } } }
}

export function prepareOperation(client: OBSWebSocketClient, kind: OperationReceipt["kind"], details: Details = {}): OperationReceipt {
  const now = new Date().toISOString();
  return persist({ version: 1, operationId: crypto.randomUUID(), kind,
    endpointHash: hash(client.getConnectionStatus().url),
    ...(details.sceneCollection === undefined ? {} : { collectionHash: hash(details.sceneCollection) }),
    ...(details.sceneUuid === undefined ? {} : { sceneUuid: details.sceneUuid }),
    ...(details.stageId === undefined ? {} : { stageId: details.stageId }),
    ...(details.takeId === undefined ? {} : { takeId: details.takeId }),
    generation: client.getConnectionStatus().generation, createdAt: now, updatedAt: now,
    phase: "prepared", requestDispatched: false, requestAccepted: null,
    postconditionObserved: false, renderedComposition: "unverified" });
}

export function updateOperation(receipt: OperationReceipt, update: Update): OperationReceipt {
  return persist({ ...receipt, ...update, updatedAt: new Date().toISOString() });
}

/** Reading never reconciles historical dispatch from present state. A process
 * may have died after an effect but before its reply or final durable write. */
export function operationSummary(client: OBSWebSocketClient) {
  try {
    let names: string[];
    try { names = readdirSync(directory()); }
    catch (error) { if ((error as NodeJS.ErrnoException).code === "ENOENT") return { status: "observed", operations: [], total: 0, truncated: false }; throw error; }
    names = names.filter((name) => name.endsWith(".json"));
    if (names.length > 1024) throw new OperationReceiptError();
    const operations = names.map((name) => {
      if (!z.uuid().safeParse(name.slice(0, -5)).success) throw new OperationReceiptError();
      const bytes = readFileSync(join(directory(), name));
      if (bytes.length > 16_384) throw new OperationReceiptError();
      const receipt = receiptSchema.parse(JSON.parse(bytes.toString("utf8")) as unknown);
      if (`${receipt.operationId}.json` !== name) throw new OperationReceiptError();
      return receipt;
    }).filter((receipt) => receipt.endpointHash === hash(client.getConnectionStatus().url))
      .sort((a, b) => b.createdAt.localeCompare(a.createdAt) || a.operationId.localeCompare(b.operationId))
      .map(({ endpointHash: _endpoint, ...receipt }) => ({ ...receipt,
        status: ["dispatching", "accepted", "unknown"].includes(receipt.phase) ? "unknown" : receipt.phase,
        nextCheck: ["dispatching", "accepted", "unknown"].includes(receipt.phase)
          ? "Inspect fresh OBS state; this receipt does not authorize replay or prove a rendered result"
          : "Current OBS state and rendered composition require separate inspection" }));
    // Unknown operations must not disappear behind a page of newer successes.
    const pending = operations.filter((receipt) => receipt.status === "unknown");
    const selected = [...pending, ...operations.filter((receipt) => receipt.status !== "unknown")].slice(0, 64);
    return { status: "observed", operations: selected, total: operations.length, truncated: operations.length > selected.length };
  } catch { return { status: "unknown", operations: [], reason: "receipt_unavailable", nextCheck: "Operation receipts could not be inspected; do not infer that no operation is pending" }; }
}
