/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import { EventEmitter, once } from "node:events";
import type { AddressInfo } from "node:net";
import WebSocket, { WebSocketServer, type RawData } from "ws";

export const OBS_OP = {
  Hello: 0,
  Identify: 1,
  Identified: 2,
  Reidentify: 3,
  Event: 5,
  Request: 6,
  RequestResponse: 7,
  RequestBatch: 8,
  RequestBatchResponse: 9,
} as const;

/** Milliseconds per frame when a fake batch sleeps for frames. */
const FAKE_FRAME_MS = 1000 / 60;

type JsonObject = Record<string, unknown>;

export type OBSFrame = {
  op: number;
  d: JsonObject;
};

export type RecordedOBSFrame = {
  cursor: number;
  connectionId: number;
  frame: OBSFrame;
};

export type RecordedOBSRequest = RecordedOBSFrame & {
  frame: OBSFrame & {
    op: typeof OBS_OP.Request;
    d: JsonObject & {
      requestId: string;
      requestType: string;
    };
  };
};

export type FakeOBSOptions = {
  port?: number;
  password?: string;
  autoGetVersion?: boolean;
  availableRequests?: readonly string[];
  obsStudioVersion?: string;
  obsWebSocketVersion?: string;
  platform?: string;
  supportedImageFormats?: readonly string[];
};

type ResponseStatus = {
  result: boolean;
  code: number;
  comment?: string;
};

type ResponseAction = {
  kind: "response";
  data: JsonObject;
  status: ResponseStatus;
  delayMs: number;
};

type DisconnectAction = {
  kind: "disconnect";
  code: number;
  reason: string;
  delayMs: number;
};

type ScriptedAction = ResponseAction | DisconnectAction;

/**
 * Answers a request from test-owned state. Return response data for success,
 * or throw a FakeOBSRequestError to answer with a failed request status.
 */
export type FakeOBSResponder = (requestData: JsonObject) => JsonObject;

export class FakeOBSRequestError extends Error {
  constructor(readonly code: number, comment: string) {
    super(comment);
  }
}

type InternalRecordedFrame = RecordedOBSFrame & { socket: WebSocket };

const DEFAULT_TIMEOUT_MS = 1_000;

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseFrame(data: RawData): OBSFrame | null {
  try {
    const parsed: unknown = JSON.parse(data.toString());
    if (!isObject(parsed) || typeof parsed.op !== "number" || !isObject(parsed.d)) return null;
    return { op: parsed.op, d: parsed.d };
  } catch {
    return null;
  }
}

function expectedAuthentication(password: string, salt: string, challenge: string): string {
  const secret = crypto.createHash("sha256").update(password + salt).digest("base64");
  return crypto.createHash("sha256").update(secret + challenge).digest("base64");
}

export class FakeOBSServer {
  readonly url: string;
  readonly password: string | undefined;
  readonly salt = "fake-obs-salt";
  readonly challenge = "fake-obs-challenge";

  private readonly server: WebSocketServer;
  private readonly events = new EventEmitter();
  private readonly frames: InternalRecordedFrame[] = [];
  private readonly sockets = new Map<WebSocket, number>();
  private readonly subscriptions = new Map<number, number>();
  private readonly scripts = new Map<string, ScriptedAction[]>();
  private readonly responders = new Map<string, FakeOBSResponder>();
  private readonly timers = new Set<NodeJS.Timeout>();
  private readonly availableRequests: string[];
  private readonly autoGetVersion: boolean;
  private readonly obsStudioVersion: string;
  private readonly obsWebSocketVersion: string;
  private readonly platform: string;
  private readonly supportedImageFormats: readonly string[];
  private nextConnectionId = 1;
  private nextCursor = 1;
  private closed = false;

  private constructor(server: WebSocketServer, options: FakeOBSOptions) {
    this.server = server;
    this.password = options.password;
    this.autoGetVersion = options.autoGetVersion ?? true;
    this.obsStudioVersion = options.obsStudioVersion ?? "32.2.2";
    this.obsWebSocketVersion = options.obsWebSocketVersion ?? "5.7.0";
    this.platform = options.platform ?? "macos";
    this.supportedImageFormats = options.supportedImageFormats ?? ["png", "jpeg", "webp"];
    this.availableRequests = Array.from(new Set(["GetVersion", "Sleep", ...(options.availableRequests ?? [])]));

    const address = server.address() as AddressInfo;
    this.url = `ws://127.0.0.1:${address.port}`;
    server.on("connection", (socket) => this.handleConnection(socket));
  }

  static async start(options: FakeOBSOptions = {}): Promise<FakeOBSServer> {
    const server = new WebSocketServer({ host: "127.0.0.1", port: options.port ?? 0 });
    await once(server, "listening");
    return new FakeOBSServer(server, options);
  }

  get connectionCount(): number {
    return this.nextConnectionId - 1;
  }

  get openConnectionCount(): number {
    return this.sockets.size;
  }

  /** The event subscriptions a connection last identified with. */
  eventSubscriptions(connectionId = this.nextConnectionId - 1): number | undefined {
    return this.subscriptions.get(connectionId);
  }

  cursor(): number {
    return this.nextCursor - 1;
  }

  history(): readonly RecordedOBSFrame[] {
    return this.frames.map(({ socket: _socket, ...record }) => record);
  }

  queueSuccess(requestType: string, data: JsonObject = {}, delayMs = 0): void {
    this.queueAction(requestType, {
      kind: "response",
      data,
      status: { result: true, code: 100 },
      delayMs,
    });
  }

  queueError(requestType: string, code: number, comment: string, delayMs = 0): void {
    this.queueAction(requestType, {
      kind: "response",
      data: {},
      status: { result: false, code, comment },
      delayMs,
    });
  }

  queueDisconnect(requestType: string, reason = "scripted disconnect", delayMs = 0): void {
    this.queueAction(requestType, {
      kind: "disconnect",
      code: 1011,
      reason,
      delayMs,
    });
  }

  /**
   * Answers every unscripted request of this type from the responder, so tests
   * can model OBS state instead of scripting each reply in order. Queued
   * scripted actions still take precedence.
   */
  respondWith(requestType: string, responder: FakeOBSResponder): void {
    this.responders.set(requestType, responder);
    // Advertised on the next GetVersion, so wire responders before connecting.
    if (!this.availableRequests.includes(requestType)) this.availableRequests.push(requestType);
  }

  async waitForFrame(
    predicate: (record: RecordedOBSFrame) => boolean,
    options: { after?: number; timeoutMs?: number } = {},
  ): Promise<RecordedOBSFrame> {
    const after = options.after ?? 0;
    const existing = this.frames.find((record) => record.cursor > after && predicate(record));
    if (existing) return this.publicRecord(existing);

    return new Promise<RecordedOBSFrame>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.events.off("frame", onFrame);
        reject(new Error(`Timed out waiting for an OBS frame after cursor ${after}`));
      }, options.timeoutMs ?? DEFAULT_TIMEOUT_MS);

      const onFrame = (record: InternalRecordedFrame) => {
        if (record.cursor <= after || !predicate(record)) return;
        clearTimeout(timeout);
        this.events.off("frame", onFrame);
        resolve(this.publicRecord(record));
      };

      this.events.on("frame", onFrame);
    });
  }

  async waitForRequest(
    requestType: string,
    options: { after?: number; timeoutMs?: number } = {},
  ): Promise<RecordedOBSRequest> {
    const record = await this.waitForFrame(
      ({ frame }) => frame.op === OBS_OP.Request && frame.d.requestType === requestType,
      options,
    );

    if (!this.isRequest(record)) {
      throw new Error(`Recorded ${requestType} frame was not a valid OBS request`);
    }
    return record;
  }

  async waitForConnectionCount(count: number, timeoutMs = DEFAULT_TIMEOUT_MS): Promise<void> {
    if (this.connectionCount >= count) return;
    await new Promise<void>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this.events.off("connection", onConnection);
        reject(new Error(`Timed out waiting for ${count} OBS connection(s)`));
      }, timeoutMs);
      const onConnection = () => {
        if (this.connectionCount < count) return;
        clearTimeout(timeout);
        this.events.off("connection", onConnection);
        resolve();
      };
      this.events.on("connection", onConnection);
    });
  }

  sendEvent(eventType: string, eventData: JsonObject = {}, connectionId?: number): void {
    const socket = this.findOpenSocket(connectionId);
    socket.send(JSON.stringify({
      op: OBS_OP.Event,
      d: { eventType, eventIntent: 1, eventData },
    }));
  }

  sendRaw(value: unknown, connectionId?: number): void {
    this.findOpenSocket(connectionId).send(
      typeof value === "string" ? value : JSON.stringify(value),
    );
  }

  disconnect(connectionId?: number, code = 1011, reason = "fake OBS disconnected"): void {
    this.findOpenSocket(connectionId).close(code, reason);
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.closed = true;
    for (const timer of this.timers) clearTimeout(timer);
    this.timers.clear();
    for (const socket of this.sockets.keys()) socket.terminate();
    this.sockets.clear();
    this.events.removeAllListeners();
    await new Promise<void>((resolve) => this.server.close(() => resolve()));
  }

  private handleConnection(socket: WebSocket): void {
    const connectionId = this.nextConnectionId++;
    this.sockets.set(socket, connectionId);
    this.events.emit("connection", connectionId);

    const authentication = this.password
      ? { salt: this.salt, challenge: this.challenge }
      : undefined;
    socket.send(JSON.stringify({
      op: OBS_OP.Hello,
      d: {
        obsStudioVersion: this.obsStudioVersion,
        obsWebSocketVersion: this.obsWebSocketVersion,
        rpcVersion: 1,
        ...(authentication ? { authentication } : {}),
      },
    }));

    socket.on("message", (data) => {
      const frame = parseFrame(data);
      if (!frame) return;
      const record: InternalRecordedFrame = {
        cursor: this.nextCursor++,
        connectionId,
        frame,
        socket,
      };
      this.frames.push(record);
      this.events.emit("frame", record);
      this.handleFrame(record);
    });
    socket.once("close", () => {
      this.sockets.delete(socket);
      this.events.emit("connectionClosed", connectionId);
    });
  }

  private handleFrame(record: InternalRecordedFrame): void {
    const { frame, socket } = record;
    if (frame.op === OBS_OP.Reidentify) {
      if (typeof frame.d.eventSubscriptions === "number") {
        this.subscriptions.set(record.connectionId, frame.d.eventSubscriptions);
      }
      socket.send(JSON.stringify({ op: OBS_OP.Identified, d: { negotiatedRpcVersion: 1 } }));
      return;
    }
    if (frame.op === OBS_OP.Identify) {
      if (typeof frame.d.eventSubscriptions === "number") {
        this.subscriptions.set(record.connectionId, frame.d.eventSubscriptions);
      }
      if (this.password) {
        const expected = expectedAuthentication(this.password, this.salt, this.challenge);
        if (frame.d.authentication !== expected) {
          socket.close(4009, "Authentication failed");
          return;
        }
      }
      socket.send(JSON.stringify({
        op: OBS_OP.Identified,
        d: { negotiatedRpcVersion: 1 },
      }));
      return;
    }

    if (frame.op === OBS_OP.RequestBatch) {
      void this.handleBatch(record);
      return;
    }

    if (!this.isRequest(record)) return;
    const action = this.scripts.get(record.frame.d.requestType)?.shift();
    if (action) {
      this.runAction(record, action);
      return;
    }
    const responder = this.responders.get(record.frame.d.requestType);
    if (responder) {
      const requestData = isObject(record.frame.d.requestData) ? record.frame.d.requestData : {};
      try {
        this.sendResponse(record, { result: true, code: 100 }, responder(requestData));
      } catch (error) {
        this.sendResponse(record, error instanceof FakeOBSRequestError
          ? { result: false, code: error.code, comment: error.message }
          : { result: false, code: 500, comment: String(error) }, {});
      }
      return;
    }
    if (record.frame.d.requestType === "GetVersion" && this.autoGetVersion) {
      this.sendResponse(record, {
        result: true,
        code: 100,
      }, this.versionResponse());
    }
  }

  /**
   * Answers a request batch from the same scripts and responders as single
   * requests. Each request in it is recorded in history() as its own Request
   * frame carrying batchRequestId, so request assertions see batched requests.
   */
  private async handleBatch({ frame, socket, connectionId }: InternalRecordedFrame): Promise<void> {
    const batchRequestId = String(frame.d.requestId);
    const executionType = typeof frame.d.executionType === "number" ? frame.d.executionType : 0;
    const requests = Array.isArray(frame.d.requests) ? frame.d.requests.filter(isObject) : [];
    const results: JsonObject[] = [];

    for (const [index, request] of requests.entries()) {
      const requestType = String(request.requestType);
      const requestData = isObject(request.requestData) ? request.requestData : {};
      const subRecord: InternalRecordedFrame = {
        cursor: this.nextCursor++,
        connectionId,
        socket,
        frame: {
          op: OBS_OP.Request,
          d: { requestType, requestId: `${batchRequestId}#${index}`, requestData, batchRequestId },
        },
      };
      this.frames.push(subRecord);
      this.events.emit("frame", subRecord);

      const answer = await this.answerBatchedRequest(requestType, requestData, executionType);
      if (answer === "disconnect") {
        socket.close(1011, "scripted disconnect");
        return;
      }
      results.push({
        requestType,
        ...(typeof request.requestId === "string" ? { requestId: request.requestId } : {}),
        requestStatus: answer.status,
        responseData: answer.data,
      });
      if (!answer.status.result && frame.d.haltOnFailure === true) break;
    }

    if (socket.readyState !== WebSocket.OPEN) return;
    // Real OBS does not return parallel results in request order; neither does the fake.
    if (executionType === 2) results.reverse();
    socket.send(JSON.stringify({ op: OBS_OP.RequestBatchResponse, d: { requestId: batchRequestId, results } }));
  }

  private async answerBatchedRequest(
    requestType: string,
    requestData: JsonObject,
    executionType: number,
  ): Promise<{ status: ResponseStatus; data: JsonObject } | "disconnect"> {
    if (requestType === "Sleep") {
      if (executionType === 2) {
        return { status: { result: false, code: 206, comment: "Sleep is not available in parallel batches" }, data: {} };
      }
      const millis = typeof requestData.sleepMillis === "number"
        ? requestData.sleepMillis
        : Number(requestData.sleepFrames ?? 0) * FAKE_FRAME_MS;
      await new Promise((resolve) => setTimeout(resolve, millis));
      return { status: { result: true, code: 100 }, data: {} };
    }

    const action = this.scripts.get(requestType)?.shift();
    if (action?.kind === "disconnect") return "disconnect";
    if (action) return { status: action.status, data: action.data };

    const responder = this.responders.get(requestType);
    if (responder) {
      try {
        return { status: { result: true, code: 100 }, data: responder(requestData) };
      } catch (error) {
        return {
          status: error instanceof FakeOBSRequestError
            ? { result: false, code: error.code, comment: error.message }
            : { result: false, code: 500, comment: String(error) },
          data: {},
        };
      }
    }
    if (requestType === "GetVersion" && this.autoGetVersion) {
      return { status: { result: true, code: 100 }, data: this.versionResponse() };
    }
    return { status: { result: false, code: 204, comment: `Fake OBS has no answer for ${requestType}` }, data: {} };
  }

  private queueAction(requestType: string, action: ScriptedAction): void {
    const actions = this.scripts.get(requestType) ?? [];
    actions.push(action);
    this.scripts.set(requestType, actions);
  }

  private runAction(record: RecordedOBSRequest & { socket: WebSocket }, action: ScriptedAction): void {
    const run = () => {
      if (action.kind === "disconnect") {
        record.socket.close(action.code, action.reason);
      } else {
        this.sendResponse(record, action.status, action.data);
      }
    };
    if (action.delayMs === 0) {
      run();
      return;
    }
    const timer = setTimeout(() => {
      this.timers.delete(timer);
      run();
    }, action.delayMs);
    this.timers.add(timer);
  }

  private sendResponse(
    record: RecordedOBSRequest & { socket: WebSocket },
    status: ResponseStatus,
    responseData: JsonObject,
  ): void {
    if (record.socket.readyState !== WebSocket.OPEN) return;
    record.socket.send(JSON.stringify({
      op: OBS_OP.RequestResponse,
      d: {
        requestType: record.frame.d.requestType,
        requestId: record.frame.d.requestId,
        requestStatus: status,
        responseData,
      },
    }));
  }

  private versionResponse(): JsonObject {
    return {
      obsVersion: this.obsStudioVersion,
      obsWebSocketVersion: this.obsWebSocketVersion,
      rpcVersion: 1,
      availableRequests: this.availableRequests,
      supportedImageFormats: this.supportedImageFormats,
      platform: this.platform,
      platformDescription: "Fake OBS Studio",
    };
  }

  private findOpenSocket(connectionId?: number): WebSocket {
    const entry = Array.from(this.sockets.entries()).find(
      ([socket, id]) => socket.readyState === WebSocket.OPEN && (connectionId === undefined || id === connectionId),
    );
    if (!entry) throw new Error(`No open fake OBS connection${connectionId ? ` ${connectionId}` : ""}`);
    return entry[0];
  }

  private isRequest(record: RecordedOBSFrame): record is RecordedOBSRequest {
    return record.frame.op === OBS_OP.Request
      && typeof record.frame.d.requestId === "string"
      && typeof record.frame.d.requestType === "string";
  }

  private publicRecord(record: InternalRecordedFrame): RecordedOBSFrame {
    const { socket: _socket, ...publicRecord } = record;
    return publicRecord;
  }
}
