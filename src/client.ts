/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import { EventEmitter } from "node:events";
import WebSocket from "ws";
import { logger } from "./logger.js";


enum OpCode {
  Hello = 0,
  Identify = 1,
  Identified = 2,
  Reidentify = 3,
  Event = 5,
  Request = 6,
  RequestResponse = 7,
  RequestBatch = 8,
  RequestBatchResponse = 9,
}

export enum EventSubscription {
  None = 0,
  General = 1 << 0,
  Config = 1 << 1,
  Scenes = 1 << 2,
  Inputs = 1 << 3,
  Transitions = 1 << 4,
  Filters = 1 << 5,
  Outputs = 1 << 6,
  SceneItems = 1 << 7,
  MediaInputs = 1 << 8,
  Vendors = 1 << 9,
  Ui = 1 << 10,
  Canvases = 1 << 11,
  All = (1 << 12) - 1,
  // High-volume events are left out of All; subscribe with subscribeHighVolume.
  InputVolumeMeters = 1 << 16,
  InputActiveStateChanged = 1 << 17,
  InputShowStateChanged = 1 << 18,
  SceneItemTransformChanged = 1 << 19,
}

/**
 * How OBS runs the requests in a batch (RequestBatchExecutionType). Parallel
 * (2) is left out on purpose: in obs-websocket 5.7 each parallel batch holds a
 * thread-pool thread while waiting for its requests on the same pool, so a few
 * at once deadlock the WebSocket server until OBS restarts, and its results
 * come back in completion order with the wrong request labels.
 */
export enum RequestBatchExecutionType {
  /** One after another, as fast as possible. */
  SerialRealtime = 0,
  /** One per rendered video frame; Sleep counts frames. */
  SerialFrame = 1,
}

export type BatchRequest = { requestType: string; requestData?: unknown };

export type BatchResult = {
  requestType: string;
  ok: boolean;
  code: number;
  comment?: string;
  responseData: unknown;
};

export type BatchOptions = {
  executionType?: RequestBatchExecutionType;
  /** Stop at the first failed request; later requests get no result. */
  haltOnFailure?: boolean;
  /** Defaults to 10 s plus the batch's own Sleep time. */
  timeout?: number;
};

type JsonObject = Record<string, unknown>;
type BaseMessage = { op: number; d: JsonObject };

type HelloData = {
  obsStudioVersion: string;
  obsWebSocketVersion: string;
  rpcVersion: number;
  authentication?: { challenge: string; salt: string };
};

type RequestResponseData = {
  requestType: string;
  requestId: string;
  requestStatus: { result: boolean; code: number; comment?: string };
  responseData?: unknown;
};

type EventData = {
  eventType: string;
  eventIntent: number;
  eventData?: unknown;
};

type VersionResponse = {
  obsVersion?: unknown;
  obsWebSocketVersion?: unknown;
  rpcVersion?: unknown;
  availableRequests?: unknown;
  supportedImageFormats?: unknown;
  platform?: unknown;
  platformDescription?: unknown;
};

type PendingRequest = {
  socket: WebSocket;
  requestType: string;
  resolve: (value: unknown) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
};

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseBaseMessage(value: unknown): BaseMessage | null {
  if (!isObject(value) || typeof value.op !== "number" || !isObject(value.d)) return null;
  return { op: value.op, d: value.d };
}

function parseHello(data: JsonObject): HelloData | null {
  if (
    typeof data.obsStudioVersion !== "string"
    || typeof data.obsWebSocketVersion !== "string"
    || typeof data.rpcVersion !== "number"
  ) return null;

  let authentication: HelloData["authentication"];
  if (data.authentication !== undefined) {
    if (
      !isObject(data.authentication)
      || typeof data.authentication.challenge !== "string"
      || typeof data.authentication.salt !== "string"
    ) return null;
    authentication = {
      challenge: data.authentication.challenge,
      salt: data.authentication.salt,
    };
  }

  return {
    obsStudioVersion: data.obsStudioVersion,
    obsWebSocketVersion: data.obsWebSocketVersion,
    rpcVersion: data.rpcVersion,
    ...(authentication ? { authentication } : {}),
  };
}

function parseRequestResponse(data: JsonObject): RequestResponseData | null {
  if (
    typeof data.requestType !== "string"
    || typeof data.requestId !== "string"
    || !isObject(data.requestStatus)
    || typeof data.requestStatus.result !== "boolean"
    || typeof data.requestStatus.code !== "number"
    || (data.requestStatus.comment !== undefined && typeof data.requestStatus.comment !== "string")
  ) return null;

  return {
    requestType: data.requestType,
    requestId: data.requestId,
    requestStatus: {
      result: data.requestStatus.result,
      code: data.requestStatus.code,
      ...(typeof data.requestStatus.comment === "string"
        ? { comment: data.requestStatus.comment }
        : {}),
    },
    ...(data.responseData === undefined ? {} : { responseData: data.responseData }),
  };
}

type RawBatchResult = BatchResult & { requestId?: string };

function parseBatchResponse(data: JsonObject): { requestId: string; results: RawBatchResult[] } | null {
  if (typeof data.requestId !== "string" || !Array.isArray(data.results)) return null;
  const results: RawBatchResult[] = [];
  for (const result of data.results) {
    if (
      !isObject(result)
      || typeof result.requestType !== "string"
      || !isObject(result.requestStatus)
      || typeof result.requestStatus.result !== "boolean"
      || typeof result.requestStatus.code !== "number"
    ) return null;
    const { comment } = result.requestStatus;
    results.push({
      ...(typeof result.requestId === "string" ? { requestId: result.requestId } : {}),
      requestType: result.requestType,
      ok: result.requestStatus.result,
      code: result.requestStatus.code,
      ...(typeof comment === "string" ? { comment } : {}),
      responseData: result.responseData ?? {},
    });
  }
  return { requestId: data.requestId, results };
}

/** Orders batch results by the index each request carried as its ID, dropping the IDs. */
function inRequestOrder(results: RawBatchResult[]): BatchResult[] {
  const indexed = results.map((result, position) => ({ result, index: result.requestId === undefined ? position : Number(result.requestId) }));
  return indexed
    .sort((a, b) => a.index - b.index)
    .map(({ result: { requestId: _requestId, ...result } }) => result);
}

/** Wall-clock time a batch spends in Sleep requests, assuming 30 fps for frames. */
function batchSleepMillis(requests: readonly BatchRequest[]): number {
  let total = 0;
  for (const { requestType, requestData } of requests) {
    if (requestType !== "Sleep" || !isObject(requestData)) continue;
    if (typeof requestData.sleepMillis === "number") total += requestData.sleepMillis;
    if (typeof requestData.sleepFrames === "number") total += (requestData.sleepFrames / 30) * 1000;
  }
  return total;
}

function parseEvent(data: JsonObject): EventData | null {
  if (typeof data.eventType !== "string" || typeof data.eventIntent !== "number") return null;
  return {
    eventType: data.eventType,
    eventIntent: data.eventIntent,
    ...(data.eventData === undefined ? {} : { eventData: data.eventData }),
  };
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

export class OBSWebSocketClient extends EventEmitter {
  private ws: WebSocket | null = null;
  private readonly url: string;
  private readonly password: string | null;
  private connected = false;
  private identified = false;
  // OBS reads output settings only when it rebuilds its outputs (restart,
  // Settings dialog, or profile switch). A new connection usually means OBS
  // restarted, so the flag resets on identify.
  private outputSettingsPending = false;
  private connectionPromise: Promise<void> | null = null;
  private availableRequests: Set<string> | null = null;
  private versionInfo: VersionResponse | null = null;
  private readonly pendingRequests = new Map<string, PendingRequest>();
  // How many callers want each high-volume subscription bit.
  private readonly highVolumeCounts = new Map<number, number>();
  // The subscriptions OBS was last told about.
  private sentSubscriptions: number = EventSubscription.All;

  constructor(url = "ws://localhost:4455", password: string | null = null) {
    super();
    this.url = url;
    this.password = password;
  }

  public connect(): Promise<void> {
    if (this.isConnected()) return Promise.resolve();
    if (this.connectionPromise) return this.connectionPromise;

    this.connectionPromise = this.openConnection().finally(() => {
      this.connectionPromise = null;
    });
    return this.connectionPromise;
  }

  public isConnected(): boolean {
    return this.connected && this.identified;
  }

  public getConnectionStatus(): {
    connected: boolean;
    identified: boolean;
    url: string;
    hasPassword: boolean;
    availableRequestCount: number | null;
    versionInfo: VersionResponse | null;
  } {
    return {
      connected: this.connected,
      identified: this.identified,
      url: this.url,
      hasPassword: this.password !== null,
      availableRequestCount: this.availableRequests?.size ?? null,
      versionInfo: this.versionInfo,
    };
  }

  /** The event subscriptions this client identifies with. */
  public eventSubscriptions(): number {
    let mask: number = EventSubscription.All;
    for (const [bit, count] of this.highVolumeCounts) if (count > 0) mask |= bit;
    return mask;
  }

  /**
   * Subscribes to a high-volume event such as InputVolumeMeters until the
   * returned function is called. Callers share one subscription per event;
   * the last release turns it off. Survives reconnects.
   */
  public subscribeHighVolume(event: EventSubscription): () => void {
    const before = this.eventSubscriptions();
    this.highVolumeCounts.set(event, (this.highVolumeCounts.get(event) ?? 0) + 1);
    this.reidentifyIfChanged(before);
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const previous = this.eventSubscriptions();
      const count = (this.highVolumeCounts.get(event) ?? 1) - 1;
      if (count > 0) this.highVolumeCounts.set(event, count);
      else this.highVolumeCounts.delete(event);
      this.reidentifyIfChanged(previous);
    };
  }

  private reidentifyIfChanged(before: number): void {
    const eventSubscriptions = this.eventSubscriptions();
    const socket = this.ws;
    if (eventSubscriptions === before || !this.isConnected() || !socket || socket.readyState !== WebSocket.OPEN) return;
    try {
      this.sentSubscriptions = eventSubscriptions;
      socket.send(JSON.stringify({ op: OpCode.Reidentify, d: { eventSubscriptions } }));
    } catch (error) {
      logger.error(`Unable to change OBS event subscriptions: ${asError(error).message}`);
    }
  }

  public markOutputSettingsPending(): void {
    this.outputSettingsPending = true;
  }

  public hasPendingOutputSettings(): boolean {
    return this.outputSettingsPending;
  }

  public supportsRequest(requestType: string): boolean | null {
    return this.availableRequests?.has(requestType) ?? null;
  }

  public disconnect(): Promise<void> {
    const socket = this.ws;
    if (!socket) return Promise.resolve();

    this.ws = null;
    this.resetConnectionState();
    this.rejectPendingForSocket(socket, new Error("Disconnected from OBS WebSocket server"));

    if (socket.readyState === WebSocket.CLOSED) return Promise.resolve();
    return new Promise<void>((resolve) => {
      socket.once("close", resolve);
      try {
        socket.close();
      } catch {
        socket.terminate();
      }
    });
  }

  public async sendRequest<T = any>(
    requestType: string,
    requestData?: unknown,
    timeout = 10_000,
  ): Promise<T> {
    const socket = await this.readySocket();
    this.assertAdvertised(requestType);
    return this.sendTracked<T>(socket, requestType, timeout, (requestId) => ({
      op: OpCode.Request,
      d: {
        requestType,
        requestId,
        ...(requestData === undefined ? {} : { requestData }),
      },
    }));
  }

  /**
   * Sends several requests in one message. OBS answers each one; a failed
   * request does not reject the batch, so check each result's `ok`.
   */
  public async sendBatch(requests: readonly BatchRequest[], options: BatchOptions = {}): Promise<BatchResult[]> {
    if (requests.length === 0) return [];
    const {
      executionType = RequestBatchExecutionType.SerialRealtime,
      haltOnFailure = false,
      timeout = 10_000 + batchSleepMillis(requests),
    } = options;
    const socket = await this.readySocket();
    for (const { requestType } of requests) this.assertAdvertised(requestType);

    const results = await this.sendTracked<RawBatchResult[]>(socket, "RequestBatch", timeout, (requestId) => ({
      op: OpCode.RequestBatch,
      d: {
        requestId,
        haltOnFailure,
        executionType,
        // Each request carries its index as its ID, so results are matched by ID, not position.
        requests: requests.map(({ requestType, requestData }, index) => ({
          requestType,
          requestId: String(index),
          ...(requestData === undefined ? {} : { requestData }),
        })),
      },
    }));
    return inRequestOrder(results);
  }

  private async readySocket(): Promise<WebSocket> {
    if (!this.isConnected()) {
      try {
        await this.connect();
      } catch (error) {
        throw new Error(
          `Unable to connect to OBS WebSocket server: ${asError(error).message}`,
          { cause: error },
        );
      }
    }

    const socket = this.ws;
    if (!socket || socket.readyState !== WebSocket.OPEN) {
      throw new Error("OBS WebSocket connection was not available after connecting");
    }
    return socket;
  }

  private assertAdvertised(requestType: string): void {
    if (this.availableRequests && !this.availableRequests.has(requestType)) {
      throw new Error(`OBS WebSocket does not advertise support for request '${requestType}'`);
    }
  }

  private sendTracked<T>(
    socket: WebSocket,
    requestType: string,
    timeout: number,
    buildMessage: (requestId: string) => BaseMessage,
  ): Promise<T> {
    return new Promise<T>((resolve, reject) => {
      const requestId = crypto.randomUUID();
      const timeoutId = setTimeout(() => {
        const pending = this.pendingRequests.get(requestId);
        if (!pending) return;
        this.pendingRequests.delete(requestId);
        pending.reject(new Error(`Request ${requestType} timed out after ${timeout}ms`));
      }, timeout);

      const pending: PendingRequest = {
        socket,
        requestType,
        resolve: (value) => resolve(value as T),
        reject,
        timeout: timeoutId,
      };
      this.pendingRequests.set(requestId, pending);

      try {
        socket.send(JSON.stringify(buildMessage(requestId)), (error) => {
          if (error) this.rejectPending(requestId, asError(error));
        });
      } catch (error) {
        this.rejectPending(requestId, asError(error));
      }
    });
  }

  private openConnection(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      logger.log(`Attempting to connect to OBS WebSocket at: ${this.url}`);
      let socket: WebSocket;
      try {
        socket = new WebSocket(this.url);
      } catch (error) {
        reject(asError(error));
        return;
      }

      this.ws = socket;
      let settled = false;
      let helloHandled = false;
      const connectionTimeout = setTimeout(() => {
        finishReject(new Error(
          "WebSocket connection timeout - OBS may not be running or WebSocket may be disabled",
        ));
        socket.terminate();
      }, 10_000);

      const cleanupAttempt = () => clearTimeout(connectionTimeout);
      const finishResolve = () => {
        if (settled) return;
        settled = true;
        cleanupAttempt();
        resolve();
      };
      const finishReject = (error: unknown) => {
        if (settled) return;
        settled = true;
        cleanupAttempt();
        reject(asError(error));
      };

      socket.on("open", () => {
        if (this.ws !== socket) return;
        this.connected = true;
        logger.log("WebSocket connection opened successfully");
      });

      socket.on("message", (data) => {
        if (this.ws !== socket) return;
        let parsed: unknown;
        try {
          parsed = JSON.parse(data.toString());
        } catch (error) {
          logger.error(`Error parsing message: ${asError(error).message}`);
          return;
        }

        const message = parseBaseMessage(parsed);
        if (!message) {
          logger.error("Ignoring malformed OBS WebSocket message");
          return;
        }

        if (message.op === OpCode.Hello) {
          const hello = parseHello(message.d);
          if (!hello) {
            logger.error("Ignoring malformed OBS Hello message");
            return;
          }
          if (helloHandled) return;
          helloHandled = true;
          void this.completeHandshake(socket, hello).then(
            () => {
              if (this.ws === socket) finishResolve();
              else finishReject(new Error("OBS WebSocket connection changed during identification"));
            },
            (error: unknown) => {
              finishReject(error);
              if (socket.readyState === WebSocket.OPEN) socket.close();
            },
          );
          return;
        }

        this.handleMessage(socket, message);
      });

      socket.on("close", (code, reason) => {
        const reasonText = reason.toString() || "No reason provided";
        logger.log(`WebSocket connection closed with code ${code}: ${reasonText}`);
        this.rejectPendingForSocket(
          socket,
          new Error(`WebSocket connection closed: ${reasonText}`),
        );

        if (this.ws === socket) {
          this.ws = null;
          this.resetConnectionState();
          this.emit("disconnected");
        }
        finishReject(new Error(`WebSocket connection closed before identification: ${reasonText}`));
      });

      socket.on("error", (error) => {
        const message = asError(error).message;
        logger.error(`WebSocket connection error: ${message}`);
        if (message.includes("ECONNREFUSED")) {
          logger.error("Connection refused. Make sure OBS Studio is running and WebSocket is enabled.");
        } else if (message.includes("ENOTFOUND")) {
          logger.error("Host not found. Check the OBS_WEBSOCKET_URL environment variable.");
        } else if (message.includes("ETIMEDOUT")) {
          logger.error("Connection timed out. Check network connectivity and firewall settings.");
        }
        finishReject(error);
      });
    });
  }

  private async completeHandshake(socket: WebSocket, hello: HelloData): Promise<void> {
    await this.identify(socket, hello);
    if (this.ws !== socket) throw new Error("OBS WebSocket connection changed during identification");
    await this.refreshCapabilities(socket);
  }

  private handleMessage(socket: WebSocket, message: BaseMessage): void {
    if (this.ws !== socket) return;
    switch (message.op) {
      case OpCode.Identified:
        if (typeof message.d.negotiatedRpcVersion !== "number") {
          logger.error("Ignoring malformed OBS Identified message");
          return;
        }
        // OBS answers Reidentify with Identified too; only a new session means
        // OBS may have restarted and rebuilt its outputs.
        if (!this.identified) {
          this.outputSettingsPending = false;
          this.identified = true;
          // A subscription added while Identify was in flight is not in what OBS was sent.
          this.reidentifyIfChanged(this.sentSubscriptions);
        }
        this.emit("identified", socket);
        break;
      case OpCode.RequestResponse: {
        const response = parseRequestResponse(message.d);
        if (response) this.handleRequestResponse(socket, response);
        else logger.error("Ignoring malformed OBS request response");
        break;
      }
      case OpCode.RequestBatchResponse: {
        const batch = parseBatchResponse(message.d);
        if (batch) this.handleBatchResponse(socket, batch);
        else logger.error("Ignoring malformed OBS request batch response");
        break;
      }
      case OpCode.Event: {
        const event = parseEvent(message.d);
        if (event) this.handleEvent(event);
        else logger.error("Ignoring malformed OBS event");
        break;
      }
      default:
        logger.debug(`Unhandled message type: ${message.op}`);
    }
  }

  private handleRequestResponse(socket: WebSocket, response: RequestResponseData): void {
    const pending = this.pendingRequests.get(response.requestId);
    if (!pending || pending.socket !== socket) return;
    this.pendingRequests.delete(response.requestId);
    clearTimeout(pending.timeout);

    if (response.requestStatus.result) {
      pending.resolve(response.responseData ?? {});
      return;
    }

    const comment = response.requestStatus.comment ? `: ${response.requestStatus.comment}` : "";
    pending.reject(new Error(
      `OBS request ${response.requestType} failed with code ${response.requestStatus.code}${comment}`,
    ));
  }

  private handleBatchResponse(socket: WebSocket, batch: { requestId: string; results: RawBatchResult[] }): void {
    const pending = this.pendingRequests.get(batch.requestId);
    if (!pending || pending.socket !== socket) return;
    this.pendingRequests.delete(batch.requestId);
    clearTimeout(pending.timeout);
    pending.resolve(batch.results);
  }

  private handleEvent(event: EventData): void {
    this.emit("event", event.eventType, event.eventData);
    this.emit(event.eventType, event.eventData);
  }

  private identify(socket: WebSocket, hello: HelloData): Promise<void> {
    if (this.ws !== socket || !this.connected) {
      return Promise.reject(new Error("Not connected to OBS WebSocket server"));
    }

    logger.log(
      `Received hello from OBS WebSocket v${hello.obsWebSocketVersion} (OBS v${hello.obsStudioVersion})`,
    );

    let authentication: string | undefined;
    if (hello.authentication) {
      if (!this.password) {
        return Promise.reject(new Error(
          "Password required for authentication but not provided. Set OBS_WEBSOCKET_PASSWORD, or OBS_MCP_READ_OBS_CONFIG=true to use the password saved in OBS.",
        ));
      }
      authentication = this.generateAuthenticationString(
        this.password,
        hello.authentication.salt,
        hello.authentication.challenge,
      );
    }

    const identifyMessage = {
      op: OpCode.Identify,
      d: {
        rpcVersion: hello.rpcVersion,
        eventSubscriptions: this.sentSubscriptions = this.eventSubscriptions(),
        ...(authentication ? { authentication } : {}),
      },
    };

    return new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        clearTimeout(timeout);
        this.off("identified", onIdentified);
        socket.off("close", onClose);
      };
      const onIdentified = (identifiedSocket: WebSocket) => {
        if (identifiedSocket !== socket) return;
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error("WebSocket closed during identification"));
      };
      const timeout = setTimeout(() => {
        cleanup();
        reject(new Error("Identification timed out - OBS may be unresponsive or authentication failed"));
      }, 5_000);

      this.on("identified", onIdentified);
      socket.once("close", onClose);
      try {
        socket.send(JSON.stringify(identifyMessage), (error) => {
          if (!error) return;
          cleanup();
          reject(asError(error));
        });
      } catch (error) {
        cleanup();
        reject(asError(error));
      }
    });
  }

  private async refreshCapabilities(socket: WebSocket): Promise<void> {
    try {
      const versionInfo = await this.sendRequest<VersionResponse>("GetVersion");
      if (this.ws !== socket) return;
      this.versionInfo = versionInfo;
      this.availableRequests = Array.isArray(versionInfo.availableRequests)
        ? new Set(versionInfo.availableRequests.filter(
          (requestType): requestType is string => typeof requestType === "string",
        ))
        : null;
    } catch (error) {
      if (this.ws !== socket) return;
      logger.error(`Unable to read OBS WebSocket capabilities: ${asError(error).message}`);
      this.availableRequests = null;
      this.versionInfo = null;
    }
  }

  private rejectPending(requestId: string, error: Error): void {
    const pending = this.pendingRequests.get(requestId);
    if (!pending) return;
    this.pendingRequests.delete(requestId);
    clearTimeout(pending.timeout);
    pending.reject(error);
  }

  private rejectPendingForSocket(socket: WebSocket, error: Error): void {
    for (const [requestId, pending] of this.pendingRequests) {
      if (pending.socket !== socket) continue;
      this.pendingRequests.delete(requestId);
      clearTimeout(pending.timeout);
      pending.reject(error);
    }
  }

  private resetConnectionState(): void {
    this.connected = false;
    this.identified = false;
    this.availableRequests = null;
    this.versionInfo = null;
  }

  private generateAuthenticationString(password: string, salt: string, challenge: string): string {
    const secret = crypto.createHash("sha256").update(password + salt).digest("base64");
    return crypto.createHash("sha256").update(secret + challenge).digest("base64");
  }
}

export default OBSWebSocketClient;
