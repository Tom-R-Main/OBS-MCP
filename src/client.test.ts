/*
 * Modified for the independent OBS MCP project in August 2026.
 * See NOTICE.md and Git history for authorship and change dates.
 * SPDX-License-Identifier: GPL-2.0-only
 */
import crypto from "node:crypto";
import { once } from "node:events";
import WebSocket from "ws";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EventSubscription, OBSWebSocketClient, RequestBatchExecutionType } from "./client.js";
import {
  FakeOBSRequestError,
  FakeOBSServer,
  OBS_OP,
  type OBSFrame,
} from "../test/support/fake-obs-server.js";

type ClientInternals = {
  ws: WebSocket | null;
  pendingRequests: Map<string, unknown>;
  handleMessage(socket: WebSocket, message: OBSFrame): void;
};

const servers: FakeOBSServer[] = [];
const clients: OBSWebSocketClient[] = [];

function internals(client: OBSWebSocketClient): ClientInternals {
  return client as unknown as ClientInternals;
}

async function createServer(
  options: Parameters<typeof FakeOBSServer.start>[0] = {},
): Promise<FakeOBSServer> {
  const server = await FakeOBSServer.start(options);
  servers.push(server);
  return server;
}

function createClient(server: FakeOBSServer, password: string | null = null): OBSWebSocketClient {
  const client = new OBSWebSocketClient(server.url, password);
  clients.push(client);
  return client;
}

beforeEach(() => {
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(async () => {
  await Promise.allSettled(clients.splice(0).map((client) => client.disconnect()));
  await Promise.all(servers.splice(0).map((server) => server.close()));
  vi.restoreAllMocks();
});

describe("OBSWebSocketClient connection", () => {
  it("shares one in-flight connection and resolves after canonical capability discovery", async () => {
    const server = await createServer({ availableRequests: ["GetStats"] });
    const client = createClient(server);
    const cursor = server.cursor();

    const first = client.connect();
    const second = client.connect();

    expect(first).toBe(second);
    expect(client.isConnected()).toBe(false);
    await first;

    const identify = await server.waitForFrame(
      ({ frame }) => frame.op === OBS_OP.Identify,
      { after: cursor },
    );
    expect(identify.frame.d).toMatchObject({
      rpcVersion: 1,
      eventSubscriptions: EventSubscription.All,
    });
    expect(client.isConnected()).toBe(true);
    expect(client.supportsRequest("GetVersion")).toBe(true);
    expect(client.supportsRequest("GetStats")).toBe(true);
    expect(client.supportsRequest("StartStream")).toBe(false);
    expect(client.getConnectionStatus().versionInfo).toMatchObject({
      obsVersion: "32.2.2",
      obsWebSocketVersion: "5.7.0",
    });
    expect(server.connectionCount).toBe(1);
  });

  it("computes the OBS authentication response", async () => {
    const password = "correct horse battery staple";
    const server = await createServer({ password });
    const client = createClient(server, password);
    await client.connect();

    const identify = await server.waitForFrame(({ frame }) => frame.op === OBS_OP.Identify);
    const secret = crypto.createHash("sha256")
      .update(password + server.salt)
      .digest("base64");
    const expected = crypto.createHash("sha256")
      .update(secret + server.challenge)
      .digest("base64");
    expect(identify.frame.d.authentication).toBe(expected);
  });

  it("rejects authentication challenges when no password was supplied", async () => {
    const server = await createServer({ password: "required" });
    const client = createClient(server);

    await expect(client.connect()).rejects.toThrow("Password required for authentication");
    expect(client.isConnected()).toBe(false);
  });

  it("keeps the connection usable when capability discovery fails", async () => {
    const server = await createServer({ autoGetVersion: false });
    server.queueError("GetVersion", 500, "capabilities unavailable");
    const client = createClient(server);

    await client.connect();
    expect(client.isConnected()).toBe(true);
    expect(client.supportsRequest("AnythingAtAll")).toBeNull();
    expect(client.getConnectionStatus().versionInfo).toBeNull();

    server.queueSuccess("FallbackRequest", { recovered: true });
    await expect(client.sendRequest("FallbackRequest")).resolves.toEqual({ recovered: true });
  });

  it("ignores malformed and unknown frames without breaking the session", async () => {
    const server = await createServer({ availableRequests: ["GetStats"] });
    const client = createClient(server);
    await client.connect();

    server.sendRaw("not-json");
    server.sendRaw({ op: OBS_OP.Event, d: { eventType: 42 } });
    server.sendRaw({ op: 999, d: {} });
    server.queueSuccess("GetStats", { activeFps: 60 });

    await expect(client.sendRequest("GetStats")).resolves.toEqual({ activeFps: 60 });
  });
});

describe("OBSWebSocketClient requests", () => {
  it("correlates responses that arrive out of order", async () => {
    const server = await createServer({ availableRequests: ["Slow", "Fast"] });
    const client = createClient(server);
    await client.connect();
    server.queueSuccess("Slow", { value: "slow" }, 40);
    server.queueSuccess("Fast", { value: "fast" });

    const slow = client.sendRequest("Slow");
    const fast = client.sendRequest("Fast");

    await expect(fast).resolves.toEqual({ value: "fast" });
    await expect(slow).resolves.toEqual({ value: "slow" });
  });

  it("propagates OBS status codes and comments", async () => {
    const server = await createServer({ availableRequests: ["StartStream"] });
    const client = createClient(server);
    await client.connect();
    server.queueError("StartStream", 501, "output already running");

    await expect(client.sendRequest("StartStream")).rejects.toThrow(
      "OBS request StartStream failed with code 501: output already running",
    );
  });

  it("removes timed-out requests and ignores late responses", async () => {
    const server = await createServer({ availableRequests: ["Slow", "Fast"] });
    const client = createClient(server);
    await client.connect();
    server.queueSuccess("Slow", { tooLate: true }, 40);

    await expect(client.sendRequest("Slow", undefined, 10)).rejects.toThrow(
      "Request Slow timed out after 10ms",
    );
    expect(internals(client).pendingRequests.size).toBe(0);

    await new Promise((resolve) => setTimeout(resolve, 50));
    server.queueSuccess("Fast", { stillConnected: true });
    await expect(client.sendRequest("Fast")).resolves.toEqual({ stillConnected: true });
  });

  it("rejects every pending request when the socket disconnects", async () => {
    const server = await createServer({ availableRequests: ["One", "Two"] });
    const client = createClient(server);
    await client.connect();
    server.queueSuccess("One", {}, 500);
    server.queueSuccess("Two", {}, 500);

    const cursor = server.cursor();
    const one = client.sendRequest("One");
    const two = client.sendRequest("Two");
    const oneRejected = expect(one).rejects.toThrow("WebSocket connection closed");
    const twoRejected = expect(two).rejects.toThrow("WebSocket connection closed");
    await server.waitForRequest("Two", { after: cursor });
    server.disconnect();

    await oneRejected;
    await twoRejected;
    expect(internals(client).pendingRequests.size).toBe(0);
    expect(client.isConnected()).toBe(false);
  });

  it("cleans up pending state after synchronous send failures", async () => {
    const server = await createServer({ availableRequests: ["GetStats"] });
    const client = createClient(server);
    await client.connect();
    const socket = internals(client).ws;
    if (!socket) throw new Error("Expected an active client socket");
    const originalSend = socket.send;
    Object.defineProperty(socket, "send", {
      configurable: true,
      value: () => { throw new Error("synchronous send failure"); },
    });

    await expect(client.sendRequest("GetStats")).rejects.toThrow("synchronous send failure");
    expect(internals(client).pendingRequests.size).toBe(0);
    Object.defineProperty(socket, "send", { configurable: true, value: originalSend });
  });

  it("cleans up pending state after send callback failures", async () => {
    const server = await createServer({ availableRequests: ["GetStats"] });
    const client = createClient(server);
    await client.connect();
    const socket = internals(client).ws;
    if (!socket) throw new Error("Expected an active client socket");
    const originalSend = socket.send;
    Object.defineProperty(socket, "send", {
      configurable: true,
      value: (_data: unknown, callback: (error?: Error) => void) => {
        callback(new Error("send callback failure"));
      },
    });

    await expect(client.sendRequest("GetStats")).rejects.toThrow("send callback failure");
    expect(internals(client).pendingRequests.size).toBe(0);
    Object.defineProperty(socket, "send", { configurable: true, value: originalSend });
  });
});

describe("OBSWebSocketClient request batches", () => {
  it("sends one batch message and returns a result per request, including failures", async () => {
    const server = await createServer();
    server.respondWith("GetInputMute", ({ inputName }) => {
      if (inputName === "Camera") throw new FakeOBSRequestError(604, "Camera has no audio");
      return { inputMuted: true };
    });
    const client = createClient(server);

    const results = await client.sendBatch([
      { requestType: "GetInputMute", requestData: { inputName: "Mic" } },
      { requestType: "GetInputMute", requestData: { inputName: "Camera" } },
    ], { executionType: RequestBatchExecutionType.Parallel });

    expect(results).toEqual([
      { requestType: "GetInputMute", ok: true, code: 100, responseData: { inputMuted: true } },
      { requestType: "GetInputMute", ok: false, code: 604, comment: "Camera has no audio", responseData: {} },
    ]);
    const batch = server.history().find(({ frame }) => frame.op === OBS_OP.RequestBatch);
    expect(batch?.frame.d).toMatchObject({ executionType: 2, haltOnFailure: false });
    expect(server.history().filter(({ frame }) => frame.op === OBS_OP.Request && frame.d.batchRequestId)).toHaveLength(2);
  });

  it("stops at the first failure when asked to", async () => {
    const server = await createServer();
    server.respondWith("SetInputMute", () => {
      throw new FakeOBSRequestError(600, "No such input");
    });
    server.respondWith("SetInputAudioTracks", () => ({}));
    const client = createClient(server);

    const results = await client.sendBatch([
      { requestType: "SetInputMute", requestData: { inputName: "x", inputMuted: true } },
      { requestType: "SetInputAudioTracks", requestData: { inputName: "x", inputAudioTracks: {} } },
    ], { haltOnFailure: true });

    expect(results.map(({ ok }) => ok)).toEqual([false]);
  });

  it("refuses request types OBS does not advertise before sending anything", async () => {
    const server = await createServer();
    const client = createClient(server);
    await client.connect();
    const cursor = server.cursor();

    await expect(client.sendBatch([{ requestType: "MadeUpRequest" }])).rejects.toThrow("MadeUpRequest");
    expect(server.cursor()).toBe(cursor);
  });

  it("extends the timeout by the time the batch sleeps", async () => {
    const server = await createServer();
    const client = createClient(server);

    const results = await client.sendBatch([
      { requestType: "Sleep", requestData: { sleepMillis: 150 } },
    ], { timeout: undefined });

    expect(results).toEqual([{ requestType: "Sleep", ok: true, code: 100, responseData: {} }]);
  });

  it("returns no results without contacting OBS for an empty batch", async () => {
    const server = await createServer();
    const client = createClient(server);

    expect(await client.sendBatch([])).toEqual([]);
    expect(server.connectionCount).toBe(0);
  });
});

describe("OBSWebSocketClient high-volume events", () => {
  it("adds a subscription while anyone holds it and restores it after a reconnect", async () => {
    const server = await createServer();
    const client = createClient(server);
    await client.connect();
    expect(server.eventSubscriptions()).toBe(EventSubscription.All);

    const first = client.subscribeHighVolume(EventSubscription.InputVolumeMeters);
    const second = client.subscribeHighVolume(EventSubscription.InputVolumeMeters);
    const withMeters = EventSubscription.All | EventSubscription.InputVolumeMeters;
    await vi.waitFor(() => expect(server.eventSubscriptions()).toBe(withMeters));
    const reidentifies = () => server.history().filter(({ frame }) => frame.op === OBS_OP.Reidentify).length;
    expect(reidentifies()).toBe(1);

    first();
    first();
    expect(reidentifies()).toBe(1);

    server.disconnect();
    await vi.waitFor(() => expect(client.isConnected()).toBe(false));
    await client.connect();
    expect(server.eventSubscriptions(2)).toBe(withMeters);

    second();
    await vi.waitFor(() => expect(server.eventSubscriptions(2)).toBe(EventSubscription.All));
  });

  it("keeps unapplied output settings flagged when OBS acknowledges a Reidentify", async () => {
    const server = await createServer();
    const client = createClient(server);
    await client.connect();
    client.markOutputSettingsPending();

    const release = client.subscribeHighVolume(EventSubscription.InputVolumeMeters);
    await server.waitForFrame(({ frame }) => frame.op === OBS_OP.Reidentify);
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(client.hasPendingOutputSettings()).toBe(true);
    release();
  });
});

describe("OBSWebSocketClient events and socket ownership", () => {
  it("emits both generic and event-specific OBS events", async () => {
    const server = await createServer();
    const client = createClient(server);
    await client.connect();

    const generic = once(client, "event");
    const specific = once(client, "CurrentProgramSceneChanged");
    server.sendEvent("CurrentProgramSceneChanged", { sceneName: "Program" });

    await expect(generic).resolves.toEqual([
      "CurrentProgramSceneChanged",
      { sceneName: "Program" },
    ]);
    await expect(specific).resolves.toEqual([{ sceneName: "Program" }]);
  });

  it("ignores stale socket messages and closes after reconnect", async () => {
    const server = await createServer({ availableRequests: ["GetStats"] });
    const client = createClient(server);
    await client.connect();
    const staleSocket = internals(client).ws;
    if (!staleSocket) throw new Error("Expected the first client socket");

    await client.disconnect();
    await client.connect();
    const currentSocket = internals(client).ws;
    if (!currentSocket || currentSocket === staleSocket) {
      throw new Error("Expected a replacement client socket");
    }

    const eventSpy = vi.fn();
    client.on("StaleEvent", eventSpy);
    internals(client).handleMessage(staleSocket, {
      op: OBS_OP.Event,
      d: { eventType: "StaleEvent", eventIntent: 1, eventData: { stale: true } },
    });
    staleSocket.emit("close", 1000, Buffer.alloc(0));

    expect(eventSpy).not.toHaveBeenCalled();
    expect(client.isConnected()).toBe(true);
    server.queueSuccess("GetStats", { activeFps: 60 }, 20);
    const cursor = server.cursor();
    const currentRequest = client.sendRequest("GetStats");
    const request = await server.waitForRequest("GetStats", { after: cursor });
    internals(client).handleMessage(staleSocket, {
      op: OBS_OP.RequestResponse,
      d: {
        requestType: "GetStats",
        requestId: request.frame.d.requestId,
        requestStatus: { result: true, code: 100 },
        responseData: { stale: true },
      },
    });
    await expect(currentRequest).resolves.toEqual({ activeFps: 60 });
  });

  it("waits for the current WebSocket to close and rejects pending work on disconnect", async () => {
    const server = await createServer({ availableRequests: ["Slow"] });
    const client = createClient(server);
    await client.connect();
    server.queueSuccess("Slow", {}, 500);
    const cursor = server.cursor();
    const pending = client.sendRequest("Slow");
    const pendingRejected = expect(pending).rejects.toThrow(
      "Disconnected from OBS WebSocket server",
    );
    await server.waitForRequest("Slow", { after: cursor });

    await client.disconnect();

    await pendingRejected;
    await vi.waitFor(() => expect(server.openConnectionCount).toBe(0));
    expect(internals(client).pendingRequests.size).toBe(0);
  });
});
