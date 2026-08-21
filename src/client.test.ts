import { once } from "node:events";
import type { AddressInfo } from "node:net";
import { afterEach, describe, expect, it } from "vitest";
import { WebSocketServer } from "ws";
import { OBSWebSocketClient } from "./client.js";

const servers: WebSocketServer[] = [];

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map(
      (server) => new Promise<void>((resolve) => server.close(() => resolve())),
    ),
  );
});

describe("OBSWebSocketClient", () => {
  it("shares one in-flight connection and resolves only after identification", async () => {
    const server = new WebSocketServer({ port: 0 });
    servers.push(server);
    await once(server, "listening");

    let connectionCount = 0;
    let identifyCount = 0;

    server.on("connection", (socket) => {
      connectionCount += 1;
      socket.send(JSON.stringify({
        op: 0,
        d: {
          obsStudioVersion: "32.2.2",
          obsWebSocketVersion: "5.7.0",
          rpcVersion: 1,
        },
      }));

      socket.on("message", (data) => {
        const message = JSON.parse(data.toString()) as {
          op: number;
          d: { requestId?: string; requestType?: string };
        };
        if (message.op === 1) {
          identifyCount += 1;
          socket.send(JSON.stringify({ op: 2, d: { negotiatedRpcVersion: 1 } }));
        } else if (message.op === 6) {
          socket.send(JSON.stringify({
            op: 7,
            d: {
              requestId: message.d.requestId,
              requestType: message.d.requestType,
              requestStatus: { result: true, code: 100 },
              responseData: {
                obsStudioVersion: "32.2.2",
                availableRequests: ["GetVersion"],
              },
            },
          }));
        }
      });
    });

    const { port } = server.address() as AddressInfo;
    const client = new OBSWebSocketClient(`ws://127.0.0.1:${port}`);
    const firstConnection = client.connect();
    const secondConnection = client.connect();

    expect(firstConnection).toBe(secondConnection);
    expect(client.isConnected()).toBe(false);
    await firstConnection;

    expect(client.isConnected()).toBe(true);
    expect(client.supportsRequest("GetVersion")).toBe(true);
    expect(client.supportsRequest("StartStream")).toBe(false);
    expect(connectionCount).toBe(1);
    expect(identifyCount).toBe(1);
    client.disconnect();
  });

  it("correlates request responses after identification", async () => {
    const server = new WebSocketServer({ port: 0 });
    servers.push(server);
    await once(server, "listening");

    server.on("connection", (socket) => {
      socket.send(JSON.stringify({
        op: 0,
        d: {
          obsStudioVersion: "32.2.2",
          obsWebSocketVersion: "5.7.0",
          rpcVersion: 1,
        },
      }));

      socket.on("message", (data) => {
        const message = JSON.parse(data.toString()) as {
          op: number;
          d: { requestId?: string; requestType?: string };
        };

        if (message.op === 1) {
          socket.send(JSON.stringify({ op: 2, d: { negotiatedRpcVersion: 1 } }));
        } else if (message.op === 6) {
          socket.send(JSON.stringify({
            op: 7,
            d: {
              requestId: message.d.requestId,
              requestType: message.d.requestType,
              requestStatus: { result: true, code: 100 },
              responseData: {
                obsStudioVersion: "32.2.2",
                availableRequests: ["GetVersion"],
              },
            },
          }));
        }
      });
    });

    const { port } = server.address() as AddressInfo;
    const client = new OBSWebSocketClient(`ws://127.0.0.1:${port}`);
    await client.connect();

    await expect(client.sendRequest("GetVersion")).resolves.toEqual({
      obsStudioVersion: "32.2.2",
      availableRequests: ["GetVersion"],
    });
    await expect(client.sendRequest("StartStream")).rejects.toThrow(
      "does not advertise support",
    );
    client.disconnect();
  });
});
