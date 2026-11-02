/* SPDX-License-Identifier: GPL-2.0-only */
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { OBSWebSocketClient } from "../../src/client.js";
import { stateDirectory } from "../../src/state-dir.js";
import { OperationReceiptError, prepareOperation, updateOperation } from "../../src/tools/operation-receipts.js";

const io = vi.hoisted(() => ({
  host: process.platform,
  handles: new Map<number, { path: string; directory: boolean }>(),
  directoryOpens: [] as string[],
  directorySyncs: [] as string[],
  directoryCloses: [] as string[],
  fileSyncs: 0,
  refuseDirectoryOpenAt: 0,
  refuseDirectorySyncAt: 0,
  refuseFileSync: false,
}));

vi.mock("node:fs", async (original) => {
  const fs = await original<typeof import("node:fs")>();
  return {
    ...fs,
    openSync: (path: Parameters<typeof fs.openSync>[0], flags: string | number, mode?: number) => {
      const directory = flags === "r" && fs.statSync(path).isDirectory();
      if (directory) {
        io.directoryOpens.push(String(path));
        if (io.directoryOpens.length === io.refuseDirectoryOpenAt) {
          throw Object.assign(new Error("Directory open refused"), { code: "EISDIR" });
        }
      }
      // Windows cannot open directory handles through this API. Simulated
      // POSIX cases there use directory-only handles; file I/O remains real.
      const fd = directory && io.host === "win32"
        ? -1 - io.directoryOpens.length : fs.openSync(path, flags, mode);
      io.handles.set(fd, { path: String(path), directory });
      return fd;
    },
    fsyncSync: (fd: number) => {
      const handle = io.handles.get(fd);
      if (handle?.directory) {
        io.directorySyncs.push(handle.path);
        if (io.directorySyncs.length === io.refuseDirectorySyncAt) {
          throw Object.assign(new Error("Directory sync failed"), { code: "EIO" });
        }
      } else {
        io.fileSyncs++;
        if (io.refuseFileSync) throw Object.assign(new Error("File sync failed"), { code: "EIO" });
      }
      if (fd >= 0) fs.fsyncSync(fd);
    },
    closeSync: (fd: number) => {
      const handle = io.handles.get(fd);
      if (handle?.directory) io.directoryCloses.push(handle.path);
      io.handles.delete(fd);
      if (fd >= 0) fs.closeSync(fd);
    },
  };
});

function onPlatform<T>(platform: NodeJS.Platform, action: () => T): T {
  const descriptor = Object.getOwnPropertyDescriptor(process, "platform")!;
  Object.defineProperty(process, "platform", { ...descriptor, value: platform });
  try { return action(); }
  finally { Object.defineProperty(process, "platform", descriptor); }
}

const client = () => new OBSWebSocketClient("ws://127.0.0.1:4455");
const directory = () => join(stateDirectory(), "operation-receipts");
const persisted = () => {
  const names = readdirSync(directory());
  expect(names).toHaveLength(1);
  expect(names[0]).toMatch(/\.json$/);
  return JSON.parse(readFileSync(join(directory(), names[0]!), "utf8")) as Record<string, unknown>;
};

beforeEach(() => {
  expect(io.handles.size).toBe(0);
  io.directoryOpens.length = io.directorySyncs.length = io.directoryCloses.length = 0;
  io.fileSyncs = io.refuseDirectoryOpenAt = io.refuseDirectorySyncAt = 0;
  io.refuseFileSync = false;
});

describe("operation receipt persistence across platforms", () => {
  it("persists and updates on Windows without attempting unsupported directory opens, retaining file fsync", () => {
    io.refuseDirectoryOpenAt = 1;
    onPlatform("win32", () => {
      const receipt = prepareOperation(client(), "record-start", { takeId: "fixture-take" });
      expect(persisted()).toEqual(receipt);
      const updated = updateOperation(receipt, { phase: "dispatching", requestDispatched: null });
      expect(persisted()).toEqual(updated);
      expect(updated).toMatchObject({ phase: "dispatching", requestDispatched: null, postconditionObserved: false });
    });
    expect(io.fileSyncs).toBe(2);
    expect(io.directoryOpens).toEqual([]);
    expect(io.handles.size).toBe(0);
  });

  it("syncs and closes both directories after the file on POSIX", () => {
    onPlatform("linux", () => expect(persistedAfterPrepare()).toMatchObject({ phase: "prepared", requestDispatched: false }));
    expect(io.fileSyncs).toBe(1);
    expect(io.directoryOpens).toEqual([directory(), stateDirectory()]);
    expect(io.directorySyncs).toEqual(io.directoryOpens);
    expect(io.directoryCloses).toEqual(io.directoryOpens);
    expect(io.handles.size).toBe(0);
  });

  for (const at of [1, 2]) {
    it(`preserves POSIX directory-open failure ${at} after rename without claiming persistence success`, () => {
      io.refuseDirectoryOpenAt = at;
      onPlatform("linux", () => expect(() => prepareOperation(client(), "take-scene")).toThrow(OperationReceiptError));
      expect(persisted()).toMatchObject({ kind: "take-scene", phase: "prepared", requestDispatched: false });
      expect(io.directoryOpens).toHaveLength(at);
      expect(io.directorySyncs).toHaveLength(at - 1);
      expect(io.directoryCloses).toHaveLength(at - 1);
      expect(io.handles.size).toBe(0);
    });

    it(`closes a failing POSIX directory-sync handle ${at} and retains the generic failure contract`, () => {
      io.refuseDirectorySyncAt = at;
      onPlatform("linux", () => expect(() => prepareOperation(client(), "take-scene")).toThrow(OperationReceiptError));
      expect(persisted()).toMatchObject({ phase: "prepared", requestDispatched: false });
      expect(io.directorySyncs).toHaveLength(at);
      expect(io.directoryCloses).toEqual(io.directoryOpens);
      expect(io.handles.size).toBe(0);
    });
  }

  for (const platform of ["win32", "linux"] as const) {
    it(`still refuses file fsync failure and removes its uncommitted temporary file on ${platform}`, () => {
      io.refuseFileSync = true;
      onPlatform(platform, () => expect(() => prepareOperation(client(), "record-stop")).toThrow(OperationReceiptError));
      expect(readdirSync(directory())).toEqual([]);
      expect(io.directoryOpens).toEqual([]);
      expect(io.handles.size).toBe(0);
    });
  }
});

function persistedAfterPrepare() {
  const receipt = prepareOperation(client(), "stage-scene");
  expect(persisted()).toEqual(receipt);
  return receipt;
}
