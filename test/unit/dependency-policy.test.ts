import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import { validateDependencyPolicy } from "../../scripts/check-dependencies.mjs";

const integrity = `sha512-${Buffer.alloc(64).toString("base64")}`;

function fixture() {
  const manifest = {
    name: "dependency-policy-fixture",
    version: "1.0.0",
    dependencies: { ws: "8.21.3" },
    devDependencies: { fsevents: "2.3.3" },
  };
  const lock = {
    name: manifest.name,
    version: manifest.version,
    lockfileVersion: 3,
    packages: {
      "": structuredClone(manifest),
      "node_modules/ws": {
        version: "8.21.3",
        resolved: "https://registry.npmjs.org/ws/-/ws-8.21.3.tgz",
        integrity,
        hasInstallScript: false,
        dev: false,
        optional: false,
        link: false,
      },
      "node_modules/fsevents": {
        version: "2.3.3",
        resolved: "https://registry.npmjs.org/fsevents/-/fsevents-2.3.3.tgz",
        integrity,
        hasInstallScript: true,
        dev: true,
        optional: true,
        link: false,
      },
    },
  };
  return { manifest, lock };
}

describe("offline dependency policy", () => {
  it("accepts the pinned registry graph and reviewed optional development watcher", () => {
    const { manifest, lock } = fixture();
    expect(validateDependencyPolicy(manifest, lock)).toEqual({ packageCount: 2 });
  });

  it("accepts the current project graph without an install", () => {
    const readJson = (file: string): Record<string, unknown> => JSON.parse(
      readFileSync(new URL(`../../${file}`, import.meta.url), "utf8"),
    ) as Record<string, unknown>;
    expect(validateDependencyPolicy(readJson("package.json"), readJson("package-lock.json")))
      .toEqual({ packageCount: expect.any(Number) });
  });

  it.each(["^8.21.3", "~8.21.3", "*", "latest", "npm:ws@8.21.3", "file:../ws", "github:example/ws"])(
    "rejects non-exact direct dependency %s even when root metadata agrees",
    (version) => {
      const { manifest, lock } = fixture();
      manifest.dependencies.ws = version;
      lock.packages[""].dependencies.ws = version;
      expect(() => validateDependencyPolicy(manifest, lock)).toThrow("must use an exact version");
    },
  );

  it.each([
    "http://registry.npmjs.org/ws/-/ws-8.21.3.tgz",
    "https://registry.npmjs.org.example.org/ws.tgz",
    "https://example.org/ws.tgz",
    "https://user:password@registry.npmjs.org/ws.tgz",
    "https://registry.npmjs.org:444/ws.tgz",
    "https://registry.npmjs.org/ws.tgz?token=example",
    "https://registry.npmjs.org/ws.tgz#fragment",
    "git+https://github.com/example/ws.git",
    "file:../ws",
    "https://registry.npmjs.org/ws/package.json",
  ])("rejects untrusted dependency source %s", (resolved) => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].resolved = resolved;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("HTTPS npm registry tarballs");
  });

  it.each(["", "sha1-abcdef", "sha512-invalid", "sha512-"])("rejects invalid integrity %s", (value) => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].integrity = value;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("SHA-512 integrity");
  });

  it("rejects a different lockfile format", () => {
    const { manifest, lock } = fixture();
    lock.lockfileVersion = 2;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("version 3");
  });

  it("rejects stale package identity", () => {
    const { manifest, lock } = fixture();
    lock.version = "0.9.0";
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("name/version must match");
  });

  it("rejects divergent dependency root metadata", () => {
    const { manifest, lock } = fixture();
    lock.packages[""].dependencies.ws = "8.21.2";
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("dependencies must match");
  });

  it("rejects a direct dependency resolving to a different locked version", () => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].version = "8.21.2";
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("installed lockfile entry");
  });

  it("rejects workspace links even with registry metadata", () => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].link = true;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("links are not allowed");
  });

  it("rejects production installation hooks", () => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].hasInstallScript = true;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("install scripts are not allowed");
  });

  it("rejects new installation hooks even if marked development-only", () => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/ws"].hasInstallScript = true;
    lock.packages["node_modules/ws"].dev = true;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("install scripts are not allowed");
  });

  it.each(["dev", "optional"] as const)("requires the reviewed watcher to remain %s", (flag) => {
    const { manifest, lock } = fixture();
    lock.packages["node_modules/fsevents"][flag] = false;
    expect(() => validateDependencyPolicy(manifest, lock)).toThrow("install scripts are not allowed");
  });
});
