/* SPDX-License-Identifier: GPL-2.0-only */
import { spawnSync } from "node:child_process";
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";

const checker = fileURLToPath(new URL("../../scripts/check-native-migration.mjs", import.meta.url));
const roots: string[] = [];
function fixture() {
  const root = mkdtempSync(join(tmpdir(), "obs-native-ratchet-")); roots.push(root);
  for (const dir of ["src", "native/src", "native/assets", "docs"]) mkdirSync(join(root, dir), { recursive: true });
  writeFileSync(join(root, "src/tool.ts"), "export const value = 1;\n");
  writeFileSync(join(root, "src/tool.test.ts"), "// compatibility behavior test fixture\n");
  writeFileSync(join(root, "native/src/main.rs"), 'fn main() { println!("fixture"); }\n');
  writeFileSync(join(root, "native/Cargo.toml"), '[package]\nname="fixture"\n');
  writeFileSync(join(root, "native/Cargo.lock"), "version = 4\n");
  writeFileSync(join(root, "native/rust-toolchain.toml"), '[toolchain]\nchannel="1.95.0"\n');
  writeFileSync(join(root, "native/assets/tools.json"), JSON.stringify({ formatVersion: 1, tools: [
    { name: "obs-get-version", annotations: { readOnlyHint: true } },
    { name: "obs-transition", annotations: { readOnlyHint: false } },
  ], requests: { "obs-get-version": { requestType: "GetVersion", inputSchema: { type: "object" } } } }));
  writeFileSync(join(root, "docs/native-migration-ledger.json"), JSON.stringify({ schemaVersion: 1,
    sourceOwnership: { "src/tool.ts": { role: "production", owner: "compat" }, "src/tool.test.ts": { role: "test", owner: "compat" } },
    contractManifest: { path: "native/assets/tools.json" }, toolRoutes: {}, sealed: null }));
  return root;
}
function run(root: string, ...args: string[]) {
  return spawnSync(process.execPath, [checker, "--root", root, ...args], { encoding: "utf8" });
}
function seal(root: string) {
  const result = run(root, "--update", "--adopt-manifest-routes");
  expect(result.status, result.stderr).toBe(0);
}
afterEach(() => { for (const root of roots.splice(0)) rmSync(root, { recursive: true, force: true }); });

describe("native migration ratchet public CLI", () => {
  it("requires an explicit seal and then checks without rewriting the ledger", () => {
    const root = fixture();
    expect(run(root).status).toBe(1);
    seal(root);
    const before = readFileSync(join(root, "docs/native-migration-ledger.json"), "utf8");
    const checked = run(root);
    expect(checked.status, checked.stderr).toBe(0);
    expect(JSON.parse(checked.stdout)).toMatchObject({ mode: "checked", sourceFiles: 2, productionSourceFiles: 1, nativeReadOnlyTools: 1, compatibilityTools: 1 });
    expect(readFileSync(join(root, "docs/native-migration-ledger.json"), "utf8")).toBe(before);
  });
  it.each(["src/tool.ts", "src/tool.test.ts", "native/src/main.rs", "native/Cargo.lock", "native/rust-toolchain.toml", "native/assets/tools.json"])("rejects sealed artifact drift: %s", (path) => {
    const root = fixture(); seal(root);
    const file = join(root, path);
    writeFileSync(file, `${readFileSync(file, "utf8")}\n`);
    const checked = run(root);
    expect(checked.status).toBe(1);
    expect(checked.stderr).toContain("drifted");
  });
  it("does not silently classify a new TypeScript source, even during update", () => {
    const root = fixture(); seal(root);
    writeFileSync(join(root, "src/new.ts"), "export const newOwner = true;\n");
    const result = run(root, "--update");
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("unclassified=src/new.ts");
  });
  it("requires a separate routing decision when a read-only route moves to native", () => {
    const root = fixture(); seal(root);
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    delete manifest.requests["obs-get-version"];
    writeFileSync(path, JSON.stringify(manifest));
    expect(run(root, "--update").stderr).toContain("Tool routing changed");
    seal(root);
    expect(JSON.parse(run(root).stdout)).toMatchObject({ nativeReadOnlyTools: 0, compatibilityTools: 2 });
  });
  it("refuses mutating manifest requests even under explicit route adoption", () => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.requests["obs-transition"] = { requestType: "TriggerStudioModeTransition" };
    writeFileSync(path, JSON.stringify(manifest));
    const result = run(root, "--update", "--adopt-manifest-routes");
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("explicit read-only Get request");
  });
  it("does not treat downloaded dependency or target Rust as project-owned production code", () => {
    const root = fixture();
    for (const path of ["native/.cargo-home/registry/src/crate/lib.rs", "native/target/debug/build/generated.rs"]) {
      mkdirSync(join(root, path, ".."), { recursive: true });
      writeFileSync(join(root, path), "fn dependency_stub() { todo!(); }\n");
    }
    seal(root);
    expect(JSON.parse(run(root).stdout)).toMatchObject({ nativeFiles: 1 });
  });
  it("keeps unsafe OBS property enumeration out of native routes despite a read-only Get declaration", () => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.requests["obs-get-version"].requestType = "GetInputPropertiesListPropertyItems";
    writeFileSync(path, JSON.stringify(manifest));
    expect(run(root, "--update", "--adopt-manifest-routes").stderr).toContain("Unsafe native property enumeration");
  });
  it.each(["missing-tool", "unknown-tool", "unknown-group", "unknown-core", "duplicate-group"])("refuses inconsistent generated filter metadata: %s", (fault) => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.toolsets = { groups: ["general", "scenes"], coreGroups: ["general"],
      toolGroups: { "obs-get-version": "general", "obs-transition": "scenes" } };
    if (fault === "missing-tool") delete manifest.toolsets.toolGroups["obs-transition"];
    if (fault === "unknown-tool") manifest.toolsets.toolGroups["obs-hidden"] = "general";
    if (fault === "unknown-group") manifest.toolsets.toolGroups["obs-transition"] = "missing";
    if (fault === "unknown-core") manifest.toolsets.coreGroups.push("missing");
    if (fault === "duplicate-group") manifest.toolsets.groups.push("general");
    writeFileSync(path, JSON.stringify(manifest));
    const result = run(root, "--update", "--adopt-manifest-routes");
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("toolset metadata");
  });
  it("seals complete generated toolset metadata without adding runtime routes", () => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    manifest.toolsets = { groups: ["general", "scenes"], coreGroups: ["general"],
      toolGroups: { "obs-get-version": "general", "obs-transition": "scenes" } };
    writeFileSync(path, JSON.stringify(manifest));
    seal(root);
    expect(JSON.parse(run(root).stdout)).toMatchObject({ nativeReadOnlyTools: 1, compatibilityTools: 1 });
  });
  it.each(["unknown-handler", "unknown-tool", "mutating-annotation", "changed-schema"])("refuses unimplemented or inconsistent local handlers: %s", (fault) => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    const inputSchema = { type: "object", properties: { requestType: { type: "string" } } };
    manifest.tools.push({ name: "obs-describe-request", annotations: { readOnlyHint: true }, inputSchema });
    manifest.locals = { "obs-describe-request": { handler: "describe-request", inputSchema: structuredClone(inputSchema) } };
    if (fault === "unknown-handler") manifest.locals["obs-describe-request"].handler = "invented";
    if (fault === "unknown-tool") manifest.locals["obs-missing"] = manifest.locals["obs-describe-request"];
    if (fault === "mutating-annotation") manifest.tools.at(-1).annotations.readOnlyHint = false;
    if (fault === "changed-schema") manifest.locals["obs-describe-request"].inputSchema.properties.requestType.type = "number";
    writeFileSync(path, JSON.stringify(manifest));
    const result = run(root, "--update", "--adopt-manifest-routes");
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("native local handler");
  });
  it("requires explicit route adoption for a native local handler", () => {
    const root = fixture(); seal(root);
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    const inputSchema = { type: "object" };
    manifest.tools.push({ name: "obs-describe-request", annotations: { readOnlyHint: true }, inputSchema });
    manifest.locals = { "obs-describe-request": { handler: "describe-request", inputSchema } };
    writeFileSync(path, JSON.stringify(manifest));
    expect(run(root, "--update").stderr).toContain("Tool routing changed");
    seal(root);
    expect(JSON.parse(run(root).stdout)).toMatchObject({ nativeReadOnlyTools: 1, nativeLocalTools: 1, compatibilityTools: 1 });
  });
  it("binds the embedded OBS protocol data to the native build graph", () => {
    const root = fixture();
    writeFileSync(join(root, "native/src/protocol_catalog.rs"), 'pub fn catalog() -> &\'static str { include_str!("../../docs/protocol.json") }\n');
    // The source's embedded input must exist before a seal may be created.
    expect(run(root, "--update", "--adopt-manifest-routes").status).toBe(1);
    writeFileSync(join(root, "docs/protocol.json"), '{"requests":[]}\n');
    seal(root);
    writeFileSync(join(root, "docs/protocol.json"), '{"requests":[{"requestType":"Changed"}]}\n');
    const checked = run(root);
    expect(checked.status).toBe(1);
    expect(checked.stderr).toContain("Native source/build graph drifted");
  });
  it("requires and seals the embedded prompt catalog as a build input", () => {
    const root = fixture();
    writeFileSync(join(root, "native/src/prompt_catalog.rs"), 'pub fn catalog() -> &\'static str { include_str!("../assets/prompts.json") }\n');
    expect(run(root, "--update", "--adopt-manifest-routes").status).toBe(1);
    writeFileSync(join(root, "native/assets/prompts.json"), '{"listing":{"prompts":[]}}\n');
    seal(root);
    writeFileSync(join(root, "native/assets/prompts.json"), '{"listing":{"prompts":[{"name":"changed"}]}}\n');
    expect(run(root).stderr).toContain("Native source/build graph drifted");
  });
  describe.each([["obs-plan-scene-template", "scene-template-plan"], ["obs-audit-scene", "scene-audit"]])("read workflow %s", (name, handler) => {
  it.each(["unknown-handler", "mutating-annotation", "changed-schema", "unknown-tool"])("refuses unsafe read-workflow declarations: %s", (fault) => {
    const root = fixture();
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    const inputSchema = { type: "object", properties: { sceneName: { type: "string" } } };
    manifest.tools.push({ name, annotations: { readOnlyHint: true }, inputSchema });
    manifest.workflows = { [name]: { handler, inputSchema: structuredClone(inputSchema) } };
    if (fault === "unknown-handler") manifest.workflows[name].handler = "invented";
    if (fault === "mutating-annotation") manifest.tools.at(-1).annotations.readOnlyHint = false;
    if (fault === "changed-schema") manifest.workflows[name].inputSchema.properties.sceneName.type = "number";
    if (fault === "unknown-tool") manifest.workflows["obs-unknown"] = manifest.workflows[name];
    writeFileSync(path, JSON.stringify(manifest));
    const result = run(root, "--update", "--adopt-manifest-routes");
    expect(result.status).toBe(1);
    expect(result.stderr).toContain("native read-only workflow");
  });
  it("requires explicit adoption of a read-only native workflow", () => {
    const root = fixture(); seal(root);
    const path = join(root, "native/assets/tools.json");
    const manifest = JSON.parse(readFileSync(path, "utf8"));
    const inputSchema = { type: "object" };
    manifest.tools.push({ name, annotations: { readOnlyHint: true }, inputSchema });
    manifest.workflows = { [name]: { handler, inputSchema } };
    writeFileSync(path, JSON.stringify(manifest));
    expect(run(root, "--update").stderr).toContain("Tool routing changed");
    seal(root);
    expect(JSON.parse(run(root).stdout)).toMatchObject({ nativeReadOnlyTools: 1, nativeLocalTools: 0, nativeWorkflowTools: 1, compatibilityTools: 1 });
  });
  });
  it.each(["fn main() {}", "fn main() { todo!(); }", "fn main() { todo!{} }", "fn main() { unimplemented![]; }", "fn main() { unimplemented!(); }", "fn stub_response() {} fn main() { stub_response(); }"])("refuses production placeholders", (rust) => {
    const root = fixture(); writeFileSync(join(root, "native/src/main.rs"), rust);
    expect(run(root, "--update", "--adopt-manifest-routes").stderr).toContain("placeholder/stub");
  });
  it("does not mistake a comment, raw string, or inline test fixture for production placeholders", () => {
    const root = fixture();
    writeFileSync(join(root, "native/src/main.rs"), 'fn main() { let _ = r#"todo!()"#; let _ = \'{\'; }\n/* nested /* todo!() */ comment */\n#[cfg(test)] mod tests { fn fixture() { unimplemented!(); } }\n');
    seal(root);
    expect(run(root).status).toBe(0);
  });
});
