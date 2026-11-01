#!/usr/bin/env node
/* SPDX-License-Identifier: GPL-2.0-only */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { existsSync, lstatSync, readFileSync, readdirSync, renameSync, writeFileSync } from "node:fs";
import { dirname, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";

const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const hash = (bytes) => createHash("sha256").update(bytes).digest("hex");
const sorted = (values) => [...values].sort();
const same = (a, b) => JSON.stringify(a) === JSON.stringify(b);
function files(root, directory, extension) {
  const found = [];
  const walk = (path) => {
    for (const entry of readdirSync(path, { withFileTypes: true })) {
      if (["target", "node_modules", ".git", ".cargo-home"].includes(entry.name)) continue;
      const full = resolve(path, entry.name);
      if (entry.isSymbolicLink()) throw new Error(`Symlinks are not migration source: ${relative(root, full)}`);
      if (entry.isDirectory()) walk(full);
      else if (entry.isFile() && entry.name.endsWith(extension)) found.push(relative(root, full).split(sep).join("/"));
    }
  };
  walk(resolve(root, directory));
  return sorted(found);
}
function ownedPath(root, path) {
  if (typeof path !== "string" || path.includes("\\") || path.startsWith("/") || path.split("/").includes("..")) throw new Error("Ledger paths must be project-relative");
  const full = resolve(root, path);
  if (lstatSync(full).isSymbolicLink()) throw new Error(`Symlink is not a sealed artifact: ${path}`);
  return full;
}

/** Blank comments and strings before recognizing production placeholder tokens.
 * This is deliberately a narrow ratchet, not a Rust parser or proof of completeness. */
export function productionRust(source) {
  let result = "", i = 0, commentDepth = 0;
  while (i < source.length) {
    if (commentDepth) {
      if (source.startsWith("/*", i)) { commentDepth++; result += "  "; i += 2; }
      else if (source.startsWith("*/", i)) { commentDepth--; result += "  "; i += 2; }
      else { result += source[i] === "\n" ? "\n" : " "; i++; }
    } else if (source.startsWith("/*", i)) { commentDepth = 1; result += "  "; i += 2; }
    else if (source.startsWith("//", i)) {
      while (i < source.length && source[i] !== "\n") { result += " "; i++; }
    } else if (/^'(?:\\.|[^'\\])'/.test(source.slice(i))) {
      const length = /^'(?:\\.|[^'\\])'/.exec(source.slice(i))[0].length;
      result += " ".repeat(length); i += length;
    } else if (source[i] === '"' || /^r#*"/.test(source.slice(i))) {
      const raw = /^r(#+)?"/.exec(source.slice(i));
      if (raw) {
        const end = source.indexOf(`"${raw[1] ?? ""}`, i + raw[0].length);
        const length = end < 0 ? source.length - i : end + 1 + (raw[1]?.length ?? 0) - i;
        result += " ".repeat(length); i += length;
      } else {
        result += " "; i++;
        while (i < source.length) {
          if (source[i] === "\\") { result += "  "; i += 2; }
          else if (source[i] === '"') { result += " "; i++; break; }
          else { result += source[i] === "\n" ? "\n" : " "; i++; }
        }
      }
    } else { result += source[i]; i++; }
  }
  // Inline #[cfg(test)] modules are hashed, but excluded from production stub policy.
  const testModule = /#\s*\[\s*cfg\s*\(\s*test\s*\)\s*\]\s*(?:pub\s+)?mod\s+\w+\s*\{/g;
  let match;
  while ((match = testModule.exec(result))) {
    let end = testModule.lastIndex, depth = 1;
    while (end < result.length && depth) { if (result[end] === "{") depth++; else if (result[end] === "}") depth--; end++; }
    result = result.slice(0, match.index) + " ".repeat(end - match.index) + result.slice(end);
  }
  return result;
}

export function checkNativeMigration({ root = defaultRoot, ledgerPath = "docs/native-migration-ledger.json", update = false, adoptManifestRoutes = false } = {}) {
  if (adoptManifestRoutes && !update) throw new Error("--adopt-manifest-routes requires explicit --update");
  const ledgerFile = ownedPath(root, ledgerPath);
  const ledger = JSON.parse(readFileSync(ledgerFile, "utf8"));
  if (ledger.schemaVersion !== 1 || typeof ledger.sourceOwnership !== "object" || !ledger.sourceOwnership) throw new Error("Unsupported or missing migration ledger schema");
  const sourcePaths = files(root, "src", ".ts");
  const declared = sorted(Object.keys(ledger.sourceOwnership));
  if (!same(sourcePaths, declared)) throw new Error(`Every src .ts file must have an explicit owner; unclassified=${sourcePaths.filter((p) => !declared.includes(p)).join(",")}; missing=${declared.filter((p) => !sourcePaths.includes(p)).join(",")}`);
  const sourceHashes = {};
  for (const path of sourcePaths) {
    const entry = ledger.sourceOwnership[path];
    const expectedRole = /\.(?:test|spec)\.ts$/.test(path) ? "test" : "production";
    if (entry?.owner !== "compat" || entry?.role !== expectedRole) throw new Error(`Invalid source ownership/role for ${path}; TS remains explicit compatibility authority`);
    sourceHashes[path] = hash(readFileSync(ownedPath(root, path)));
  }
  const nativePaths = sorted([...files(root, "native/src", ".rs"),
    ...(existsSync(resolve(root, "native/tests")) ? files(root, "native/tests", ".rs") : []),
    ...(existsSync(resolve(root, "native/build.rs")) ? ["native/build.rs"] : [])]);
  if (!nativePaths.includes("native/src/main.rs")) throw new Error("Missing native service entry point");
  const nativeHashes = {};
  const embeddedInputs = [
    ...(nativePaths.includes("native/src/protocol_catalog.rs") ? ["docs/protocol.json"] : []),
    ...(nativePaths.includes("native/src/prompt_catalog.rs") ? ["native/assets/prompts.json"] : []),
  ];
  for (const path of [...nativePaths, "native/Cargo.toml", "native/Cargo.lock", "native/rust-toolchain.toml", ...embeddedInputs]) {
    const source = readFileSync(ownedPath(root, path));
    nativeHashes[path] = hash(source);
    if (path.endsWith(".rs") && !path.startsWith("native/tests/")) {
      const code = productionRust(source.toString("utf8"));
      if (/\b(?:todo|unimplemented)\s*!/.test(code)
        || /\bfn\s+main\s*\([^)]*\)\s*\{\s*\}/.test(code)
        || /\bfn\s+\w*stub\w*\s*\(/i.test(code)) throw new Error(`Production Rust contains a placeholder/stub: ${path}`);
    }
  }
  const contractPath = ledger.contractManifest?.path;
  if (contractPath !== "native/assets/tools.json") throw new Error("Ledger must bind native/assets/tools.json");
  const contractBytes = readFileSync(ownedPath(root, contractPath));
  const manifest = JSON.parse(contractBytes.toString("utf8"));
  if (manifest.formatVersion !== 1 || !Array.isArray(manifest.tools) || !manifest.requests || typeof manifest.requests !== "object") throw new Error("Unsupported native contract manifest");
  const names = manifest.tools.map((tool) => tool.name);
  if (names.some((name) => typeof name !== "string") || new Set(names).size !== names.length) throw new Error("Invalid or duplicate manifest tool names");
  // Older format-v1 contracts have no filter metadata. When present it must be
  // complete: an omitted or misgrouped tool must never widen native admission.
  if (Object.hasOwn(manifest, "toolsets")) {
    const { groups, coreGroups, toolGroups } = manifest.toolsets ?? {};
    const uniqueStrings = (values) => Array.isArray(values)
      && values.every((value) => typeof value === "string" && value.length > 0)
      && new Set(values).size === values.length;
    if (!uniqueStrings(groups) || groups.length === 0 || !uniqueStrings(coreGroups)
      || coreGroups.some((group) => !groups.includes(group))
      || !toolGroups || typeof toolGroups !== "object" || Array.isArray(toolGroups)
      || !same(sorted(Object.keys(toolGroups)), sorted(names))
      || Object.values(toolGroups).some((group) => !groups.includes(group))) {
      throw new Error("Invalid or incomplete generated toolset metadata");
    }
  }
  const requests = manifest.requests;
  for (const name of Object.keys(requests)) {
    const tool = manifest.tools.find((entry) => entry.name === name);
    if (requests[name]?.requestType === "GetInputPropertiesListPropertyItems") throw new Error(`Unsafe native property enumeration must remain compatibility-owned: ${name}`);
    if (tool?.annotations?.readOnlyHint !== true || !/^Get[A-Za-z0-9]+$/.test(requests[name]?.requestType ?? "")) throw new Error(`Native route must be an explicit read-only Get request: ${name}`);
  }
  const locals = Object.hasOwn(manifest, "locals") ? manifest.locals : {};
  if (!locals || typeof locals !== "object" || Array.isArray(locals)) throw new Error("Invalid native local handler catalog");
  for (const [name, definition] of Object.entries(locals)) {
    const tool = manifest.tools.find((entry) => entry.name === name);
    if (name !== "obs-describe-request" || definition?.handler !== "describe-request"
      || tool?.annotations?.readOnlyHint !== true || Object.hasOwn(requests, name)
      || !tool?.inputSchema || !same(definition.inputSchema, tool.inputSchema)) {
      throw new Error(`Unimplemented or inconsistent native local handler: ${name}`);
    }
  }
  const workflows = Object.hasOwn(manifest, "workflows") ? manifest.workflows : {};
  if (!workflows || typeof workflows !== "object" || Array.isArray(workflows)) throw new Error("Invalid native read-only workflow catalog");
  const workflowHandlers = new Map([["obs-plan-scene-template", "scene-template-plan"], ["obs-audit-scene", "scene-audit"]]);
  for (const [name, definition] of Object.entries(workflows)) {
    const tool = manifest.tools.find((entry) => entry.name === name);
    if (workflowHandlers.get(name) !== definition?.handler
      || tool?.annotations?.readOnlyHint !== true || Object.hasOwn(requests, name) || Object.hasOwn(locals, name)
      || !tool?.inputSchema || !same(definition.inputSchema, tool.inputSchema)) {
      throw new Error(`Unimplemented or inconsistent native read-only workflow: ${name}`);
    }
  }
  // An explicitly activated qualification lane never changes default routing.
  if (Object.hasOwn(manifest, "service")) {
    const service=manifest.service;
    const handlers=new Map([["obs-restore","restore"],["obs-record-clip","record-clip"],["obs-toggle-stream","literal-setter"],["obs-toggle-virtual-cam","literal-setter"],["obs-toggle-replay-buffer","literal-setter"],["obs-stop-record","manual-record-stop"],["obs-start-stream","confirmed-output-start"],["obs-start-virtual-cam","confirmed-output-start"],["obs-start-replay-buffer","confirmed-output-start"],["obs-stop-stream","literal-setter"],["obs-stop-virtual-cam","literal-setter"],["obs-stop-replay-buffer","literal-setter"],["obs-set-studio-mode","literal-setter"],["obs-open-input-properties","literal-setter"],["obs-open-input-filters","literal-setter"],["obs-open-input-interact","literal-setter"],["obs-open-video-mix-projector","literal-setter"],["obs-open-source-projector","literal-setter"],["obs-pause-record","literal-setter"],["obs-resume-record","literal-setter"],["obs-toggle-record-pause","literal-setter"],["obs-split-record-file","literal-setter"],["obs-create-record-chapter","literal-setter"],["obs-set-scene-item-transform","literal-setter"],["obs-broadcast-custom-event","literal-setter"],["obs-set-transition-settings","literal-setter"],["obs-create-source-filter","literal-setter"],["obs-set-source-filter-settings","literal-setter"],["obs-press-input-property-button","literal-setter"],["obs-save-source-screenshot","literal-setter"],["obs-create-input","literal-setter"],["obs-set-input-settings","literal-setter"],["obs-set-persistent-data","literal-setter"],["obs-set-current-scene-collection","literal-setter"],["obs-create-scene-collection","literal-setter"],["obs-set-current-profile","literal-setter"],["obs-create-profile","literal-setter"],["obs-remove-profile","literal-setter"],["obs-set-stream-service-settings","literal-setter"],["obs-set-record-directory","literal-setter"],["obs-set-output-settings","literal-setter"],["obs-set-video-settings","literal-setter"],["obs-set-profile-parameter","literal-setter"],["obs-set-media-input-cursor","literal-setter"],["obs-offset-media-input-cursor","literal-setter"],["obs-trigger-media-input-action","literal-setter"],["obs-set-input-audio-tracks","literal-setter"],["obs-toggle-input-mute","literal-setter"],["obs-set-input-volume","literal-setter"],["obs-send-stream-caption","literal-setter"],["obs-save-replay-buffer","literal-setter"],["obs-create-scene","literal-setter"],["obs-remove-scene","literal-setter"],["obs-remove-scene-item","literal-setter"],["obs-remove-source-filter","literal-setter"],["obs-set-source-filter-name","literal-setter"],["obs-set-source-filter-index","literal-setter"],["obs-remove-input","literal-setter"],["obs-set-input-name","literal-setter"],["obs-duplicate-scene-item","literal-setter"],["obs-set-scene-name","literal-setter"],["obs-create-scene-item","literal-setter"],["obs-set-scene-item-enabled","literal-setter"],["obs-set-current-scene","literal-setter"],["obs-set-preview-scene","literal-setter"],["obs-trigger-studio-transition","literal-setter"],["obs-set-current-transition","literal-setter"],["obs-trigger-transition","literal-setter"],["obs-set-scene-transition-override","literal-setter"],["obs-set-tbar-position","literal-setter"],["obs-set-transition-duration","literal-setter"],["obs-snapshot","snapshot"],["obs-get-input-property-list-items","property-list"],["obs-set-input-mute","literal-setter"],["obs-set-input-audio-balance","literal-setter"],["obs-set-input-audio-sync-offset","literal-setter"],["obs-set-input-audio-monitor-type","literal-setter"],["obs-set-input-deinterlace-field-order","literal-setter"],["obs-set-input-deinterlace-mode","literal-setter"],["obs-set-scene-item-index","literal-setter"],["obs-set-scene-item-locked","literal-setter"],["obs-set-scene-item-blend-mode","literal-setter"],["obs-set-source-filter-enabled","literal-setter"],["obs-get-status","get-status"],["obs-test-connection","test-connection"],["obs-sleep","sleep"],["obs-get-input-list","input-list"],["obs-get-input-kind-list","input-kind-list"],["obs-get-source-screenshot","source-screenshot"],["obs-preflight","preflight"],["obs-observe","observe-production"],["obs-stage-scene","stage-scene"],["obs-take-scene","take-scene"],["obs-take-start","take-start"],["obs-take-stop","take-stop"],["obs-take-mark","take-mark"],["obs-take-status","take-status"],["obs-claim-control","claim-control"],["obs-release-control","release-control"],["obs-control-status","control-status"],["obs-prepare-demo","prepare-demo"],["obs-recover-demo","recover-demo"],["obs-plan-scene-template","scene-template-plan"],["obs-audit-scene","scene-audit"],["obs-contact-sheet","contact-sheet"],["obs-trim-take","trim-take"]]);
    const readOnly=new Set(["obs-snapshot","obs-get-input-property-list-items","obs-observe","obs-take-status","obs-control-status","obs-plan-scene-template","obs-audit-scene","obs-contact-sheet","obs-get-status","obs-test-connection","obs-sleep","obs-get-input-list","obs-get-input-kind-list","obs-get-source-screenshot","obs-preflight"]);
    if (!service || service.activation!=="OBS_MCP_NATIVE_STATEFUL" || !service.handlers || Array.isArray(service.handlers)
      || !same(sorted(Object.keys(service.handlers)),sorted([...handlers.keys()]))) throw new Error("Invalid opt-in persistent service declaration");
    for(const [name,definition] of Object.entries(service.handlers)) {
      const tool=manifest.tools.find(tool=>tool.name===name);
      if (handlers.get(name)!==definition?.handler || tool?.annotations?.readOnlyHint!==readOnly.has(name)
        || !tool?.inputSchema || !same(definition.inputSchema,tool.inputSchema)) throw new Error(`Unimplemented opt-in service route: ${name}`);
    }
  }
  const routes = Object.fromEntries(sorted(names).map((name) => [name, Object.hasOwn(requests, name) ? "native-readonly" : Object.hasOwn(locals, name) ? "native-local" : Object.hasOwn(workflows, name) ? "native-readonly-workflow" : "compat"]));
  if (!adoptManifestRoutes && !same(ledger.toolRoutes, routes)) throw new Error("Tool routing changed; review manifest and explicitly reseal with --update --adopt-manifest-routes");
  const contractHash = hash(contractBytes);
  if (update) {
    let sourceRevision = null;
    try { sourceRevision = execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim(); } catch { /* Fixtures and source archives may have no Git metadata. */ }
    const sealed = { ...ledger, toolRoutes: routes, contractManifest: { path: contractPath, sha256: contractHash },
      sealed: { at: new Date().toISOString(), sourceRevision, sourceHashes, nativeHashes } };
    const temporary = `${ledgerFile}.${process.pid}.tmp`;
    writeFileSync(temporary, `${JSON.stringify(sealed, null, 2)}\n`, { flag: "wx" });
    renameSync(temporary, ledgerFile);
  } else {
    if (!ledger.sealed) throw new Error("Migration ledger is unsealed; review then run --update");
    if (!same(ledger.sealed.sourceHashes, sourceHashes)) throw new Error("TypeScript source drifted after migration seal");
    if (!same(ledger.sealed.nativeHashes, nativeHashes)) throw new Error("Native source/build graph drifted after migration seal");
    if (ledger.contractManifest.sha256 !== contractHash) throw new Error("Contract manifest drifted after migration seal");
  }
  return { mode: update ? "sealed" : "checked", sourceFiles: sourcePaths.length, productionSourceFiles: sourcePaths.filter((p) => ledger.sourceOwnership[p].role === "production").length, nativeFiles: nativePaths.length, nativeReadOnlyTools: Object.values(routes).filter((owner) => owner === "native-readonly").length, nativeLocalTools: Object.values(routes).filter((owner) => owner === "native-local").length, nativeWorkflowTools: Object.values(routes).filter((owner) => owner === "native-readonly-workflow").length, compatibilityTools: Object.values(routes).filter((owner) => owner === "compat").length, contractSha256: contractHash };
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { values } = parseArgs({ options: { root: { type: "string" }, ledger: { type: "string" }, update: { type: "boolean", default: false }, "adopt-manifest-routes": { type: "boolean", default: false } } });
    console.log(JSON.stringify(checkNativeMigration({ root: values.root ? resolve(values.root) : defaultRoot, ledgerPath: values.ledger, update: values.update, adoptManifestRoutes: values["adopt-manifest-routes"] })));
  } catch (error) { console.error(`Native migration check failed: ${error.message}`); process.exitCode = 1; }
}
