#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const defaultRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const exactVersion = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;
const dependencyFields = ["dependencies", "devDependencies", "optionalDependencies"];

function sameDependencies(left = {}, right = {}) {
  return Object.keys(left).length === Object.keys(right).length
    && Object.entries(left).every(([name, version]) => right[name] === version);
}

/** Validate the graph without installing packages or contacting a registry. */
export function validateDependencyPolicy(manifest, lock) {
  const errors = [];
  if (lock.lockfileVersion !== 3) errors.push("The dependency lockfile must use version 3.");
  const packages = lock.packages;
  const root = packages?.[""];
  if (!root) errors.push("The lockfile must contain root package metadata.");
  if (lock.name !== manifest.name || lock.version !== manifest.version
    || root?.name !== manifest.name || root?.version !== manifest.version) {
    errors.push("Package name/version must match the lockfile and its root metadata.");
  }
  for (const field of dependencyFields) {
    const dependencies = manifest[field] ?? {};
    if (!sameDependencies(dependencies, root?.[field])) {
      errors.push(`${field} must match the lockfile root metadata.`);
    }
    for (const [name, version] of Object.entries(dependencies)) {
      if (typeof version !== "string" || !exactVersion.test(version)) {
        errors.push(`${field}.${name} must use an exact version.`);
      }
      if (packages?.[`node_modules/${name}`]?.version !== version) {
        errors.push(`${field}.${name} must match its installed lockfile entry.`);
      }
    }
  }
  for (const [location, entry] of Object.entries(packages ?? {})) {
    if (location === "") continue;
    if (!location.startsWith("node_modules/") || entry.link) {
      errors.push(`${location}: local/workspace links are not allowed.`);
    }
    let source;
    try { source = new URL(entry.resolved); } catch { /* Report as an invalid source below. */ }
    if (!source || source.protocol !== "https:" || source.hostname !== "registry.npmjs.org"
      || source.port || source.username || source.password || source.search || source.hash
      || !source.pathname.endsWith(".tgz")) {
      errors.push(`${location}: dependencies must resolve to HTTPS npm registry tarballs.`);
    }
    if (typeof entry.integrity !== "string" || !/^sha512-[A-Za-z0-9+/]{86}==$/.test(entry.integrity)) {
      errors.push(`${location}: dependencies must have SHA-512 integrity.`);
    }
    // fsevents is an optional macOS watcher used by development tooling. Its
    // install hook stays disabled; every new hook requires an explicit review.
    if (entry.hasInstallScript && !(location === "node_modules/fsevents"
      && entry.version === "2.3.3" && entry.dev === true && entry.optional === true)) {
      errors.push(`${location}: unreviewed dependency install scripts are not allowed.`);
    }
  }
  if (errors.length) throw new Error(`Dependency policy failed:\n${errors.map((error) => `- ${error}`).join("\n")}`);
  return { packageCount: Object.keys(packages).length - 1 };
}

export function checkDependencies({ root = defaultRoot } = {}) {
  const readJson = (file) => JSON.parse(readFileSync(resolve(root, file), "utf8"));
  return validateDependencyPolicy(readJson("package.json"), readJson("package-lock.json"));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    const { packageCount } = checkDependencies();
    console.log(`Dependency policy passed (${packageCount} locked packages).`);
  } catch (error) {
    console.error(error.message);
    process.exitCode = 1;
  }
}
