import { defineConfig } from "vitest/config";
// Historical83 comparison is explicit and never a prerequisite for current CI.
if (!process.env.RECEIPT_POLICY_ARCHIVED_BINARY) {
  throw new Error("Set RECEIPT_POLICY_ARCHIVED_BINARY to the explicitly selected immutable83 executable; current checks use native:check");
}
export default defineConfig({ test: { include: ["test/native/fixtures/receipt-policy-oracle.test.ts"], maxWorkers: 1, testTimeout: 20_000, hookTimeout: 15_000, setupFiles: ["test/support/isolated-state.ts"] } });
