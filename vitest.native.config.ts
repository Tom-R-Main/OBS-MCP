import { defineConfig } from "vitest/config";
export default defineConfig({ test: { include: ["test/native/**/*.test.ts"], exclude: ["test/native/fixtures/receipt-policy-oracle.test.ts"], maxWorkers: 2, testTimeout: 15_000, hookTimeout: 15_000, setupFiles: ["test/support/isolated-state.ts"] } });
