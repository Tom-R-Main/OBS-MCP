import { defineConfig } from "vitest/config";
export default defineConfig({ test: { include: ["test/native/**/*.test.ts"], testTimeout: 15_000, hookTimeout: 15_000, setupFiles: ["test/support/isolated-state.ts"] } });
