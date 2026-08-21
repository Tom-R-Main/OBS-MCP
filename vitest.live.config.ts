import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["test/live/**/*.test.ts"],
    testTimeout: 30_000,
    hookTimeout: 30_000,
  },
});
