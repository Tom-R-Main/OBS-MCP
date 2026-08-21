import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    include: ["src/**/*.test.ts", "test/unit/**/*.test.ts"],
    exclude: ["test/e2e/**", "test/package/**", "test/live/**"],
  },
});
