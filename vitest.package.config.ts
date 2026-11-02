import {defineConfig} from 'vitest/config';
// Product artifacts are native-only. The former Node artifact suite remains
// development history in mcpb-artifact.test.ts and is not a shipping entry.
export default defineConfig({test:{include:['test/package/native-artifact.test.ts'],testTimeout:20_000,hookTimeout:15_000,maxWorkers:2,setupFiles:['test/support/isolated-state.ts']}});
