import {defineConfig} from 'vitest/config';
export default defineConfig({test:{include:['test/qualification/source-connect-timing.test.ts'],maxWorkers:1,testTimeout:15000,hookTimeout:15000}});
