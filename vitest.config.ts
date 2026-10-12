import { defineConfig } from "vitest/config";
import { fileURLToPath } from "node:url";

export default defineConfig({
  // The same source-not-dist resolution the root tsconfig's `paths` set up for
  // tsc. `packages/backend-redis/src` imports `@dianemo/core/backend` by name,
  // so without this every suite resolves it through the workspace symlink to
  // a `dist` that a fresh clone has not built yet and fails to collect.
  //
  // Anchored patterns, not string keys: a string alias also rewrites every
  // subpath under it, which would send `@dianemo/core/backend` to
  // `src/index.ts/backend`.
  resolve: {
    alias: [
      {
        find: /^@dianemo\/core$/,
        replacement: fileURLToPath(
          new URL("./packages/core/src/index.ts", import.meta.url)
        ),
      },
      {
        find: /^@dianemo\/core\/backend$/,
        replacement: fileURLToPath(
          new URL("./packages/core/src/backend/index.ts", import.meta.url)
        ),
      },
      {
        find: /^@dianemo\/backend-redis$/,
        replacement: fileURLToPath(
          new URL("./packages/backend-redis/src/index.ts", import.meta.url)
        ),
      },
    ],
  },
  test: {
    include: ["test/**/*.test.ts"],
    environment: "node",
    // Run test files one at a time. Several suites assert on timing and on
    // occupancy at a shared HTTP upstream, and four of them competing for one
    // CPU and one Redis made those assertions measure the machine rather than
    // the code — a suite that timed out would leave its client still draining
    // into the next suite's measurements.
    fileParallelism: false,
    typecheck: {
      // Type-level assertions guard the plugin namespace inference — if `use()`
      // ever returns `any`, the runtime tests would still pass while every
      // consumer silently loses type safety.
      include: ["test/**/*.test-d.ts"],
      tsconfig: "./tsconfig.json",
    },
    coverage: {
      include: ["packages/*/src/**/*.ts"],
    },
  },
});
