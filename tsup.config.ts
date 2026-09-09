import { defineConfig } from "tsup";

export default defineConfig({
  entry: {
    index: "src/index.ts",
    utils: "src/utils.ts",
    subscriber: "src/subscriber.ts",
  },
  format: ["esm", "cjs"],
  dts: true,
  sourcemap: true,
  clean: true,
  target: "node18",
});
