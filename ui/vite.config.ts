import { createLogger, defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import legacy from "@vitejs/plugin-legacy";

const logger = createLogger();
const loggerWarn = logger.warn;
logger.warn = (msg, options) => {
  // Why this warning is intentionally suppressed (and why this is the least-bad option):
  //
  // Context:
  // - Our HTML template includes optional runtime-injected user customization:
  //     {{ if ne .CustomJS "" }}
  //       <script src="./custom.js"></script>
  //     {{ end }}
  // - That script is intentionally a classic script (NOT type="module") because operators
  //   can provide arbitrary browser-side JS overrides via karma's custom JS feature.
  // - During Vite/Rolldown build, HTML script tags are analyzed for bundling and this emits:
  //     "<script src=\"./custom.js\"> ... can't be bundled without type=\"module\""
  //
  // Why we do NOT "fix" by changing HTML semantics:
  // - Switching to type="module" would change runtime behavior (module scope, loading,
  //   execution timing, import rules), which can break existing operator-provided custom.js.
  // - We must preserve backward compatibility of the customization contract.
  //
  // Why we do NOT bundle custom.js:
  // - custom.js is not project source; it is external operator-provided runtime content.
  // - Treating it as a build-time module would couple deploy-time customization to frontend
  //   build pipeline, defeating the feature's purpose.
  //
  // Why suppression is scoped this narrowly:
  // - We suppress only this exact known non-actionable message pattern.
  // - All other warnings still go through unchanged.
  // - This avoids hiding unrelated build problems while keeping CI output signal clean.
  if (msg.includes('src="./custom.js"') && msg.includes('type="module"')) {
    return;
  }
  loggerWarn(msg, options);
};

export default defineConfig({
  customLogger: logger,
  base: "./",
  plugins: [react(), legacy()],
  server: {
    open: true,
    port: 3000,
  },
  css: {
    preprocessorOptions: {
      scss: {
        silenceDeprecations: [
          "abs-percent",
          "import",
          "if-function",
          "duplicate-var-flags",
          "global-builtin",
          "color-functions",
        ],
      },
    },
  },
  legacy: {
    // e2e tests broke after upgrade to Vite v8, this fixes it.
    inconsistentCjsInterop: true,
  },
  build: {
    sourcemap: true,
  },
  resolve: {
    tsconfigPaths: true
  }
});
