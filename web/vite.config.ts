import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The console is served from the master at the root path. During development,
// proxy the operator API and health endpoint to a locally running master so
// `npm run dev` works against live data without CORS concerns.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "../internal/webui/static",
    emptyOutDir: true,
  },
  server: {
    proxy: {
      "/api": "http://127.0.0.1:8787",
      "/healthz": "http://127.0.0.1:8787",
    },
  },
});
