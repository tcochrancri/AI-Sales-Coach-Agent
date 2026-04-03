import path from "node:path";
import react from "@vitejs/plugin-react-swc";
import { defineConfig } from "vite";

const apiTarget = process.env.VITE_API_URL ?? "http://127.0.0.1:8000";

export default defineConfig({
  envDir: path.resolve(__dirname, ".."),
  plugins: [react()],
  optimizeDeps: {
    include: ["pdf-lib"],
  },
  server: {
    port: 3001,
    strictPort: true,
    host: "0.0.0.0",
    proxy: {
      "/api": {
        target: apiTarget,
        changeOrigin: true,
      },
    },
  },
  preview: {
    port: 3001,
    strictPort: true,
    host: "0.0.0.0",
    allowedHosts: [
      "ai-sales-coach-agent-production.up.railway.app"
    ],
    // Same as dev: without this, relative /api/* from the browser hits preview only.
    proxy: {
      "/api": {
        target: apiTarget,
        changeOrigin: true,
      },
    },
  },
});
