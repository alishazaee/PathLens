import react from "@vitejs/plugin-react";
import { defineConfig, loadEnv } from "vite";

// https://vite.dev/config/
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const deviceApiTarget = env.VITE_DEV_DEVICE_API_URL || "http://localhost:8081";
  const alertingApiTarget = env.VITE_DEV_ALERTING_API_URL || "http://localhost:8082";

  return {
    plugins: [react()],
    server: {
      proxy: {
        "/api/device": { target: deviceApiTarget, changeOrigin: true, rewrite: (p) => p.replace(/^\/api\/device/, "") },
        "/api/alerting": {
          target: alertingApiTarget,
          changeOrigin: true,
          rewrite: (p) => p.replace(/^\/api\/alerting/, ""),
        },
      },
    },
  };
});
