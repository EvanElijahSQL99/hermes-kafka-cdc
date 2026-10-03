import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// `npm run dev` proxies the API to a local `docker compose up` (port 8080).
// Open http://localhost:5173/?mock to run the dashboard against a built-in simulator instead.
export default defineConfig({
  plugins: [react()],
  server: { proxy: { "/api": "http://localhost:8080" } },
});
