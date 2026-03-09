import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    host: '0.0.0.0',  // accept connections from outside the container
    port: 5173,
    hmr: {
      // When running in Docker the browser must connect to the host-mapped port.
      // HMR_HOST / HMR_PORT are injected by docker-compose.override.yml.
      // Omitting them (undefined) leaves Vite to use its defaults for local dev.
      host: process.env.HMR_HOST || undefined,
      port: process.env.HMR_PORT ? parseInt(process.env.HMR_PORT) : undefined,
    },
    proxy: {
      // Proxy /api/* to the backend during development.
      // e.g. fetch('/api/task-list') → http://localhost:8000/task-list
      // In Docker, VITE_PROXY_TARGET is set to http://manager:8000 via the override.
      '/api': {
        target: process.env.VITE_PROXY_TARGET ?? 'http://localhost:8000',
        changeOrigin: true,
        rewrite: (path) => path.replace(/^\/api/, ''),
      },
    },
  },
})
