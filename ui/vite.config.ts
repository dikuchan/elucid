import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { fileURLToPath } from 'node:url';

const apiTarget =
  process.env['ELUCID_UI_API_TARGET'] ?? 'http://127.0.0.1:58080';

export default defineConfig({
  plugins: [react()],
  server: {
    host: '127.0.0.1',
    port: 5173,
    strictPort: true,
    proxy: {
      '/api': {
        target: apiTarget,
      },
    },
  },
  build: {
    emptyOutDir: true,
    outDir: fileURLToPath(
      new URL('../elucid/elucid-service/ui-assets', import.meta.url),
    ),
    sourcemap: false,
  },
});
