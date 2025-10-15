// Copyright (c) 2025 Oleksiy Oleksandrovych Sayankin. All Rights Reserved.
// Refer to the LICENSE file in the root directory for full license details.

import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import { resolve } from 'path';

// Building MDDS wen client
export default defineConfig({
  plugins: [react()],
  root: 'src', // link to source root
  base: './',
  build: {
    outDir: '../mdds-server/web-app', // target dir
    emptyOutDir: true,
  },
  resolve: {
    alias: {
      '@': resolve(__dirname, './src'),
    },
  },
  server: {
    port: 5173,
    open: true,
    proxy: {
      '/solve': 'http://localhost:8863',
    },
  },
});
