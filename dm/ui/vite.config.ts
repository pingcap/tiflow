import path from 'path'

import { defineConfig } from 'vite'
import reactRefresh from '@vitejs/plugin-react-refresh'
import WindiCSS from 'vite-plugin-windicss'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [reactRefresh(), WindiCSS()],
  resolve: {
    alias: [{ find: '~', replacement: path.resolve(__dirname, 'src') }],
  },
  server: {
    port: 8080,
    proxy: {
      '/api/v1': {
        target: 'http://172.16.5.32:48261',
        changeOrigin: true,
      },
    },
  },
})
