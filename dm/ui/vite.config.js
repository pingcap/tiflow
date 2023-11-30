import { defineConfig } from 'vite'
import WindiCSS from 'vite-plugin-windicss'
import tsConfigPath from 'vite-tsconfig-paths'
import reactPlugin from '@vitejs/plugin-react'
import { visualizer } from 'rollup-plugin-visualizer'

import { reactRouterPlugin } from './plugins/vite-plugin-next-react-router'
import { i18nextScanner } from './plugins/vite-plugin-i18next-scanner'
import packageJson from './package.json'

function renderChunks(deps) {
  let chunks = {}
  Object.keys(deps).forEach(key => {
    if (['react', 'react-router-dom', 'react-dom'].includes(key)) return
    chunks[key] = [key]
  })
  return chunks
}

// https://vitejs.dev/config/
export default defineConfig({
  base: '/dashboard/',
  plugins: [
    reactPlugin(),
    WindiCSS(),
    tsConfigPath(),
    reactRouterPlugin({ async: false }),
    i18nextScanner({ langs: ['en', 'zh'] }),
  ],
  server: {
    port: 8080,
    proxy: {
      '/api/v1': {
        target: 'http://172.16.6.148:8261',
        changeOrigin: true,
      },
    },
  },
  css: {
    preprocessorOptions: {
      less: {
        javascriptEnabled: true,
      },
    },
  },
  build: {
    rollupOptions: {
      plugins: [visualizer({ template: 'treemap' })],
      output: {
        manualChunks: {
          vendor: ['react', 'react-router-dom', 'react-dom'],
          ...renderChunks(packageJson.dependencies),
        },
      },
    },
  },
})
