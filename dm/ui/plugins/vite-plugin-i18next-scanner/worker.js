// src/worker.ts
const workerpool = require('workerpool')
const vfs = require('vinyl-fs')
const { createStream } = require('i18next-scanner')

// src/options.ts
const path = require('path')
const { defaults, defaultsDeep } = require('lodash')
var defaultOptions = {
  input: ['src/**/*.{js,jsx,ts,tsx}'],
  output: './',
  options: {
    debug: false,
    removeUnusedKeys: true,
    sort: true,
    attr: {
      list: ['data-i18n'],
      extensions: ['.html', '.htm'],
    },
    func: {
      list: ['t', 'i18next.t', 'i18n.t'],
      extensions: ['.ts', '.tsx', '.js', '.jsx'],
    },
    trans: {
      component: 'Trans',
      i18nKey: 'i18nKey',
      defaultsKey: 'defaults',
      extensions: [],
      fallbackKey: false,
    },
    lngs: ['en'],
    defaultLng: 'en',
    defaultValue: function (_, __, key) {
      return key
    },
    resource: {
      loadPath: './locales/{{lng}}.json',
      savePath: './locales/{{lng}}.json',
      jsonIndent: 2,
      lineEnding: '\n',
    },
    nsSeparator: ':',
    keySeparator: '.',
    pluralSeparator: '_',
    contextSeparator: '_',
    contextDefaultValues: [],
    interpolation: {
      prefix: '{{',
      suffix: '}}',
    },
  },
}
var defaultPluginOptions = {
  langs: ['en'],
  outDir: 'locales',
  includes: ['src/**/*.{js,jsx,ts,tsx}'],
}
function mergePluginOptionToScannerOption(a, b) {
  const o = defaults(b, defaultPluginOptions)
  a.input = o.includes
  a.options.lngs = o.langs
  a.options.resource.savePath = path.join(o.outDir, '{{lng}}.json')
  a.options.resource.loadPath = path.join(o.outDir, '{{lng}}.json')
  return a
}
function normalizeOptions(o = {}) {
  const options = defaultsDeep({}, defaultOptions)
  return mergePluginOptionToScannerOption(options, o)
}

// src/worker.ts
async function scanAndGenerateResource(input, output, options) {
  const scannerOptions = normalizeOptions(options)
  return new Promise((resolve, reject) => {
    vfs
      .src(input)
      .pipe(createStream(scannerOptions.options))
      .pipe(vfs.dest(output))
      .on('finish', () => resolve())
      .on('error', e => reject(e))
  })
}
workerpool.worker({
  scanAndGenerateResource,
})
