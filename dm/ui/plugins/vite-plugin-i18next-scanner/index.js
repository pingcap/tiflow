// src/context.ts
import fs2 from 'fs'
import { Parser } from 'i18next-scanner'
import debug from 'debug'
import workerpool from 'workerpool'

// src/options.ts
import path from 'path'
import { defaults, defaultsDeep } from 'lodash'
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

// src/context.ts
import path2 from 'path'

// src/fs.ts
import fs from 'fs'

// node_modules/.pnpm/detect-indent@7.0.0/node_modules/detect-indent/index.js
var INDENT_REGEX = /^(?:( )+|\t+)/
var INDENT_TYPE_SPACE = 'space'
var INDENT_TYPE_TAB = 'tab'
function makeIndentsMap(string, ignoreSingleSpaces) {
  const indents = new Map()
  let previousSize = 0
  let previousIndentType
  let key
  for (const line of string.split(/\n/g)) {
    if (!line) {
      continue
    }
    let indent
    let indentType
    let weight
    let entry
    const matches = line.match(INDENT_REGEX)
    if (matches === null) {
      previousSize = 0
      previousIndentType = ''
    } else {
      indent = matches[0].length
      indentType = matches[1] ? INDENT_TYPE_SPACE : INDENT_TYPE_TAB
      if (
        ignoreSingleSpaces &&
        indentType === INDENT_TYPE_SPACE &&
        indent === 1
      ) {
        continue
      }
      if (indentType !== previousIndentType) {
        previousSize = 0
      }
      previousIndentType = indentType
      weight = 0
      const indentDifference = indent - previousSize
      previousSize = indent
      if (indentDifference === 0) {
        weight++
      } else {
        const absoluteIndentDifference =
          indentDifference > 0 ? indentDifference : -indentDifference
        key = encodeIndentsKey(indentType, absoluteIndentDifference)
      }
      entry = indents.get(key)
      entry = entry === void 0 ? [1, 0] : [++entry[0], entry[1] + weight]
      indents.set(key, entry)
    }
  }
  return indents
}
function encodeIndentsKey(indentType, indentAmount) {
  const typeCharacter = indentType === INDENT_TYPE_SPACE ? 's' : 't'
  return typeCharacter + String(indentAmount)
}
function decodeIndentsKey(indentsKey) {
  const keyHasTypeSpace = indentsKey[0] === 's'
  const type = keyHasTypeSpace ? INDENT_TYPE_SPACE : INDENT_TYPE_TAB
  const amount = Number(indentsKey.slice(1))
  return { type, amount }
}
function getMostUsedKey(indents) {
  let result
  let maxUsed = 0
  let maxWeight = 0
  for (const [key, [usedCount, weight]] of indents) {
    if (usedCount > maxUsed || (usedCount === maxUsed && weight > maxWeight)) {
      maxUsed = usedCount
      maxWeight = weight
      result = key
    }
  }
  return result
}
function makeIndentString(type, amount) {
  const indentCharacter = type === INDENT_TYPE_SPACE ? ' ' : '	'
  return indentCharacter.repeat(amount)
}
function detectIndent(string) {
  if (typeof string !== 'string') {
    throw new TypeError('Expected a string')
  }
  let indents = makeIndentsMap(string, true)
  if (indents.size === 0) {
    indents = makeIndentsMap(string, false)
  }
  const keyOfMostUsedIndent = getMostUsedKey(indents)
  let type
  let amount = 0
  let indent = ''
  if (keyOfMostUsedIndent !== void 0) {
    ;({ type, amount } = decodeIndentsKey(keyOfMostUsedIndent))
    indent = makeIndentString(type, amount)
  }
  return {
    amount,
    type,
    indent,
  }
}

// src/fs.ts
var DEFAULT_INDENT = '  '
function readJsonFile(path3) {
  const file = fs.readFileSync(path3, 'utf8') || '{}'
  const indent = detectIndent(path3).indent || DEFAULT_INDENT
  return {
    path: path3,
    json: JSON.parse(file),
    indent,
  }
}

// src/context.ts
var dbg = debug('vite-plugin-i18next-scanner:context')
var Context = class {
  constructor(options = {}) {
    this.server = null
    this.pool = null
    this.pluginOptions = options
    this.scannerOptions = normalizeOptions(options)
    dbg('scannerOptions: %o', this.scannerOptions)
  }
  async startScanner(server) {
    if (this.server === server) {
      return
    }
    if (this.pool) {
      await this.pool.terminate()
    }
    this.server = server
    this.pool = workerpool.pool(__dirname + '/worker.js', {
      minWorkers: 'max',
      maxWorkers: 1,
    })
    await this.scanAll()
    this.watch(server.watcher)
  }
  watch(watcher) {
    watcher.on('change', p => this.handleFileChange(p))
    watcher.on('unlink', p => this.handleFileUnlink(p))
  }
  passExtensionCheck(p) {
    const extname = path2.extname(p)
    return (
      this.scannerOptions.options.func.extensions.includes(extname) ||
      this.scannerOptions.options.attr.extensions.includes(extname) ||
      this.scannerOptions.options.trans.extensions.includes(extname)
    )
  }
  async handleFileUnlink(p) {
    if (this.passExtensionCheck(p)) {
      await this.scanAll()
    }
  }
  async handleFileChange(p) {
    dbg(`scanning ${p}`)
    if (!this.passExtensionCheck(p)) {
      return
    }
    const content = fs2.readFileSync(p, 'utf8')
    const parser = new Parser(this.scannerOptions.options)
    if (!content) {
      return
    }
    parser.parseFuncFromString(content)
    const translations = parser.get()
    const resourceFromFile = Object.keys(translations).reduce((acc, key) => {
      acc[key] = translations[key].translation
      return acc
    }, {})
    dbg('resource from file: %o', resourceFromFile)
    const hasKey = Object.keys(resourceFromFile).some(lang => {
      return Object.keys(resourceFromFile[lang]).length > 0
    })
    if (!hasKey) {
      dbg('no key found')
      return
    }
    let shouldScanAll = false
    Object.keys(resourceFromFile).forEach(lang => {
      const languageResource = path2.resolve(
        this.scannerOptions.options.resource.savePath.replace('{{lng}}', lang)
      )
      const { json } = readJsonFile(languageResource)
      if (Object.keys(resourceFromFile[lang]).some(key => !(key in json))) {
        shouldScanAll = true
      }
    })
    if (shouldScanAll) {
      await this.scanAll()
    } else {
      dbg('no need to scan all')
    }
  }
  async scanAll() {
    if (!this.pool) {
      return
    }
    dbg('scanning and regenerating all resources...')
    const worker = await this.pool.proxy()
    await worker.scanAndGenerateResource(
      this.scannerOptions.input,
      this.scannerOptions.output,
      this.pluginOptions
    )
    dbg('done scanning and regenerating all resources')
  }
}

// src/index.ts
function i18nextScanner(options) {
  const ctx = new Context(options)
  return {
    name: 'vite-plugin-i18next-scanner',
    apply: 'serve',
    async configureServer(server) {
      await ctx.startScanner(server)
    },
  }
}
export { i18nextScanner }
