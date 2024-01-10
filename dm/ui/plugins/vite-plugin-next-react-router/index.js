// src/context.ts
import path4 from 'path'
import fs2 from 'fs'

// src/codegen.ts
import fs from 'fs'
import path3 from 'path'

// src/resolver.ts
import path2 from 'path'
import { normalizePath } from 'vite'
import fg from 'fast-glob'

// src/utils.ts
import path from 'path'
import consola from 'consola'
import { upperFirst, camelCase } from 'lodash'

// package.json
var name = 'vite-plugin-next-react-router'

// src/const.ts
var MATCH_ALL_ROUTE = '*'
var DEFAULT_EXT = ['tsx', 'ts', 'jsx', 'js']
var DEFAULT_PAGE_DIR = 'src/pages'

// src/utils.ts
var debug = (message, ...args) => {
  if (process.env.DEBUG === name) {
    consola.info(message, ...args)
  }
}
function isCatchAll(filename) {
  return /^\[\.{3}/.test(filename)
}
function isDynamic(filename) {
  return /^\[(.+)\]$/.test(filename)
}
function normalizeFilenameToRoute(filename) {
  if (isCatchAll(filename)) {
    return MATCH_ALL_ROUTE
  }
  if (filename === 'index') {
    return '/'
  }
  return isDynamic(filename) ? parameterizeDynamicRoute(filename) : filename
}
function parameterizeDynamicRoute(s) {
  return s.replace(/^\[(.+)\]$/, (_, p) => `:${p}`)
}
function normalizeDirPathToRoute(dirPath) {
  return dirPath
    .split('/')
    .map(s => (isDynamic(s) ? parameterizeDynamicRoute(s) : s))
    .join('/')
}
function normalizePathToRoute(p) {
  const { dir, name: name2 } = path.parse(p)
  const route = normalizeFilenameToRoute(name2)
  if (route === MATCH_ALL_ROUTE) {
    return route
  }
  return path.resolve(path.join('/', normalizeDirPathToRoute(dir), route))
}
function countLength(p) {
  return path.resolve(p).split('/').filter(Boolean).length
}
function sorter(a, b) {
  const len = countLength(a) - countLength(b)
  if (len !== 0) {
    return len
  }
  return a.localeCompare(b)
}
function sortRoutes(routes) {
  return [...routes].sort(sorter)
}
function getComponentName(filePath) {
  const segments = filePath.split(path.sep)
  const extname = path.extname(filePath)
  const fileName = path.basename(filePath, extname)
  segments[segments.length - 1] = fileName
  const name2 = segments.reduce((acc, segment) => {
    if (isDynamic(segment)) {
      return acc + upperFirst(camelCase(segment.replace(/^\[(.+)\]$/, '$1')))
    }
    return acc + upperFirst(camelCase(segment))
  }, '')
  return name2
}

// src/resolver.ts
function resolvePages(options) {
  const { root, pageDir, extensions } = options
  const files = scan(pageDir, extensions, root)
  return fileToRouteMap(files)
}
function resolveOptions(userOptions, viteRoot) {
  var _a, _b, _c, _d
  const root = viteRoot != null ? viteRoot : normalizePath(process.cwd())
  return {
    root,
    async:
      (_a = userOptions == null ? void 0 : userOptions.async) != null
        ? _a
        : true,
    pageDir:
      (_b = userOptions == null ? void 0 : userOptions.pageDir) != null
        ? _b
        : DEFAULT_PAGE_DIR,
    extensions:
      (_c = userOptions == null ? void 0 : userOptions.extensions) != null
        ? _c
        : DEFAULT_EXT,
    output:
      (_d = userOptions == null ? void 0 : userOptions.output) != null
        ? _d
        : path2.join(root, 'src', 'routes.tsx'),
  }
}
function scan(targetDir, extensions, root) {
  const fullPathOfTargetDir = path2.resolve(root, targetDir)
  return fg.sync([`**/*.{${extensions.join()}}`, `!_layout.*`], {
    onlyFiles: true,
    cwd: fullPathOfTargetDir,
  })
}
function fileToRouteMap(files) {
  const map = new Map()
  files.forEach(file => {
    const route = normalizePathToRoute(file)
    map.set(route, file)
  })
  return map
}
function resolveRoutes(map, options) {
  const sortedRoutes = sortRoutes([...map.keys()])
  return sortedRoutes.map(route => {
    const filePath = map.get(route)
    const absolutePath = path2.resolve(options.pageDir, filePath)
    const routeObj = {
      path: route,
      componentPath: absolutePath,
      componentName: getComponentName(filePath),
    }
    if (path2.basename(filePath, path2.extname(filePath)) === 'index') {
      routeObj.index = true
    }
    return routeObj
  })
}
function resolveGlobalLayout(options) {
  const globalLayoutFiles = fg.sync(
    [`_layout.{${options.extensions.join()}}`],
    {
      onlyFiles: true,
      cwd: options.pageDir,
    }
  )
  if (globalLayoutFiles.length === 1) {
    const filePath = globalLayoutFiles[0]
    return {
      componentPath: path2.resolve(options.pageDir, globalLayoutFiles[0]),
      componentName: 'GlobalLayout',
      path: filePath,
    }
  } else if (globalLayoutFiles.length > 1) {
    throw new Error('Multiple _layout files found')
  }
  return null
}

// src/template.ts
var generateRoutesCode = ({
  layoutImport: layoutImport2,
  pageImports,
  layoutElement,
  routes,
  staticPageMetaImports,
  pages,
}) => `
import React from 'react';
${layoutImport2}
${staticPageMetaImports}
${pageImports}

export const routes = [
  {
    path: '/',
    element: ${layoutElement},
    children: [
      ${routes}
    ]
  }
]

export const pages = [
  ${pages}
]
`

// src/codegen.ts
function generateComponentCall(route, options) {
  return `<${
    options.async
      ? `Dynamic${route.componentName}`
      : `Static${route.componentName}`
  } />`
}
function stripExt(p) {
  return p.replace(path3.extname(p), '')
}
function transformToRelativePath(to, from) {
  return './' + stripExt(path3.relative(path3.dirname(path3.resolve(from)), to))
}
var dynamicPageImports = (routes, options) =>
  routes.reduce((acc, route) => {
    return (
      acc +
      `const Dynamic${
        route.componentName
      } = React.lazy(() => import('${transformToRelativePath(
        route.componentPath,
        options.output
      )}'));
`
    )
  }, '')
var staticPageImports = (routes, options) =>
  routes.reduce((acc, route) => {
    return (
      acc +
      `import Static${route.componentName} from '${transformToRelativePath(
        route.componentPath,
        options.output
      )}'
`
    )
  }, '')
var staticMetaImports = (routes, options) => {
  return routes.reduce((acc, route) => {
    var _a
    const routeComponentCode = fs.readFileSync(route.componentPath, 'utf8')
    if (routeComponentCode.includes('export const meta')) {
      return (
        acc +
        `import { meta as Static${
          route.componentName
        }Meta } from '${transformToRelativePath(
          route.componentPath,
          (_a = options.output) != null
            ? _a
            : path3.join(options.root, 'src', 'pages.ts')
        )}'
`
      )
    }
    return acc
  }, '')
}
var layoutImport = options => {
  const layout = resolveGlobalLayout(options)
  let imports = ''
  if (!layout) {
    imports += `import { Outlet } from 'react-router-dom';`
  }
  imports = layout
    ? `import ${layout.componentName} from '${transformToRelativePath(
        layout.componentPath,
        options.output
      )}'`
    : ''
  const element = `<${layout ? layout.componentName : 'Outlet'} />`
  return { imports, element }
}
var routeObjects = (routes, options) =>
  routes
    .map(route => {
      return `{ path: '${route.path}', element: ${generateComponentCall(
        route,
        options
      )}, ${route.index ? 'index: true' : ''}},
`
    })
    .join(' '.repeat(6))
    .trim()
var pageObjects = routes =>
  routes
    .filter(r => r.path !== '*')
    .map(route => {
      const routeComponentCode = fs.readFileSync(route.componentPath, 'utf8')
      if (routeComponentCode.includes('export const meta')) {
        return `{ route: '${route.path}', meta: Static${route.componentName}Meta },
`
      }
      return `{ route: '${route.path}' },
`
    })
    .join(' '.repeat(2))
    .trim()
function generateRoutesModuleCode(routes, options) {
  const { imports, element } = layoutImport(options)
  const staticPageMetaImports = staticMetaImports(routes, options)
  const pages = pageObjects(routes)
  return generateRoutesCode({
    layoutImport: imports,
    pageImports: options.async
      ? dynamicPageImports(routes, options)
      : staticPageImports(routes, options),
    layoutElement: element,
    routes: routeObjects(routes, options),
    pages,
    staticPageMetaImports,
  })
}

// src/context.ts
function isTarget(p, options) {
  return (
    p.startsWith(path4.resolve(options.pageDir)) &&
    options.extensions.some(ext => p.endsWith(ext))
  )
}
var Context = class {
  constructor(userOptions) {
    this._pages = new Map()
    this._server = null
    this.root = '.'
    this._userOptions = userOptions
  }
  resolveOptions() {
    this._resolvedOptions = resolveOptions(this._userOptions, this.root)
  }
  search() {
    if (!this._resolvedOptions) {
      this.resolveOptions()
    }
    this._pages = resolvePages(this._resolvedOptions)
    debug('pages: ', this._pages)
  }
  configureServer(server) {
    this._server = server
    this._server.watcher.on('unlink', filaPath =>
      this.invalidateAndRegenerateRoutes(filaPath)
    )
    this._server.watcher.on('add', filaPath =>
      this.invalidateAndRegenerateRoutes(filaPath)
    )
  }
  invalidateAndRegenerateRoutes(filaPath) {
    if (!isTarget(filaPath, this._resolvedOptions)) {
      return
    }
    this._pages.clear()
    this.generateRoutesModuleCode()
  }
  generateRoutesModuleCode() {
    if (this._pages.size === 0) {
      this.search()
    }
    const routes = resolveRoutes(this._pages, this._resolvedOptions)
    const code = generateRoutesModuleCode(routes, this._resolvedOptions)
    debug('generateVirtualRoutesCode: \n', code)
    fs2.writeFileSync(this._resolvedOptions.output, code)
  }
}

// src/index.ts
function reactRouterPlugin(userOptions) {
  const ctx = new Context(userOptions)
  return {
    name: 'vite-plugin-next-router',
    enforce: 'pre',
    async configResolved({ root }) {
      ctx.root = root
      ctx.resolveOptions()
      ctx.search()
      ctx.generateRoutesModuleCode()
    },
    configureServer(server) {
      ctx.configureServer(server)
    },
  }
}
export { reactRouterPlugin }
