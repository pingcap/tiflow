const { override, fixBabelImports, addLessLoader } = require('customize-cra')
// https://github.com/SylvanasGone/react-app-rewire-hot-loader-for-customize-cra
// https://github.com/cdharris/react-app-rewire-hot-loader
const rewireReactHotLoader = require('react-app-rewire-hot-loader-for-customize-cra')

module.exports = override(
  fixBabelImports('import', {
    libraryName: 'antd',
    libraryDirectory: 'es',
    style: true
  }),
  addLessLoader({
    javascriptEnabled: true,
    modifyVars: { '@primary-color': '#475edd' }
  }),
  rewireReactHotLoader()
)
