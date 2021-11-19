module.exports = {
  input: ['src/**/*.{ts,tsx}'],
  output: './',
  sort: true,
  options: {
    func: {
      list: ['t'],
      extensions: ['.ts', '.tsx'],
    },
    lngs: ['en', 'zh'],
    defaultLng: 'en',
    resource: {
      loadPath: './src/locales/{{lng}}.json',
      savePath: './src/locales/{{lng}}.json',
    },
  },
}
