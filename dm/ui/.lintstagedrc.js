module.exports = {
  '*.{js,mjs,ts,jsx,tsx}': [
    'eslint --fix',
    'prettier --write --ignore-unknown',
  ],
  '*.+(json|css|md)': ['prettier --write'],
}
