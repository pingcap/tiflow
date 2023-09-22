module.exports = {
  extends: ['@ti-fe/eslint-config'],
  ignorePatterns: ['src/routes.tsx', 'plugins/**/*'],
  env: {
    // Your custom env variables, e.g., browser: true, jest: true
  },
  globals: {
    // Your global variables
  },
  rules: {
    // Your custom rules.
  },
  settings: {
    react: {
      version: 'detect',
    },
  },
}
