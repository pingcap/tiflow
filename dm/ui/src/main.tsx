import 'reset-css'
import 'virtual:windi.css'
import './theme.less'
import './i18n'

import React, { StrictMode } from 'react'
import { render } from 'react-dom'

import App from './App'

if (process.env.NODE_ENV === 'development') {
  import('./mock')
}

render(
  <StrictMode>
    <App />
  </StrictMode>,
  document.getElementById('root')
)
