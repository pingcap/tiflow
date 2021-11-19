import 'reset-css'
import 'virtual:windi.css'
import 'antd/dist/antd.css'

import React, { StrictMode } from 'react'
import { render } from 'react-dom'

import App from './App'
import './i18n'

render(
  <StrictMode>
    <App />
  </StrictMode>,
  document.getElementById('root')
)
