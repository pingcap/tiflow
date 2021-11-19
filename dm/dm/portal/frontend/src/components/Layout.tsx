import React, { useState } from 'react'
import { Radio } from 'antd'
import styled from 'styled-components'

import { IntlProvider } from 'react-intl'
import getMessages from '../locales'

const Container = styled.div`
  padding-top: 60px;

  .change-locale {
    position: fixed;
    right: 12px;
    top: 12px;
  }
`

const DmLayout: React.FC = ({ children }) => {
  const [locale, setLocale] = useState('en')

  return (
    <Container>
      <div className="change-locale">
        <Radio.Group
          value={locale}
          onChange={(e: any) => setLocale(e.target.value)}
        >
          <Radio.Button key="en" value={'en'}>
            English
          </Radio.Button>
          <Radio.Button key="zh" value={'zh'}>
            中文
          </Radio.Button>
        </Radio.Group>
      </div>
      <IntlProvider locale={locale} messages={getMessages(locale)}>
        {children}
      </IntlProvider>
    </Container>
  )
}

export default DmLayout
