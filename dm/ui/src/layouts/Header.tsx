import React, { useCallback, useEffect, useState } from 'react'

import { Nav, Button, Space, RadioGroup, Radio } from '~/uikit'
import { IconSun, IconMoon } from '~/uikit/icons'

export default function HeaderNav() {
  const [darkMode, setDarkMode] = useState<boolean>(
    window?.matchMedia('(prefers-color-scheme: dark)')?.matches ?? false
  )

  const switchMode = useCallback(() => {
    setDarkMode(bool => !bool)
  }, [])

  useEffect(() => {
    if (darkMode) {
      document.body.setAttribute('theme-mode', 'dark')
    } else {
      document.body.removeAttribute('theme-mode')
    }
  }, [darkMode])

  return (
    <Nav
      mode="horizontal"
      footer={
        <Space>
          <Button
            onClick={switchMode}
            icon={darkMode ? <IconMoon /> : <IconSun />}
          />
          <RadioGroup type="button" buttonSize="middle" defaultValue="zh">
            <Radio value="zh">中文</Radio>
            <Radio value="en">EN</Radio>
          </RadioGroup>
        </Space>
      }
    />
  )
}
