import React, { useCallback } from 'react'
import { useTranslation } from 'react-i18next'

import { Select, Space } from '~/uikit'

export default function HeaderNav() {
  const { i18n } = useTranslation()

  const handleLangChange = useCallback(value => {
    i18n.changeLanguage(value)
  }, [])

  return (
    <div className="flex justify-end">
      <Space>
        <Select
          defaultValue="en"
          className="min-w-70px"
          onChange={handleLangChange}
        >
          <Select.Option value="zh">中文</Select.Option>
          <Select.Option value="en">EN</Select.Option>
        </Select>
      </Space>
    </div>
  )
}
