import React, { useCallback } from 'react'
import { useTranslation } from 'react-i18next'
import { useLocalStorageState } from 'ahooks'

import { Select, Space } from '~/uikit'

export default function HeaderNav() {
  const { i18n } = useTranslation()
  const [lang, setLang] = useLocalStorageState<'en' | 'zh'>('lang', {
    defaultValue: 'en',
  })

  const handleLangChange = useCallback(
    value => {
      i18n.changeLanguage(value).then(() => {
        setLang(value)
      })
    },
    [i18n]
  )

  return (
    <div className="flex justify-end">
      <Space>
        <Select
          defaultValue={lang}
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
