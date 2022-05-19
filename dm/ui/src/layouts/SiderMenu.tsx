import React, { useMemo } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'
import { groupBy } from 'lodash-es'

import { pages } from '~/routes'
import { Menu } from '~/uikit'
import {
  ClusterOutlined,
  DashboardOutlined,
  ExportOutlined,
} from '~/uikit/icons'
import logo from '~/assets/logo.png'
import i18n from '~/i18n'

const { SubMenu } = Menu

function usePagesToMenuItems(key: string) {
  const groupedPages = useMemo(
    () =>
      groupBy(pages, page => {
        if (page.route === '/') {
          return page.route
        }

        return '/' + page.route.split('/')[1]
      }),
    [pages]
  )

  return useMemo(() => {
    if (!groupedPages[key]) {
      return []
    }
    return groupedPages[key]
      .filter(
        page =>
          page.meta &&
          typeof page?.meta.index === 'number' &&
          page.meta.index >= 0
      )
      .sort((a, b) => a.meta!.index - b.meta!.index)
      .map(page => ({
        text: page?.meta?.title(),
        itemKey: page.route,
      }))
  }, [groupedPages, i18n.language])
}

const SiderMenu: React.FC<{
  collapsed: boolean
}> = ({ collapsed }) => {
  const [t] = useTranslation()
  const navigate = useNavigate()
  const loc = useLocation()

  const migrationItems = usePagesToMenuItems('/migration')
  const clusterItems = usePagesToMenuItems('/cluster')

  const items = useMemo(
    () => [
      {
        itemKey: '/migration',
        text: t('migration'),
        icon: <ExportOutlined />,
        items: migrationItems,
      },
      {
        itemKey: '/cluster',
        text: t('cluster management'),
        icon: <ClusterOutlined />,
        items: clusterItems,
      },
    ],
    [migrationItems, clusterItems]
  )

  return (
    <div>
      <div className="flex p-4 justify-center">
        <img src={logo} alt="" className="h-[36px]" />
        {!collapsed && (
          <h1 className="font-extrabold text-lg ml-2 leading-[36px]">
            Data Migration
          </h1>
        )}
      </div>

      <Menu
        theme="light"
        mode="inline"
        defaultSelectedKeys={[loc.pathname]}
        defaultOpenKeys={['/migration', '/cluster']}
      >
        {items.map(item => {
          if (item.items) {
            return (
              <SubMenu
                key={item.itemKey}
                title={
                  <span>
                    {item.icon}
                    <span>{item.text}</span>
                  </span>
                }
              >
                {item.items.map(subItem => (
                  <Menu.Item
                    key={subItem.itemKey}
                    onClick={() => navigate(subItem.itemKey)}
                  >
                    {subItem.text}
                  </Menu.Item>
                ))}
              </SubMenu>
            )
          } else {
            return (
              <Menu.Item
                key={item.itemKey}
                onClick={() => navigate(item.itemKey)}
              >
                {item.icon}
                <span>{item.text}</span>
              </Menu.Item>
            )
          }
        })}
      </Menu>
    </div>
  )
}

export default SiderMenu
