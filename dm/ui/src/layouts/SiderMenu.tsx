import React from 'react'
import { useNavigate, useLocation } from 'react-router-dom'
import { useTranslation } from 'react-i18next'

import { Menu } from '~/uikit'
import {
  DashboardOutlined,
  ExportOutlined,
  ClusterOutlined,
} from '~/uikit/icons'
import logo from '~/assets/logo.png'

const { SubMenu } = Menu

const SiderMenu: React.FC<{
  collapsed: boolean
}> = ({ collapsed }) => {
  const [t] = useTranslation()
  const navigate = useNavigate()
  const loc = useLocation()

  const items = [
    { itemKey: '/', text: 'Dashboard', icon: <DashboardOutlined /> },
    {
      itemKey: '/migration',
      text: t('migration'),
      icon: <ExportOutlined />,
      items: [
        {
          text: t('task list'),
          itemKey: '/migration/task',
        },
        {
          text: t('source list'),
          itemKey: '/migration/source',
        },
        {
          text: t('task config'),
          itemKey: '/migration/task-config',
        },
        {
          text: t('sync detail'),
          itemKey: '/migration/sync-detail',
        },
      ],
    },
    {
      itemKey: '/cluster',
      text: t('cluster management'),
      icon: <ClusterOutlined />,
      items: [
        {
          text: t('member list'),
          itemKey: '/cluster/member',
        },
        {
          text: t('relay log'),
          itemKey: '/cluster/relay-log',
        },
      ],
    },
  ]
  return (
    <div>
      <div className="flex p-4 justify-center">
        <img src={logo} alt="" className="h-[36px]" />
        {!collapsed && (
          <h1 className="font-extrabold text-lg ml-2 leading-[36px]">
            Data Sync Platform
          </h1>
        )}
      </div>

      <Menu
        theme="light"
        mode="inline"
        defaultSelectedKeys={[loc.pathname]}
        defaultOpenKeys={['/migration']}
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
