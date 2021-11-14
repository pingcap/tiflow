import React from 'react'
import { useNavigate } from 'react-router-dom'

import { Nav } from '~/uikit'
import { IconInherit, IconServer, IconLayers } from '~/uikit/icons'

const items = [
  { itemKey: '/', text: 'Dashboard', icon: <IconServer /> },
  {
    itemKey: '/migration',
    text: '数据迁移',
    icon: <IconInherit />,
    items: [
      {
        text: '任务列表',
        itemKey: '/migration/task',
      },
      {
        text: '上游配置',
        itemKey: '/migration/upstream',
      },
      {
        text: '任务配置',
        itemKey: '/migration/task-config',
      },
      {
        text: '同步详情',
        itemKey: '/migration/sync-detail',
      },
    ],
  },
  {
    itemKey: '/cluster',
    text: '集群管理',
    icon: <IconLayers />,
    items: [
      {
        text: '成员列表',
        itemKey: '/cluster/member',
      },
      {
        text: 'relay 日志',
        itemKey: '/cluster/relay-log',
      },
    ],
  },
]

export default function SiderMenu() {
  const navigate = useNavigate()

  return (
    <Nav
      className="h-full"
      items={items}
      defaultOpenKeys={['/migration']}
      onSelect={item => {
        navigate(item.itemKey as string)
      }}
      header={{
        logo: (
          <img
            src="https://internals.tidb.io/uploads/default/original/1X/4fde143c268ccde514d0c93e420b8a2304fc2033.png"
            alt=""
          />
        ),
        text: 'Data Sync Platform',
      }}
      footer={{
        collapseButton: true,
      }}
    />
  )
}
