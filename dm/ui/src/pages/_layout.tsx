import React, { Suspense, useCallback, useState } from 'react'
import { Outlet } from 'react-router-dom'

import { Layout, Spin } from '~/uikit'
import { LoadingOutlined } from '~/uikit/icons'
import SiderMenu from '~/layouts/SiderMenu'
import HeaderNav from '~/layouts/Header'

const { Header, Sider, Content } = Layout

const CenteredLoading = () => {
  return (
    <div className="h-full w-full flex justify-center items-center">
      <Spin size="large" indicator={<LoadingOutlined spin />} />
    </div>
  )
}

const GlobalLayout: React.FC = () => {
  const [collapsed, setCollapsed] = useState(false)
  const handleCollapseChange = useCallback((status: boolean) => {
    setCollapsed(status)
  }, [])

  return (
    <Layout className="flex-1">
      <Sider
        theme="light"
        width={300}
        collapsible
        collapsed={collapsed}
        onCollapse={handleCollapseChange}
      >
        <SiderMenu collapsed={collapsed} />
      </Sider>
      <Layout>
        <Header className="!bg-white">
          <HeaderNav />
        </Header>

        <Content>
          <Suspense fallback={<CenteredLoading />}>
            <Outlet />
          </Suspense>
        </Content>
      </Layout>
    </Layout>
  )
}

export default GlobalLayout
