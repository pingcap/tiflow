import React, { Suspense } from 'react'
import { Outlet } from 'react-router-dom'

import { Layout, Spin } from '~/uikit'
import SiderMenu from '~/layouts/SiderMenu'
import HeaderNav from '~/layouts/Header'

const { Header, Sider, Content } = Layout

const CenteredLoading = () => {
  return (
    <div className="h-full w-full flex justify-center items-center">
      <Spin size="large" />
    </div>
  )
}

const GlobalLayout: React.FC = () => {
  return (
    <Layout className="flex-1">
      <Sider>
        <SiderMenu />
      </Sider>
      <Layout>
        <Header>
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
