import React from 'react'
import { Outlet } from 'react-router-dom'

import { Layout } from '~/uikit'
import SiderMenu from '~/layouts/SiderMenu'
import HeaderNav from '~/layouts/Header'

const { Header, Sider, Content } = Layout

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
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  )
}

export default GlobalLayout
