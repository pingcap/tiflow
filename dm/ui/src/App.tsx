import React, { lazy } from 'react'
import { Provider } from 'react-redux'
import { BrowserRouter, RouteObject, useRoutes } from 'react-router-dom'

import { store } from '~/models'
import Layout from '~/pages/_layout'

const SourceList = lazy(() => import('~/pages/migration/source'))

const routes: RouteObject[] = [
  {
    path: '/',
    element: <Layout />,
    children: [
      {
        index: true,
        element: <div>TODO</div>,
      },
      {
        path: '/migration/task',
        element: <div>TODO</div>,
      },
      {
        path: '/migration/source',
        element: <SourceList />,
      },
      {
        path: '/migration/task-config',
        element: <div>TODO</div>,
      },
      {
        path: '/migration/sync-detail',
        element: <div>TODO</div>,
      },
      {
        path: '/cluster/member',
        element: <div>TODO</div>,
      },
      {
        path: '/cluster/relay-log',
        element: <div>TODO</div>,
      },
      {
        path: '*',
        element: <div>Page not found...</div>,
      },
    ],
  },
]

function RoutingApp() {
  const elements = useRoutes(routes)
  return <div className="h-screen w-screen flex">{elements}</div>
}

function App() {
  return (
    <Provider store={store}>
      <BrowserRouter basename="dashboard">
        <RoutingApp />
      </BrowserRouter>
    </Provider>
  )
}

export default App
