import React from 'react'
import { Provider } from 'react-redux'
import { BrowserRouter, useRoutes } from 'react-router-dom'

import { routes } from '~/routes'
import { store } from '~/models'

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
