import {
  configureStore,
  Middleware,
  isRejectedWithValue,
} from '@reduxjs/toolkit'
import { TypedUseSelectorHook, useDispatch, useSelector } from 'react-redux'
import { createLogger } from 'redux-logger'

import { api } from '~/models/api'
import { message } from '~/uikit'

const rtkQueryErrorLogger: Middleware = () => next => action => {
  if (isRejectedWithValue(action)) {
    console.error('RTKQ error caught: ', action)
    // insert your own error handler here
    message.error({
      content: action.payload?.data?.error_msg ?? 'Oops, somthing went wrong',
    })
  }

  return next(action)
}

export const store = configureStore({
  reducer: {
    [api.reducerPath]: api.reducer,
  },
  middleware: getDefaultMiddleware => {
    const middlewares = getDefaultMiddleware({
      serializableCheck: false,
    })

    middlewares.push(api.middleware, rtkQueryErrorLogger)

    if (import.meta.env.DEV) {
      const logger = createLogger({
        duration: true,
        collapsed: true,
      })
      middlewares.push(logger)
    }

    return middlewares
  },

  devTools: import.meta.env.DEV,
})

export type RootState = ReturnType<typeof store.getState>

export type AppDispatch = typeof store.dispatch

export const useAppDispatch = () => useDispatch<AppDispatch>()

export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector

export const actions = {}
