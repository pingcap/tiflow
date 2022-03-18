import {
  configureStore,
  Middleware,
  isRejectedWithValue,
} from '@reduxjs/toolkit'
import { TypedUseSelectorHook, useDispatch, useSelector } from 'react-redux'

import { api } from '~/models/api'
import { globalSlice } from '~/models/global'
import { message } from '~/uikit/message'

const rtkQueryErrorLogger: Middleware = () => next => action => {
  if (isRejectedWithValue(action)) {
    console.error('RTKQ error caught: ', action)
    // insert your own error handler here
    message.error({
      content:
        action.payload?.data?.error_msg ??
        action.payload?.data?.error ??
        'Oops, somthing went wrong',
      duration: 15,
    })
  }

  return next(action)
}

export const store = configureStore({
  reducer: {
    [api.reducerPath]: api.reducer,
    globals: globalSlice.reducer,
  },
  middleware: getDefaultMiddleware => {
    const middlewares = getDefaultMiddleware({
      serializableCheck: false,
    })

    middlewares.push(api.middleware, rtkQueryErrorLogger)

    return middlewares
  },

  devTools: import.meta.env.DEV,
})

export type RootState = ReturnType<typeof store.getState>

export type AppDispatch = typeof store.dispatch

export const useAppDispatch = () => useDispatch<AppDispatch>()

export const useAppSelector: TypedUseSelectorHook<RootState> = useSelector

export const actions = {
  ...globalSlice.actions,
}
