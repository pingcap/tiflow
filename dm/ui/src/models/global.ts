import { createSlice, PayloadAction } from '@reduxjs/toolkit'

import { Task } from '~/models/task'

interface GlobalSliceState {
  preloadedTask: Task | null
}

const initialState: GlobalSliceState = {
  preloadedTask: null,
}

export const globalSlice = createSlice({
  name: 'global',
  initialState,
  reducers: {
    setPreloadedTask(state, action: PayloadAction<Task | null>) {
      state.preloadedTask = action.payload
    },
  },
})
