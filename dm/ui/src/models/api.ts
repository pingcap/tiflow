import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

export const api = createApi({
  baseQuery: fetchBaseQuery({ baseUrl: '/api/v1' }),
  endpoints: () => ({}),
  tagTypes: ['Source', 'Task', 'TaskConfig', 'ClusterMaster', 'ClusterWorker'],
  reducerPath: 'api',
})

export interface ListResponse<T> {
  data: T[]
  total: number
}
