import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

export const api = createApi({
  baseQuery: fetchBaseQuery({ baseUrl: '/api/v1' }),
  endpoints: () => ({}),
  tagTypes: ['Source'],
  reducerPath: 'api',
})

export interface ListResponse<T> {
  data: T[]
  total: number
}
