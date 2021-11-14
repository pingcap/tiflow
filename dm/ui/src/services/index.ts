import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

export const api = createApi({
  reducerPath: 'api',
  baseQuery: fetchBaseQuery({
    baseUrl: '/api/v1/',
  }),
  tagTypes: [],
  endpoints: builder => ({
    getSources: builder.query({
      query: () => `/cluster/masters`,
    }),
  }),
})

export const { useGetSourcesQuery } = api
