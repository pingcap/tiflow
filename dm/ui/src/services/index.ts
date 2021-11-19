import { createApi, fetchBaseQuery } from '@reduxjs/toolkit/query/react'

import { Source } from '~/models/source'

interface ListResponse<T> {
  data: T[]
  total: number
}

export const api = createApi({
  reducerPath: 'api',
  baseQuery: fetchBaseQuery({
    baseUrl: '/api/v1/',
  }),
  tagTypes: ['Source'],
  endpoints: builder => ({
    getSources: builder.query<ListResponse<Source>, { with_status?: boolean }>({
      query: q => ({
        url: `/sources`,
        params: {
          with_status: q.with_status,
        },
      }),
      providesTags: ['Source'],
    }),
    createSource: builder.mutation<void, Partial<Source>>({
      query: body => ({
        url: `/sources`,
        method: 'POST',
        body,
      }),
      invalidatesTags: ['Source'],
    }),
    removeSource: builder.mutation<void, { name: string; force?: boolean }>({
      query: q => ({
        url: `/sources/${q.name}`,
        method: 'DELETE',
        params: {
          force: q.force,
        },
      }),
      invalidatesTags: ['Source'],
    }),
  }),
})

export const {
  useGetSourcesQuery,
  useCreateSourceMutation,
  useRemoveSourceMutation,
} = api
