import { api, ListResponse } from './api'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiGetClusterMasterList: build.query<ListResponse<ClusterMaster>, void>({
      query: () => ({ url: `/api/v1/cluster/masters` }),
      providesTags: ['ClusterMaster'],
    }),
    dmapiOfflineMasterNode: build.mutation<void, string>({
      query: masterName => ({
        url: `/api/v1/cluster/masters/${masterName}`,
        method: 'DELETE',
      }),
      invalidatesTags: ['ClusterMaster'],
    }),
    dmapiGetClusterWorkerList: build.query<ListResponse<ClusterWorker>, void>({
      query: () => ({ url: `/api/v1/cluster/workers` }),
      providesTags: ['ClusterWorker'],
    }),
    dmapiOfflineWorkerNode: build.mutation<void, string>({
      query: workerName => ({
        url: `/api/v1/cluster/workers/${workerName}`,
        method: 'DELETE',
      }),
      invalidatesTags: ['ClusterWorker'],
    }),
  }),
})

export type ClusterMaster = {
  name: string
  alive: boolean
  leader: boolean
  addr: string
}

export type ClusterWorker = {
  name: string
  addr: string
  bound_stage: string
  bound_source_name: string
}

export const {
  useDmapiGetClusterMasterListQuery,
  useDmapiGetClusterWorkerListQuery,
  useDmapiOfflineMasterNodeMutation,
  useDmapiOfflineWorkerNodeMutation,
} = injectedRtkApi
