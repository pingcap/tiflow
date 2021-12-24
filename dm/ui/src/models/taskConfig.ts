import { api, ListResponse } from './api'
import { Task } from './task'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiBatchImportTaskConfig: build.mutation<
      BatchImportTaskConfigResponse,
      { overwrite: boolean }
    >({
      query: queryArg => ({
        url: `/api/v1/task_configs/batch_import`,
        method: 'POST',
        body: queryArg,
      }),
      invalidatesTags: ['TaskConfig'],
    }),
    dmapiCreateTaskConfig: build.mutation<Task, Task>({
      query: queryArg => ({
        url: `/api/v1/task_configs`,
        method: 'POST',
        body: queryArg,
      }),
      invalidatesTags: ['TaskConfig'],
    }),
    dmapiGetTaskConfigList: build.query<ListResponse<Task>, void>({
      query: () => ({ url: `/api/v1/task_configs` }),
      providesTags: ['TaskConfig'],
    }),
    dmapiGetTaskConfig: build.query<Task, { taskName: string }>({
      query: queryArg => ({
        url: `/api/v1/task_configs/${queryArg.taskName}`,
      }),
    }),
    dmapUpdateTaskConfig: build.mutation<Task, { taskName: string }>({
      query: queryArg => ({
        url: `/api/v1/task_configs/${queryArg.taskName}`,
        method: 'PUT',
      }),
      invalidatesTags: ['TaskConfig'],
    }),
    dmapiDeleteTaskConfig: build.mutation<void, { taskName: string }>({
      query: queryArg => ({
        url: `/api/v1/task_configs/${queryArg.taskName}`,
        method: 'DELETE',
      }),
      invalidatesTags: ['TaskConfig'],
    }),
  }),
  overrideExisting: false,
})

export type BatchImportTaskConfigResponse = {
  success_task_list: string[]
  failed_task_list: {
    task_name: string
    error_msg: string
  }[]
}

export const {
  useDmapiBatchImportTaskConfigMutation,
  useDmapiCreateTaskConfigMutation,
  useDmapiGetTaskConfigListQuery,
  useDmapiGetTaskConfigQuery,
  useDmapUpdateTaskConfigMutation,
  useDmapiDeleteTaskConfigMutation,
} = injectedRtkApi
