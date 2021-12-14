import { api, ListResponse } from './api'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiCreateSource: build.mutation<
      DmapiCreateSourceApiResponse,
      DmapiCreateSourceApiArg
    >({
      query: queryArg => ({
        url: `/sources`,
        method: 'POST',
        body: queryArg,
      }),
    }),
    dmapiGetSourceList: build.query<
      DmapiGetSourceListApiResponse,
      DmapiGetSourceListApiArg
    >({
      query: queryArg => ({
        url: `/sources`,
        params: { with_status: queryArg.withStatus },
      }),
    }),
    dmapiDeleteSource: build.mutation<
      DmapiDeleteSourceApiResponse,
      DmapiDeleteSourceApiArg
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}`,
        method: 'DELETE',
        params: { force: queryArg.force },
      }),
    }),
    dmapiGetSourceStatus: build.query<
      DmapiGetSourceStatusApiResponse,
      DmapiGetSourceStatusApiArg
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/status`,
      }),
    }),
    dmapiTransferSource: build.mutation<
      DmapiTransferSourceApiResponse,
      DmapiTransferSourceApiArg
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/transfer`,
        method: 'POST',
        body: queryArg.workerNameRequest,
      }),
    }),
    dmapiGetSourceSchemaList: build.query<
      DmapiGetSourceSchemaListApiResponse,
      DmapiGetSourceSchemaListApiArg
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/schemas`,
      }),
    }),
    dmapiGetSourceTableList: build.query<
      DmapiGetSourceTableListApiResponse,
      DmapiGetSourceTableListApiArg
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/schemas/${queryArg.schemaName}`,
      }),
    }),
    dmapiGetSchemaListByTaskAndSource: build.query<
      DmapiGetSchemaListByTaskAndSourceApiResponse,
      DmapiGetSchemaListByTaskAndSourceApiArg
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg['task-name']}/sources/${queryArg.sourceName}/schemas`,
      }),
    }),
    dmapiGetTableListByTaskAndSource: build.query<
      DmapiGetTableListByTaskAndSourceApiResponse,
      DmapiGetTableListByTaskAndSourceApiArg
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg['task-name']}/sources/${queryArg.sourceName}/schemas/${queryArg.schemaName}`,
      }),
    }),
  }),
  overrideExisting: false,
})

export { injectedRtkApi as enhancedApi }

export type DmapiCreateSourceApiResponse = Source

export type DmapiCreateSourceApiArg = Partial<Source>

export type DmapiGetSourceListApiResponse = GetSourceListResponse

export type DmapiGetSourceListApiArg = {
  withStatus?: boolean
}

export type DmapiDeleteSourceApiResponse = DeleteSourceResponse

export type DmapiDeleteSourceApiArg = {
  sourceName: string
  force?: boolean
}

export type DmapiGetSourceStatusApiResponse = GetSourceStatusResponse

export type DmapiGetSourceStatusApiArg = {
  sourceName: string
}

export type DmapiTransferSourceApiResponse = undefined

export type DmapiTransferSourceApiArg = {
  sourceName: string
  workerNameRequest: WorkerNameRequest
}

export type DmapiGetSourceSchemaListApiResponse = SchemaNameList

export type DmapiGetSourceSchemaListApiArg = {
  sourceName: string
}

export type DmapiGetSourceTableListApiResponse = TableNameList

export type DmapiGetSourceTableListApiArg = {
  sourceName: string
  schemaName: string
}

export type DmapiGetSchemaListByTaskAndSourceApiResponse = SchemaNameList

export type DmapiGetSchemaListByTaskAndSourceApiArg = {
  'task-name': string
  sourceName: string
}

export type DmapiGetTableListByTaskAndSourceApiResponse = TableNameList

export type DmapiGetTableListByTaskAndSourceApiArg = {
  'task-name': string
  sourceName: string
  schemaName: string
}

export type Security = {
  ssl_ca_content: string
  ssl_cert_content: string
  ssl_key_content: string
  cert_allowed_cn?: string[]
} | null

export type Purge = {
  interval?: number | null
  expires?: number | null
  remain_space?: number | null
}

export type RelayStatus = {
  master_binlog: string
  master_binlog_gtid: string
  relay_dir: string
  relay_binlog_gtid: string
  relay_catch_up_master: boolean
  stage: string
}

export type SourceStatus = {
  source_name: string
  worker_name: string
  relay_status?: RelayStatus
  error_msg?: string
}

export type RelayConfig = {
  enable_relay?: boolean
  relay_binlog_name?: string | null
  relay_binlog_gtid?: string | null
  relay_dir?: string | null
}

export type Source = {
  source_name: string
  host: string
  port: number
  user: string
  password: string
  enable_gtid: boolean
  security?: Security
  purge?: Purge
  status_list?: SourceStatus[]
  relay_config?: RelayConfig
}

export type GetSourceListResponse = ListResponse<Source>

export type TaskNameList = string[]

export type DeleteSourceResponse = {
  task_name_list?: TaskNameList
}

export type GetSourceStatusResponse = ListResponse<SourceStatus>

export type WorkerNameRequest = {
  worker_name: string
}

export type SchemaNameList = string[]

export type TableNameList = string[]

export const {
  useDmapiCreateSourceMutation,
  useDmapiGetSourceListQuery,
  useDmapiDeleteSourceMutation,
  useDmapiGetSourceStatusQuery,
  useDmapiTransferSourceMutation,
  useDmapiGetSourceSchemaListQuery,
  useDmapiGetSourceTableListQuery,
  useDmapiGetSchemaListByTaskAndSourceQuery,
  useDmapiGetTableListByTaskAndSourceQuery,
} = injectedRtkApi
