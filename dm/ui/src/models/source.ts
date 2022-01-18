import { api, ListResponse } from './api'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiCreateSource: build.mutation<Source, Partial<Source>>({
      query: queryArg => ({
        url: `/sources`,
        method: 'POST',
        body: queryArg,
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiGetSourceList: build.query<
      ListResponse<Source>,
      {
        withStatus?: boolean
      }
    >({
      query: queryArg => ({
        url: `/sources`,
        params: { with_status: queryArg.withStatus },
      }),
      providesTags: ['Source'],
    }),
    dmapiDeleteSource: build.mutation<
      {
        task_name_list?: string[]
      },
      {
        sourceName: string
        force?: boolean
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}`,
        method: 'DELETE',
        params: { force: queryArg.force },
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiGetSourceStatus: build.query<
      ListResponse<SourceStatus>,
      {
        sourceName: string
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/status`,
      }),
    }),
    dmapiTransferSource: build.mutation<
      void,
      {
        sourceName: string
        workerName: string
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/transfer`,
        method: 'POST',
        body: {
          worker_name: queryArg.workerName,
        },
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiGetSourceSchemaList: build.query<string[], { sourceName: string }>({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/schemas`,
      }),
    }),
    dmapiGetSourceTableList: build.mutation<
      string[],
      { sourceName: string; schemaName: string }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}/schemas/${queryArg.schemaName}`,
      }),
    }),
  }),
})

export type DmapiGetSourceStatusApiArg = {
  sourceName: string
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

export const {
  useDmapiCreateSourceMutation,
  useDmapiGetSourceListQuery,
  useDmapiDeleteSourceMutation,
  useDmapiGetSourceStatusQuery,
  useDmapiTransferSourceMutation,
  useDmapiGetSourceSchemaListQuery,
  useDmapiGetSourceTableListMutation,
} = injectedRtkApi
