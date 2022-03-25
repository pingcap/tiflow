import { api, ListResponse } from './api'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiCreateSource: build.mutation<
      Source,
      { source: Source; worker_name?: string }
    >({
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
        with_status?: boolean
        enable_relay?: boolean
      }
    >({
      query: queryArg => ({
        url: `/sources`,
        params: queryArg,
      }),
      providesTags: ['Source'],
    }),
    dmapiDeleteSource: build.mutation<
      void,
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
    dmapiGetSource: build.query<
      Source,
      { sourceName: string; withStatus?: boolean }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.sourceName}`,
        params: { with_status: queryArg.withStatus },
      }),
    }),
    dmapiUpdateSource: build.mutation<Source, { source: Source }>({
      query: queryArg => ({
        url: `/sources/${queryArg.source.source_name}`,
        method: 'PUT',
        body: queryArg,
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiDisableSource: build.mutation<void, string>({
      query: queryArg => ({
        url: `/sources/${queryArg}/disable`,
        method: 'POST',
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiEnableSource: build.mutation<void, string>({
      query: queryArg => ({
        url: `/sources/${queryArg}/enable`,
        method: 'POST',
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiGetSourceStatus: build.query<ListResponse<SourceStatus>, string>({
      query: queryArg => ({
        url: `/sources/${queryArg}/status`,
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
    dmapiDisableRelay: build.mutation<
      void,
      {
        name: string
        payload?: {
          worker_name_list?: string[]
        }
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.name}/relay/disable`,
        method: 'POST',
        body: queryArg.payload,
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiEnableRelay: build.mutation<
      void,
      {
        name: string
        payload?: {
          relay_binlog_gtid?: string | null
          relay_binlog_name?: string | null
          relay_dir?: string | null
          worker_name_list: string[]
        }
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.name}/relay/enable`,
        method: 'POST',
        body: queryArg.payload,
      }),
      invalidatesTags: ['Source'],
    }),
    dmapiPurgeRelay: build.mutation<
      void,
      {
        name: string
        purgeRelayRequest: {
          relay_binlog_name: string
          relay_dir?: string | null
        }
      }
    >({
      query: queryArg => ({
        url: `/sources/${queryArg.name}/relay/purge`,
        method: 'POST',
        body: queryArg.purgeRelayRequest,
      }),
    }),
  }),
})

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
  enable: boolean
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
  flavor: string
}

export const {
  useDmapiCreateSourceMutation,
  useDmapiGetSourceListQuery,
  useDmapiDeleteSourceMutation,
  useDmapiDisableSourceMutation,
  useDmapiEnableSourceMutation,
  useDmapiGetSourceQuery,
  useDmapiUpdateSourceMutation,
  useDmapiGetSourceStatusQuery,
  useDmapiTransferSourceMutation,
  useDmapiGetSourceSchemaListQuery,
  useDmapiGetSourceTableListMutation,
  useDmapiDisableRelayMutation,
  useDmapiEnableRelayMutation,
  useDmapiPurgeRelayMutation,
} = injectedRtkApi
