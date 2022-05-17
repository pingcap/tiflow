import { api, ListResponse } from './api'

const injectedRtkApi = api.injectEndpoints({
  endpoints: build => ({
    dmapiCreateTask: build.mutation<
      { task: Task; check_result: string },
      { task: Task }
    >({
      query: queryArg => ({
        url: `/tasks`,
        method: 'POST',
        body: queryArg,
      }),
      invalidatesTags: ['Task'],
    }),
    dmapiGetTaskList: build.query<
      ListResponse<Task>,
      {
        withStatus?: boolean
        stage?: TaskStage
        sourceNameList?: string[]
      }
    >({
      query: queryArg => ({
        url: `/tasks`,
        params: {
          with_status: queryArg.withStatus,
          stage: queryArg.stage,
          source_name_list: queryArg.sourceNameList,
        },
      }),
      providesTags: ['Task'],
    }),
    dmapiDeleteTask: build.mutation<
      void,
      {
        taskName: string
        force?: boolean
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}`,
        method: 'DELETE',
        params: { force: queryArg.force },
      }),
      invalidatesTags: ['Task'],
    }),
    dmapiGetTask: build.query<Task, { taskName: string; withStatus?: boolean }>(
      {
        query: queryArg => ({
          url: `/tasks/${queryArg.taskName}`,
          params: { with_status: queryArg.withStatus },
        }),
      }
    ),
    dmapiUpdateTask: build.mutation<
      { task: Task; check_result: string },
      { task: Task }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.task.name}`,
        method: 'PUT',
        body: queryArg,
      }),
      invalidatesTags: ['Task'],
    }),
    dmapiGetTaskStatus: build.query<
      ListResponse<SubTaskStatus>,
      {
        taskName: string
        sourceNameList?: string[]
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/status`,
        params: queryArg.sourceNameList
          ? { source_name_list: queryArg.sourceNameList }
          : undefined,
      }),
    }),
    dmapiStopTask: build.mutation<
      void,
      {
        taskName: string
        source_name_list?: string[]
        timeout_duration?: any
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/stop`,
        method: 'POST',
        body: {
          source_name_list: queryArg.source_name_list,
          timeout_duration: queryArg.timeout_duration,
        },
      }),
      invalidatesTags: ['Task'],
    }),
    dmapiStartTask: build.mutation<void, DmapiStartTaskApiArg>({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/start`,
        method: 'POST',
        body: queryArg.startTaskRequest,
      }),
      invalidatesTags: ['Task'],
    }),
    dmapiGetSchemaListByTaskAndSource: build.query<
      string[],
      {
        taskName: string
        sourceName: string
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/sources/${queryArg.sourceName}/schemas`,
      }),
    }),
    dmapiGetTableListByTaskAndSource: build.query<
      string[],
      {
        taskName: string
        sourceName: string
        schemaName: string
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/sources/${queryArg.sourceName}/schemas/${queryArg.schemaName}`,
      }),
    }),
    dmapiConverterTask: build.mutation<
      { task: Task; task_config_file: string },
      {
        task?: Task
        task_config_file?: string
      }
    >({
      query: queryArg => ({
        url: `/tasks/converters`,
        method: 'POST',
        body: queryArg,
      }),
    }),
    dmapiGetTaskMigrateTargets: build.query<
      ListResponse<TaskMigrateTarget>,
      {
        taskName: string
        sourceName: string
        schemaPattern?: string
        tablePattern?: string
      }
    >({
      query: queryArg => ({
        url: `/tasks/${queryArg.taskName}/sources/${queryArg.sourceName}/migrate_targets`,
        params: {
          schema_pattern: queryArg.schemaPattern,
          table_pattern: queryArg.tablePattern,
        },
      }),
      keepUnusedDataFor: 1,
    }),
  }),
})

export type TaskMigrateTarget = {
  source_schema: string
  source_table: string
  target_schema: string
  target_table: string
}

export type DmapiStartTaskApiArg = {
  taskName: string
  startTaskRequest?: {
    remove_meta?: boolean
    safe_mode_time_duration?: string
    source_name_list?: string[]

    /**
     * RFC3339 format date string
     */
    start_time?: string
  }
}

export type Security = {
  ssl_ca_content: string
  ssl_cert_content: string
  ssl_key_content: string
  cert_allowed_cn?: string[]
} | null

export type TaskTargetDataBase = {
  host: string
  port: number
  user: string
  password: string
  security?: Security
}

export type TaskBinLogFilterRule = {
  ignore_event?: string[]
  ignore_sql?: string[]
}

export const supportedIgnorableEvents = [
  'all',
  'all dml',
  'all ddl',
  'none',
  'none ddl',
  'none dml',
  'insert',
  'update',
  'delete',
  'create database',
  'drop database',
  'create table',
  'create index',
  'drop table',
  'truncate table',
  'rename table',
  'drop index',
  'alter table',
] as const

export type TaskTableMigrateRule = {
  source: {
    source_name: string
    schema: string
    table: string
  }
  target?: {
    schema: string
    table: string
  }
  binlog_filter_rule?: string[]
}

export enum TaskMigrateConsistencyLevel {
  Flush = 'flush',
  Snapshot = 'snapshot',
  Lock = 'lock',
  None = 'none',
  Auto = 'auto',
}

export type TaskFullMigrateConf = {
  export_threads?: number
  import_threads?: number
  data_dir?: string
  consistency?: TaskMigrateConsistencyLevel
}

export type TaskIncrMigrateConf = {
  repl_threads?: number
  repl_batch?: number
}

export type TaskSourceConf = {
  source_name: string
  binlog_name?: string
  binlog_pos?: number
  binlog_gtid?: string
}

export type TaskSourceConfig = {
  full_migrate_conf?: TaskFullMigrateConf
  incr_migrate_conf?: TaskIncrMigrateConf
  source_conf: TaskSourceConf[]
}

export enum TaskMode {
  FULL = 'full',
  INCREMENTAL = 'incremental',
  ALL = 'all',
}

export enum TaskShardMode {
  PESSIMISTIC = 'pessimistic',
  OPTIMISTIC = 'optimistic',
  NONE = 'none',
}

export enum OnDuplicateBehavior {
  OVERWRITE = 'overwrite',
  ERROR = 'error',
  REPLACE = 'replace',
}

export type Task = {
  name: string
  task_mode: TaskMode
  shard_mode?: TaskShardMode
  meta_schema?: string
  enhance_online_schema_change: boolean
  on_duplicate: OnDuplicateBehavior
  target_config: TaskTargetDataBase
  binlog_filter_rule?: {
    [key: string]: TaskBinLogFilterRule
  }
  table_migrate_rule: TaskTableMigrateRule[]
  source_config: TaskSourceConfig
  status_list?: SubTaskStatus[]
}

export interface TaskFormData extends Task {
  binlog_filter_rule_array?: Array<TaskBinLogFilterRule & { name: string }>
  start_after_saved?: boolean
}

export type LoadStatus = {
  finished_bytes: number
  total_bytes: number
  progress: string
  meta_binlog: string
  meta_binlog_gtid: string
}

export type ShardingGroup = {
  target: string
  ddl_list: string[]
  first_location: string
  synced: string[]
  unsynced: string[]
}

export type SyncStatus = {
  total_events: number
  total_tps: number
  recent_tps: number
  master_binlog: string
  master_binlog_gtid: string
  syncer_binlog: string
  syncer_binlog_gtid: string
  blocking_ddls: string[]
  unresolved_groups: ShardingGroup[]
  synced: boolean
  binlog_type: string
  seconds_behind_master: number
}

export type SubTaskStatus = {
  name: string
  source_name: string
  worker_name: string
  stage: TaskStage
  unit: TaskUnit
  unresolved_ddl_lock_id?: string
  load_status?: LoadStatus | null
  sync_status?: SyncStatus | null
}

export const {
  useDmapiStartTaskMutation,
  useDmapiGetTaskListQuery,
  useDmapiDeleteTaskMutation,
  useDmapiGetTaskStatusQuery,
  useDmapiCreateTaskMutation,
  useDmapiGetTaskQuery,
  useDmapiStopTaskMutation,
  useDmapiUpdateTaskMutation,
  useDmapiGetSchemaListByTaskAndSourceQuery,
  useDmapiGetTableListByTaskAndSourceQuery,
  useDmapiConverterTaskMutation,
  useDmapiGetTaskMigrateTargetsQuery,
} = injectedRtkApi

export enum TaskUnit {
  InvalidUnit = 'InvalidUnit',
  Check = 'Check',
  Dump = 'Dump',
  Load = 'Load',
  Sync = 'Sync',
  Relay = 'Relay',
}

export enum TaskStage {
  InvalidStage = 'InvalidStage',
  New = 'New',
  Running = 'Running',
  Paused = 'Paused',
  Stopped = 'Stopped',
  Finished = 'Finished',
  Pausing = 'Pausing',
  Resuming = 'Resuming',
  Stopping = 'Stopping',
}

// https://github.com/pingcap/tiflow/blob/9261014edd93902d1b0bcb473aec911e80901721/dm/dm/ctl/master/query_status.go#L130
export const calculateTaskStatus = (subtasks?: SubTaskStatus[]): TaskStage => {
  if (!subtasks) {
    return TaskStage.InvalidStage
  }

  // TODO Error status
  if (subtasks.some(subtask => subtask.stage === TaskStage.Resuming)) {
    return TaskStage.Resuming
  }

  if (subtasks.some(subtask => subtask.stage === TaskStage.Pausing)) {
    return TaskStage.Pausing
  }

  if (subtasks.some(subtask => subtask.stage === TaskStage.Paused)) {
    return TaskStage.Paused
  }

  if (subtasks.every(subtask => subtask.stage === TaskStage.New)) {
    return TaskStage.New
  }

  if (subtasks.every(subtask => subtask.stage === TaskStage.Finished)) {
    return TaskStage.Finished
  }

  if (subtasks.every(subtask => subtask.stage === TaskStage.Stopped)) {
    return TaskStage.Stopped
  }

  return TaskStage.Running
}
