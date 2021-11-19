export interface Source {
  enable_gtid: boolean
  host: string
  password: string
  port: number
  source_name: string
  status_list: SourceStatus[]
}

export interface SourceStatus {
  error_msg: string
  relay_status: {
    master_binlog: string
    master_binlog_gtid: string
    relay_binlog_gtid: string
    relay_catch_up_master: boolean
    relay_dir: string
    stage: string
  }
  source_name: string
  worker_name: string
}
