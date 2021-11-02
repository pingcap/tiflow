// Package openapi provides primitives to interact with the openapi HTTP API.
//
// Code generated by github.com/deepmap/oapi-codegen version v1.8.2 DO NOT EDIT.
package openapi

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// Defines values for TaskOnDuplicate.
const (
	TaskOnDuplicateError TaskOnDuplicate = "error"

	TaskOnDuplicateOverwrite TaskOnDuplicate = "overwrite"
)

// Defines values for TaskShardMode.
const (
	TaskShardModeOptimistic TaskShardMode = "optimistic"

	TaskShardModePessimistic TaskShardMode = "pessimistic"
)

// Defines values for TaskTaskMode.
const (
	TaskTaskModeAll TaskTaskMode = "all"

	TaskTaskModeFull TaskTaskMode = "full"

	TaskTaskModeIncremental TaskTaskMode = "incremental"
)

// ClusterMaster defines model for ClusterMaster.
type ClusterMaster struct {
	// address of the current master node
	Addr string `json:"addr"`

	// online status of this master
	Alive bool `json:"alive"`

	// is this master the leader
	Leader bool   `json:"leader"`
	Name   string `json:"name"`
}

// ClusterWorker defines model for ClusterWorker.
type ClusterWorker struct {
	// address of the current master node
	Addr string `json:"addr"`

	// bound source name of this worker node
	BoundSourceName string `json:"bound_source_name"`

	// bound stage of this worker node
	BoundStage string `json:"bound_stage"`
	Name       string `json:"name"`
}

// CreateTaskRequest defines model for CreateTaskRequest.
type CreateTaskRequest struct {
	// whether to remove meta database in downstream database
	RemoveMeta bool `json:"remove_meta"`

	// source name list
	SourceNameList *SourceNameList `json:"source_name_list,omitempty"`

	// task
	Task Task `json:"task"`
}

// DeleteSourceResponse defines model for DeleteSourceResponse.
type DeleteSourceResponse struct {
	// task name list
	TaskNameList *TaskNameList `json:"task_name_list,omitempty"`
}

// operation error
type ErrorWithMessage struct {
	// error code
	ErrorCode int `json:"error_code"`

	// error message
	ErrorMsg string `json:"error_msg"`
}

// GetClusterMasterListResponse defines model for GetClusterMasterListResponse.
type GetClusterMasterListResponse struct {
	Data  []ClusterMaster `json:"data"`
	Total int             `json:"total"`
}

// GetClusterWorkerListResponse defines model for GetClusterWorkerListResponse.
type GetClusterWorkerListResponse struct {
	Data  []ClusterWorker `json:"data"`
	Total int             `json:"total"`
}

// GetSourceListResponse defines model for GetSourceListResponse.
type GetSourceListResponse struct {
	Data  []Source `json:"data"`
	Total int      `json:"total"`
}

// GetSourceStatusResponse defines model for GetSourceStatusResponse.
type GetSourceStatusResponse struct {
	Data  []SourceStatus `json:"data"`
	Total int            `json:"total"`
}

// GetTaskListResponse defines model for GetTaskListResponse.
type GetTaskListResponse struct {
	Data  []Task `json:"data"`
	Total int    `json:"total"`
}

// GetTaskStatusResponse defines model for GetTaskStatusResponse.
type GetTaskStatusResponse struct {
	Data  []SubTaskStatus `json:"data"`
	Total int             `json:"total"`
}

// GetTaskTableStructureResponse defines model for GetTaskTableStructureResponse.
type GetTaskTableStructureResponse struct {
	SchemaCreateSql *string `json:"schema_create_sql,omitempty"`
	SchemaName      *string `json:"schema_name,omitempty"`
	TableName       string  `json:"table_name"`
}

// status of load unit
type LoadStatus struct {
	FinishedBytes  int64  `json:"finished_bytes"`
	MetaBinlog     string `json:"meta_binlog"`
	MetaBinlogGtid string `json:"meta_binlog_gtid"`
	Progress       string `json:"progress"`
	TotalBytes     int64  `json:"total_bytes"`
}

// action to operate table request
type OperateTaskTableStructureRequest struct {
	// Writes the schema to the checkpoint so that DM can load it after restarting the task
	Flush *bool `json:"flush,omitempty"`

	// sql you want to operate
	SqlContent string `json:"sql_content"`

	// Updates the optimistic sharding metadata with this schema only used when an error occurs in the optimistic sharding DDL mode
	Sync *bool `json:"sync,omitempty"`
}

// relay log cleanup policy configuration
type Purge struct {
	// expiration time of relay log
	Expires *int64 `json:"expires"`

	// The interval to periodically check if the relay log is expired, default value: 3600, in seconds
	Interval *int64 `json:"interval"`

	// Minimum free disk space, in GB
	RemainSpace *int64 `json:"remain_space"`
}

// status of relay log
type RelayStatus struct {
	// upstream binlog file information
	MasterBinlog string `json:"master_binlog"`

	// GTID of the upstream
	MasterBinlogGtid string `json:"master_binlog_gtid"`

	// relay current GTID
	RelayBinlogGtid string `json:"relay_binlog_gtid"`

	// whether to catch up with upstream progress
	RelayCatchUpMaster bool `json:"relay_catch_up_master"`

	// the directory where the relay log is stored
	RelayDir string `json:"relay_dir"`

	// current status
	Stage string `json:"stage"`
}

// schema name list
type SchemaNameList []string

// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
type Security struct {
	// Common Name of SSL certificates
	CertAllowedCn *[]string `json:"cert_allowed_cn,omitempty"`

	// certificate file content
	SslCaContent string `json:"ssl_ca_content"`

	// File content of PEM format/X509 format certificates
	SslCertContent string `json:"ssl_cert_content"`

	// Content of the private key file in X509 format
	SslKeyContent string `json:"ssl_key_content"`
}

// ShardingGroup defines model for ShardingGroup.
type ShardingGroup struct {
	DdlList       []string `json:"ddl_list"`
	FirstLocation string   `json:"first_location"`
	Synced        []string `json:"synced"`
	Target        string   `json:"target"`
	Unsynced      []string `json:"unsynced"`
}

// source
type Source struct {
	// whether to use GTID to pull binlogs from upstream
	EnableGtid bool `json:"enable_gtid"`

	// source address
	Host string `json:"host"`

	// source password
	Password string `json:"password"`

	// source port
	Port int `json:"port"`

	// relay log cleanup policy configuration
	Purge *Purge `json:"purge,omitempty"`

	// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
	Security *Security `json:"security"`

	// source name
	SourceName string          `json:"source_name"`
	StatusList *[]SourceStatus `json:"status_list,omitempty"`

	// source username
	User string `json:"user"`
}

// source name list
type SourceNameList []string

// source status
type SourceStatus struct {
	// error message when something wrong
	ErrorMsg *string `json:"error_msg,omitempty"`

	// status of relay log
	RelayStatus *RelayStatus `json:"relay_status,omitempty"`

	// source name
	SourceName string `json:"source_name"`

	// The worker currently bound to the source
	WorkerName string `json:"worker_name"`
}

// action to start a relay request
type StartRelayRequest struct {
	// starting GTID of the upstream binlog
	RelayBinlogGtid *string `json:"relay_binlog_gtid"`

	// starting filename of the upstream binlog
	RelayBinlogName *string `json:"relay_binlog_name"`

	// the directory where the relay log is stored
	RelayDir *string `json:"relay_dir"`

	// worker name list
	WorkerNameList WorkerNameList `json:"worker_name_list"`
}

// action to stop a relay request
type StopRelayRequest struct {
	// worker name list
	WorkerNameList WorkerNameList `json:"worker_name_list"`
}

// SubTaskStatus defines model for SubTaskStatus.
type SubTaskStatus struct {
	// status of load unit
	LoadStatus *LoadStatus `json:"load_status,omitempty"`

	// task name
	Name string `json:"name"`

	// source name
	SourceName string `json:"source_name"`

	// current stage of the task
	Stage string `json:"stage"`

	// status of sync uuit
	SyncStatus *SyncStatus `json:"sync_status,omitempty"`

	// task unit type
	Unit                string  `json:"unit"`
	UnresolvedDdlLockId *string `json:"unresolved_ddl_lock_id,omitempty"`

	// worker name
	WorkerName string `json:"worker_name"`
}

// status of sync uuit
type SyncStatus struct {
	BinlogType string `json:"binlog_type"`

	// sharding DDL which current is blocking
	BlockingDdls        []string `json:"blocking_ddls"`
	MasterBinlog        string   `json:"master_binlog"`
	MasterBinlogGtid    string   `json:"master_binlog_gtid"`
	RecentTps           int64    `json:"recent_tps"`
	SecondsBehindMaster int64    `json:"seconds_behind_master"`
	Synced              bool     `json:"synced"`
	SyncerBinlog        string   `json:"syncer_binlog"`
	SyncerBinlogGtid    string   `json:"syncer_binlog_gtid"`
	TotalEvents         int64    `json:"total_events"`
	TotalTps            int64    `json:"total_tps"`

	// sharding groups which current are un-resolved
	UnresolvedGroups []ShardingGroup `json:"unresolved_groups"`
}

// schema name list
type TableNameList []string

// task
type Task struct {
	BinlogFilterRule *Task_BinlogFilterRule `json:"binlog_filter_rule,omitempty"`

	// whether to enable support for the online ddl plugin
	EnhanceOnlineSchemaChange bool `json:"enhance_online_schema_change"`

	// downstream database for storing meta information
	MetaSchema *string `json:"meta_schema,omitempty"`

	// task name
	Name string `json:"name"`

	// how to handle conflicted data
	OnDuplicate TaskOnDuplicate `json:"on_duplicate"`

	// the way to coordinate DDL
	ShardMode *TaskShardMode `json:"shard_mode,omitempty"`

	// source-related configuration
	SourceConfig TaskSourceConfig `json:"source_config"`

	// table migrate rule
	TableMigrateRule []TaskTableMigrateRule `json:"table_migrate_rule"`

	// downstream database configuration
	TargetConfig TaskTargetDataBase `json:"target_config"`

	// migrate mode
	TaskMode TaskTaskMode `json:"task_mode"`
}

// Task_BinlogFilterRule defines model for Task.BinlogFilterRule.
type Task_BinlogFilterRule struct {
	AdditionalProperties map[string]TaskBinLogFilterRule `json:"-"`
}

// how to handle conflicted data
type TaskOnDuplicate string

// the way to coordinate DDL
type TaskShardMode string

// migrate mode
type TaskTaskMode string

// Filtering rules at binlog level
type TaskBinLogFilterRule struct {
	// event type
	IgnoreEvent *[]string `json:"ignore_event,omitempty"`

	// sql pattern to filter
	IgnoreSql *[]string `json:"ignore_sql,omitempty"`
}

// configuration of full migrate tasks
type TaskFullMigrateConf struct {
	// to control the way in which data is exported for consistency assurance
	Consistency *string `json:"consistency,omitempty"`

	// storage dir name
	DataDir *string `json:"data_dir,omitempty"`

	// full export of concurrent
	ExportThreads *int `json:"export_threads,omitempty"`

	// full import of concurrent
	ImportThreads *int `json:"import_threads,omitempty"`
}

// configuration of incremental tasks
type TaskIncrMigrateConf struct {
	// incremental synchronization of batch execution sql quantities
	ReplBatch *int `json:"repl_batch,omitempty"`

	// incremental task of concurrent
	ReplThreads *int `json:"repl_threads,omitempty"`
}

// task name list
type TaskNameList []string

// TaskSourceConf defines model for TaskSourceConf.
type TaskSourceConf struct {
	BinlogGtid *string `json:"binlog_gtid,omitempty"`
	BinlogName *string `json:"binlog_name,omitempty"`
	BinlogPos  *int    `json:"binlog_pos,omitempty"`

	// source name
	SourceName string `json:"source_name"`
}

// source-related configuration
type TaskSourceConfig struct {
	// configuration of full migrate tasks
	FullMigrateConf *TaskFullMigrateConf `json:"full_migrate_conf,omitempty"`

	// configuration of incremental tasks
	IncrMigrateConf *TaskIncrMigrateConf `json:"incr_migrate_conf,omitempty"`

	// source configuration
	SourceConf []TaskSourceConf `json:"source_conf"`
}

// upstream table to downstream migrate rules
type TaskTableMigrateRule struct {
	// filter rule name
	BinlogFilterRule *[]string `json:"binlog_filter_rule,omitempty"`

	// source-related configuration
	Source struct {
		// schema name, wildcard support
		Schema string `json:"schema"`

		// source name
		SourceName string `json:"source_name"`

		// table name, wildcard support
		Table string `json:"table"`
	} `json:"source"`

	// downstream-related configuration
	Target *struct {
		// schema name, does not support wildcards
		Schema string `json:"schema"`

		// table name, does not support wildcards
		Table string `json:"table"`
	} `json:"target,omitempty"`
}

// downstream database configuration
type TaskTargetDataBase struct {
	// source address
	Host string `json:"host"`

	// source password
	Password string `json:"password"`

	// ource port
	Port int `json:"port"`

	// data source ssl configuration, the field will be hidden when getting the data source configuration from the interface
	Security *Security `json:"security"`

	// source username
	User string `json:"user"`
}

// worker name list
type WorkerNameList []string

// requests related to workers
type WorkerNameRequest struct {
	// worker name
	WorkerName string `json:"worker_name"`
}

// DMAPIGetSourceListParams defines parameters for DMAPIGetSourceList.
type DMAPIGetSourceListParams struct {
	// get source with status
	WithStatus *bool `json:"with_status,omitempty"`
}

// DMAPICreateSourceJSONBody defines parameters for DMAPICreateSource.
type DMAPICreateSourceJSONBody Source

// DMAPIDeleteSourceParams defines parameters for DMAPIDeleteSource.
type DMAPIDeleteSourceParams struct {
	// force stop task also stop the related tasks
	Force *bool `json:"force,omitempty"`
}

// DMAPIStartRelayJSONBody defines parameters for DMAPIStartRelay.
type DMAPIStartRelayJSONBody StartRelayRequest

// DMAPIStopRelayJSONBody defines parameters for DMAPIStopRelay.
type DMAPIStopRelayJSONBody StopRelayRequest

// DMAPITransferSourceJSONBody defines parameters for DMAPITransferSource.
type DMAPITransferSourceJSONBody WorkerNameRequest

// DMAPIStartTaskJSONBody defines parameters for DMAPIStartTask.
type DMAPIStartTaskJSONBody CreateTaskRequest

// DMAPIDeleteTaskParams defines parameters for DMAPIDeleteTask.
type DMAPIDeleteTaskParams struct {
	// source name list
	SourceNameList *[]string `json:"source_name_list,omitempty"`
}

// DMAPIPauseTaskJSONBody defines parameters for DMAPIPauseTask.
type DMAPIPauseTaskJSONBody SourceNameList

// DMAPIResumeTaskJSONBody defines parameters for DMAPIResumeTask.
type DMAPIResumeTaskJSONBody SourceNameList

// DMAPIOperateTableStructureJSONBody defines parameters for DMAPIOperateTableStructure.
type DMAPIOperateTableStructureJSONBody OperateTaskTableStructureRequest

// DMAPIGetTaskStatusParams defines parameters for DMAPIGetTaskStatus.
type DMAPIGetTaskStatusParams struct {
	// source name list
	SourceNameList *SourceNameList `json:"source_name_list,omitempty"`
}

// DMAPICreateSourceJSONRequestBody defines body for DMAPICreateSource for application/json ContentType.
type DMAPICreateSourceJSONRequestBody DMAPICreateSourceJSONBody

// DMAPIStartRelayJSONRequestBody defines body for DMAPIStartRelay for application/json ContentType.
type DMAPIStartRelayJSONRequestBody DMAPIStartRelayJSONBody

// DMAPIStopRelayJSONRequestBody defines body for DMAPIStopRelay for application/json ContentType.
type DMAPIStopRelayJSONRequestBody DMAPIStopRelayJSONBody

// DMAPITransferSourceJSONRequestBody defines body for DMAPITransferSource for application/json ContentType.
type DMAPITransferSourceJSONRequestBody DMAPITransferSourceJSONBody

// DMAPIStartTaskJSONRequestBody defines body for DMAPIStartTask for application/json ContentType.
type DMAPIStartTaskJSONRequestBody DMAPIStartTaskJSONBody

// DMAPIPauseTaskJSONRequestBody defines body for DMAPIPauseTask for application/json ContentType.
type DMAPIPauseTaskJSONRequestBody DMAPIPauseTaskJSONBody

// DMAPIResumeTaskJSONRequestBody defines body for DMAPIResumeTask for application/json ContentType.
type DMAPIResumeTaskJSONRequestBody DMAPIResumeTaskJSONBody

// DMAPIOperateTableStructureJSONRequestBody defines body for DMAPIOperateTableStructure for application/json ContentType.
type DMAPIOperateTableStructureJSONRequestBody DMAPIOperateTableStructureJSONBody

// Getter for additional properties for Task_BinlogFilterRule. Returns the specified
// element and whether it was found
func (a Task_BinlogFilterRule) Get(fieldName string) (value TaskBinLogFilterRule, found bool) {
	if a.AdditionalProperties != nil {
		value, found = a.AdditionalProperties[fieldName]
	}
	return
}

// Setter for additional properties for Task_BinlogFilterRule
func (a *Task_BinlogFilterRule) Set(fieldName string, value TaskBinLogFilterRule) {
	if a.AdditionalProperties == nil {
		a.AdditionalProperties = make(map[string]TaskBinLogFilterRule)
	}
	a.AdditionalProperties[fieldName] = value
}

// Override default JSON handling for Task_BinlogFilterRule to handle AdditionalProperties
func (a *Task_BinlogFilterRule) UnmarshalJSON(b []byte) error {
	object := make(map[string]json.RawMessage)
	err := json.Unmarshal(b, &object)
	if err != nil {
		return err
	}

	if len(object) != 0 {
		a.AdditionalProperties = make(map[string]TaskBinLogFilterRule)
		for fieldName, fieldBuf := range object {
			var fieldVal TaskBinLogFilterRule
			err := json.Unmarshal(fieldBuf, &fieldVal)
			if err != nil {
				return errors.Wrap(err, fmt.Sprintf("error unmarshaling field %s", fieldName))
			}
			a.AdditionalProperties[fieldName] = fieldVal
		}
	}
	return nil
}

// Override default JSON handling for Task_BinlogFilterRule to handle AdditionalProperties
func (a Task_BinlogFilterRule) MarshalJSON() ([]byte, error) {
	var err error
	object := make(map[string]json.RawMessage)

	for fieldName, field := range a.AdditionalProperties {
		object[fieldName], err = json.Marshal(field)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("error marshaling '%s'", fieldName))
		}
	}
	return json.Marshal(object)
}
