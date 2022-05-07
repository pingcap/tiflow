package runtime

import (
	"encoding/json"

	"github.com/pingcap/errors"

	"github.com/hanfei1991/microcosm/jobmaster/dm/metadata"
	"github.com/hanfei1991/microcosm/lib"
	libModel "github.com/hanfei1991/microcosm/lib/model"
)

type TaskStatus interface {
	GetUnit() libModel.WorkerType
	GetTask() string
	GetStage() metadata.TaskStage
}

type DefaultTaskStatus struct {
	Unit  libModel.WorkerType
	Task  string
	Stage metadata.TaskStage
}

func (s *DefaultTaskStatus) GetUnit() libModel.WorkerType {
	return s.Unit
}

func (s *DefaultTaskStatus) GetTask() string {
	return s.Task
}

func (s *DefaultTaskStatus) GetStage() metadata.TaskStage {
	return s.Stage
}

type DumpStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:DumpStatus
	TotalTables       int64
	CompletedTables   float64
	FinishedBytes     float64
	FinishedRows      float64
	EstimateTotalRows float64
}

type LoadStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:LoadStatus
	FinishedBytes  int64
	TotalBytes     int64
	Progress       string
	MetaBinlog     string
	MetaBinlogGTID string
}

type SyncStatus struct {
	DefaultTaskStatus
	// copy from tiflow/dm/dm/proto/dmworker.proto:SyncStatus
	TotalEvents      int64
	TotalTps         int64
	RecentTps        int64
	MasterBinlog     string
	MasterBinlogGtid string
	SyncerBinlog     string
	SyncerBinlogGtid string
	BlockingDDLs     []string
	// TODO: add sharding group
	// ShardingGroup unresolvedGroups = 9; // sharding groups which current are un-resolved
	Synced              bool
	BinlogType          string
	SecondsBehindMaster int64
	BlockDDLOwner       string
	ConflictMsg         string
}

// Use when jobmaster receive a worker offline.
// No need to serialize.
func NewOfflineStatus(taskID string) *DefaultTaskStatus {
	return &DefaultTaskStatus{
		Unit:  0,
		Task:  taskID,
		Stage: metadata.StageUnscheduled,
	}
}

// UnmarshalTaskStatus unmarshal a task status base on the unit.
func UnmarshalTaskStatus(data []byte) (TaskStatus, error) {
	var typ struct {
		Unit libModel.WorkerType
	}
	if err := json.Unmarshal(data, &typ); err != nil {
		return nil, errors.Trace(err)
	}

	var taskStatus TaskStatus
	switch typ.Unit {
	case lib.WorkerDMDump:
		taskStatus = &DumpStatus{}
	case lib.WorkerDMLoad:
		taskStatus = &LoadStatus{}
	case lib.WorkerDMSync:
		taskStatus = &SyncStatus{}
	default:
		return nil, errors.Errorf("unknown unit: %d", typ.Unit)
	}
	err := json.Unmarshal(data, taskStatus)
	return taskStatus, errors.Trace(err)
}

func MarshalTaskStatus(taskStatus TaskStatus) ([]byte, error) {
	return json.Marshal(taskStatus)
}
