package model

import (
	"encoding/json"
	"fmt"

	"github.com/hanfei1991/microcosom/pkg/adapter"
)

type ExecutorID int32

// ExecutorInfo describes an Executor.
type ExecutorInfo struct {
	ID   ExecutorID `json:"id"`
	Addr string     `json:"addr"`
	// The capability of executor, including
	// 1. cpu (goroutines)
	// 2. memory
	// 3. disk cap
	// TODO: So we should enrich the cap dimensions in the future.
	Capability int `json:"cap"`
	// What kind of information do we need?
	// LastHeartbeatTime int64
}

func (e *ExecutorInfo) EtcdKey() string {
	return adapter.ExecutorKeyAdapter.Encode(fmt.Sprintf("%d", e.ID))
}

type JobType int

const (
	JobDM JobType = iota
	JobCDC
	JobBenchmark
)

type ExecutorStatus int32

const (
	Initing ExecutorStatus = iota
	Running
	Disconnected
	Tombstone
	Busy
)

type JobInfo struct {
	Type     JobType `json:"type"`
	Config   string  `json:"config"` // describes a cdc/dm or other type of job.
	UserName string  `json:"user"`   // reserved field
	// CostQuota int // indicates how much resource this job will use.
}

//type TaskInfo struct {
//
//}

func (e *ExecutorInfo) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
