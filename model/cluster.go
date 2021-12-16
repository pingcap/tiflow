package model

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/adapter"
)

type ExecutorID string

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
	return adapter.ExecutorKeyAdapter.Encode(string(e.ID))
}

type ExecutorStatus int32

const (
	Initing ExecutorStatus = iota
	Running
	Disconnected
	Tombstone
	Busy
)

func (e *ExecutorInfo) ToJSON() (string, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
