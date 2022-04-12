package model

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterStatusCode int32
	WorkerType       int64
)

type MasterMetaKVData struct {
	ID         MasterID         `json:"id"`
	Addr       string           `json:"addr"`
	NodeID     p2p.NodeID       `json:"node-id"`
	Epoch      Epoch            `json:"epoch"`
	StatusCode MasterStatusCode `json:"status"`
	Tp         WorkerType       `json:"type"`

	// Config holds business-specific data
	Config []byte `json:"config"`
	// TODO: add master status and checkpoint data
}

func (m *MasterMetaKVData) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MasterMetaKVData) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// Job master statuses
const (
	MasterStatusUninit = MasterStatusCode(iota + 1)
	MasterStatusInit
	MasterStatusFinished
	MasterStatusStopped
)
