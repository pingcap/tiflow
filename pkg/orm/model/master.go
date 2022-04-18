package model

import (
	"encoding/json"

	"github.com/hanfei1991/microcosm/pkg/p2p"
)

type (
	MasterStatusCode int32
	WorkerType       int64
)

// TODO: check how to use in framework
type MasterMeta struct {
	Model
	ProjectID ProjectID        `json:"project-id" gorm:"column:project_id;type:char(36) not null;index:idx_st,priority:1"`
	ID        MasterID         `json:"id" gorm:"column:id;type:char(36) not null;uniqueIndex:uidx_id"`
	Type      WorkerType       `json:"type" gorm:"column:type;type:tinyint not null"`
	Status    MasterStatusCode `json:"status" gorm:"column:status;type:tinyint not null;index:idx_st,priority:2"`
	NodeID    p2p.NodeID       `json:"node-id" gorm:"column:node_id;type:char(36) not null"`
	Addr      string           `json:"addr" gorm:"column:address;type:varchar(64) not null"`
	Epoch     Epoch            `json:"epoch" gorm:"column:epoch;type:bigint not null"`

	// Config holds business-specific data
	Config []byte `json:"config" grom:"column:config;type:blob"`
	// TODO: add master status and checkpoint data
}

func (m *MasterMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MasterMeta) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// Job master statuses
const (
	MasterStatusUninit = MasterStatusCode(iota + 1)
	MasterStatusInit
	MasterStatusFinished
	MasterStatusStopped
)
