package model

import (
	"encoding/json"

	ormModel "github.com/hanfei1991/microcosm/pkg/orm/model"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

type (
	MasterStatusCode int32
	WorkerType       int64
)

type MasterMetaKVData struct {
	ormModel.Model
	ProjectID  tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:char(36) not null;index:idx_st,priority:1"`
	ID         MasterID         `json:"id" gorm:"column:id;type:char(36) not null;uniqueIndex:uidx_id"`
	Tp         WorkerType       `json:"type" gorm:"column:type;type:tinyint not null"`
	StatusCode MasterStatusCode `json:"status" gorm:"column:status;type:tinyint not null;index:idx_st,priority:2"`
	NodeID     p2p.NodeID       `json:"node-id" gorm:"column:node_id;type:char(36) not null"`
	Addr       string           `json:"addr" gorm:"column:address;type:varchar(64) not null"`
	Epoch      Epoch            `json:"epoch" gorm:"column:epoch;type:bigint not null"`

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
