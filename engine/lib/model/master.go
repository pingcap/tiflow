// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

import (
	"encoding/json"

	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

type (
	MasterStatusCode int32
	WorkerType       int64
)

// TODO: using reflect to generate it more generally
// related to some implement of gorm
var MasterUpdateColumns = []string{
	"updated_at",
	"project_id",
	"id",
	"type",
	"status",
	"node_id",
	"address",
	"epoch",
	"config",
}

type MasterMetaKVData struct {
	ormModel.Model
	ProjectID  tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:varchar(64) not null;index:idx_st,priority:1"`
	ID         MasterID         `json:"id" gorm:"column:id;type:varchar(64) not null;uniqueIndex:uidx_id"`
	Tp         WorkerType       `json:"type" gorm:"column:type;type:tinyint not null"`
	StatusCode MasterStatusCode `json:"status" gorm:"column:status;type:tinyint not null;index:idx_st,priority:2"`
	NodeID     p2p.NodeID       `json:"node-id" gorm:"column:node_id;type:varchar(64) not null"`
	Addr       string           `json:"addr" gorm:"column:address;type:varchar(64) not null"`
	Epoch      Epoch            `json:"epoch" gorm:"column:epoch;type:bigint not null"`

	// Config holds business-specific data
	Config []byte `json:"config" gorm:"column:config;type:blob"`
	// TODO: add master status and checkpoint data
}

func (m *MasterMetaKVData) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func (m *MasterMetaKVData) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// Map is used for update the orm model
func (m *MasterMetaKVData) Map() map[string]interface{} {
	return map[string]interface{}{
		"project_id": m.ProjectID,
		"id":         m.ID,
		"type":       m.Tp,
		"status":     m.StatusCode,
		"node_id":    m.NodeID,
		"address":    m.Addr,
		"epoch":      m.Epoch,
		"config":     m.Config,
	}
}

// Job master statuses
const (
	MasterStatusUninit = MasterStatusCode(iota + 1)
	MasterStatusInit
	MasterStatusFinished
	MasterStatusStopped
)
