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
	"database/sql/driver"
	"encoding/json"
	"reflect"

	ormModel "github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/label"
	"gorm.io/gorm"
)

type (
	// MasterState is used in framework to manage job status
	MasterState int8
)

// Job master statuses
// NOTICE: DO NOT CHANGE the previous status code
// Modify the MasterMeta.State comment IF you add some new status code
const (
	MasterStateUninit   = MasterState(1)
	MasterStateInit     = MasterState(2)
	MasterStateFinished = MasterState(3)
	MasterStateStopped  = MasterState(4)
	MasterStateFailed   = MasterState(5)
	// extend the status code here
)

// IsTerminatedState checks whether master state is terminated
func (code MasterState) IsTerminatedState() bool {
	switch code {
	case MasterStateFinished, MasterStateStopped, MasterStateFailed:
		return true
	default:
		return false
	}
}

// MasterMetaExt stores some attributes of job masters that do not need
// to be indexed.
type MasterMetaExt struct {
	Selectors []*label.Selector `json:"selectors"`
}

// Value implements driver.Valuer.
func (e MasterMetaExt) Value() (driver.Value, error) {
	b, err := json.Marshal(e)
	if err != nil {
		return nil, errors.Annotate(err, "failed to marshal MasterMetaExt")
	}
	return string(b), nil
}

// Scan implements sql.Scanner.
func (e *MasterMetaExt) Scan(rawInput interface{}) error {
	// Zero the fields.
	*e = MasterMetaExt{}

	if rawInput == nil {
		return nil
	}

	// As different SQL drivers might treat the JSON value differently,
	// we need to handle two cases where the JSON value is passed as a string
	// and a byte slice respectively.
	var bytes []byte
	switch input := rawInput.(type) {
	case string:
		// SQLite is this case.
		if len(input) == 0 {
			return nil
		}
		bytes = []byte(input)
	case []byte:
		// MySQL is this case.
		if len(input) == 0 {
			return nil
		}
		bytes = input
	default:
		return errors.Errorf("failed to scan MasterMetaExt. "+
			"Expected string or []byte, got %s", reflect.TypeOf(rawInput))
	}

	if err := json.Unmarshal(bytes, e); err != nil {
		return errors.Annotate(err, "failed to unmarshal MasterMetaExt")
	}

	return nil
}

// MasterMeta defines the metadata of job master
type MasterMeta struct {
	ormModel.Model
	ProjectID tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:varchar(128) not null;index:idx_mst,priority:1"`
	ID        MasterID         `json:"id" gorm:"column:id;type:varchar(128) not null;uniqueIndex:uidx_mid"`
	Type      WorkerType       `json:"type" gorm:"column:type;type:smallint not null;comment:JobManager(1),CvsJobMaster(2),FakeJobMaster(3),DMJobMaster(4),CDCJobMaster(5)"`
	State     MasterState      `json:"state" gorm:"column:state;type:tinyint not null;index:idx_mst,priority:2;comment:Uninit(1),Init(2),Finished(3),Stopped(4),Failed(5)"`
	NodeID    p2p.NodeID       `json:"node-id" gorm:"column:node_id;type:varchar(128) not null"`
	Addr      string           `json:"addr" gorm:"column:address;type:varchar(256) not null"`
	Epoch     Epoch            `json:"epoch" gorm:"column:epoch;type:bigint not null"`

	// Config holds business-specific data
	Config []byte `json:"config" gorm:"column:config;type:blob"`

	// error message for the job
	ErrorMsg string `json:"error-message" gorm:"column:error_message;type:text"`

	// if job is finished or canceled, business logic can set self-defined job info to `Detail`
	Detail []byte `json:"detail" gorm:"column:detail;type:blob"`

	Ext MasterMetaExt `json:"ext" gorm:"column:ext;type:JSON"`

	// Deleted is a nullable timestamp. Then master is deleted
	// if Deleted is not null.
	Deleted gorm.DeletedAt
}

// Marshal returns the JSON encoding of MasterMeta.
func (m *MasterMeta) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

// Unmarshal parses the JSON-encoded data and stores the result to MasterMeta
func (m *MasterMeta) Unmarshal(data []byte) error {
	return json.Unmarshal(data, m)
}

// RefreshValues is used to generate orm value map when refreshing metadata.
func (m *MasterMeta) RefreshValues() ormModel.KeyValueMap {
	return ormModel.KeyValueMap{
		"node_id": m.NodeID,
		"address": m.Addr,
		"epoch":   m.Epoch,
	}
}

// UpdateStateValues is used to generate orm value map when updating state of master meta.
func (m *MasterMeta) UpdateStateValues() ormModel.KeyValueMap {
	return ormModel.KeyValueMap{
		"state": m.State,
	}
}

// UpdateErrorValues is used to generate orm value map when job master meets error and records it.
func (m *MasterMeta) UpdateErrorValues() ormModel.KeyValueMap {
	return ormModel.KeyValueMap{
		"error_message": m.ErrorMsg,
	}
}

// ExitValues is used to generate orm value map when job master exits.
func (m *MasterMeta) ExitValues() ormModel.KeyValueMap {
	return ormModel.KeyValueMap{
		"state":         m.State,
		"error_message": m.ErrorMsg,
		"detail":        m.Detail,
	}
}

// MasterUpdateColumns is used in gorm update
// TODO: using reflect to generate it more generally
// related to some implement of gorm
var MasterUpdateColumns = []string{
	"updated_at",
	"project_id",
	"id",
	"type",
	"state",
	"node_id",
	"address",
	"epoch",
	"config",
	"error_message",
	"detail",
	"ext",
}
