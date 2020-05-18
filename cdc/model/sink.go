// Copyright 2020 PingCAP, Inc.
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
	"github.com/pingcap/parser/model"
)

// MqMessageType is the type of message
type MqMessageType int

const (
	// MqMessageTypeUnknow is unknown type of message key
	MqMessageTypeUnknow MqMessageType = iota
	// MqMessageTypeRow is row type of message key
	MqMessageTypeRow
	// MqMessageTypeDDL is ddl type of message key
	MqMessageTypeDDL
	// MqMessageTypeResolved is resolved type of message key
	MqMessageTypeResolved
)

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema string `json:"shema"`
	Table  string `json:"table"`
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	StartTs  uint64 `json:"start-ts"`
	CommitTs uint64 `json:"commit-ts"`

	RowID int64 `json:"row-id"`

	Table *TableName `json:"table"`

	Delete bool `json:"delete"`

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string             `json:"indie-mark-col"`
	Columns      map[string]*Column `json:"columns"`
	Keys         []string           `json:"keys"`
}

// Column represents a column value in row changed event
type Column struct {
	Type        byte        `json:"t"`
	WhereHandle *bool       `json:"h,omitempty"`
	Value       interface{} `json:"v"`
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Query  string
	Type   model.ActionType
}

// FromJob fills the values of DDLEvent from DDL job
func (e *DDLEvent) FromJob(job *model.Job) {
	var tableName string
	if job.BinlogInfo.TableInfo != nil {
		tableName = job.BinlogInfo.TableInfo.Name.O
	}
	e.Ts = job.BinlogInfo.FinishedTS
	e.Query = job.Query
	e.Schema = job.SchemaName
	e.Table = tableName
	e.Type = job.Type
}
