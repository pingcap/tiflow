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
	"fmt"

	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"go.uber.org/zap"
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
	Schema    string `toml:"db-name" json:"db-name"`
	Table     string `toml:"tbl-name" json:"tbl-name"`
	Partition int64  `json:"partition"`
}

// String implements fmt.Stringer interface.
func (t TableName) String() string {
	return fmt.Sprintf("%s.%s", t.Schema, t.Table)
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
	StartTs  uint64
	CommitTs uint64
	Schema   string
	Table    string
	Query    string
	Type     model.ActionType
}

// FromJob fills the values of DDLEvent from DDL job
func (e *DDLEvent) FromJob(job *model.Job) {
	var tableName string
	if job.BinlogInfo.TableInfo != nil {
		tableName = job.BinlogInfo.TableInfo.Name.O
	}
	e.StartTs = job.StartTS
	e.CommitTs = job.BinlogInfo.FinishedTS
	e.Query = job.Query
	e.Schema = job.SchemaName
	e.Table = tableName
	e.Type = job.Type
}

// Txn represents a transaction which includes many row events
type Txn struct {
	StartTs   uint64
	CommitTs  uint64
	Rows      []*RowChangedEvent
	Keys      []string
	ReplicaID uint64
}

// Append adds a row changed event into Txn
func (t *Txn) Append(row *RowChangedEvent) {
	if row.StartTs != t.StartTs || row.CommitTs != t.CommitTs {
		log.Fatal("unexpected row change event",
			zap.Uint64("startTs of txn", t.StartTs),
			zap.Uint64("commitTs of txn", t.CommitTs),
			zap.Uint64("startTs of row", row.StartTs),
			zap.Uint64("startTs of row", row.CommitTs))
	}
	t.Rows = append(t.Rows, row)
	if len(row.Keys) == 0 {
		if len(t.Keys) == 0 {
			t.Keys = []string{QuoteSchema(row.Table.Schema, row.Table.Table)}
		}
	} else {
		t.Keys = append(t.Keys, row.Keys...)
	}
}
