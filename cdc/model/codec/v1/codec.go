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

//go:generate msgp

package model

import (
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/rowcodec"
	types "github.com/pingcap/tiflow/cdc/model"
)

// TableName represents name of a table, includes table name and schema name.
type TableName struct {
	Schema      string `toml:"db-name" json:"db-name" msg:"db-name"`
	Table       string `toml:"tbl-name" json:"tbl-name" msg:"tbl-name"`
	TableID     int64  `toml:"tbl-id" json:"tbl-id" msg:"tbl-id"`
	IsPartition bool   `toml:"is-partition" json:"is-partition" msg:"is-partition"`
}

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	StartTs  uint64 `json:"start-ts" msg:"start-ts"`
	CommitTs uint64 `json:"commit-ts" msg:"commit-ts"`

	RowID int64 `json:"row-id" msg:"-"` // Deprecated. It is empty when the RowID comes from clustered index table.

	Table     *TableName         `json:"table" msg:"table"`
	ColInfos  []rowcodec.ColInfo `json:"column-infos" msg:"-"`
	TableInfo *types.TableInfo   `json:"-" msg:"-"`

	Columns      []*Column `json:"columns" msg:"-"`
	PreColumns   []*Column `json:"pre-columns" msg:"-"`
	IndexColumns [][]int   `json:"-" msg:"index-columns"`

	// ApproximateDataSize is the approximate size of protobuf binary
	// representation of this event.
	ApproximateDataSize int64 `json:"-" msg:"-"`

	// SplitTxn marks this RowChangedEvent as the first line of a new txn.
	SplitTxn bool `json:"-" msg:"-"`
	// ReplicatingTs is ts when a table starts replicating events to downstream.
	ReplicatingTs types.Ts `json:"-" msg:"-"`
}

// Column represents a column value in row changed event
type Column struct {
	Name    string               `json:"name" msg:"name"`
	Type    byte                 `json:"type" msg:"type"`
	Charset string               `json:"charset" msg:"charset"`
	Flag    types.ColumnFlagType `json:"flag" msg:"-"`
	Value   interface{}          `json:"value" msg:"value"`
	Default interface{}          `json:"default" msg:"-"`

	// ApproximateBytes is approximate bytes consumed by the column.
	ApproximateBytes int `json:"-"`
}

// DDLEvent stores DDL event
type DDLEvent struct {
	StartTs      uint64           `msg:"start-ts"`
	CommitTs     uint64           `msg:"commit-ts"`
	Query        string           `msg:"query"`
	TableInfo    *types.TableInfo `msg:"-"`
	PreTableInfo *types.TableInfo `msg:"-"`
	Type         model.ActionType `msg:"-"`
	Done         bool             `msg:"-"`
}

type RedoLog struct {
	RedoRow *RedoRowChangedEvent `msg:"row"`
	RedoDDL *RedoDDLEvent        `msg:"ddl"`
	Type    RedoLogType          `msg:"type"`
}

// RedoLogType is just like types.RedoLogType.
type RedoLogType int

// RedoRowChangedEvent represents the DML event used in RedoLog
type RedoRowChangedEvent struct {
	Row        *RowChangedEvent `msg:"row"`
	PreColumns []*RedoColumn    `msg:"pre-columns"`
	Columns    []*RedoColumn    `msg:"columns"`
}

// RedoColumn stores Column change
type RedoColumn struct {
	Column *Column `msg:"column"`
	Flag   uint64  `msg:"flag"`
}

// RedoDDLEvent represents DDL event used in redo log persistent
type RedoDDLEvent struct {
	DDL  *DDLEvent `msg:"ddl"`
	Type byte      `msg:"type"`
}
