//  Copyright 2021 PingCAP, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  See the License for the specific language governing permissions and
//  limitations under the License.

//go:generate msgp

package redo

const (
	MinSectorSize = 512

	TmpEXT  = ".tmp"
	LogEXT  = ".log"
	MetaEXT = ".meta"

	DefaultFileMode = 0o644
)

const (
	DefaultMetaFileName   = "meta"
	DefaultRowLogFileName = "row"
	DefaultDDLLogFileName = "ddl"
)

// LogType is the type of log
type LogType int

const (
	// LogTypeUnknown is unknown type of log
	LogTypeUnknown LogType = iota
	// LogTypeRow is row type of log
	LogTypeRow
	// LogTypeDDL is ddl type of log
	LogTypeDDL
)

// Log ...
type Log struct {
	Row  *RowChangedEvent `msg:"row"`
	DDL  *DDLEvent        `msg:"ddl"`
	Type LogType          `msg:"type"`
}

// LogMeta ...
type LogMeta struct {
	CheckPointTs   uint64           `msg:"checkPointTs"`
	ResolvedTs     uint64           `msg:"resolvedTs"`
	ResolvedTsList map[int64]uint64 `msg:"resolvedTsList"`
}

// ActionType ...
type ActionType byte

// DDLEvent mapping to model.DDLEvent
type DDLEvent struct {
	StartTs      uint64           `msg:"startTs"`
	CommitTs     uint64           `msg:"commitTs"`
	TableInfo    *SimpleTableInfo `msg:"tableInfo"`
	PreTableInfo *SimpleTableInfo `msg:"preTableInfo"`
	Query        string           `msg:"query"`
	Type         ActionType       `msg:"type"`
}

// ColumnInfo ...
type ColumnInfo struct {
	Name string `msg:"name"`
	Type byte   `msg:"type"`
}

// SimpleTableInfo ...
type SimpleTableInfo struct {
	// db name
	Schema string `msg:"schema"`
	// table name
	Table string `msg:"table"`
	// table ID
	TableID    int64         `msg:"tableID"`
	ColumnInfo []*ColumnInfo `msg:"columnInfo"`
}

// RowChangedEvent mapping to model.RowChangedEvent
type RowChangedEvent struct {
	StartTs          uint64     `msg:"startTs"`
	CommitTs         uint64     `msg:"commitTs"`
	Table            *TableName `msg:"table"`
	TableInfoVersion uint64     `msg:"tableInfoVersion"`
	ReplicaID        uint64     `msg:"replicaID"`
	Columns          []*Column  `msg:"columns"`
	PreColumns       []*Column  `msg:"preColumns"`
	IndexColumns     [][]int    `msg:"indexColumns"`
	ApproximateSize  int64      `msg:"approximateSize"`
}

// ColumnFlagType ...
type ColumnFlagType uint64

// Column ...
type Column struct {
	Name  string         `msg:"name"`
	Type  byte           `msg:"type"`
	Flag  ColumnFlagType `msg:"flag"`
	Value interface{}    `msg:"value"`
}

// TableName ...
type TableName struct {
	Schema      string `msg:"db-name"`
	Table       string `msg:"tbl-name"`
	TableID     int64  `msg:"tbl-id"`
	IsPartition bool   `msg:"is-partition"`
}
