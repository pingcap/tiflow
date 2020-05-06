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

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	Ts    uint64
	RowID int64

	Schema string
	Table  string

	Delete bool

	// if the table of this row only has one unique index(includes primary key),
	// IndieMarkCol will be set to the name of the unique index
	IndieMarkCol string
	Columns      map[string]*Column
	Keys         []string
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
