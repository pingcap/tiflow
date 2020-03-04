package model

import "github.com/pingcap/parser/model"

// RowChangedEvent represents a row changed event
type RowChangedEvent struct {
	Ts       uint64
	Resolved bool
	Schema   string
	Table    string
	Update   map[string]Column
	Delete   map[string]Column
}

// Column represents a column value in row changed event
type Column struct {
	Type        byte
	WhereHandle bool
	Value       interface{}
}

// DDLEvent represents a DDL event
type DDLEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Query  string
	Type   model.ActionType
}
