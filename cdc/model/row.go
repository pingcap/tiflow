package model

import "github.com/pingcap/parser/model"

type RowChangedEvent struct {
	Ts       uint64
	Resolved bool
	Schema   string
	Table    string
	Update   map[string]Column
	Delete   map[string]Column
}

type Column struct {
	Type   byte
	Unique bool
	Value  interface{}
}

type DDLEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Query  string
	Type   model.ActionType
}
