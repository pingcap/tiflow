package model

type RowChangedEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Update map[string]Column
	Delete map[string]Column
}

type Column struct {
	Name   string
	Type   byte
	Handle bool
	Value  interface{}
}

type DDLEvent struct {
	Ts     uint64
	Schema string
	Table  string
	Query  string
}

type ResolvedEvent struct {
	Ts uint64
}
