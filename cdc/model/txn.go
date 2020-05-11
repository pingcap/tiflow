package model

import (
	"github.com/pingcap/parser/model"
	"github.com/pingcap/tidb/types"
)

// RawTxn represents a complete collection of Entries that belong to the same transaction
type RawTxn struct {
	StartTs    uint64
	CRTs       uint64
	IsResolved bool
	Entries    []*RawKVEntry
}

// IsFake returns true if this RawTxn is fake txn.
func (r RawTxn) IsFake() bool {
	return len(r.Entries) == 0
}

// DMLType represents the dml type
type DMLType int

// DMLType types
const (
	UnknownDMLType DMLType = iota
	InsertDMLType
	UpdateDMLType
	DeleteDMLType
)

// DML holds the dml info
type DML struct {
	Database string
	Table    string
	Tp       DMLType
	Values   map[string]types.Datum
	// only set when Tp = UpdateDMLType
	OldValues map[string]types.Datum
}

// TableName returns the fully qualified name of the DML's table
func (dml *DML) TableName() string {
	return QuoteSchema(dml.Database, dml.Table)
}

// DDL holds the ddl info
type DDL struct {
	Database string
	Table    string
	Job      *model.Job
}

// Txn holds transaction info, an DDL or DML sequences
type Txn struct {
	// TODO: Group changes by tables to improve efficiency
	DMLs []*DML
	DDL  *DDL

	Ts uint64
}

// IsDDL returns true if it's a DDL transaction
func (t Txn) IsDDL() bool {
	return t.DDL != nil
}

// IsDML returns true if it's a DML transaction
func (t Txn) IsDML() bool {
	return len(t.DMLs) != 0
}

// IsFake returns true if it's a Fake transaction
func (t Txn) IsFake() bool {
	return !t.IsDDL() && !t.IsDML()
}
