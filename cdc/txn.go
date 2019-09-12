// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-cdc/cdc/entry"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb/types"
)

type sqlType int

const (
	sqlDML sqlType = iota
	sqlDDL sqlType = iota
)

// RawTxn represents a complete collection of entries that belong to the same transaction
type RawTxn struct {
	ts      uint64
	entries []*kv.RawKVEntry
}

// DMLType represents the dml type
type DMLType int

// DMLType types
const (
	UnknownDMLType DMLType = iota
	InsertDMLType  DMLType = iota
	UpdateDMLType  DMLType = iota
	DeleteDMLType  DMLType = iota
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
	return quoteSchema(dml.Database, dml.Table)
}

// DDL holds the ddl info
type DDL struct {
	Database string
	Table    string
	SQL      string
	Type     model.ActionType
}

// Txn holds transaction info, an DDL or DML sequences
type Txn struct {
	// TODO: Group changes by tables to improve efficiency
	DMLs []*DML
	DDL  *DDL

	Ts uint64
}

// Txn holds transaction info, an DDL or DML sequences
type TableTxn struct {
	replaceDMLs []*DML
	deleteDMLs  []*DML
	DDL         *DDL

	Ts uint64
}

func (t Txn) IsDDL() bool {
	return t.DDL != nil
}

func collectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (BufferEntry, error),
	outputFn func(context.Context, RawTxn) error,
) error {
	entryGroups := make(map[uint64][]*kv.RawKVEntry)
	for {
		be, err := inputFn(ctx)
		if err != nil {
			return err
		}
		if be.KV != nil {
			entryGroups[be.KV.Ts] = append(entryGroups[be.KV.Ts], be.KV)
		} else if be.Resolved != nil {
			resolvedTs := be.Resolved.Timestamp
			var readyTxns []RawTxn
			for ts, entries := range entryGroups {
				if ts <= resolvedTs {
					readyTxns = append(readyTxns, RawTxn{ts, entries})
					delete(entryGroups, ts)
				}
			}
			// TODO: Handle the case when readyTsList is empty
			sort.Slice(readyTxns, func(i, j int) bool {
				return readyTxns[i].ts < readyTxns[j].ts
			})
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return err
				}
			}
		}
	}
}

type TableTxnMounter struct {
	schema      *Schema
	loc         *time.Location
	tableInfo   *model.TableInfo
	pkColOffset int
}

func NewTxnMounter(schema *Schema, tableId int64, loc *time.Location) (*TableTxnMounter, error) {
	m := &TableTxnMounter{schema: schema, loc: loc}

	tableInfo, exist := m.schema.TableByID(tableId)
	if !exist {
		return nil, errors.Errorf("can not find table, id: %d", tableId)
	}
	m.tableInfo = tableInfo
	m.pkColOffset = -1
	for i, col := range tableInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			m.pkColOffset = i
		}
	}
	return m, nil
}

func (m *TableTxnMounter) Mount(rawTxn *RawTxn) (*TableTxn, error) {
	tableTxn := &TableTxn{
		Ts: rawTxn.ts,
	}
	for _, raw := range rawTxn.entries {
		kvEntry, err := entry.Unmarshal(raw)
		if err != nil {
			return nil, errors.Trace(err)
		}
		switch e := kvEntry.(type) {
		case *entry.RowKVEntry:
			err := e.Unflatten(m.tableInfo, m.loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dml, err := m.mountRowKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				if dml.Tp == InsertDMLType {
					tableTxn.replaceDMLs = append(tableTxn.replaceDMLs, dml)
				} else {
					tableTxn.deleteDMLs = append(tableTxn.deleteDMLs, dml)
				}
			}
		case *entry.IndexKVEntry:
			err := e.Unflatten(m.tableInfo, m.loc)
			if err != nil {
				return nil, errors.Trace(err)
			}
			dml, err := m.mountIndexKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				tableTxn.deleteDMLs = append(tableTxn.deleteDMLs, dml)
			}
		case *entry.DDLJobHistoryKVEntry:
			//return m.mountDDL(e)
		}
	}
	return tableTxn, nil
}

func (m *TableTxnMounter) mountRowKVEntry(row *entry.RowKVEntry) (*DML, error) {
	if row.Delete {
		if m.tableInfo.PKIsHandle {
			databaseName, tableName, exist := m.schema.SchemaAndTableName(row.TableId)
			if !exist {
				return nil, errors.Errorf("can not find table, id: %d", row.TableId)
			}
			pkColName := m.tableInfo.Columns[m.pkColOffset].Name.O
			values := map[string]types.Datum{pkColName: types.NewIntDatum(row.RecordId)}
			return &DML{
				Database: databaseName,
				Table:    tableName,
				Tp:       DeleteDMLType,
				Values:   values,
			}, nil
		}
		return nil, nil
	}

	values := make(map[string]types.Datum, len(row.Row))
	for index, colValue := range row.Row {
		colName := m.tableInfo.Columns[index-1].Name.O
		values[colName] = colValue
	}
	if m.tableInfo.PKIsHandle {
		pkColName := m.tableInfo.Columns[m.pkColOffset].Name.O
		values[pkColName] = types.NewIntDatum(row.RecordId)
	}
	databaseName, tableName, exist := m.schema.SchemaAndTableName(row.TableId)
	if !exist {
		return nil, errors.Errorf("can not find table, id: %d", row.TableId)
	}
	return &DML{
		Database: databaseName,
		Table:    tableName,
		Tp:       InsertDMLType,
		Values:   values,
	}, nil
}

func (m *TableTxnMounter) mountIndexKVEntry(idx *entry.IndexKVEntry) (*DML, error) {

	indexInfo := m.tableInfo.Indices[idx.IndexId-1]
	if !indexInfo.Primary && !indexInfo.Unique {
		return nil, nil
	}
	values := make(map[string]types.Datum, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		colName := m.tableInfo.Columns[idxCol.Offset].Name.O
		values[colName] = idx.IndexValue[i]
	}
	databaseName, tableName, exist := m.schema.SchemaAndTableName(idx.TableId)
	if !exist {
		return nil, errors.Errorf("can not find table, id: %d", idx.TableId)
	}
	return &DML{
		Database: databaseName,
		Table:    tableName,
		Tp:       DeleteDMLType,
		Values:   values,
	}, nil
}
