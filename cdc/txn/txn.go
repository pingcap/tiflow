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

package txn

import (
	"context"
	"sort"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb-cdc/cdc/entry"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/schema"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

// RawTxn represents a complete collection of Entries that belong to the same transaction
type RawTxn struct {
	Ts      uint64
	Entries []*kv.RawKVEntry
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
	return util.QuoteSchema(dml.Database, dml.Table)
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

func (t Txn) IsDDL() bool {
	return t.DDL != nil
}

type ResolveTsTracker interface {
	Forward(span util.Span, ts uint64) bool
	Frontier() uint64
}

func CollectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (kv.KvOrResolved, error),
	outputFn func(context.Context, RawTxn) error,
	tracker ResolveTsTracker,
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
			// 1. Forward is called in a single thread
			// 2. The only way the global minimum resolved Ts can be forwarded is that
			// 	  the resolveTs we pass in replaces the original one
			// Thus, we can just use resolvedTs here as the new global minimum resolved Ts.
			forwarded := tracker.Forward(be.Resolved.Span, resolvedTs)
			if !forwarded {
				continue
			}
			var readyTxns []RawTxn
			for ts, entries := range entryGroups {
				if ts <= resolvedTs {
					readyTxns = append(readyTxns, RawTxn{ts, entries})
					delete(entryGroups, ts)
				}
			}
			sort.Slice(readyTxns, func(i, j int) bool {
				return readyTxns[i].Ts < readyTxns[j].Ts
			})
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return err
				}
			}
			if len(readyTxns) == 0 {
				log.Info("Forwarding fake txn", zap.Uint64("ts", resolvedTs))
				fakeTxn := RawTxn{
					Ts:      resolvedTs,
					Entries: nil,
				}
				outputFn(ctx, fakeTxn)
			}
		}
	}
}

type Mounter struct {
	schema *schema.Schema
	loc    *time.Location
}

func NewTxnMounter(schema *schema.Schema, loc *time.Location) (*Mounter, error) {
	m := &Mounter{schema: schema, loc: loc}
	return m, nil
}

func (m *Mounter) Mount(rawTxn RawTxn) (*Txn, error) {
	txn := &Txn{
		Ts: rawTxn.Ts,
	}
	var replaceDMLs, deleteDMLs []*DML
	err := m.schema.HandlePreviousDDLJobIfNeed(rawTxn.Ts)
	if err != nil {
		return nil, errors.Trace(err)
	}
	for _, raw := range rawTxn.Entries {
		kvEntry, err := entry.Unmarshal(raw)
		if err != nil {
			return nil, errors.Trace(err)
		}

		switch e := kvEntry.(type) {
		case *entry.RowKVEntry:
			dml, err := m.mountRowKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				if dml.Tp == InsertDMLType {
					replaceDMLs = append(replaceDMLs, dml)
				} else {
					deleteDMLs = append(deleteDMLs, dml)
				}
			}
		case *entry.IndexKVEntry:
			dml, err := m.mountIndexKVEntry(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			if dml != nil {
				deleteDMLs = append(deleteDMLs, dml)
			}
		case *entry.DDLJobKVEntry:
			txn.DDL, err = m.mountDDL(e)
			if err != nil {
				return nil, errors.Trace(err)
			}
			return txn, nil
		case *entry.UnknownKVEntry:
			log.Warn("Found unknown kv entry", zap.Reflect("UnknownKVEntry", e))
		}
	}
	txn.DMLs = append(deleteDMLs, replaceDMLs...)
	return txn, nil
}

func (m *Mounter) mountRowKVEntry(row *entry.RowKVEntry) (*DML, error) {
	tableInfo, tableName, handleColName, err := m.fetchTableInfo(row.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = row.Unflatten(tableInfo, m.loc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if row.Delete {
		if tableInfo.PKIsHandle {
			values := map[string]types.Datum{handleColName: types.NewIntDatum(row.RecordID)}
			return &DML{
				Database: tableName.Schema,
				Table:    tableName.Table,
				Tp:       DeleteDMLType,
				Values:   values,
			}, nil
		}
		return nil, nil
	}

	values := make(map[string]types.Datum, len(row.Row)+1)
	for index, colValue := range row.Row {
		colName := tableInfo.Columns[index-1].Name.O
		values[colName] = colValue
	}
	if tableInfo.PKIsHandle {
		values[handleColName] = types.NewIntDatum(row.RecordID)
	}
	return &DML{
		Database: tableName.Schema,
		Table:    tableName.Table,
		Tp:       InsertDMLType,
		Values:   values,
	}, nil
}

func (m *Mounter) mountIndexKVEntry(idx *entry.IndexKVEntry) (*DML, error) {
	tableInfo, tableName, _, err := m.fetchTableInfo(idx.TableID)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = idx.Unflatten(tableInfo, m.loc)
	if err != nil {
		return nil, errors.Trace(err)
	}

	indexInfo := tableInfo.Indices[idx.IndexID-1]
	if !indexInfo.Primary && !indexInfo.Unique {
		return nil, nil
	}

	values := make(map[string]types.Datum, len(idx.IndexValue))
	for i, idxCol := range indexInfo.Columns {
		values[idxCol.Name.O] = idx.IndexValue[i]
	}
	return &DML{
		Database: tableName.Schema,
		Table:    tableName.Table,
		Tp:       DeleteDMLType,
		Values:   values,
	}, nil
}

func (m *Mounter) fetchTableInfo(tableID int64) (tableInfo *model.TableInfo, tableName *schema.TableName, handleColName string, err error) {
	tableInfo, exist := m.schema.TableByID(tableID)
	if !exist {
		return nil, nil, "", errors.Errorf("can not find table, id: %d", tableID)
	}

	database, table, exist := m.schema.SchemaAndTableName(tableID)
	if !exist {
		return nil, nil, "", errors.Errorf("can not find table, id: %d", tableID)
	}
	tableName = &schema.TableName{Schema: database, Table: table}

	pkColOffset := -1
	for i, col := range tableInfo.Columns {
		if mysql.HasPriKeyFlag(col.Flag) {
			pkColOffset = i
			handleColName = tableInfo.Columns[i].Name.O
			break
		}
	}
	if tableInfo.PKIsHandle && pkColOffset == -1 {
		return nil, nil, "", errors.Errorf("this table (%d) is handled by pk, but pk column not found", tableID)
	}

	return
}

func (m *Mounter) mountDDL(jobEntry *entry.DDLJobKVEntry) (*DDL, error) {
	var databaseName, tableName string
	var err error
	getTableName := false
	//TODO support create schema and drop schema
	if jobEntry.Job.Type == model.ActionDropTable {
		databaseName, tableName, err = m.tryGetTableName(jobEntry)
		if err != nil {
			return nil, errors.Trace(err)
		}
		getTableName = true
	}

	_, _, _, err = m.schema.HandleDDL(jobEntry.Job)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if !getTableName {
		databaseName, tableName, err = m.tryGetTableName(jobEntry)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return &DDL{
		databaseName,
		tableName,
		jobEntry.Job.Query,
		jobEntry.Job.Type,
	}, nil
}

func (m *Mounter) tryGetTableName(jobHistory *entry.DDLJobKVEntry) (databaseName string, tableName string, err error) {
	if tableID := jobHistory.Job.TableID; tableID > 0 {
		var exist bool
		databaseName, tableName, exist = m.schema.SchemaAndTableName(tableID)
		if !exist {
			return "", "", errors.Errorf("can not find table, id: %d", tableID)
		}
	} else if schemaID := jobHistory.Job.SchemaID; schemaID > 0 {
		dbInfo, exist := m.schema.SchemaByID(schemaID)
		if !exist {
			return "", "", errors.Errorf("can not find schema, id: %d", schemaID)
		}
		databaseName = dbInfo.Name.O
	}
	return
}
