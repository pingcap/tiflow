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
	"database/sql"
	"fmt"
	"strings"

	"github.com/pingcap/tidb/table"

	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/types"

	"github.com/pingcap/errors"

	"github.com/pingcap/parser/model"

	"github.com/cenkalti/backoff"

	_ "github.com/pingcap/tidb/types/parser_driver"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

type tableInspector interface {
	// Get returns information about the specified table
	Get(schema, table string) (*tableInfo, error)
	// Refresh invalidates any cached information about the specified table
	Refresh(schema, table string)
}

// TableInfoGetter is used to get table info by table id of TiDB
type TableInfoGetter interface {
	TableByID(id int64) (info *model.TableInfo, ok bool)
	GetTableIDByName(schema, table string) (int64, bool)
}

type mysqlSink struct {
	db           *sql.DB
	tblInspector tableInspector
	infoGetter   TableInfoGetter
}

var _ Sink = &mysqlSink{}

func (s *mysqlSink) Emit(ctx context.Context, txn Txn) error {
	if txn.IsDDL() {
		err := s.execDDLWithMaxRetries(ctx, txn.DDL, 5)
		if err == nil && isTableChanged(txn.DDL) {
			s.tblInspector.Refresh(txn.DDL.Database, txn.DDL.Table)
		}
		return err
	}
	// TODO: Add retry
	dmls, err := s.formatDMLs(txn.DMLs)
	if err != nil {
		return err
	}
	return s.execDMLs(ctx, dmls)
}

func (s *mysqlSink) EmitResolvedTimestamp(ctx context.Context, encoder Encoder, resolved uint64) error {
	return nil
}

func (s *mysqlSink) Flush(ctx context.Context) error {
	return nil
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *DDL, maxRetries uint64) error {
	retryCfg := backoff.WithMaxRetries(
		backoff.WithContext(
			backoff.NewExponentialBackOff(), ctx),
		maxRetries,
	)
	return backoff.Retry(func() error {
		// TODO: Wrap context canceled or deadline exceeded as permanent errors
		return s.execDDL(ctx, ddl)
	}, retryCfg)
}

func (s *mysqlSink) execDDL(ctx context.Context, ddl *DDL) error {
	shouldSwitchDB := len(ddl.Database) > 0 && ddl.Type != model.ActionCreateSchema

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quoteName(ddl.Database)+";")
		if err != nil {
			tx.Rollback()
			return err
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.SQL); err != nil {
		tx.Rollback()
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.SQL))
	return nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, dmls []*DML) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	for _, dml := range dmls {
		var fPrepare func(*DML) (string, []interface{}, error)
		switch dml.Tp {
		case InsertDMLType, UpdateDMLType:
			fPrepare = s.prepareReplace
		case DeleteDMLType:
			fPrepare = s.prepareDelete
		default:
			return fmt.Errorf("invalid dml type: %v", dml.Tp)
		}
		query, args, err := fPrepare(dml)
		if err != nil {
			tx.Rollback()
			return err
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			tx.Rollback()
			return err
		}
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	log.Info("Exec DML succeeded", zap.Int("num of DMLs", len(dmls)))
	return nil
}

func (s *mysqlSink) formatDMLs(dmls []*DML) ([]*DML, error) {
	result := make([]*DML, 0, len(dmls))
	for _, dml := range dmls {
		tableInfo, ok := s.getTableDefinition(dml.Database, dml.Table)
		if !ok {
			return nil, fmt.Errorf("table not found: %s.%s", dml.Database, dml.Table)
		}
		var err error
		dml.Values, err = formatValues(tableInfo, dml.Values)
		if err != nil {
			return nil, err
		}
		result = append(result, dml)
	}
	return result, nil
}

func (s *mysqlSink) getTableDefinition(schema, table string) (*model.TableInfo, bool) {
	tblID, ok := s.infoGetter.GetTableIDByName(schema, table)
	if !ok {
		return nil, false
	}
	tableInfo, ok := s.infoGetter.TableByID(tblID)
	return tableInfo, ok
}

func (s *mysqlSink) prepareReplace(dml *DML) (string, []interface{}, error) {
	info, err := s.tblInspector.Get(dml.Database, dml.Table)
	if err != nil {
		return "", nil, err
	}
	var builder strings.Builder
	cols := "(" + buildColumnList(info.columns) + ")"
	tblName := quoteSchema(dml.Database, dml.Table)
	builder.WriteString("REPLACE INTO " + tblName + cols + " VALUES ")
	builder.WriteString("(" + holderString(len(info.columns)) + ");")

	args := make([]interface{}, 0, len(info.columns))
	for _, name := range info.columns {
		val, ok := dml.Values[name]
		if !ok {
			return "", nil, fmt.Errorf("missing value for column: %s", name)
		}
		args = append(args, val.GetValue())
	}

	return builder.String(), args, nil
}

func (s *mysqlSink) prepareDelete(dml *DML) (string, []interface{}, error) {
	info, err := s.tblInspector.Get(dml.Database, dml.Table)
	if err != nil {
		return "", nil, err
	}

	var builder strings.Builder
	builder.WriteString("DELETE FROM " + dml.TableName() + " WHERE ")

	colNames, wargs := whereSlice(info, dml.Values)
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i].IsNull() {
			builder.WriteString(quoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i].GetValue())
		}
	}
	builder.WriteString(" LIMIT 1;")
	sql := builder.String()
	return sql, args, nil
}

func formatColumnValues(table *model.TableInfo, colVals map[string]types.Datum) (map[string]interface{}, error) {
	columns := writableColumns(table)

	formatted := make(map[string]interface{}, len(columns))
	for _, col := range columns {
		val, ok := colVals[col.Name.O]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		value, err := formatColVal(val, col.FieldType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		formatted[col.Name.O] = value.GetValue()
	}

	return formatted, nil
}

func formatValues(table *model.TableInfo, colVals map[string]types.Datum) (map[string]types.Datum, error) {
	columns := writableColumns(table)

	formatted := make(map[string]types.Datum, len(columns))
	for _, col := range columns {
		val, ok := colVals[col.Name.O]
		if !ok {
			val = getDefaultOrZeroValue(col)
		}

		value, err := formatColVal(val, col.FieldType)
		if err != nil {
			return nil, errors.Trace(err)
		}
		formatted[col.Name.O] = value
	}

	return formatted, nil
}

// writableColumns returns all columns which can be written. This excludes
// generated and non-public columns.
func writableColumns(table *model.TableInfo) []*model.ColumnInfo {
	cols := make([]*model.ColumnInfo, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.State == model.StatePublic && !col.IsGenerated() {
			cols = append(cols, col)
		}
	}
	return cols
}

func formatColVal(datum types.Datum, ft types.FieldType) (types.Datum, error) {
	if datum.GetValue() == nil {
		return datum, nil
	}

	switch ft.Tp {
	case mysql.TypeDate, mysql.TypeDatetime, mysql.TypeNewDate, mysql.TypeTimestamp, mysql.TypeDuration, mysql.TypeDecimal, mysql.TypeNewDecimal, mysql.TypeJSON:
		datum = types.NewDatum(fmt.Sprintf("%v", datum.GetValue()))
	case mysql.TypeEnum:
		datum = types.NewDatum(datum.GetMysqlEnum().Value)
	case mysql.TypeSet:
		datum = types.NewDatum(datum.GetMysqlSet().Value)
	case mysql.TypeBit:
		// Encode bits as integers to avoid pingcap/tidb#10988 (which also affects MySQL itself)
		val, err := datum.GetBinaryLiteral().ToInt(nil)
		if err != nil {
			return types.Datum{}, err
		}
		datum = types.NewUintDatum(val)
	}

	return datum, nil
}

func getDefaultOrZeroValue(col *model.ColumnInfo) types.Datum {
	// see https://github.com/pingcap/tidb/issues/9304
	// must use null if TiDB not write the column value when default value is null
	// and the value is null
	if !mysql.HasNotNullFlag(col.Flag) {
		return types.NewDatum(nil)
	}

	if col.GetDefaultValue() != nil {
		return types.NewDatum(col.GetDefaultValue())
	}

	if col.Tp == mysql.TypeEnum {
		// For enum type, if no default value and not null is set,
		// the default value is the first element of the enum list
		return types.NewDatum(col.FieldType.Elems[0])
	}

	return table.GetZeroValue(col)
}
func whereValues(colVals map[string]types.Datum, names []string) (values []types.Datum) {
	for _, name := range names {
		v := colVals[name]
		values = append(values, v)
	}
	return
}

func whereSlice(table *tableInfo, colVals map[string]types.Datum) (colNames []string, args []types.Datum) {
	// Try to use unique key values when available
	for _, index := range table.uniqueKeys {
		values := whereValues(colVals, index.columns)
		notAnyNil := true
		for i := 0; i < len(values); i++ {
			if values[i].IsNull() {
				notAnyNil = false
				break
			}
		}
		if notAnyNil {
			return index.columns, values
		}
	}

	// Fallback to use all columns
	return table.columns, whereValues(colVals, table.columns)
}
