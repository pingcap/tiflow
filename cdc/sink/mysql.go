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

package sink

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/ticdc/pkg/retry"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/schema"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/table"
	"github.com/pingcap/tidb/types"
	"go.uber.org/zap"
)

type tableInspector interface {
	// Get returns information about the specified table
	Get(schema, table string) (*tableInfo, error)
	// Refresh invalidates any cached information about the specified table
	Refresh(schema, table string)
}

type mysqlSink struct {
	db           *sql.DB
	tblInspector tableInspector
	infoGetter   TableInfoGetter
	ddlOnly      bool
}

var _ Sink = &mysqlSink{}

// NewMySQLSink creates a new MySQL sink
func NewMySQLSink(
	sinkURI string,
	infoGetter TableInfoGetter,
	opts map[string]string,
) (Sink, error) {
	sinkURI, err := configureSinkURI(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := sql.Open("mysql", sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	cachedInspector := newCachedInspector(db)
	sink := newMySQLSink(db, infoGetter, cachedInspector, false)
	return sink, nil
}

func configureSinkURI(sinkURI string) (string, error) {
	dsnCfg, err := dmysql.ParseDSN(sinkURI)
	if err != nil {
		return "", errors.Trace(err)
	}
	dsnCfg.Loc = time.UTC
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.Params["time_zone"] = "UTC"
	return dsnCfg.FormatDSN(), nil
}

// NewMySQLSinkUsingSchema creates a new MySQL sink
func NewMySQLSinkUsingSchema(db *sql.DB, schemaStorage *schema.Storage) Sink {
	inspector := &cachedInspector{
		db:    db,
		cache: make(map[string]*tableInfo),
		tableGetter: func(_ *sql.DB, schemaName string, tableName string) (*tableInfo, error) {
			info, err := getTableInfoFromSchemaStorage(schemaStorage, schemaName, tableName)
			return info, err
		},
	}
	return newMySQLSink(db, schemaStorage, inspector, false)
}

// NewMySQLSinkDDLOnly returns a sink that only processes DDL
func NewMySQLSinkDDLOnly(db *sql.DB) Sink {
	return newMySQLSink(db, nil, nil, true)
}

func newMySQLSink(db *sql.DB, infoGetter TableInfoGetter, tblInspector tableInspector, ddlOnly bool) Sink {
	return &mysqlSink{
		db:           db,
		infoGetter:   infoGetter,
		tblInspector: tblInspector,
		ddlOnly:      ddlOnly,
	}
}

func (s *mysqlSink) Emit(ctx context.Context, txns ...model.Txn) error {
	// TODO: Merge txns to reduce the number of transactions needed
	// TODO: Run txns concurrently
	for _, t := range txns {
		if t.IsDDL() {
			err := s.execDDLWithMaxRetries(ctx, t.DDL, 5)
			if err == nil && !s.ddlOnly && isTableChanged(t.DDL) {
				s.tblInspector.Refresh(t.DDL.Database, t.DDL.Table)
			}
			return errors.Trace(err)
		}
		if s.ddlOnly {
			return errors.New("dmls disallowed in ddl-only mode")
		}
		dmls, err := s.formatDMLs(t.DMLs)
		if err != nil {
			return errors.Trace(err)
		}
		// TODO: Add retry
		if err := s.execDMLs(ctx, dmls); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDL, maxRetries uint64) error {
	return retry.Run(func() error {
		err := s.execDDL(ctx, ddl)
		if isIgnorableDDLError(err) {
			return nil
		}
		return err
	}, maxRetries)
}

func (s *mysqlSink) execDDL(ctx context.Context, ddl *model.DDL) error {
	shouldSwitchDB := len(ddl.Database) > 0 && ddl.Job.Type != timodel.ActionCreateSchema

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+util.QuoteName(ddl.Database)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.Job.Query); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", ddl.Job.Query), zap.Error(err))
		}
		return errors.Trace(err)
	}

	if err = tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Job.Query))
	return nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, dmls []*model.DML) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	for _, dml := range dmls {
		var fPrepare func(*model.DML) (string, []interface{}, error)
		switch dml.Tp {
		case model.InsertDMLType, model.UpdateDMLType:
			fPrepare = s.prepareReplace
		case model.DeleteDMLType:
			fPrepare = s.prepareDelete
		default:
			return fmt.Errorf("invalid dml type: %v", dml.Tp)
		}
		query, args, err := fPrepare(dml)
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return errors.Trace(err)
		}
		log.Debug("exec dml", zap.String("sql", query), zap.Any("args", args))
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("sql", query), zap.Error(err))
			}
			return errors.Trace(err)
		}
	}

	if err = tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	log.Info("Exec DML succeeded", zap.Int("num of DMLs", len(dmls)))
	return nil
}

func (s *mysqlSink) formatDMLs(dmls []*model.DML) ([]*model.DML, error) {
	result := make([]*model.DML, 0, len(dmls))
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

func (s *mysqlSink) getTableDefinition(schema, table string) (*timodel.TableInfo, bool) {
	tblID, ok := s.infoGetter.GetTableIDByName(schema, table)
	if !ok {
		return nil, false
	}
	tableInfo, ok := s.infoGetter.TableByID(tblID)
	return tableInfo, ok
}

func (s *mysqlSink) prepareReplace(dml *model.DML) (string, []interface{}, error) {
	info, err := s.tblInspector.Get(dml.Database, dml.Table)
	if err != nil {
		return "", nil, err
	}
	var builder strings.Builder
	cols := "(" + buildColumnList(info.columns) + ")"
	tblName := util.QuoteSchema(dml.Database, dml.Table)
	builder.WriteString("REPLACE INTO " + tblName + cols + " VALUES ")
	builder.WriteString("(" + util.HolderString(len(info.columns)) + ");")

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

func (s *mysqlSink) prepareDelete(dml *model.DML) (string, []interface{}, error) {
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
			builder.WriteString(util.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(util.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i].GetValue())
		}
	}
	builder.WriteString(" LIMIT 1;")
	sql := builder.String()
	return sql, args, nil
}

func formatValues(table *timodel.TableInfo, colVals map[string]types.Datum) (map[string]types.Datum, error) {
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
func writableColumns(table *timodel.TableInfo) []*timodel.ColumnInfo {
	cols := make([]*timodel.ColumnInfo, 0, len(table.Columns))
	for _, col := range table.Columns {
		if col.State == timodel.StatePublic && !col.IsGenerated() {
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

func getDefaultOrZeroValue(col *timodel.ColumnInfo) types.Datum {
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

func isIgnorableDDLError(err error) bool {
	errCode, ok := getSQLErrCode(err)
	if !ok {
		return false
	}
	// we can get error code from:
	// infoschema's error definition: https://github.com/pingcap/tidb/blob/master/infoschema/infoschema.go
	// DDL's error definition: https://github.com/pingcap/tidb/blob/master/ddl/ddl.go
	// tidb/mysql error code definition: https://github.com/pingcap/tidb/blob/master/mysql/errcode.go
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(), infoschema.ErrIndexExists.Code(),
		infoschema.ErrKeyNotExists.Code(), tddl.ErrCantDropFieldOrKey.Code(), mysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

func getSQLErrCode(err error) (terror.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return terror.ErrCode(mysqlErr.Number), true
}

func buildColumnList(names []string) string {
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(util.QuoteName(name))

	}

	return b.String()
}
