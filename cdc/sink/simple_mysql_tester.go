// Copyright 2020 PingCAP, Inc.
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
	"net/url"
	"strings"
	"sync"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/quotes"
	"go.uber.org/zap"
)

func init() {
	failpoint.Inject("SimpleMySQLSinkTester", func() {
		sinkIniterMap["simple-mysql"] = func(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL,
			filter *filter.Filter, config *config.ReplicaConfig, opts map[string]string, errCh chan error) (Sink, error) {
			return newSimpleMySQLSink(ctx, sinkURI, config)
		}
	})
}

type simpleMySQLSink struct {
	enableOldValue      bool
	enableCheckOldValue bool
	db                  *sql.DB
	rowsBuffer          []*model.RowChangedEvent
	rowsBufferLock      sync.Mutex
}

func newSimpleMySQLSink(ctx context.Context, sinkURI *url.URL, config *config.ReplicaConfig) (*simpleMySQLSink, error) {
	var db *sql.DB

	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	password, _ := sinkURI.User.Password()
	port := sinkURI.Port()
	if username == "" {
		username = "root"
	}
	if port == "" {
		port = "4000"
	}

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/?multiStatements=true", username, password, sinkURI.Hostname(), port)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}

	// create test db used for parameter detection
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	testDB, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection when configuring sink")
	}
	defer testDB.Close()

	db, err = sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "Open database connection failed")
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection")
	}

	sink := &simpleMySQLSink{
		db:             db,
		enableOldValue: config.EnableOldValue,
	}
	if strings.ToLower(sinkURI.Query().Get("check-old-value")) == "true" {
		sink.enableCheckOldValue = true
		log.Info("the old value checker is enabled")
	}
	return sink, nil
}

// EmitRowChangedEvents sends Row Changed Event to Sink
// EmitRowChangedEvents may write rows to downstream directly;
func (s *simpleMySQLSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	s.rowsBufferLock.Lock()
	defer s.rowsBufferLock.Unlock()
	s.rowsBuffer = append(s.rowsBuffer, rows...)
	return nil
}

func (s *simpleMySQLSink) executeRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	var sql string
	var args []interface{}
	if s.enableOldValue {
		for _, row := range rows {
			if len(row.PreColumns) != 0 && len(row.Columns) != 0 {
				// update
				if s.enableCheckOldValue {
					err := s.checkOldValue(ctx, row)
					if err != nil {
						return errors.Trace(err)
					}
				}
				sql, args = prepareReplace(row.Table.QuoteString(), row.Columns, true, false /* translateToInsert */)
			} else if len(row.PreColumns) == 0 {
				// insert
				sql, args = prepareReplace(row.Table.QuoteString(), row.Columns, true, false /* translateToInsert */)
			} else if len(row.Columns) == 0 {
				// delete
				if s.enableCheckOldValue {
					err := s.checkOldValue(ctx, row)
					if err != nil {
						return errors.Trace(err)
					}
				}
				sql, args = prepareDelete(row.Table.QuoteString(), row.PreColumns, true)
			}
			_, err := s.db.ExecContext(ctx, sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	} else {
		for _, row := range rows {
			if row.IsDelete() {
				sql, args = prepareDelete(row.Table.QuoteString(), row.PreColumns, true)
			} else {
				sql, args = prepareReplace(row.Table.QuoteString(), row.Columns, true, false)
			}
			_, err := s.db.ExecContext(ctx, sql, args...)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

// EmitDDLEvent sends DDL Event to Sink
// EmitDDLEvent should execute DDL to downstream synchronously
func (s *simpleMySQLSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	var sql string
	if len(ddl.TableInfo.Table) == 0 {
		sql = ddl.Query
	} else {
		sql = fmt.Sprintf("use %s;%s", ddl.TableInfo.Schema, ddl.Query)
	}
	_, err := s.db.ExecContext(ctx, sql)
	if err != nil && isIgnorableDDLError(err) {
		log.Info("execute DDL failed, but error can be ignored", zap.String("query", ddl.Query), zap.Error(err))
		return nil
	}
	return err
}

// FlushRowChangedEvents flushes each row which of commitTs less than or equal to `resolvedTs` into downstream.
// TiCDC guarantees that all of Event which of commitTs less than or equal to `resolvedTs` are sent to Sink through `EmitRowChangedEvents`
func (s *simpleMySQLSink) FlushRowChangedEvents(ctx context.Context, _ model.TableID, resolvedTs uint64) (uint64, error) {
	s.rowsBufferLock.Lock()
	defer s.rowsBufferLock.Unlock()
	newBuffer := make([]*model.RowChangedEvent, 0, len(s.rowsBuffer))
	for _, row := range s.rowsBuffer {
		if row.CommitTs <= resolvedTs {
			err := s.executeRowChangedEvents(ctx, row)
			if err != nil {
				return 0, err
			}
		} else {
			newBuffer = append(newBuffer, row)
		}
	}
	s.rowsBuffer = newBuffer
	return resolvedTs, nil
}

// EmitCheckpointTs sends CheckpointTs to Sink
// TiCDC guarantees that all Events **in the cluster** which of commitTs less than or equal `checkpointTs` are sent to downstream successfully.
func (s *simpleMySQLSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// do nothing
	return nil
}

// Close closes the Sink
func (s *simpleMySQLSink) Close(ctx context.Context) error {
	return s.db.Close()
}

func (s *simpleMySQLSink) Barrier(ctx context.Context, tableID model.TableID) error {
	return nil
}

func prepareCheckSQL(quoteTable string, cols []*model.Column) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("SELECT count(1) FROM " + quoteTable + " WHERE ")

	colNames, wargs := whereSlice(cols, true)
	if len(wargs) == 0 {
		return "", nil
	}
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1;")
	sql := builder.String()
	return sql, args
}

func (s *simpleMySQLSink) checkOldValue(ctx context.Context, row *model.RowChangedEvent) error {
	sql, args := prepareCheckSQL(row.Table.QuoteString(), row.PreColumns)
	result, err := s.db.QueryContext(ctx, sql, args...)
	if err != nil {
		return errors.Trace(err)
	}
	var count int
	if result.Next() {
		err := result.Scan(&count)
		if err != nil {
			return errors.Trace(err)
		}
	}
	if count == 0 {
		log.Error("can't pass the check, the old value of this row is not exist", zap.Any("row", row))
		return errors.New("check failed")
	}
	log.Debug("pass the old value check", zap.String("sql", sql), zap.Any("args", args), zap.Int("count", count))
	return nil
}
