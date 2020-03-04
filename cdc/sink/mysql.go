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
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/pkg/retry"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"go.uber.org/zap"
)

const dryRunOpt = "_dry-run"

type mysqlSink struct {
	db               *sql.DB
	globalResolvedTs uint64
	sinkResolvedTs   uint64
	checkpointTs     uint64

	unresolvedRowsMu sync.Mutex
	unresolvedRows   map[string][]*model.RowChangedEvent
}

func (s *mysqlSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&s.globalResolvedTs, ts)
	return nil
}

func (s *mysqlSink) EmitCheckpointEvent(ctx context.Context, ts uint64) error {
	return nil
}

func (s *mysqlSink) EmitRowChangedEvent(ctx context.Context, rows ...*model.RowChangedEvent) error {
	var resolvedTs uint64
	s.unresolvedRowsMu.Lock()
	defer s.unresolvedRowsMu.Unlock()
	for _, row := range rows {
		if row.Resolved {
			resolvedTs = row.Ts
			continue
		}
		key := util.QuoteSchema(row.Schema, row.Table)
		s.unresolvedRows[key] = append(s.unresolvedRows[key], row)
	}
	atomic.StoreUint64(&s.sinkResolvedTs, resolvedTs)
	return nil
}

func checkMap(msg string, group map[string][]*model.RowChangedEvent) (minTs uint64, maxTs uint64) {
	minTs = uint64(math.MaxUint64)
	for k, rows := range group {
		var ts uint64
		for _, r := range rows {
			if minTs > r.Ts {
				minTs = r.Ts
			}
			if maxTs < r.Ts {
				maxTs = r.Ts
			}
			if r.Ts < ts {
				log.Info("checkMap ts fillback", zap.String("msg", msg), zap.Reflect("row", r))
			}
			ts = r.Ts
			if util.QuoteSchema(r.Schema, r.Table) != k {
				log.Info("checkMap ts wrong key", zap.String("msg", msg), zap.String("key", k), zap.Reflect("row", r))
			}
		}
	}
	return
}

func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	err := s.execDDLWithMaxRetries(ctx, ddl, 5)
	return errors.Trace(err)
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent, maxRetries uint64) error {
	return retry.Run(func() error {
		err := s.execDDL(ctx, ddl)
		if isIgnorableDDLError(err) {
			return nil
		}
		return err
	}, maxRetries)
}

func (s *mysqlSink) execDDL(ctx context.Context, ddl *model.DDLEvent) error {
	shouldSwitchDB := len(ddl.Schema) > 0 && ddl.Type != timodel.ActionCreateSchema

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+util.QuoteName(ddl.Schema)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return errors.Trace(err)
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", ddl.Query), zap.Error(err))
		}
		return errors.Trace(err)
	}

	if err = tx.Commit(); err != nil {
		return errors.Trace(err)
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query))
	return nil
}

func (s *mysqlSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&s.checkpointTs)
}

func (s *mysqlSink) Run(ctx context.Context) error {
	execRows := func(rowsMap map[string][]*model.RowChangedEvent) error {
		groups := make([][]*model.RowChangedEvent, 0, len(rowsMap))
		for _, rows := range rowsMap {
			groups = append(groups, rows)
		}
		return s.concurrentExec(ctx, groups)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		globalResolvedTs := atomic.LoadUint64(&s.globalResolvedTs)
		if globalResolvedTs == atomic.LoadUint64(&s.checkpointTs) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		sinkResolvedTs := atomic.LoadUint64(&s.sinkResolvedTs)
		for globalResolvedTs > sinkResolvedTs {
			time.Sleep(10 * time.Millisecond)
			sinkResolvedTs = atomic.LoadUint64(&s.sinkResolvedTs)
		}

		s.unresolvedRowsMu.Lock()
		if len(s.unresolvedRows) == 0 {
			atomic.StoreUint64(&s.checkpointTs, globalResolvedTs)
			s.unresolvedRowsMu.Unlock()
			continue
		}
		_, resolvedRowsMap := splitRowsGroup(globalResolvedTs, s.unresolvedRows)
		s.unresolvedRowsMu.Unlock()

		if len(resolvedRowsMap) == 0 {
			atomic.StoreUint64(&s.checkpointTs, globalResolvedTs)
			continue
		}

		if err := execRows(resolvedRowsMap); err != nil {
			return errors.Trace(err)
		}
		atomic.StoreUint64(&s.checkpointTs, globalResolvedTs)
	}
}

func splitRowsGroup(resolvedTs uint64, unresolvedRows map[string][]*model.RowChangedEvent) (minTs uint64, resolvedRowsMap map[string][]*model.RowChangedEvent) {
	resolvedRowsMap = make(map[string][]*model.RowChangedEvent, len(unresolvedRows))
	minTs = resolvedTs
	for key, rows := range unresolvedRows {
		i := sort.Search(len(rows), func(i int) bool {
			return rows[i].Ts > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedRows []*model.RowChangedEvent
		if i == len(rows) {
			resolvedRows = rows
			delete(unresolvedRows, key)
		} else {
			resolvedRows = make([]*model.RowChangedEvent, i)
			copy(resolvedRows, rows[:i])
			unresolvedRows[key] = rows[i:]
		}
		resolvedRowsMap[key] = resolvedRows

		if len(resolvedRows) > 0 && resolvedRows[0].Ts < minTs {
			minTs = resolvedRows[0].Ts
		}
	}
	return
}

var _ Sink = &mysqlSink{}

func configureSinkURI(sinkURI string) (string, error) {
	dsnCfg, err := dmysql.ParseDSN(sinkURI)
	if err != nil {
		return "", errors.Trace(err)
	}
	dsnCfg.Loc = time.UTC
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.Params["time_zone"] = "UTC"
	return dsnCfg.FormatDSN(), nil
}

// NewMySQLSink creates a new MySQL sink using schema storage
func NewMySQLSink(sinkURI string, opts map[string]string) (Sink, error) {
	sinkURI, err := configureSinkURI(sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := sql.Open("mysql", sinkURI)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return newMySQLSink(db), nil
}

func newMySQLSink(db *sql.DB) Sink {
	return &mysqlSink{
		db:             db,
		unresolvedRows: make(map[string][]*model.RowChangedEvent),
	}
}

func (s *mysqlSink) concurrentExec(ctx context.Context, rowGroups [][]*model.RowChangedEvent) error {
	jobs := make(chan []*model.RowChangedEvent, len(rowGroups))
	for _, dmls := range rowGroups {
		jobs <- dmls
	}
	close(jobs)

	nWorkers := 16
	if len(rowGroups) < nWorkers {
		nWorkers = len(rowGroups)
	}
	eg, _ := errgroup.WithContext(ctx)
	for i := 0; i < nWorkers; i++ {
		eg.Go(func() error {
			for rows := range jobs {
				// TODO: Add retry
				if err := s.execDMLs(ctx, rows); err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent) error {
	startTime := time.Now()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	for _, row := range rows {
		var query string
		var args []interface{}
		var err error
		if len(row.Delete) > 0 {
			query, args, err = s.prepareDelete(row.Schema, row.Table, row.Delete)
		} else if len(row.Update) > 0 {
			query, args, err = s.prepareReplace(row.Schema, row.Table, row.Update)
		} else {
			continue
		}
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return errors.Trace(err)
		}
		log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("sql", query), zap.Error(err))
			}
			log.Info("exec row failed", zap.String("sql", query), zap.Any("args", args))
			return errors.Annotatef(err, "row commitTs: %d", row.Ts)
		}
	}

	if err = tx.Commit(); err != nil {
		return errors.Trace(err)
	}
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	execTxnHistogram.WithLabelValues(captureID, changefeedID).Observe(time.Since(startTime).Seconds())
	execBatchHistogram.WithLabelValues(captureID, changefeedID).Observe(float64(len(rows)))
	log.Info("Exec Rows succeeded", zap.Int("num of Rows", len(rows)))
	return nil
}

func (s *mysqlSink) prepareReplace(schema, table string, cols map[string]model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for k, v := range cols {
		columnNames = append(columnNames, k)
		args = append(args, v.Value)
	}

	colList := "(" + buildColumnList(columnNames) + ")"
	tblName := util.QuoteSchema(schema, table)
	builder.WriteString("REPLACE INTO " + tblName + colList + " VALUES ")
	builder.WriteString("(" + util.HolderString(len(columnNames)) + ");")

	return builder.String(), args, nil
}

func (s *mysqlSink) prepareDelete(schema, table string, cols map[string]model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", util.QuoteSchema(schema, table)))

	colNames, wargs := whereSlice(cols)
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(util.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(util.QuoteName(colNames[i]) + " = ?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1;")
	sql := builder.String()
	return sql, args, nil
}

func whereSlice(cols map[string]model.Column) (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for colName, col := range cols {
		if !col.WhereHandle {
			continue
		}
		colNames = append(colNames, colName)
		args = append(args, col.Value)
	}
	return
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

// splitIndependentGroups splits DMLs into independent groups.
// Two groups of DMLs are considered independent if they can be
// executed concurrently.
func splitIndependentGroups(rows []*model.RowChangedEvent) [][]*model.RowChangedEvent {
	// TODO: Detect causality of changes to achieve more fine-grain split
	tables := make(map[string][]*model.RowChangedEvent)
	for _, row := range rows {
		if row.Resolved {
			continue
		}
		tbl := row.Table
		tables[tbl] = append(tables[tbl], row)
	}
	groups := make([][]*model.RowChangedEvent, 0, len(tables))
	for _, rows := range tables {
		groups = append(groups, rows)
	}
	return groups
}
