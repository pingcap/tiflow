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
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const defaultWorkerCount = 16
const defaultMaxTxnRow = 256

var (
	printStatusInterval = 30 * time.Second
)

type mysqlSink struct {
	db               *sql.DB
	globalResolvedTs uint64
	sinkResolvedTs   uint64
	checkpointTs     uint64
	params           params

	filter *util.Filter

	globalForwardCh chan struct{}

	unresolvedRowsMu sync.Mutex
	unresolvedRows   map[string][]*model.RowChangedEvent

	count int64
}

func (s *mysqlSink) EmitResolvedEvent(ctx context.Context, ts uint64) error {
	atomic.StoreUint64(&s.globalResolvedTs, ts)
	select {
	case s.globalForwardCh <- struct{}{}:
	default:
	}
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
		if s.filter.ShouldIgnoreEvent(row.Ts, row.Schema, row.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.Ts))
			continue
		}
		key := util.QuoteSchema(row.Schema, row.Table)
		s.unresolvedRows[key] = append(s.unresolvedRows[key], row)
	}
	atomic.StoreUint64(&s.sinkResolvedTs, resolvedTs)
	return nil
}

func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if s.filter.ShouldIgnoreEvent(ddl.Ts, ddl.Schema, ddl.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("ts", ddl.Ts),
		)
		return nil
	}
	err := s.execDDLWithMaxRetries(ctx, ddl, 5)
	return errors.Trace(err)
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent, maxRetries uint64) error {
	return retry.Run(func() error {
		err := s.execDDL(ctx, ddl)
		if isIgnorableDDLError(err) {
			log.Info("execute DDL failed, but error can be ignored", zap.String("query", ddl.Query), zap.Error(err))
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

	atomic.AddInt64(&s.count, 1)
	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query))
	return nil
}

func (s *mysqlSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&s.checkpointTs)
}

func (s *mysqlSink) Run(ctx context.Context) error {
	if util.IsOwnerFromCtx(ctx) {
		<-ctx.Done()
		return ctx.Err()
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-s.globalForwardCh:
			globalResolvedTs := atomic.LoadUint64(&s.globalResolvedTs)
			if globalResolvedTs == atomic.LoadUint64(&s.checkpointTs) {
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

			if err := s.concurrentExec(ctx, resolvedRowsMap); err != nil {
				return errors.Trace(err)
			}
			atomic.StoreUint64(&s.checkpointTs, globalResolvedTs)
		}
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

type params struct {
	workerCount  int
	maxTxnRow    int
	changefeedID string
	captureID    string
}

var defaultParams = params{
	workerCount: defaultWorkerCount,
	maxTxnRow:   defaultMaxTxnRow,
}

func configureSinkURI(dsnCfg *dmysql.Config) (string, error) {
	dsnCfg.Loc = time.UTC
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.Params["time_zone"] = "UTC"
	return dsnCfg.FormatDSN(), nil
}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(sinkURI *url.URL, dsn *dmysql.Config, filter *util.Filter, opts map[string]string) (Sink, error) {
	var db *sql.DB
	params := defaultParams

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		params.captureID = cid
	}

	switch {
	case sinkURI != nil:
		scheme := strings.ToLower(sinkURI.Scheme)
		if scheme != "mysql" && scheme != "tidb" {
			return nil, errors.New("can create mysql sink with unsupported scheme")
		}
		s := sinkURI.Query().Get("worker-count")
		if s != "" {
			c, err := strconv.Atoi(s)
			if err != nil {
				return nil, errors.Trace(err)
			}
			params.workerCount = c
		}
		s = sinkURI.Query().Get("max-txn-row")
		if s != "" {
			c, err := strconv.Atoi(s)
			if err != nil {
				return nil, errors.Trace(err)
			}
			params.maxTxnRow = c
		}
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

		// Assume all the timestamp type is in the UTC zone when passing into mysql sink.
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/?interpolateParams=true&multiStatements=true&time_zone=UTC", username,
			password, sinkURI.Hostname(), port)
		var err error
		db, err = sql.Open("mysql", dsn)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case dsn != nil:
		dsnStr, err := configureSinkURI(dsn)
		if err != nil {
			return nil, errors.Trace(err)
		}
		db, err = sql.Open("mysql", dsnStr)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	sink := &mysqlSink{
		db:              db,
		unresolvedRows:  make(map[string][]*model.RowChangedEvent),
		params:          params,
		filter:          filter,
		globalForwardCh: make(chan struct{}, 1),
	}

	sink.db.SetMaxIdleConns(params.workerCount)
	sink.db.SetMaxOpenConns(params.workerCount)

	return sink, nil
}

func (s *mysqlSink) concurrentExec(ctx context.Context, rowGroups map[string][]*model.RowChangedEvent) error {
	jobs := make(chan []*model.RowChangedEvent, len(rowGroups))
	for _, dmls := range rowGroups {
		jobs <- dmls
	}
	close(jobs)

	nWorkers := s.params.workerCount
	if nWorkers == 0 {
		nWorkers = defaultParams.workerCount
	}

	if len(rowGroups) < nWorkers {
		nWorkers = len(rowGroups)
	}
	eg, _ := errgroup.WithContext(ctx)
	for i := 0; i < nWorkers; i++ {
		eg.Go(func() error {
			for rows := range jobs {
				err := rowLimitIterator(rows, s.params.maxTxnRow,
					func(rows []*model.RowChangedEvent) error {
						// TODO: Add retry
						return errors.Trace(s.execDMLs(ctx, rows))
					})
				if err != nil {
					return errors.Trace(err)
				}
			}
			return nil
		})
	}
	return eg.Wait()
}

func rowLimitIterator(rows []*model.RowChangedEvent, maxTxnRow int, fn func([]*model.RowChangedEvent) error) error {
	start := 0
	end := maxTxnRow
	for end < len(rows) {
		lastTs := rows[end-1].Ts
		for ; end < len(rows); end++ {
			if lastTs < rows[end].Ts {
				break
			}
		}
		if err := fn(rows[start:end]); err != nil {
			return errors.Trace(err)
		}
		start = end
		end += maxTxnRow
	}
	if start < len(rows) {
		if err := fn(rows[start:]); err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) PrintStatus(ctx context.Context) error {
	lastTime := time.Now()
	var lastCount int64
	timer := time.NewTicker(printStatusInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - lastTime.Unix()
			total := atomic.LoadInt64(&s.count)
			count := total - lastCount
			qps := int64(0)
			if seconds > 0 {
				qps = count / seconds
			}
			lastCount = total
			lastTime = now
			log.Info("mysql sink replication status",
				zap.String("changefeed", s.params.changefeedID),
				zap.Int64("count", count),
				zap.Int64("qps", qps))
		}
	}
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
		if row.Delete {
			query, args, err = s.prepareDelete(row.Schema, row.Table, row.Columns)
		} else {
			query, args, err = s.prepareReplace(row.Schema, row.Table, row.Columns)
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
	execTxnHistogram.WithLabelValues(s.params.captureID, s.params.changefeedID).Observe(time.Since(startTime).Seconds())
	execBatchHistogram.WithLabelValues(s.params.captureID, s.params.changefeedID).Observe(float64(len(rows)))
	atomic.AddInt64(&s.count, int64(len(rows)))
	log.Debug("Exec Rows succeeded", zap.Int("num of Rows", len(rows)))
	return nil
}

func (s *mysqlSink) prepareReplace(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
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

func (s *mysqlSink) prepareDelete(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
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

func whereSlice(cols map[string]*model.Column) (colNames []string, args []interface{}) {
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
