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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultWorkerCount     = 16
	defaultMaxTxnRow       = 256
	defaultDMLMaxRetryTime = 20
	defaultDDLMaxRetryTime = 20
)

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

	count uint64

	metricExecTxnHis   prometheus.Observer
	metricExecBatchHis prometheus.Observer
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
	if resolvedTs != 0 {
		atomic.StoreUint64(&s.sinkResolvedTs, resolvedTs)
	}
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
	err := s.execDDLWithMaxRetries(ctx, ddl, defaultDDLMaxRetryTime)
	return errors.Trace(err)
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent, maxRetries uint64) error {
	return retry.Run(500*time.Millisecond, maxRetries,
		func() error {
			err := s.execDDL(ctx, ddl)
			if isIgnorableDDLError(err) {
				log.Info("execute DDL failed, but error can be ignored", zap.String("query", ddl.Query), zap.Error(err))
				return nil
			}
			if errors.Cause(err) == context.Canceled {
				return backoff.Permanent(err)
			}
			if err != nil {
				log.Warn("execute DDL with error, retry later", zap.String("query", ddl.Query), zap.Error(err))
			}
			return err
		})
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

	atomic.AddUint64(&s.count, 1)
	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query))
	return nil
}

func (s *mysqlSink) CheckpointTs() uint64 {
	return atomic.LoadUint64(&s.checkpointTs)
}

func (s *mysqlSink) Count() uint64 {
	return atomic.LoadUint64(&s.count)
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

func configureSinkURI(dsnCfg *dmysql.Config, tz *time.Location) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	dsnCfg.Params["time_zone"] = fmt.Sprintf(`"%s"`, tz.String())
	return dsnCfg.FormatDSN(), nil
}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(ctx context.Context, sinkURI *url.URL, dsn *dmysql.Config, filter *util.Filter, opts map[string]string) (Sink, error) {
	var db *sql.DB
	params := defaultParams

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		params.captureID = cid
	}
	tz := util.TimezoneFromCtx(ctx)

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

		dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/", username, password, sinkURI.Hostname(), port)
		dsn, err := dmysql.ParseDSN(dsnStr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dsnStr, err = configureSinkURI(dsn, tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
		db, err = sql.Open("mysql", dsnStr)
		if err != nil {
			return nil, errors.Annotatef(err, "Open database connection failed, dsn: %s", dsnStr)
		}
		log.Info("Start mysql sink", zap.String("dsn", dsnStr))
	case dsn != nil:
		dsnStr, err := configureSinkURI(dsn, tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
		db, err = sql.Open("mysql", dsnStr)
		if err != nil {
			return nil, errors.Annotatef(err, "Open database connection failed, dsn: %s", dsnStr)
		}
		log.Info("Start mysql sink", zap.String("dsn", dsnStr))
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

	sink.metricExecTxnHis = execTxnHistogram.WithLabelValues(params.captureID, params.changefeedID)
	sink.metricExecBatchHis = execBatchHistogram.WithLabelValues(params.captureID, params.changefeedID)

	return sink, nil
}

func (s *mysqlSink) concurrentExec(ctx context.Context, rowGroups map[string][]*model.RowChangedEvent) error {
	return concurrentExec(ctx, rowGroups, s.params.workerCount, s.params.maxTxnRow, s.execDMLs)
}

func concurrentExec(ctx context.Context, rowGroups map[string][]*model.RowChangedEvent, nWorkers, maxTxnRow int, execDMLs func(context.Context, []*model.RowChangedEvent) error) error {
	if nWorkers == 0 {
		nWorkers = defaultParams.workerCount
	}

	var workerWg, jobWg sync.WaitGroup
	errCh := make(chan error, 1)
	rowChs := make([]chan *model.RowChangedEvent, 0, nWorkers)
	for i := 0; i < nWorkers; i++ {
		rowCh := make(chan *model.RowChangedEvent, 1024)
		worker := mysqlSinkWorker{
			rowCh:     rowCh,
			errCh:     errCh,
			workerWg:  &workerWg,
			jobWg:     &jobWg,
			maxTxnRow: maxTxnRow,
			execDMLs:  execDMLs,
		}
		workerWg.Add(1)
		go worker.run(ctx)
		rowChs = append(rowChs, rowCh)
	}
	causality := newCausality()
	rowChIdx := 0
	sendFn := func(row *model.RowChangedEvent, keys []string, idx int) {
		causality.add(keys, idx)
		jobWg.Add(1)
		rowChs[idx] <- row
	}
	for groupKey, rows := range rowGroups {
		for _, row := range rows {
			keys := row.Keys
			if len(keys) == 0 {
				keys = []string{groupKey}
			}
			if conflict, idx := causality.detectConflict(keys); conflict {
				if idx >= 0 {
					sendFn(row, keys, idx)
					continue
				}
				jobWg.Wait()
				causality.reset()
			}
			idx := rowChIdx % len(rowChs)
			sendFn(row, keys, idx)
			rowChIdx++
		}
	}
	for i := range rowChs {
		close(rowChs[i])
	}
	workerWg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

type mysqlSinkWorker struct {
	rowCh     chan *model.RowChangedEvent
	errCh     chan error
	workerWg  *sync.WaitGroup
	jobWg     *sync.WaitGroup
	maxTxnRow int
	execDMLs  func(context.Context, []*model.RowChangedEvent) error
}

func (w *mysqlSinkWorker) run(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			w.trySendErr(errors.Errorf("mysql sink concurrent execute panic, stack: %v", string(buf)))
			log.Error("mysql sink worker panic", zap.Reflect("r", r), zap.Stack("stack trace"))
		}
		w.workerWg.Done()
	}()
	var rows []*model.RowChangedEvent
	for row := range w.rowCh {
		rows = append(rows, row)
		rows = w.fetchAllPendingEvent(rows)
		err := w.execDMLs(ctx, rows)
		if err != nil {
			w.trySendErr(err)
		}

		// clean cache to avoid memory leak.
		for i := range rows {
			w.jobWg.Done()
			rows[i] = nil
		}
		rows = rows[:0]
	}
}

func (w *mysqlSinkWorker) trySendErr(err error) {
	select {
	case w.errCh <- err:
	default:
		return
	}
}

func (w *mysqlSinkWorker) fetchAllPendingEvent(rows []*model.RowChangedEvent) []*model.RowChangedEvent {
	for len(rows) < w.maxTxnRow {
		select {
		case row, ok := <-w.rowCh:
			if !ok {
				return rows
			}
			rows = append(rows, row)
		default:
			return rows
		}
	}
	return rows
}

func (s *mysqlSink) Close() error {
	return nil
}

func (s *mysqlSink) PrintStatus(ctx context.Context) error {
	lastTime := time.Now()
	var lastCount uint64
	timer := time.NewTicker(printStatusInterval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - lastTime.Unix()
			total := atomic.LoadUint64(&s.count)
			count := total - lastCount
			qps := uint64(0)
			if seconds > 0 {
				qps = count / uint64(seconds)
			}
			lastCount = total
			lastTime = now
			log.Info("mysql sink replication status",
				zap.String("changefeed", s.params.changefeedID),
				zap.Uint64("count", count),
				zap.Uint64("qps", qps))
		}
	}
}

func (s *mysqlSink) execDMLWithMaxRetries(ctx context.Context, sqls []string, values [][]interface{}, maxRetries uint64) error {
	checkTxnErr := func(err error) error {
		if errors.Cause(err) == context.Canceled {
			return backoff.Permanent(err)
		}
		log.Warn("execute DMLs with error, retry later", zap.Error(err))
		return err
	}
	return retry.Run(500*time.Millisecond, maxRetries,
		func() error {
			failpoint.Inject("MySQLSinkTxnRandomError", func() {
				failpoint.Return(checkTxnErr(errors.Trace(dmysql.ErrInvalidConn)))
			})
			startTime := time.Now()
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return checkTxnErr(errors.Trace(err))
			}
			for i, query := range sqls {
				args := values[i]
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					return checkTxnErr(errors.Trace(err))
				}
				log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
			}
			if err = tx.Commit(); err != nil {
				return checkTxnErr(errors.Trace(err))
			}
			s.metricExecTxnHis.Observe(time.Since(startTime).Seconds())
			s.metricExecBatchHis.Observe(float64(len(sqls)))
			atomic.AddUint64(&s.count, uint64(len(sqls)))
			log.Debug("Exec Rows succeeded", zap.String("changefeed", s.params.changefeedID), zap.Int("num of Rows", len(sqls)))
			return nil
		},
	)
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent) ([]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
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
			return nil, nil, errors.Trace(err)
		}
		sqls = append(sqls, query)
		values = append(values, args)
	}
	return sqls, values, nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent) error {
	sqls, values, err := s.prepareDMLs(rows)
	if err != nil {
		return errors.Trace(err)
	}
	return errors.Trace(s.execDMLWithMaxRetries(ctx, sqls, values, defaultDMLMaxRetryTime))
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
		if col.WhereHandle == nil || !*col.WhereHandle {
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
