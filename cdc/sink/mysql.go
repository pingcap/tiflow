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
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/config"

	"github.com/cenkalti/backoff"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/filter"
	tifilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"go.uber.org/zap"
)

const (
	defaultWorkerCount     = 16
	defaultMaxTxnRow       = 256
	defaultDMLMaxRetryTime = 8
	defaultDDLMaxRetryTime = 20
)

type mysqlSink struct {
	db           *sql.DB
	checkpointTs uint64
	params       params

	filter *filter.Filter
	cyclic *cyclic.Cyclic

	unresolvedRowsMu sync.Mutex
	unresolvedRows   map[model.TableName][]*model.RowChangedEvent

	statistics *Statistics
}

func (s *mysqlSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	s.unresolvedRowsMu.Lock()
	defer s.unresolvedRowsMu.Unlock()
	for _, row := range rows {
		if s.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("ts", row.CommitTs))
			continue
		}
		key := *row.Table
		s.unresolvedRows[key] = append(s.unresolvedRows[key], row)
	}
	return nil
}

func (s *mysqlSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	if resolvedTs <= atomic.LoadUint64(&s.checkpointTs) {
		return nil
	}

	defer s.statistics.PrintStatus()

	s.unresolvedRowsMu.Lock()
	if len(s.unresolvedRows) == 0 {
		atomic.StoreUint64(&s.checkpointTs, resolvedTs)
		s.unresolvedRowsMu.Unlock()
		return nil
	}

	_, resolvedRowsMap := splitRowsGroup(resolvedTs, s.unresolvedRows)
	s.unresolvedRowsMu.Unlock()
	if len(resolvedRowsMap) == 0 {
		atomic.StoreUint64(&s.checkpointTs, resolvedTs)
		return nil
	}

	if s.cyclic != nil {
		// Filter rows if it is origined from downstream.
		txnMap, markMap := cyclic.MapMarkRowsGroup(resolvedRowsMap)
		resolvedRowsMap = cyclic.ReduceCyclicRowsGroup(
			txnMap, markMap, s.cyclic.FilterReplicaID())
	}

	if err := s.concurrentExec(ctx, resolvedRowsMap); err != nil {
		return errors.Trace(err)
	}
	atomic.StoreUint64(&s.checkpointTs, resolvedTs)
	return nil
}

func (s *mysqlSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// do nothing
	return nil
}

func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if s.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Schema, ddl.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
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
		_, err = tx.ExecContext(ctx, "USE "+model.QuoteName(ddl.Schema)+";")
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

func splitRowsGroup(
	resolvedTs uint64, unresolvedRows map[model.TableName][]*model.RowChangedEvent,
) (minTs uint64, resolvedRowsMap map[model.TableName][][]*model.RowChangedEvent) {
	resolvedRowsMap = make(map[model.TableName][][]*model.RowChangedEvent, len(unresolvedRows))
	minTs = resolvedTs
	for key, rows := range unresolvedRows {
		i := sort.Search(len(rows), func(i int) bool {
			return rows[i].CommitTs > resolvedTs
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
		resolvedRowsMap[key] = [][]*model.RowChangedEvent{resolvedRows}

		if len(resolvedRows) > 0 && resolvedRows[0].CommitTs < minTs {
			minTs = resolvedRows[0].CommitTs
		}
	}
	return
}

// adjustSQLMode adjust sql mode according to sink config.
func (s *mysqlSink) adjustSQLMode(ctx context.Context) error {
	// Must relax sql mode to support cyclic replication, as downstream may have
	// extra columns (not null and no default value).
	if s.cyclic != nil {
		return nil
	}
	var oldMode, newMode string
	row := s.db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode;")
	err := row.Scan(&oldMode)
	if err != nil {
		return errors.Trace(err)
	}

	newMode = cyclic.RelaxSQLMode(oldMode)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SET sql_mode = '%s';", newMode))
	if err != nil {
		return errors.Trace(err)
	}
	err = rows.Close()
	return errors.Trace(err)
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

func configureSinkURI(ctx context.Context, dsnCfg *dmysql.Config, tz *time.Location) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	dsnCfg.Params["time_zone"] = fmt.Sprintf(`"%s"`, tz.String())

	testDB, err := sql.Open("mysql", dsnCfg.FormatDSN())
	if err != nil {
		return "", errors.Annotate(err, "fail to open MySQL connection when configuring sink")
	}
	defer testDB.Close()
	log.Debug("Opened connection to test whether allow_auto_random_explicit_insert is present.")

	var variableName string
	var autoRandomInsertEnabled string
	queryStr := "show session variables like 'allow_auto_random_explicit_insert';"
	err = testDB.QueryRowContext(ctx, queryStr).Scan(&variableName, &autoRandomInsertEnabled)
	if err != nil && err != sql.ErrNoRows {
		return "", errors.Annotate(err, "fail to query sink for support of auto-random")
	}

	if err == nil && autoRandomInsertEnabled == "off" {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = "1"
		log.Debug("Set allow_auto_random_explicit_insert to 1")
	}

	return dsnCfg.FormatDSN(), nil
}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(ctx context.Context, sinkURI *url.URL, dsn *dmysql.Config, filter *tifilter.Filter, opts map[string]string) (Sink, error) {
	var db *sql.DB
	params := defaultParams

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if cid, ok := opts[OptCaptureID]; ok {
		params.captureID = cid
	}
	tz := util.TimezoneFromCtx(ctx)

	var dsnStr string
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

		dsnStr = fmt.Sprintf("%s:%s@tcp(%s:%s)/", username, password, sinkURI.Hostname(), port)
		dsn, err := dmysql.ParseDSN(dsnStr)
		if err != nil {
			return nil, errors.Trace(err)
		}
		dsnStr, err = configureSinkURI(ctx, dsn, tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
	case dsn != nil:
		var err error
		dsnStr, err = configureSinkURI(ctx, dsn, tz)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, errors.Annotate(err, "Open database connection failed")
	}
	err = db.PingContext(ctx)
	if err != nil {
		return nil, errors.Annotatef(err, "fail to open MySQL connection")
	}

	log.Info("Start mysql sink")

	db.SetMaxIdleConns(params.workerCount)
	db.SetMaxOpenConns(params.workerCount)

	sink := &mysqlSink{
		db:             db,
		unresolvedRows: make(map[model.TableName][]*model.RowChangedEvent),
		params:         params,
		filter:         filter,
		statistics:     NewStatistics("mysql", opts),
	}

	if val, ok := opts[cyclic.OptCyclicConfig]; ok {
		cfg := new(config.CyclicConfig)
		err := cfg.Unmarshal([]byte(val))
		if err != nil {
			return nil, errors.Trace(err)
		}
		sink.cyclic = cyclic.NewCyclic(cfg)

		err = sink.adjustSQLMode(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	return sink, nil
}

func (s *mysqlSink) concurrentExec(ctx context.Context, rowGroups map[model.TableName][][]*model.RowChangedEvent) error {
	return concurrentExec(ctx, rowGroups, s.params.workerCount, s.params.maxTxnRow, s.execDMLs)
}

func concurrentExec(
	ctx context.Context, rowGroups map[model.TableName][][]*model.RowChangedEvent, nWorkers, maxTxnRow int,
	execDMLs func(context.Context, []*model.RowChangedEvent, int) error,
) error {
	if nWorkers == 0 {
		nWorkers = defaultParams.workerCount
	}

	var workerWg, jobWg sync.WaitGroup
	errCh := make(chan error, 1)
	rowsChs := make([]chan []*model.RowChangedEvent, 0, nWorkers)
	for i := 0; i < nWorkers; i++ {
		rowsCh := make(chan []*model.RowChangedEvent, 1024)
		worker := mysqlSinkWorker{
			rowsCh:    rowsCh,
			errCh:     errCh,
			workerWg:  &workerWg,
			jobWg:     &jobWg,
			maxTxnRow: maxTxnRow,
			bucket:    i,
			execDMLs:  execDMLs,
		}
		workerWg.Add(1)
		go worker.run(ctx)
		rowsChs = append(rowsChs, rowsCh)
	}
	causality := newCausality()
	rowsChIdx := 0
	sendFn := func(rows []*model.RowChangedEvent, keys []string, idx int) {
		causality.add(keys, idx)
		for _, r := range rowsLenLimiter(rows, maxTxnRow) {
			jobWg.Add(1)
			select {
			case <-ctx.Done():
				jobWg.Done()
			case rowsChs[idx] <- r:
			}
		}
	}
	for groupKey, multiRows := range rowGroups {
		for _, rows := range multiRows {
			rowsCopy := make([]*model.RowChangedEvent, 0, len(rows))
			rowsCopy = append(rowsCopy, rows...)
			keys := make([]string, 0, len(rows))
			for _, row := range rows {
				if len(row.Keys) == 0 {
					keys = []string{model.QuoteSchema(groupKey.Schema, groupKey.Table)}
				} else {
					keys = append(keys, row.Keys...)
				}
			}
			if conflict, idx := causality.detectConflict(keys); conflict {
				if idx >= 0 {
					sendFn(rowsCopy, keys, idx)
					continue
				}
				jobWg.Wait()
				causality.reset()
			}
			idx := rowsChIdx % len(rowsChs)
			sendFn(rowsCopy, keys, idx)
			rowsChIdx++
		}
	}
	for i := range rowsChs {
		close(rowsChs[i])
	}
	workerWg.Wait()
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func rowsLenLimiter(rows []*model.RowChangedEvent, maxLen int) [][]*model.RowChangedEvent {
	result := make([][]*model.RowChangedEvent, 0, (len(rows)/maxLen)+1)
	for start := 0; start < len(rows); {
		end := start + maxLen
		if end > len(rows) {
			end = len(rows)
		}
		result = append(result, rows[start:end])
		start = end
	}
	return result
}

type mysqlSinkWorker struct {
	rowsCh    chan []*model.RowChangedEvent
	errCh     chan error
	workerWg  *sync.WaitGroup
	jobWg     *sync.WaitGroup
	maxTxnRow int
	bucket    int
	execDMLs  func(context.Context, []*model.RowChangedEvent, int) error
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
	var multiRows [][]*model.RowChangedEvent
	for rows := range w.rowsCh {
		multiRows = append(multiRows, rows)
		multiRows = w.fetchAllPendingEvents(multiRows)
		for _, rows := range multiRows {
			err := w.execDMLs(ctx, rows, w.bucket)
			if err != nil {
				w.trySendErr(err)
			}
		}
		// clean cache to avoid memory leak.
		for i := range multiRows {
			w.jobWg.Done()
			multiRows[i] = nil
		}
		multiRows = multiRows[:0]
	}
}

func (w *mysqlSinkWorker) trySendErr(err error) {
	select {
	case w.errCh <- err:
	default:
		return
	}
}

func (w *mysqlSinkWorker) fetchAllPendingEvents(
	multiRows [][]*model.RowChangedEvent,
) [][]*model.RowChangedEvent {
	// TODO(neil) count rows instead
	for len(multiRows) < w.maxTxnRow {
		select {
		case rows, ok := <-w.rowsCh:
			if !ok {
				return multiRows
			}
			multiRows = append(multiRows, rows)
		default:
			return multiRows
		}
	}
	return multiRows
}

func (s *mysqlSink) Close() error {
	return s.db.Close()
}

func (s *mysqlSink) execDMLWithMaxRetries(
	ctx context.Context, sqls []string, values [][]interface{}, maxRetries uint64, bucket int,
) error {
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
			err := s.statistics.RecordBatchExecution(func() (int, error) {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					return 0, checkTxnErr(errors.Trace(err))
				}
				for i, query := range sqls {
					args := values[i]
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						return 0, checkTxnErr(errors.Trace(err))
					}
					log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
				}
				if err = tx.Commit(); err != nil {
					return 0, checkTxnErr(errors.Trace(err))
				}
				return len(sqls), nil
			})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("Exec Rows succeeded",
				zap.String("changefeed", s.params.changefeedID),
				zap.Int("num of Rows", len(sqls)),
				zap.Int("bucket", bucket))
			return nil
		},
	)
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent, bucket int) ([]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	markRowFound := false
	for _, row := range rows {
		var query string
		var args []interface{}
		var err error
		if s.cyclic != nil && cyclic.IsMarkTable(row.Table.Schema, row.Table.Table) {
			// Write mark table based on bucket ID and table ID.
			replicaID := cyclic.ExtractReplicaID(row)
			// Mark row's table ID is set to corresponding table ID.
			query = s.cyclic.UdpateTableCyclicMark(
				row.Table.Schema, row.Table.Table, uint64(bucket), replicaID)
			markRowFound = true
		} else if row.Delete {
			query, args, err = s.prepareDelete(row.Table.Schema, row.Table.Table, row.Columns)
		} else {
			query, args, err = s.prepareReplace(row.Table.Schema, row.Table.Table, row.Columns)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		sqls = append(sqls, query)
		values = append(values, args)
	}
	if s.cyclic != nil && !markRowFound && len(rows) > 0 {
		// Write mark table with the current replica ID.
		updateMark := s.cyclic.UdpateTableCyclicMark(
			rows[0].Table.Schema, rows[0].Table.Table, uint64(bucket), s.cyclic.ReplicaID())
		sqls = append(sqls, updateMark)
		values = append(values, nil)
	}
	return sqls, values, nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent, bucket int) error {
	sqls, values, err := s.prepareDMLs(rows, bucket)
	if err != nil {
		return errors.Trace(err)
	}
	if err := s.execDMLWithMaxRetries(ctx, sqls, values, defaultDMLMaxRetryTime, bucket); err != nil {
		ts := make([]uint64, 0, len(rows))
		for _, row := range rows {
			if len(ts) == 0 || ts[len(ts)-1] != row.CommitTs {
				ts = append(ts, row.CommitTs)
			}
		}
		log.Error("execute DMLs failed", zap.String("err", err.Error()), zap.Uint64s("ts", ts))
		return errors.Trace(err)
	}
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
	tblName := model.QuoteSchema(schema, table)
	builder.WriteString("REPLACE INTO " + tblName + colList + " VALUES ")
	builder.WriteString("(" + model.HolderString(len(columnNames)) + ");")

	return builder.String(), args, nil
}

func (s *mysqlSink) prepareDelete(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	builder.WriteString(fmt.Sprintf("DELETE FROM %s WHERE ", model.QuoteSchema(schema, table)))

	colNames, wargs := whereSlice(cols)
	args := make([]interface{}, 0, len(wargs))
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(model.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(model.QuoteName(colNames[i]) + " = ?")
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
		infoschema.ErrKeyNotExists.Code(), tddl.ErrCantDropFieldOrKey.Code(), mysql.ErrDupKeyName, mysql.ErrSameNamePartition,
		mysql.ErrDropPartitionNonExistent, mysql.ErrMultiplePriKey:
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
		b.WriteString(model.QuoteName(name))

	}

	return b.String()
}
