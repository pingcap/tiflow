// Copyright 2022 PingCAP, Inc.
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

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	lru "github.com/hashicorp/golang-lru"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/dmlsink"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	"github.com/pingcap/tiflow/cdc/sink/metrics/txn"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Max interval for flushing transactions to the downstream.
	maxFlushInterval = 10 * time.Millisecond

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second

	defaultDMLMaxRetry uint64 = 8
)

type mysqlBackend struct {
	workerID    int
	changefeed  string
	db          *sql.DB
	cfg         *pmysql.Config
	dmlMaxRetry uint64

	events []*dmlsink.TxnCallbackableEvent
	rows   int

	statistics                    *metrics.Statistics
	metricTxnSinkDMLBatchCommit   prometheus.Observer
	metricTxnSinkDMLBatchCallback prometheus.Observer
	// implement stmtCache to improve performance, especially when the downstream is TiDB
	stmtCache *lru.Cache
	// Indicate if the CachePrepStmts should be enabled or not
	cachePrepStmts bool
}

// NewMySQLBackends creates a new MySQL sink using schema storage
func NewMySQLBackends(
	ctx context.Context,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	dbConnFactory pmysql.Factory,
	statistics *metrics.Statistics,
) ([]*mysqlBackend, error) {
	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	changefeed := fmt.Sprintf("%s.%s", changefeedID.Namespace, changefeedID.ID)

	cfg := pmysql.NewConfig()
	err := cfg.Apply(ctx, changefeedID, sinkURI, replicaConfig)
	if err != nil {
		return nil, err
	}

	dsnStr, err := pmysql.GenerateDSN(ctx, sinkURI, cfg, dbConnFactory)
	if err != nil {
		return nil, err
	}

	db, err := dbConnFactory(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	cfg.IsTiDB, err = pmysql.CheckIsTiDB(ctx, db)
	if err != nil {
		return nil, err
	}

	// By default, cache-prep-stmts=true, an LRU cache is used for prepared statements,
	// two connections are required to process a transaction.
	// The first connection is held in the tx variable, which is used to manage the transaction.
	// The second connection is requested through a call to s.db.Prepare
	// in case of a cache miss for the statement query.
	// The connection pool for CDC is configured with a static size, equal to the number of workers.
	// CDC may hang at the "Get Connection" call is due to the limited size of the connection pool.
	// When the connection pool is small,
	// the chance of all connections being active at the same time increases,
	// leading to exhaustion of available connections and a hang at the "Get Connection" call.
	// This issue is less likely to occur when the connection pool is larger,
	// as there are more connections available for use.
	// Adding an extra connection to the connection pool solves the connection exhaustion issue.
	db.SetMaxIdleConns(cfg.WorkerCount + 1)
	db.SetMaxOpenConns(cfg.WorkerCount + 1)

	// Inherit the default value of the prepared statement cache from the SinkURI Options
	cachePrepStmts := cfg.CachePrepStmts
	prepStmtCacheSize := cfg.PrepStmtCacheSize

	// query the size of the prepared statement cache on serverside
	maxPreparedStmtCount, err := pmysql.QueryMaxPreparedStmtCount(ctx, db)
	if err != nil {
		return nil, err
	}
	// if maxPreparedStmtCount == 0, it means that the prepared statement cache is disabled on serverside.
	// if maxPreparedStmtCount/(cfg.WorkerCount+1) == 0, for each single connection,
	// it means that the prepared statement cache is disabled on clientsize.
	// Because each connection can not hold at lease one prepared statement.
	if maxPreparedStmtCount == 0 || int(maxPreparedStmtCount/(cfg.WorkerCount+1)) == 0 {
		cachePrepStmts = false
		maxPreparedStmtCount = 1
	}
	// if maxPreparedStmtCount/(cfg.WorkerCount+1) < prepStmtCacheSize,
	// it means that the prepared statement cache is too large on clientsize.
	// adjust the size of the prepared statement cache on clientsize.
	// to avoid error `Can't create more than max_prepared_stmt_count statements`
	if int(maxPreparedStmtCount/(cfg.WorkerCount+1)) < prepStmtCacheSize {
		prepStmtCacheSize = int(maxPreparedStmtCount / (cfg.WorkerCount + 1))
	}
	stmtCache, err := lru.NewWithEvict(prepStmtCacheSize, func(key, value interface{}) {
		stmt := value.(*sql.Stmt)
		stmt.Close()
	})
	if err != nil {
		return nil, err
	}

	backends := make([]*mysqlBackend, 0, cfg.WorkerCount)
	for i := 0; i < cfg.WorkerCount; i++ {
		backends = append(backends, &mysqlBackend{
			workerID:    i,
			changefeed:  changefeed,
			db:          db,
			cfg:         cfg,
			dmlMaxRetry: defaultDMLMaxRetry,
			statistics:  statistics,

			metricTxnSinkDMLBatchCommit:   txn.SinkDMLBatchCommit.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
			metricTxnSinkDMLBatchCallback: txn.SinkDMLBatchCallback.WithLabelValues(changefeedID.Namespace, changefeedID.ID),
			stmtCache:                     stmtCache,
			cachePrepStmts:                cachePrepStmts,
		})
	}

	log.Info("MySQL backends is created",
		zap.String("changefeed", changefeed),
		zap.Int("workerCount", cfg.WorkerCount),
		zap.Bool("forceReplicate", cfg.ForceReplicate),
		zap.Bool("enableOldValue", cfg.EnableOldValue))
	return backends, nil
}

// OnTxnEvent implements interface backend.
// It adds the event to the buffer, and return true if it needs flush immediately.
func (s *mysqlBackend) OnTxnEvent(event *dmlsink.TxnCallbackableEvent) (needFlush bool) {
	s.events = append(s.events, event)
	s.rows += len(event.Event.Rows)
	return event.Event.ToWaitFlush() || s.rows >= s.cfg.MaxTxnRow
}

// Flush implements interface backend.
func (s *mysqlBackend) Flush(ctx context.Context) (err error) {
	if s.rows == 0 {
		return
	}

	failpoint.Inject("MySQLSinkExecDMLError", func() {
		// Add a delay to ensure the sink worker with `MySQLSinkHangLongTime`
		// failpoint injected is executed first.
		time.Sleep(time.Second * 2)
		failpoint.Return(errors.Trace(dmysql.ErrInvalidConn))
	})

	for _, event := range s.events {
		s.statistics.ObserveRows(event.Event.Rows...)
	}

	dmls := s.prepareDMLs()
	log.Debug("prepare DMLs", zap.Any("rows", s.rows),
		zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))

	start := time.Now()
	if err := s.execDMLWithMaxRetries(ctx, dmls); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Error("execute DMLs failed", zap.Error(err))
		}
		return errors.Trace(err)
	}
	startCallback := time.Now()
	for _, callback := range dmls.callbacks {
		callback()
	}
	s.metricTxnSinkDMLBatchCommit.Observe(startCallback.Sub(start).Seconds())
	s.metricTxnSinkDMLBatchCallback.Observe(time.Since(startCallback).Seconds())

	// Be friently to GC.
	for i := 0; i < len(s.events); i++ {
		s.events[i] = nil
	}
	if cap(s.events) > 1024 {
		s.events = make([]*dmlsink.TxnCallbackableEvent, 0)
	}
	s.events = s.events[:0]
	s.rows = 0
	return
}

// Close implements interface backend.
func (s *mysqlBackend) Close() (err error) {
	if s.stmtCache != nil {
		s.stmtCache.Purge()
	}
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	return
}

// MaxFlushInterval implements interface backend.
func (s *mysqlBackend) MaxFlushInterval() time.Duration {
	return maxFlushInterval
}

type preparedDMLs struct {
	startTs   []model.Ts
	sqls      []string
	values    [][]interface{}
	callbacks []dmlsink.CallbackFunc
	rowCount  int
}

// convert2RowChanges is a helper function that convert the row change representation
// of CDC into a general one.
func convert2RowChanges(
	row *model.RowChangedEvent,
	tableInfo *timodel.TableInfo,
	changeType sqlmodel.RowChangeType,
) *sqlmodel.RowChange {
	preValues := make([]interface{}, 0, len(row.PreColumns))
	for _, col := range row.PreColumns {
		if col == nil {
			// will not use this value, just append a dummy value
			preValues = append(preValues, "omitted value")
			continue
		}
		preValues = append(preValues, col.Value)
	}

	postValues := make([]interface{}, 0, len(row.Columns))
	for _, col := range row.Columns {
		if col == nil {
			postValues = append(postValues, "omitted value")
			continue
		}
		postValues = append(postValues, col.Value)
	}

	var res *sqlmodel.RowChange

	switch changeType {
	case sqlmodel.RowChangeInsert:
		res = sqlmodel.NewRowChange(
			row.Table,
			nil,
			nil,
			postValues,
			tableInfo,
			nil, nil)
	case sqlmodel.RowChangeUpdate:
		res = sqlmodel.NewRowChange(
			row.Table,
			nil,
			preValues,
			postValues,
			tableInfo,
			nil, nil)
	case sqlmodel.RowChangeDelete:
		res = sqlmodel.NewRowChange(
			row.Table,
			nil,
			preValues,
			nil,
			tableInfo,
			nil, nil)
	}
	res.SetApproximateDataSize(row.ApproximateDataSize)
	return res
}

func convertBinaryToString(row *model.RowChangedEvent) {
	for i, col := range row.Columns {
		if col == nil {
			continue
		}
		if col.Charset != "" && col.Charset != charset.CharsetBin {
			colValBytes, ok := col.Value.([]byte)
			if ok {
				row.Columns[i].Value = string(colValBytes)
			}
		}
	}
}

func (s *mysqlBackend) groupRowsByType(
	event *dmlsink.TxnCallbackableEvent,
	tableInfo *timodel.TableInfo,
	spiltUpdate bool,
) (insertRows, updateRows, deleteRows [][]*sqlmodel.RowChange) {
	preAllocateSize := len(event.Event.Rows)
	if preAllocateSize > s.cfg.MaxTxnRow {
		preAllocateSize = s.cfg.MaxTxnRow
	}

	insertRow := make([]*sqlmodel.RowChange, 0, preAllocateSize)
	updateRow := make([]*sqlmodel.RowChange, 0, preAllocateSize)
	deleteRow := make([]*sqlmodel.RowChange, 0, preAllocateSize)

	for _, row := range event.Event.Rows {
		convertBinaryToString(row)

		if row.IsInsert() {
			insertRow = append(
				insertRow,
				convert2RowChanges(row, tableInfo, sqlmodel.RowChangeInsert))
			if len(insertRow) >= s.cfg.MaxTxnRow {
				insertRows = append(insertRows, insertRow)
				insertRow = make([]*sqlmodel.RowChange, 0, preAllocateSize)
			}
		}

		if row.IsDelete() {
			deleteRow = append(
				deleteRow,
				convert2RowChanges(row, tableInfo, sqlmodel.RowChangeDelete))
			if len(deleteRow) >= s.cfg.MaxTxnRow {
				deleteRows = append(deleteRows, deleteRow)
				deleteRow = make([]*sqlmodel.RowChange, 0, preAllocateSize)
			}
		}

		if row.IsUpdate() {
			if spiltUpdate {
				deleteRow = append(
					deleteRow,
					convert2RowChanges(row, tableInfo, sqlmodel.RowChangeDelete))
				if len(deleteRow) >= s.cfg.MaxTxnRow {
					deleteRows = append(deleteRows, deleteRow)
					deleteRow = make([]*sqlmodel.RowChange, 0, preAllocateSize)
				}
				insertRow = append(
					insertRow,
					convert2RowChanges(row, tableInfo, sqlmodel.RowChangeInsert))
				if len(insertRow) >= s.cfg.MaxTxnRow {
					insertRows = append(insertRows, insertRow)
					insertRow = make([]*sqlmodel.RowChange, 0, preAllocateSize)
				}
			} else {
				updateRow = append(
					updateRow,
					convert2RowChanges(row, tableInfo, sqlmodel.RowChangeUpdate))
				if len(updateRow) >= s.cfg.MaxMultiUpdateRowCount {
					updateRows = append(updateRows, updateRow)
					updateRow = make([]*sqlmodel.RowChange, 0, preAllocateSize)
				}
			}
		}
	}

	if len(insertRow) > 0 {
		insertRows = append(insertRows, insertRow)
	}
	if len(updateRow) > 0 {
		updateRows = append(updateRows, updateRow)
	}
	if len(deleteRow) > 0 {
		deleteRows = append(deleteRows, deleteRow)
	}

	return
}

func (s *mysqlBackend) batchSingleTxnDmls(
	event *dmlsink.TxnCallbackableEvent,
	tableInfo *timodel.TableInfo,
	translateToInsert bool,
) (sqls []string, values [][]interface{}) {
	insertRows, updateRows, deleteRows := s.groupRowsByType(event, tableInfo, !translateToInsert)

	if len(deleteRows) > 0 {
		for _, rows := range deleteRows {
			sql, value := sqlmodel.GenDeleteSQL(rows...)
			sqls = append(sqls, sql)
			values = append(values, value)
		}
	}

	// handle insert
	if len(insertRows) > 0 {
		for _, rows := range insertRows {
			if translateToInsert {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLInsert, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)
			} else {
				sql, value := sqlmodel.GenInsertSQL(sqlmodel.DMLReplace, rows...)
				sqls = append(sqls, sql)
				values = append(values, value)
			}
		}
	}

	// handle update
	if len(updateRows) > 0 {
		for _, rows := range updateRows {
			s, v := s.genUpdateSQL(rows...)
			sqls = append(sqls, s...)
			values = append(values, v...)
		}
	}

	return
}

func (s *mysqlBackend) genUpdateSQL(rows ...*sqlmodel.RowChange) ([]string, [][]interface{}) {
	size, count := 0, 0
	for _, r := range rows {
		size += int(r.GetApproximateDataSize())
		count++
	}
	if size < s.cfg.MaxMultiUpdateRowSize*count {
		// use multi update in one SQL
		sql, value := sqlmodel.GenUpdateSQLFast(rows...)
		return []string{sql}, [][]interface{}{value}
	}
	// each row has one independent update SQL.
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		sql, value := row.GenSQL(sqlmodel.DMLUpdate)
		sqls = append(sqls, sql)
		values = append(values, value)
	}
	return sqls, values
}

func hasHandleKey(cols []*model.Column) bool {
	for _, col := range cols {
		if col == nil {
			continue
		}
		if col.Flag.IsHandleKey() {
			return true
		}
	}
	return false
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlBackend) prepareDMLs() *preparedDMLs {
	// TODO: use a sync.Pool to reduce allocations.
	startTs := make([]uint64, 0, s.rows)
	sqls := make([]string, 0, s.rows)
	values := make([][]interface{}, 0, s.rows)
	callbacks := make([]dmlsink.CallbackFunc, 0, len(s.events))

	// translateToInsert control the update and insert behavior
	// we only translate into insert when old value is enabled and safe mode is disabled
	translateToInsert := s.cfg.EnableOldValue && !s.cfg.SafeMode

	rowCount := 0
	for _, event := range s.events {
		if len(event.Event.Rows) == 0 {
			continue
		}
		rowCount += len(event.Event.Rows)

		firstRow := event.Event.Rows[0]
		if len(startTs) == 0 || startTs[len(startTs)-1] != firstRow.StartTs {
			startTs = append(startTs, firstRow.StartTs)
		}

		// A row can be translated in to INSERT, when it was committed after
		// the table it belongs to been replicating by TiCDC, which means it must not be
		// replicated before, and there is no such row in downstream MySQL.
		translateToInsert = translateToInsert && firstRow.CommitTs > firstRow.ReplicatingTs
		log.Debug("translate to insert",
			zap.Bool("translateToInsert", translateToInsert),
			zap.Uint64("firstRowCommitTs", firstRow.CommitTs),
			zap.Uint64("firstRowReplicatingTs", firstRow.ReplicatingTs),
			zap.Bool("enableOldValue", s.cfg.EnableOldValue),
			zap.Bool("safeMode", s.cfg.SafeMode))

		if event.Callback != nil {
			callbacks = append(callbacks, event.Callback)
		}

		// Determine whether to use batch dml feature here.
		if s.cfg.BatchDMLEnable {
			tableColumns := firstRow.Columns
			if firstRow.IsDelete() {
				tableColumns = firstRow.PreColumns
			}
			// only use batch dml when the table has a handle key
			if hasHandleKey(tableColumns) {
				// TODO(dongmen): find a better way to get table info.
				tableInfo := model.BuildTiDBTableInfo(tableColumns, firstRow.IndexColumns)
				sql, value := s.batchSingleTxnDmls(event, tableInfo, translateToInsert)
				sqls = append(sqls, sql...)
				values = append(values, value...)
				continue
			}
		}

		quoteTable := firstRow.Table.QuoteString()
		for _, row := range event.Event.Rows {
			var query string
			var args []interface{}
			// If the old value is enabled, is not in safe mode and is an update event, then translate to UPDATE.
			// NOTICE: Only update events with the old value feature enabled will have both columns and preColumns.
			if translateToInsert && len(row.PreColumns) != 0 && len(row.Columns) != 0 {
				query, args = prepareUpdate(quoteTable, row.PreColumns, row.Columns, s.cfg.ForceReplicate)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
				continue
			}

			// Case for update event or delete event.
			// For update event:
			// If old value is disabled or in safe mode, update will be translated to DELETE + REPLACE SQL.
			// So we will prepare a DELETE SQL here.
			// For delete event:
			// It will be translated directly into a DELETE SQL.
			if len(row.PreColumns) != 0 {
				query, args = prepareDelete(quoteTable, row.PreColumns, s.cfg.ForceReplicate)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}

			// Case for update event or insert event.
			// For update event:
			// If old value is disabled or in safe mode, update will be translated to DELETE + REPLACE SQL.
			// So we will prepare a REPLACE SQL here.
			// For insert event:
			// It will be translated directly into a
			// INSERT(old value is enabled and not in safe mode)
			// or REPLACE(old value is disabled or in safe mode) SQL.
			if len(row.Columns) != 0 {
				query, args = prepareReplace(quoteTable, row.Columns, true /* appendPlaceHolder */, translateToInsert)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
				}
			}
		}
	}

	if len(callbacks) == 0 {
		callbacks = nil
	}

	return &preparedDMLs{
		startTs:   startTs,
		sqls:      sqls,
		values:    values,
		callbacks: callbacks,
		rowCount:  rowCount,
	}
}

func (s *mysqlBackend) execDMLWithMaxRetries(pctx context.Context, dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Panic("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}

	start := time.Now()
	return retry.Do(pctx, func() error {
		writeTimeout, _ := time.ParseDuration(s.cfg.WriteTimeout)
		writeTimeout += networkDriftDuration

		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			fmt.Printf("start to random error")
			err := logDMLTxnErr(errors.Trace(driver.ErrBadConn), start, s.changefeed, "failpoint", 0, nil)
			failpoint.Return(err)
		})
		failpoint.Inject("MySQLSinkHangLongTime", func() {
			time.Sleep(time.Hour)
		})

		err := s.statistics.RecordBatchExecution(func() (int, error) {
			tx, err := s.db.BeginTx(pctx, nil)
			if err != nil {
				return 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeed, "BEGIN", dmls.rowCount, dmls.startTs)
			}

			for i, query := range dmls.sqls {
				args := dmls.values[i]
				log.Debug("exec row", zap.Int("workerID", s.workerID),
					zap.String("sql", query), zap.Any("args", args))
				ctx, cancelFunc := context.WithTimeout(pctx, writeTimeout)
				var execError error
				if s.cachePrepStmts {
					stmt, ok := s.stmtCache.Get(query)
					if !ok {
						var err error
						stmt, err = s.db.Prepare(query)
						if err != nil {
							cancelFunc()
							return 0, errors.Trace(err)
						}

						s.stmtCache.Add(query, stmt)
					}
					//nolint:sqlclosecheck
					_, execError = tx.Stmt(stmt.(*sql.Stmt)).ExecContext(ctx, args...)
				} else {
					_, execError = tx.ExecContext(ctx, query, args...)
				}
				if execError != nil {
					err := logDMLTxnErr(
						cerror.WrapError(cerror.ErrMySQLTxnError, execError),
						start, s.changefeed, query, dmls.rowCount, dmls.startTs)
					if rbErr := tx.Rollback(); rbErr != nil {
						if errors.Cause(rbErr) != context.Canceled {
							log.Warn("failed to rollback txn", zap.Error(rbErr))
						}
					}
					cancelFunc()
					return 0, err
				}
				cancelFunc()
			}

			// we set write source for each txn,
			// so we can use it to trace the data source
			if err = s.setWriteSource(pctx, tx); err != nil {
				err := logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeed,
					fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source",
						s.cfg.SourceID),
					dmls.rowCount, dmls.startTs)
				if rbErr := tx.Rollback(); rbErr != nil {
					if errors.Cause(rbErr) != context.Canceled {
						log.Warn("failed to rollback txn", zap.Error(rbErr))
					}
				}
				return 0, err
			}

			if err = tx.Commit(); err != nil {
				return 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeed, "COMMIT", dmls.rowCount, dmls.startTs)
			}
			return dmls.rowCount, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Exec Rows succeeded",
			zap.Int("workerID", s.workerID),
			zap.String("changefeed", s.changefeed),
			zap.Int("numOfRows", dmls.rowCount))
		return nil
	}, retry.WithBackoffBaseDelay(pmysql.BackoffBaseDelay.Milliseconds()),
		retry.WithBackoffMaxDelay(pmysql.BackoffMaxDelay.Milliseconds()),
		retry.WithMaxTries(s.dmlMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

func logDMLTxnErr(
	err error, start time.Time, changefeed string,
	query string, count int, startTs []model.Ts,
) error {
	if isRetryableDMLError(err) {
		log.Warn("execute DMLs with error, retry later",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.Uint64s("startTs", startTs),
			zap.String("changefeed", changefeed))
	} else {
		log.Error("execute DMLs with error, can not retry",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.String("changefeed", changefeed))
	}
	return err
}

func isRetryableDMLError(err error) bool {
	if !cerror.IsRetryableError(err) {
		return false
	}

	errCode, ok := getSQLErrCode(err)
	if !ok {
		return true
	}

	switch errCode {
	case mysql.ErrNoSuchTable, mysql.ErrBadDB:
		return false
	}
	return true
}

func getSQLErrCode(err error) (errors.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return errors.ErrCode(mysqlErr.Number), true
}

// Only for testing.
func (s *mysqlBackend) setDMLMaxRetry(maxRetry uint64) {
	s.dmlMaxRetry = maxRetry
}

// setWriteSource sets write source for the transaction.
func (s *mysqlBackend) setWriteSource(ctx context.Context, txn *sql.Tx) error {
	// we only set write source when donwstream is TiDB
	if !s.cfg.IsTiDB {
		return nil
	}
	// downstream is TiDB, set system variables.
	// We should always try to set this variable, and ignore the error if
	// downstream does not support this variable, it is by design.
	query := fmt.Sprintf("SET SESSION %s = %d", "tidb_cdc_write_source", s.cfg.SourceID)
	_, err := txn.ExecContext(ctx, query)
	if err != nil {
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError); ok &&
			mysqlErr.Number == mysql.ErrUnknownSystemVariable {
			return nil
		}
		return err
	}
	return nil
}
