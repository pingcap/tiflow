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
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics/txn"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	// Max interval for flushing transactions to the downstream.
	maxFlushInterval = 10 * time.Millisecond

	defaultDMLMaxRetry uint64 = 8
)

type mysqlBackend struct {
	workerID    int
	changefeed  string
	db          *sql.DB
	cfg         *pmysql.Config
	dmlMaxRetry uint64

	events []*eventsink.TxnCallbackableEvent
	rows   int

	statistics                    *metrics.Statistics
	metricTxnSinkDMLBatchCommit   prometheus.Observer
	metricTxnSinkDMLBatchCallback prometheus.Observer
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
	db.SetMaxIdleConns(cfg.WorkerCount)
	db.SetMaxOpenConns(cfg.WorkerCount)

	err = setSystemVariable(ctx, db, replicaConfig)
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
func (s *mysqlBackend) OnTxnEvent(event *eventsink.TxnCallbackableEvent) (needFlush bool) {
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
		s.events = make([]*eventsink.TxnCallbackableEvent, 0)
	}
	s.events = s.events[:0]
	s.rows = 0
	return
}

// Close implements interface backend.
func (s *mysqlBackend) Close() (err error) {
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
	callbacks []eventsink.CallbackFunc
	rowCount  int
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlBackend) prepareDMLs() *preparedDMLs {
	// TODO: use a sync.Pool to reduce allocations.
	startTs := make([]uint64, 0, s.rows)
	sqls := make([]string, 0, s.rows)
	values := make([][]interface{}, 0, s.rows)
	callbacks := make([]eventsink.CallbackFunc, 0, len(s.events))
	replaces := make(map[string][][]interface{})

	// flush cached batch replace or insert, to keep the sequence of DMLs
	flushCacheDMLs := func() {
		if s.cfg.BatchReplaceEnabled && len(replaces) > 0 {
			replaceSqls, replaceValues := reduceReplace(replaces, s.cfg.BatchReplaceSize)
			sqls = append(sqls, replaceSqls...)
			values = append(values, replaceValues...)
			replaces = make(map[string][][]interface{})
		}
	}

	// translateToInsert control the update and insert behavior
	translateToInsert := s.cfg.EnableOldValue && !s.cfg.SafeMode
	for _, event := range s.events {
		for _, row := range event.Event.Rows {
			if !translateToInsert {
				break
			}
			// It can be translated in to INSERT, if the row is committed after
			// we starting replicating the table, which means it must not be
			// replicated before, and there is no such row in downstream MySQL.
			translateToInsert = row.CommitTs > row.ReplicatingTs
		}
	}

	rowCount := 0
	for _, event := range s.events {
		if event.Callback != nil {
			callbacks = append(callbacks, event.Callback)
		}

		for _, row := range event.Event.Rows {
			if len(startTs) == 0 || startTs[len(startTs)-1] != row.StartTs {
				startTs = append(startTs, row.StartTs)
			}

			var query string
			var args []interface{}
			quoteTable := quotes.QuoteSchema(row.Table.Schema, row.Table.Table)

			// If the old value is enabled, is not in safe mode and is an update event, then translate to UPDATE.
			// NOTICE: Only update events with the old value feature enabled will have both columns and preColumns.
			if translateToInsert && len(row.PreColumns) != 0 && len(row.Columns) != 0 {
				flushCacheDMLs()
				query, args = prepareUpdate(quoteTable, row.PreColumns, row.Columns, s.cfg.ForceReplicate)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
					rowCount++
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
				flushCacheDMLs()
				query, args = prepareDelete(quoteTable, row.PreColumns, s.cfg.ForceReplicate)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
					rowCount++
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
				if s.cfg.BatchReplaceEnabled {
					query, args = prepareReplace(quoteTable, row.Columns, false /* appendPlaceHolder */, translateToInsert)
					if query != "" {
						if _, ok := replaces[query]; !ok {
							replaces[query] = make([][]interface{}, 0)
						}
						replaces[query] = append(replaces[query], args)
						rowCount++
					}
				} else {
					query, args = prepareReplace(quoteTable, row.Columns, true /* appendPlaceHolder */, translateToInsert)
					if query != "" {
						sqls = append(sqls, query)
						values = append(values, args)
						rowCount++
					}
				}
			}
		}
	}
	flushCacheDMLs()

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

func (s *mysqlBackend) execDMLWithMaxRetries(ctx context.Context, dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Panic("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}

	start := time.Now()
	return retry.Do(ctx, func() error {
		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			err := logDMLTxnErr(errors.Trace(driver.ErrBadConn), start, s.changefeed, "failpoint", 0, nil)
			failpoint.Return(err)
		})
		failpoint.Inject("MySQLSinkHangLongTime", func() {
			time.Sleep(time.Hour)
		})

		err := s.statistics.RecordBatchExecution(func() (int, error) {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeed, "BEGIN", dmls.rowCount, dmls.startTs)
			}

			for i, query := range dmls.sqls {
				args := dmls.values[i]
				log.Debug("exec row", zap.Int("workerID", s.workerID),
					zap.String("sql", query), zap.Any("args", args))
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					err := logDMLTxnErr(
						cerror.WrapError(cerror.ErrMySQLTxnError, err),
						start, s.changefeed, query, dmls.rowCount, dmls.startTs)
					if rbErr := tx.Rollback(); rbErr != nil {
						if errors.Cause(rbErr) != context.Canceled {
							log.Warn("failed to rollback txn", zap.Error(rbErr))
						}
					}
					return 0, err
				}
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

func setSystemVariable(ctx context.Context, db *sql.DB, cfg *config.ReplicaConfig) error {
	var tidbVer string
	// check if downstream is TiDB
	row := db.QueryRowContext(ctx, "select tidb_version()")
	err := row.Scan(&tidbVer)
	if err != nil {
		// downstream is not TiDB, wo nothing
		if mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError);
		// means downstream is not TiDB
		ok && mysqlErr.Number == mysql.ErrNoDB {
			return nil
		}
		return err
	}

	// downstream is TiDB, set system variable: tidb_write_by_ticdc = true
	// We should always try to set this variable.
	_, err = db.ExecContext(ctx, "SET SESSION tidb_write_by_ticdc = true")
	if err == nil {
		return nil
	}

	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return err
	}

	if mysqlErr.Number == mysql.ErrUnknownSystemVariable {
		// If IgnoreRowsWrittenByTiCDC is true, we need throw this error
		// immediately.
		if cfg.Filter.IgnoreRowsWrittenByTiCDC {
			errMessage := fmt.Sprintf("This version of TiDB "+
				"does not support system variable: tidb_write_by_ticdc, "+
				"version: %s", tidbVer)
			return cerror.ErrChangefeedUnretryable.
				GenWithStackByArgs(errMessage)
		}
		return nil
	}

	return err
}
