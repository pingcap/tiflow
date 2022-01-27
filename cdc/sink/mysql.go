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
	"strconv"
	"strings"
	"sync"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/common"
	dmutils "github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/cyclic"
	"github.com/pingcap/tiflow/pkg/cyclic/mark"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	tifilter "github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	backoffBaseDelayInMs = 500
	// in previous/backoff retry pkg, the DefaultMaxInterval = 60 * time.Second
	backoffMaxDelayInMs = 60 * 1000
)

type mysqlSink struct {
	db     *sql.DB
	params *sinkParams

	filter *tifilter.Filter
	cyclic *cyclic.Cyclic

	txnCache           *common.UnresolvedTxnCache
	workers            []*mysqlSinkWorker
	tableCheckpointTs  sync.Map
	tableMaxResolvedTs sync.Map

	execWaitNotifier *notify.Notifier
	resolvedNotifier *notify.Notifier
	errCh            chan error
	flushSyncWg      sync.WaitGroup

	statistics *Statistics

	// metrics used by mysql sink only
	metricConflictDetectDurationHis prometheus.Observer
	metricBucketSizeCounters        []prometheus.Counter

	forceReplicate bool
	cancel         func()
}

var _ Sink = &mysqlSink{}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	filter *tifilter.Filter,
	replicaConfig *config.ReplicaConfig,
	opts map[string]string,
) (Sink, error) {
	opts[OptChangefeedID] = changefeedID
	params, err := parseSinkURIToParams(ctx, sinkURI, opts)
	if err != nil {
		return nil, err
	}

	params.enableOldValue = replicaConfig.EnableOldValue

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

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, sinkURI.Hostname(), port, params.tls)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}

	// create test db used for parameter detection
	// Refer https://github.com/go-sql-driver/mysql#parameters
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	if params.timezone != "" {
		dsn.Params["time_zone"] = params.timezone
	}
	dsn.Params["readTimeout"] = params.readTimeout
	dsn.Params["writeTimeout"] = params.writeTimeout
	dsn.Params["timeout"] = params.dialTimeout
	testDB, err := GetDBConnImpl(ctx, dsn.FormatDSN())
	if err != nil {
		return nil, err
	}
	defer testDB.Close()

	// Adjust sql_mode for compatibility.
	dsn.Params["sql_mode"], err = querySQLMode(ctx, testDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dsn.Params["sql_mode"], err = dmutils.AdjustSQLModeCompatible(dsn.Params["sql_mode"])
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Adjust sql_mode for cyclic replication.
	var sinkCyclic *cyclic.Cyclic = nil
	if val, ok := opts[mark.OptCyclicConfig]; ok {
		cfg := new(config.CyclicConfig)
		err := cfg.Unmarshal([]byte(val))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		sinkCyclic = cyclic.NewCyclic(cfg)
		dsn.Params["sql_mode"] = cyclic.RelaxSQLMode(dsn.Params["sql_mode"])
	}
	// NOTE: quote the string is necessary to avoid ambiguities.
	dsn.Params["sql_mode"] = strconv.Quote(dsn.Params["sql_mode"])

	dsnStr, err = generateDSNByParams(ctx, dsn, params, testDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err := GetDBConnImpl(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	log.Info("Start mysql sink")

	db.SetMaxIdleConns(params.workerCount)
	db.SetMaxOpenConns(params.workerCount)

	metricConflictDetectDurationHis := conflictDetectDurationHis.WithLabelValues(
		params.captureAddr, params.changefeedID)
	metricBucketSizeCounters := make([]prometheus.Counter, params.workerCount)
	for i := 0; i < params.workerCount; i++ {
		metricBucketSizeCounters[i] = bucketSizeCounter.WithLabelValues(
			params.captureAddr, params.changefeedID, strconv.Itoa(i))
	}
	ctx, cancel := context.WithCancel(ctx)

	sink := &mysqlSink{
		db:                              db,
		params:                          params,
		filter:                          filter,
		cyclic:                          sinkCyclic,
		txnCache:                        common.NewUnresolvedTxnCache(),
		statistics:                      NewStatistics(ctx, "mysql", opts),
		metricConflictDetectDurationHis: metricConflictDetectDurationHis,
		metricBucketSizeCounters:        metricBucketSizeCounters,
		errCh:                           make(chan error, 1),
		forceReplicate:                  replicaConfig.ForceReplicate,
		cancel:                          cancel,
	}

	sink.execWaitNotifier = new(notify.Notifier)
	sink.resolvedNotifier = new(notify.Notifier)

	err = sink.createSinkWorkers(ctx)
	if err != nil {
		return nil, err
	}

	receiver, err := sink.resolvedNotifier.NewReceiver(50 * time.Millisecond)
	if err != nil {
		return nil, err
	}
	go sink.flushRowChangedEvents(ctx, receiver)

	return sink, nil
}

// TryEmitRowChangedEvents just calls EmitRowChangedEvents internally.
func (s *mysqlSink) TryEmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) (bool, error) {
	_ = s.EmitRowChangedEvents(ctx, rows...)
	return true, nil
}

func (s *mysqlSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	count := s.txnCache.Append(s.filter, rows...)
	s.statistics.AddRowsCount(count)
	return nil
}

// FlushRowChangedEvents will flush all received events, we don't allow mysql
// sink to receive events before resolving
func (s *mysqlSink) FlushRowChangedEvents(ctx context.Context, tableID model.TableID, resolvedTs uint64) (uint64, error) {
	// Since CDC does not guarantee exactly once semantic, it won't cause any problem
	// here even if the table was moved or removed.
	// ref: https://github.com/pingcap/tiflow/pull/4356#discussion_r787405134
	v, ok := s.tableMaxResolvedTs.Load(tableID)
	if !ok || v.(uint64) < resolvedTs {
		s.tableMaxResolvedTs.Store(tableID, resolvedTs)
	}
	s.resolvedNotifier.Notify()

	// check and throw error
	select {
	case err := <-s.errCh:
		return 0, err
	default:
	}

	checkpointTs := s.getTableCheckpointTs(tableID)
	s.statistics.PrintStatus(ctx)
	return checkpointTs, nil
}

func (s *mysqlSink) flushRowChangedEvents(ctx context.Context, receiver *notify.Receiver) {
	defer func() {
		for _, worker := range s.workers {
			worker.closedCh <- struct{}{}
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-receiver.C:
		}
		flushedResolvedTsMap, resolvedTxnsMap := s.txnCache.Resolved(&s.tableMaxResolvedTs)
		if len(resolvedTxnsMap) == 0 {
			s.tableMaxResolvedTs.Range(func(key, value interface{}) bool {
				s.tableCheckpointTs.Store(key, value)
				return true
			})
			continue
		}

		if s.cyclic != nil {
			// Filter rows if it is origin from downstream.
			skippedRowCount := cyclic.FilterAndReduceTxns(
				resolvedTxnsMap, s.cyclic.FilterReplicaID(), s.cyclic.ReplicaID())
			s.statistics.SubRowsCount(skippedRowCount)
		}

		s.dispatchAndExecTxns(ctx, resolvedTxnsMap)
		for tableID, resolvedTs := range flushedResolvedTsMap {
			s.tableCheckpointTs.Store(tableID, resolvedTs)
		}
	}
}

func (s *mysqlSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// do nothing
	return nil
}

func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if s.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.Type, ddl.TableInfo.Schema, ddl.TableInfo.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
		)
		return cerror.ErrDDLEventIgnored.GenWithStackByArgs()
	}
	s.statistics.AddDDLCount()
	err := s.execDDLWithMaxRetries(ctx, ddl)
	return errors.Trace(err)
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
	return retry.Do(ctx, func() error {
		err := s.execDDL(ctx, ddl)
		if errorutil.IsIgnorableMySQLDDLError(err) {
			log.Info("execute DDL failed, but error can be ignored", zap.String("query", ddl.Query), zap.Error(err))
			return nil
		}
		if err != nil {
			log.Warn("execute DDL with error, retry later", zap.String("query", ddl.Query), zap.Error(err))
		}
		return err
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs), retry.WithBackoffMaxDelay(backoffMaxDelayInMs), retry.WithMaxTries(defaultDDLMaxRetryTime), retry.WithIsRetryableErr(cerror.IsRetryableError))
}

func (s *mysqlSink) execDDL(ctx context.Context, ddl *model.DDLEvent) error {
	shouldSwitchDB := needSwitchDB(ddl)

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})
	err := s.statistics.RecordDDLExecution(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if shouldSwitchDB {
			_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.Schema)+";")
			if err != nil {
				if rbErr := tx.Rollback(); rbErr != nil {
					log.Error("Failed to rollback", zap.Error(err))
				}
				return err
			}
		}

		if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.String("sql", ddl.Query), zap.Error(err))
			}
			return err
		}

		return tx.Commit()
	})
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query))
	return nil
}

func needSwitchDB(ddl *model.DDLEvent) bool {
	if len(ddl.TableInfo.Schema) == 0 {
		return false
	}
	if ddl.Type == timodel.ActionCreateSchema || ddl.Type == timodel.ActionDropSchema {
		return false
	}
	return true
}

func querySQLMode(ctx context.Context, db *sql.DB) (sqlMode string, err error) {
	row := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode;")
	err = row.Scan(&sqlMode)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return
}

func (s *mysqlSink) createSinkWorkers(ctx context.Context) error {
	s.workers = make([]*mysqlSinkWorker, s.params.workerCount)
	for i := range s.workers {
		receiver, err := s.execWaitNotifier.NewReceiver(defaultFlushInterval)
		if err != nil {
			return err
		}
		worker := newMySQLSinkWorker(
			s.params.maxTxnRow, i, s.metricBucketSizeCounters[i], receiver, s.execDMLs)
		s.workers[i] = worker
		go func() {
			err := worker.run(ctx)
			if err != nil && errors.Cause(err) != context.Canceled {
				select {
				case s.errCh <- err:
				default:
					log.Info("mysql sink receives redundant error", zap.Error(err))
				}
			}
			worker.cleanup()
		}()
	}
	return nil
}

func (s *mysqlSink) notifyAndWaitExec(ctx context.Context) {
	s.broadcastFinishTxn()
	s.execWaitNotifier.Notify()
	done := make(chan struct{})
	go func() {
		s.flushSyncWg.Wait()
		close(done)
	}()
	// This is a hack code to avoid io wait in some routine blocks others to exit.
	// As the network io wait is blocked in kernel code, the goroutine is in a
	// D-state that we could not even stop it by cancel the context. So if this
	// scenario happens, the blocked goroutine will be leak.
	select {
	case <-ctx.Done():
	case <-done:
	}
}

func (s *mysqlSink) broadcastFinishTxn() {
	// Note all data txn is sent via channel, the control txn must come after all
	// data txns in each worker. So after worker receives the control txn, it can
	// flush txns immediately and call wait group done once.
	for _, worker := range s.workers {
		worker.appendFinishTxn(&s.flushSyncWg)
	}
}

func (s *mysqlSink) dispatchAndExecTxns(ctx context.Context, txnsGroup map[model.TableID][]*model.SingleTableTxn) {
	nWorkers := s.params.workerCount
	causality := newCausality()
	rowsChIdx := 0

	sendFn := func(txn *model.SingleTableTxn, keys [][]byte, idx int) {
		causality.add(keys, idx)
		s.workers[idx].appendTxn(ctx, txn)
	}
	resolveConflict := func(txn *model.SingleTableTxn) {
		keys := genTxnKeys(txn)
		if conflict, idx := causality.detectConflict(keys); conflict {
			if idx >= 0 {
				sendFn(txn, keys, idx)
				return
			}
			s.notifyAndWaitExec(ctx)
			causality.reset()
		}
		sendFn(txn, keys, rowsChIdx)
		rowsChIdx++
		rowsChIdx = rowsChIdx % nWorkers
	}
	h := newTxnsHeap(txnsGroup)
	h.iter(func(txn *model.SingleTableTxn) {
		startTime := time.Now()
		resolveConflict(txn)
		s.metricConflictDetectDurationHis.Observe(time.Since(startTime).Seconds())
	})
	s.notifyAndWaitExec(ctx)
}

func (s *mysqlSink) Close(ctx context.Context) error {
	s.execWaitNotifier.Close()
	s.resolvedNotifier.Close()
	err := s.db.Close()
	s.cancel()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}

func (s *mysqlSink) Barrier(ctx context.Context, tableID model.TableID) error {
	warnDuration := 3 * time.Minute
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			maxResolvedTs, ok := s.tableMaxResolvedTs.Load(tableID)
			log.Warn("Barrier doesn't return in time, may be stuck",
				zap.Int64("tableID", tableID),
				zap.Bool("hasResolvedTs", ok),
				zap.Any("resolvedTs", maxResolvedTs),
				zap.Uint64("checkpointTs", s.getTableCheckpointTs(tableID)))
		default:
			v, ok := s.tableMaxResolvedTs.Load(tableID)
			if !ok {
				log.Info("No table resolvedTs is found", zap.Int64("tableID", tableID))
				return nil
			}
			maxResolvedTs := v.(uint64)
			if s.getTableCheckpointTs(tableID) >= maxResolvedTs {
				return nil
			}
			checkpointTs, err := s.FlushRowChangedEvents(ctx, tableID, maxResolvedTs)
			if err != nil {
				return err
			}
			if checkpointTs >= maxResolvedTs {
				return nil
			}
			// short sleep to avoid cpu spin
			time.Sleep(time.Second)
		}
	}
}

func (s *mysqlSink) getTableCheckpointTs(tableID model.TableID) uint64 {
	v, ok := s.tableCheckpointTs.Load(tableID)
	if ok {
		return v.(uint64)
	}
	return uint64(0)
}

func logDMLTxnErr(err error) error {
	if isRetryableDMLError(err) {
		log.Warn("execute DMLs with error, retry later", zap.Error(err))
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

func (s *mysqlSink) execDMLWithMaxRetries(ctx context.Context, dmls *preparedDMLs, bucket int) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Panic("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}

	return retry.Do(ctx, func() error {
		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			failpoint.Return(logDMLTxnErr(errors.Trace(dmysql.ErrInvalidConn)))
		})
		failpoint.Inject("MySQLSinkHangLongTime", func() {
			time.Sleep(time.Hour)
		})

		err := s.statistics.RecordBatchExecution(func() (int, error) {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return 0, logDMLTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
			}

			for i, query := range dmls.sqls {
				args := dmls.values[i]
				log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					if rbErr := tx.Rollback(); rbErr != nil {
						log.Warn("failed to rollback txn", zap.Error(err))
					}
					return 0, logDMLTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
				}
			}

			if len(dmls.markSQL) != 0 {
				log.Debug("exec row", zap.String("sql", dmls.markSQL))
				if _, err := tx.ExecContext(ctx, dmls.markSQL); err != nil {
					if rbErr := tx.Rollback(); rbErr != nil {
						log.Warn("failed to rollback txn", zap.Error(err))
					}
					return 0, logDMLTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
				}
			}

			if err = tx.Commit(); err != nil {
				return 0, logDMLTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
			}
			return dmls.rowCount, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Exec Rows succeeded",
			zap.String("changefeed", s.params.changefeedID),
			zap.Int("num of Rows", dmls.rowCount),
			zap.Int("bucket", bucket))
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs), retry.WithBackoffMaxDelay(backoffMaxDelayInMs), retry.WithMaxTries(defaultDMLMaxRetryTime), retry.WithIsRetryableErr(isRetryableDMLError))
}

type preparedDMLs struct {
	sqls     []string
	values   [][]interface{}
	markSQL  string
	rowCount int
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent, replicaID uint64, bucket int) *preparedDMLs {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	replaces := make(map[string][][]interface{})
	rowCount := 0
	// translateToInsert control the update and insert behavior
	translateToInsert := s.params.enableOldValue && !s.params.safeMode

	// flush cached batch replace or insert, to keep the sequence of DMLs
	flushCacheDMLs := func() {
		if s.params.batchReplaceEnabled && len(replaces) > 0 {
			replaceSqls, replaceValues := reduceReplace(replaces, s.params.batchReplaceSize)
			sqls = append(sqls, replaceSqls...)
			values = append(values, replaceValues...)
			replaces = make(map[string][][]interface{})
		}
	}

	for _, row := range rows {
		var query string
		var args []interface{}
		quoteTable := quotes.QuoteSchema(row.Table.Schema, row.Table.Table)

		// If the old value is enabled, is not in safe mode and is an update event, then translate to UPDATE.
		// NOTICE: Only update events with the old value feature enabled will have both columns and preColumns.
		if translateToInsert && len(row.PreColumns) != 0 && len(row.Columns) != 0 {
			flushCacheDMLs()
			query, args = prepareUpdate(quoteTable, row.PreColumns, row.Columns, s.forceReplicate)
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
			query, args = prepareDelete(quoteTable, row.PreColumns, s.forceReplicate)
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
			if s.params.batchReplaceEnabled {
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
				sqls = append(sqls, query)
				values = append(values, args)
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
					rowCount++
				}
			}
		}
	}
	flushCacheDMLs()

	dmls := &preparedDMLs{
		sqls:   sqls,
		values: values,
	}
	if s.cyclic != nil && len(rows) > 0 {
		// Write mark table with the current replica ID.
		row := rows[0]
		updateMark := s.cyclic.UdpateSourceTableCyclicMark(
			row.Table.Schema, row.Table.Table, uint64(bucket), replicaID, row.StartTs)
		dmls.markSQL = updateMark
		// rowCount is used in statistics, and for simplicity,
		// we do not count mark table rows in rowCount.
	}
	dmls.rowCount = rowCount
	return dmls
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent, replicaID uint64, bucket int) error {
	failpoint.Inject("SinkFlushDMLPanic", func() {
		time.Sleep(time.Second)
		log.Fatal("SinkFlushDMLPanic")
	})
	failpoint.Inject("MySQLSinkExecDMLError", func() {
		// Add a delay to ensure the sink worker with `MySQLSinkHangLongTime`
		// failpoint injected is executed first.
		time.Sleep(time.Second * 2)
		failpoint.Return(errors.Trace(dmysql.ErrInvalidConn))
	})
	dmls := s.prepareDMLs(rows, replicaID, bucket)
	log.Debug("prepare DMLs", zap.Any("rows", rows), zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))
	if err := s.execDMLWithMaxRetries(ctx, dmls, bucket); err != nil {
		log.Error("execute DMLs failed", zap.String("err", err.Error()))
		return errors.Trace(err)
	}
	return nil
}

func prepareReplace(
	quoteTable string,
	cols []*model.Column,
	appendPlaceHolder bool,
	translateToInsert bool,
) (string, []interface{}) {
	var builder strings.Builder
	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for _, col := range cols {
		if col == nil || col.Flag.IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, col.Name)
		args = append(args, col.Value)
	}
	if len(args) == 0 {
		return "", nil
	}

	colList := "(" + buildColumnList(columnNames) + ")"
	if translateToInsert {
		builder.WriteString("INSERT INTO " + quoteTable + colList + " VALUES ")
	} else {
		builder.WriteString("REPLACE INTO " + quoteTable + colList + " VALUES ")
	}
	if appendPlaceHolder {
		builder.WriteString("(" + model.HolderString(len(columnNames)) + ");")
	}

	return builder.String(), args
}

// reduceReplace groups SQLs with the same replace statement format, as following
// sql: `REPLACE INTO `test`.`t` (`a`,`b`) VALUES (?,?,?,?,?,?)`
// args: (1,"",2,"2",3,"")
func reduceReplace(replaces map[string][][]interface{}, batchSize int) ([]string, [][]interface{}) {
	nextHolderString := func(query string, valueNum int, last bool) string {
		query += "(" + model.HolderString(valueNum) + ")"
		if !last {
			query += ","
		}
		return query
	}
	sqls := make([]string, 0)
	args := make([][]interface{}, 0)
	for replace, vals := range replaces {
		query := replace
		cacheCount := 0
		cacheArgs := make([]interface{}, 0)
		last := false
		for i, val := range vals {
			cacheCount++
			if i == len(vals)-1 || cacheCount >= batchSize {
				last = true
			}
			query = nextHolderString(query, len(val), last)
			cacheArgs = append(cacheArgs, val...)
			if last {
				sqls = append(sqls, query)
				args = append(args, cacheArgs)
				query = replace
				cacheCount = 0
				cacheArgs = make([]interface{}, 0, len(cacheArgs))
				last = false
			}
		}
	}
	return sqls, args
}

func prepareUpdate(quoteTable string, preCols, cols []*model.Column, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("UPDATE " + quoteTable + " SET ")

	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols)+len(preCols))
	for _, col := range cols {
		if col == nil || col.Flag.IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, col.Name)
		args = append(args, col.Value)
	}
	if len(args) == 0 {
		return "", nil
	}
	for i, column := range columnNames {
		if i == len(columnNames)-1 {
			builder.WriteString("`" + quotes.EscapeName(column) + "`=?")
		} else {
			builder.WriteString("`" + quotes.EscapeName(column) + "`=?,")
		}
	}

	builder.WriteString(" WHERE ")
	colNames, wargs := whereSlice(preCols, forceReplicate)
	if len(wargs) == 0 {
		return "", nil
	}
	for i := 0; i < len(colNames); i++ {
		if i > 0 {
			builder.WriteString(" AND ")
		}
		if wargs[i] == nil {
			builder.WriteString(quotes.QuoteName(colNames[i]) + " IS NULL")
		} else {
			builder.WriteString(quotes.QuoteName(colNames[i]) + "=?")
			args = append(args, wargs[i])
		}
	}
	builder.WriteString(" LIMIT 1;")
	sql := builder.String()
	return sql, args
}

func prepareDelete(quoteTable string, cols []*model.Column, forceReplicate bool) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("DELETE FROM " + quoteTable + " WHERE ")

	colNames, wargs := whereSlice(cols, forceReplicate)
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

func whereSlice(cols []*model.Column, forceReplicate bool) (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for _, col := range cols {
		if col == nil || !col.Flag.IsHandleKey() {
			continue
		}
		colNames = append(colNames, col.Name)
		args = append(args, col.Value)
	}
	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 && forceReplicate {
		colNames = make([]string, 0, len(cols))
		args = make([]interface{}, 0, len(cols))
		for _, col := range cols {
			colNames = append(colNames, col.Name)
			args = append(args, col.Value)
		}
	}
	return
}

func getSQLErrCode(err error) (errors.ErrCode, bool) {
	mysqlErr, ok := errors.Cause(err).(*dmysql.MySQLError)
	if !ok {
		return -1, false
	}

	return errors.ErrCode(mysqlErr.Number), true
}

func buildColumnList(names []string) string {
	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(quotes.QuoteName(name))

	}

	return b.String()
}

// GetDBConnImpl is the implement holder to get db connection. Export it for tests
var GetDBConnImpl = getDBConn

func getDBConn(ctx context.Context, dsnStr string) (*sql.DB, error) {
	db, err := sql.Open("mysql", dsnStr)
	if err != nil {
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	err = db.PingContext(ctx)
	if err != nil {
		// close db to recycle resources
		if closeErr := db.Close(); closeErr != nil {
			log.Warn("close db failed", zap.Error(err))
		}
		return nil, cerror.ErrMySQLConnectionError.Wrap(err).GenWithStack("fail to open MySQL connection")
	}
	return db, nil
}
