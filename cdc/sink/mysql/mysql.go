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

package mysql

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/metrics"
	dmutils "github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/errorutil"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
	pmysql "github.com/pingcap/tiflow/pkg/sink/mysql"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

const (
	backoffBaseDelayInMs = 500
	// in previous/backoff retry pkg, the DefaultMaxInterval = 60 * time.Second
	backoffMaxDelayInMs = 60 * 1000

	// networkDriftDuration is used to construct a context timeout for database operations.
	networkDriftDuration = 5 * time.Second
)

type mysqlSink struct {
	db     *sql.DB
	params *sinkParams

	txnCache           *unresolvedTxnCache
	workers            []*mysqlSinkWorker
	tableCheckpointTs  sync.Map
	tableMaxResolvedTs sync.Map

	execWaitNotifier *notify.Notifier
	resolvedCh       chan struct{}
	errCh            chan error
	flushSyncWg      sync.WaitGroup

	statistics *metrics.Statistics

	// metrics used by mysql sink only
	metricConflictDetectDurationHis prometheus.Observer
	metricBucketSizeCounters        []prometheus.Counter

	forceReplicate bool
	cancel         func()

	// error is set when the sink has encountered an
	// error and cannot work anymore.
	error atomic.Error
}

// NewMySQLSink creates a new MySQL sink using schema storage
func NewMySQLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
) (*mysqlSink, error) {
	params, err := parseSinkURIToParams(ctx, changefeedID, sinkURI)
	if err != nil {
		return nil, err
	}

	params.enableOldValue = replicaConfig.EnableOldValue

	// dsn format of the driver:
	// [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
	username := sinkURI.User.Username()
	if username == "" {
		username = "root"
	}
	password, _ := sinkURI.User.Password()

	hostName := sinkURI.Hostname()
	port := sinkURI.Port()
	if port == "" {
		port = "4000"
	}

	// This will handle the IPv6 address format.
	host := net.JoinHostPort(hostName, port)
	dsnStr := fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, host, params.tls)
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

	testDB, err := pmysql.GetTestDB(ctx, dsn, GetDBConnImpl)
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

	// NOTE: quote the string is necessary to avoid ambiguities.
	dsn.Params["sql_mode"] = strconv.Quote(dsn.Params["sql_mode"])

	dsnStr, err = generateDSNByParams(ctx, dsn, params, testDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
	// check if GBK charset is supported by downstream
	gbkSupported, err := checkCharsetSupport(ctx, testDB, charset.CharsetGBK)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !gbkSupported {
		log.Warn("gbk charset is not supported by downstream, "+
			"some types of DDL may fail to be executed",
			zap.String("hostname", hostName), zap.String("port", port))
	}
	db, err := GetDBConnImpl(ctx, dsnStr)
	if err != nil {
		return nil, err
	}

	log.Info("Start mysql sink")

	db.SetMaxIdleConns(params.workerCount)
	db.SetMaxOpenConns(params.workerCount)

	metricConflictDetectDurationHis := metrics.ConflictDetectDurationHis.
		WithLabelValues(params.changefeedID.Namespace, params.changefeedID.ID)
	metricBucketSizeCounters := make([]prometheus.Counter, params.workerCount)
	for i := 0; i < params.workerCount; i++ {
		metricBucketSizeCounters[i] = metrics.BucketSizeCounter.
			WithLabelValues(params.changefeedID.Namespace, params.changefeedID.ID, strconv.Itoa(i))
	}

	ctx, cancel := context.WithCancel(ctx)
	captureAddr := contextutil.CaptureAddrFromCtx(ctx)
	statistics := metrics.NewStatistics(ctx, captureAddr, metrics.SinkTypeDB)
	sink := &mysqlSink{
		db:                              db,
		params:                          params,
		txnCache:                        newUnresolvedTxnCache(),
		statistics:                      statistics,
		metricConflictDetectDurationHis: metricConflictDetectDurationHis,
		metricBucketSizeCounters:        metricBucketSizeCounters,
		execWaitNotifier:                new(notify.Notifier),
		resolvedCh:                      make(chan struct{}, 1),
		errCh:                           make(chan error, 1),
		forceReplicate:                  replicaConfig.ForceReplicate,
		cancel:                          cancel,
	}

	err = sink.createSinkWorkers(ctx)
	if err != nil {
		return nil, err
	}

	go sink.flushRowChangedEvents(ctx)

	return sink, nil
}

// EmitRowChangedEvents appends row changed events to the txn cache.
// Concurrency Note: EmitRowChangedEvents is thread-safe.
func (s *mysqlSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	count := s.txnCache.Append(rows...)
	s.statistics.AddRowsCount(count)
	return nil
}

// FlushRowChangedEvents will flush all received events,
// we do not write data downstream until we receive resolvedTs.
// Concurrency Note: FlushRowChangedEvents is thread-safe.
func (s *mysqlSink) FlushRowChangedEvents(
	ctx context.Context, tableID model.TableID, resolved model.ResolvedTs,
) (model.ResolvedTs, error) {
	if err := s.error.Load(); err != nil {
		return model.NewResolvedTs(0), err
	}

	v, ok := s.getTableResolvedTs(tableID)
	if !ok || v.Less(resolved) {
		s.tableMaxResolvedTs.Store(tableID, resolved)
	}

	// check and throw error
	select {
	case <-ctx.Done():
		return model.NewResolvedTs(0), ctx.Err()
	case s.resolvedCh <- struct{}{}:
		// Notify `flushRowChangedEvents` to asynchronously write data.
	default:
	}

	checkpointTs := s.getTableCheckpointTs(tableID)
	s.statistics.PrintStatus(ctx)
	return checkpointTs, nil
}

func (s *mysqlSink) flushRowChangedEvents(ctx context.Context) {
	defer func() {
		for _, worker := range s.workers {
			worker.close()
		}
	}()
outer:
	for {
		select {
		case <-ctx.Done():
			return
		case err := <-s.errCh:
			log.Error("mysqlSink encountered error",
				zap.Error(err),
				zap.String("namespace", s.params.changefeedID.Namespace),
				zap.String("changefeed", s.params.changefeedID.ID))
			s.error.Store(err)
			return
		case <-s.resolvedCh:
		}
		checkpointTsMap, resolvedTxnsMap := s.txnCache.Resolved(&s.tableMaxResolvedTs)

		if len(resolvedTxnsMap) != 0 {
			s.dispatchAndExecTxns(ctx, resolvedTxnsMap)
		}

		// This is an ad-hoc fix to prevent the checkpoint
		// from being updated after a DML has failed to execute.
		for _, worker := range s.workers {
			if !worker.isNormal() {
				// We still need to loop to the next iteration
				// because we would like to read from `s.errCh`.
				continue outer
			}
		}
		for tableID, resolved := range checkpointTsMap {
			s.tableCheckpointTs.Store(tableID, resolved)
		}
	}
}

func (s *mysqlSink) EmitCheckpointTs(_ context.Context, ts uint64, _ []*model.TableInfo) error {
	// do nothing
	log.Debug("emit checkpointTs", zap.Uint64("checkpointTs", ts))
	return nil
}

// EmitDDLEvent executes DDL event.
// Concurrency Note: EmitDDLEvent is thread-safe.
func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	s.statistics.AddDDLCount()
	err := s.execDDLWithMaxRetries(ctx, ddl)
	// we should not retry changefeed if DDL failed by return an unretryable error.
	if !errorutil.IsRetryableDDLError(err) {
		return cerror.WrapChangefeedUnretryableErr(err)
	}
	return errors.Trace(err)
}

func (s *mysqlSink) execDDLWithMaxRetries(ctx context.Context, ddl *model.DDLEvent) error {
	return retry.Do(ctx, func() error {
		err := s.execDDL(ctx, ddl)
		if errorutil.IsIgnorableMySQLDDLError(err) {
			log.Info("Execute DDL failed, but error can be ignored",
				zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
				zap.Error(err))
			return nil
		}
		if err != nil {
			log.Warn("execute DDL with error, retry later",
				zap.Uint64("startTs", ddl.StartTs), zap.String("ddl", ddl.Query),
				zap.Error(err))
		}
		return err
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithMaxTries(defaultDDLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDDLError))
}

func (s *mysqlSink) execDDL(pctx context.Context, ddl *model.DDLEvent) error {
	writeTimeout, _ := time.ParseDuration(s.params.writeTimeout)
	writeTimeout += networkDriftDuration
	ctx, cancelFunc := context.WithTimeout(pctx, writeTimeout)
	defer cancelFunc()

	shouldSwitchDB := needSwitchDB(ddl)

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})
	log.Info("start exec DDL", zap.Any("DDL", ddl))
	err := s.statistics.RecordDDLExecution(func() error {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if shouldSwitchDB {
			_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.TableName.Schema)+";")
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
	if len(ddl.TableInfo.TableName.Schema) == 0 {
		return false
	}
	if ddl.Type == timodel.ActionCreateSchema || ddl.Type == timodel.ActionDropSchema {
		return false
	}
	return true
}

func querySQLMode(ctx context.Context, db *sql.DB) (string, error) {
	row := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode;")
	var sqlMode sql.NullString
	err := row.Scan(&sqlMode)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	if !sqlMode.Valid {
		sqlMode.String = ""
	}
	return sqlMode.String, err
}

// check whether the target charset is supported
func checkCharsetSupport(ctx context.Context, db *sql.DB, charsetName string) (bool, error) {
	// validate charsetName
	_, err := charset.GetCharsetInfo(charsetName)
	if err != nil {
		return false, errors.Trace(err)
	}

	var characterSetName string
	querySQL := "select character_set_name from information_schema.character_sets " +
		"where character_set_name = '" + charsetName + "';"
	err = db.QueryRowContext(ctx, querySQL).Scan(&characterSetName)
	if err != nil && err != sql.ErrNoRows {
		return false, cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	if err != nil {
		return false, nil
	}

	return true, nil
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
	// notifyAndWaitExec may return because of context cancellation,
	// and s.flushSyncWg.Wait() goroutine is still running, check context first to
	// avoid data race
	select {
	case <-ctx.Done():
		return
	default:
	}
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
		return
	case <-done:
		return
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

// dispatchAndExecTxns dispatches txns to workers and waits for them to finish.
// 1) Try to distribute all transactions equally to each worker
// 2) Each conflicting transaction will be executed in the order of the CommitTs
// 3）Conflict-free transactions will be executed concurrently
func (s *mysqlSink) dispatchAndExecTxns(ctx context.Context, txnsGroup map[model.TableID][]*model.SingleTableTxn) {
	nWorkers := s.params.workerCount
	causality := newCausality()
	workerIndex := 0

	sendFn := func(txn *model.SingleTableTxn, keys [][]byte, workerIndex int) {
		causality.add(keys, workerIndex)
		s.workers[workerIndex].appendTxn(ctx, txn)
	}

	resolveConflict := func(txn *model.SingleTableTxn) {
		keys := genTxnKeys(txn)
		if conflict, conflictWorkerIndex := causality.detectConflict(keys); conflict {
			// This means that the conflict only occurs on one worker,
			// and we can just send the transaction to that worker to queue it.
			if conflictWorkerIndex >= 0 {
				// Send all these conflicting transactions
				// to the same worker queue.
				sendFn(txn, keys, conflictWorkerIndex)
				return
			}
			// This means that the conflict occurs on multiple workers,
			// and we have to wait all workers to finish their current transactions.
			s.notifyAndWaitExec(ctx)
			causality.reset()
		}
		// Distribute the data to the next worker.
		// Make txn more evenly distributed to each worker.
		sendFn(txn, keys, workerIndex)
		workerIndex++
		workerIndex = workerIndex % nWorkers
	}
	h := newTxnsHeap(txnsGroup)
	h.iter(func(txn *model.SingleTableTxn) {
		startTime := time.Now()
		resolveConflict(txn)
		s.metricConflictDetectDurationHis.Observe(time.Since(startTime).Seconds())
	})
	s.notifyAndWaitExec(ctx)
}

func (s *mysqlSink) AddTable(tableID model.TableID) error {
	s.cleanTableResource(tableID)
	return nil
}

func (s *mysqlSink) cleanTableResource(tableID model.TableID) {
	// We need to clean up the old values of the table,
	// otherwise when the table is dispatched back again,
	// it may read the old values.
	// See: https://github.com/pingcap/tiflow/issues/4464#issuecomment-1085385382.
	if resolved, loaded := s.tableMaxResolvedTs.LoadAndDelete(tableID); loaded {
		log.Info("clean up table max resolved ts in MySQL sink",
			zap.Int64("tableID", tableID),
			zap.Uint64("resolvedTs", resolved.(model.ResolvedTs).Ts))
	}
	if checkpoint, loaded := s.tableCheckpointTs.LoadAndDelete(tableID); loaded {
		log.Info("clean up table checkpoint ts in MySQL sink",
			zap.Int64("tableID", tableID),
			zap.Uint64("checkpointTs", checkpoint.(model.ResolvedTs).Ts))
	}
	// try to remove table txn cache
	s.txnCache.RemoveTableTxn(tableID)
}

func (s *mysqlSink) Close(ctx context.Context) error {
	s.execWaitNotifier.Close()
	err := s.db.Close()
	s.cancel()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}

func (s *mysqlSink) RemoveTable(ctx context.Context, tableID model.TableID) error {
	defer s.cleanTableResource(tableID)

	warnDuration := 3 * time.Minute
	ticker := time.NewTicker(warnDuration)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-ticker.C:
			maxResolved, ok := s.getTableResolvedTs(tableID)
			log.Warn("RemoveTable doesn't return in time, may be stuck",
				zap.Int64("tableID", tableID),
				zap.Bool("hasResolvedTs", ok),
				zap.Any("resolvedTs", maxResolved.Ts),
				zap.Uint64("checkpointTs", s.getTableCheckpointTs(tableID).Ts))
		default:
			if err := s.error.Load(); err != nil {
				return err
			}
			maxResolved, ok := s.getTableResolvedTs(tableID)
			if !ok {
				log.Info("No table resolvedTs is found", zap.Int64("tableID", tableID))
				return nil
			}
			checkpoint := s.getTableCheckpointTs(tableID)
			if checkpoint.EqualOrGreater(maxResolved) {
				return nil
			}
			checkpoint, err := s.FlushRowChangedEvents(ctx, tableID, maxResolved)
			if err != nil {
				return err
			}
			if checkpoint.Ts >= maxResolved.Ts {
				return nil
			}
			// short sleep to avoid cpu spin
			time.Sleep(time.Second)
		}
	}
}

func (s *mysqlSink) getTableCheckpointTs(tableID model.TableID) model.ResolvedTs {
	v, ok := s.tableCheckpointTs.Load(tableID)
	if ok {
		return v.(model.ResolvedTs)
	}
	return model.NewResolvedTs(0)
}

func (s *mysqlSink) getTableResolvedTs(tableID model.TableID) (model.ResolvedTs, bool) {
	v, ok := s.tableMaxResolvedTs.Load(tableID)
	var resolved model.ResolvedTs
	if ok {
		resolved = v.(model.ResolvedTs)
	}
	return resolved, ok
}

func logDMLTxnErr(
	err error, start time.Time, changefeed model.ChangeFeedID,
	query string, count int, startTs []model.Ts,
) error {
	if errorutil.IsRetryableDMLError(err) {
		log.Warn("execute DMLs with error, retry later",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.Uint64s("startTs", startTs),
			zap.String("namespace", changefeed.Namespace),
			zap.String("changefeed", changefeed.ID))
	} else {
		log.Error("execute DMLs with error, can not retry",
			zap.Error(err), zap.Duration("duration", time.Since(start)),
			zap.String("query", query), zap.Int("count", count),
			zap.String("namespace", changefeed.Namespace),
			zap.String("changefeed", changefeed.ID))
	}
	return err
}

func (s *mysqlSink) execDMLWithMaxRetries(pctx context.Context, dmls *preparedDMLs, bucket int) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Panic("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}

	start := time.Now()
	return retry.Do(pctx, func() error {
		writeTimeout, _ := time.ParseDuration(s.params.writeTimeout)
		writeTimeout += networkDriftDuration

		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			failpoint.Return(
				logDMLTxnErr(
					errors.Trace(driver.ErrBadConn),
					start, s.params.changefeedID, "failpoint", 0, nil))
		})
		failpoint.Inject("MySQLSinkHangLongTime", func() {
			time.Sleep(time.Hour)
		})
		err := s.statistics.RecordBatchExecution(func() (int, int64, error) {
			tx, err := s.db.BeginTx(pctx, nil)
			if err != nil {
				return 0, 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.params.changefeedID, "BEGIN", dmls.rowCount, dmls.startTs)
			}

			for i, query := range dmls.sqls {
				args := dmls.values[i]
				log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
				ctx, cancelFunc := context.WithTimeout(pctx, writeTimeout)
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					if rbErr := tx.Rollback(); rbErr != nil {
						if errors.Cause(rbErr) != context.Canceled {
							log.Warn("failed to rollback txn", zap.Error(err))
							_ = logDMLTxnErr(
								cerror.WrapError(cerror.ErrMySQLTxnError, err),
								start, s.params.changefeedID, query, dmls.rowCount, dmls.startTs)
						}
					}
					cancelFunc()
					return 0, 0, logDMLTxnErr(
						cerror.WrapError(cerror.ErrMySQLTxnError, err),
						start, s.params.changefeedID, query, dmls.rowCount, dmls.startTs)
				}
				cancelFunc()
			}

			if err = tx.Commit(); err != nil {
				return 0, 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.params.changefeedID, "COMMIT", dmls.rowCount, dmls.startTs)
			}
			return dmls.rowCount, dmls.approximateSize, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Exec Rows succeeded",
			zap.String("namespace", s.params.changefeedID.Namespace),
			zap.String("changefeed", s.params.changefeedID.ID),
			zap.Int("numOfRows", dmls.rowCount),
			zap.Int("bucket", bucket))
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithMaxTries(defaultDMLMaxRetry),
		retry.WithIsRetryableErr(errorutil.IsRetryableDMLError))
}

type preparedDMLs struct {
	startTs         []model.Ts
	sqls            []string
	values          [][]interface{}
	rowCount        int
	approximateSize int64
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent) *preparedDMLs {
	startTs := make([]model.Ts, 0, 1)
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	replaces := make(map[string][][]interface{})
	rowCount := 0
	approximateSize := int64(0)

	// translateToInsert control the update and insert behavior
	translateToInsert := s.params.enableOldValue && !s.params.safeMode
	for _, row := range rows {
		if !translateToInsert {
			break
		}
		// It can be translated in to INSERT, if the row is committed after
		// we starting replicating the table, which means it must not be
		// replicated before, and there is no such row in downstream MySQL.
		translateToInsert = row.CommitTs > row.ReplicatingTs
	}

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
		if len(startTs) == 0 || // Always add the first row's start ts.
			startTs[len(startTs)-1] != row.StartTs { // Try to deduplicate starts ts.
			startTs = append(startTs, row.StartTs)
		}

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
			approximateSize += int64(len(query)) + row.ApproximateDataSize
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
				if query != "" {
					sqls = append(sqls, query)
					values = append(values, args)
					rowCount++
				}
			}
		}
		approximateSize += int64(len(query)) + row.ApproximateDataSize
	}
	flushCacheDMLs()

	dmls := &preparedDMLs{
		startTs:         startTs,
		sqls:            sqls,
		values:          values,
		rowCount:        rowCount,
		approximateSize: approximateSize,
	}
	return dmls
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent, bucket int) error {
	failpoint.Inject("MySQLSinkExecDMLError", func() {
		// Add a delay to ensure the sink worker with `MySQLSinkHangLongTime`
		// failpoint injected is executed first.
		time.Sleep(time.Second * 2)
		failpoint.Return(errors.Trace(dmysql.ErrInvalidConn))
	})
	s.statistics.ObserveRows(rows...)
	dmls := s.prepareDMLs(rows)
	log.Debug("prepare DMLs", zap.Any("rows", rows), zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))
	if err := s.execDMLWithMaxRetries(ctx, dmls, bucket); err != nil {
		if errors.Cause(err) != context.Canceled {
			log.Error("execute DMLs failed", zap.Error(err))
		}
		return errors.Trace(err)
	}
	return nil
}

// if the column value type is []byte and charset is not binary, we get its string
// representation. Because if we use the byte array respresentation, the go-sql-driver
// will automatically set `_binary` charset for that column, which is not expected.
// See https://github.com/go-sql-driver/mysql/blob/ce134bfc/connection.go#L267
func appendQueryArgs(args []interface{}, col *model.Column) []interface{} {
	if col.Charset != "" && col.Charset != charset.CharsetBin {
		colValBytes, ok := col.Value.([]byte)
		if ok {
			args = append(args, string(colValBytes))
		} else {
			args = append(args, col.Value)
		}
	} else {
		args = append(args, col.Value)
	}

	return args
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
		args = appendQueryArgs(args, col)
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
		builder.WriteString("(" + placeHolder(len(columnNames)) + ");")
	}

	return builder.String(), args
}

// reduceReplace groups SQLs with the same replace statement format, as following
// sql: `REPLACE INTO `test`.`t` (`a`,`b`) VALUES (?,?,?,?,?,?)`
// args: (1,"",2,"2",3,"")
func reduceReplace(replaces map[string][][]interface{}, batchSize int) ([]string, [][]interface{}) {
	nextHolderString := func(query string, valueNum int, last bool) string {
		query += "(" + placeHolder(valueNum) + ")"
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
		args = appendQueryArgs(args, col)
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
		args = appendQueryArgs(args, col)
	}
	// if no explicit row id but force replicate, use all key-values in where condition
	if len(colNames) == 0 && forceReplicate {
		colNames = make([]string, 0, len(cols))
		args = make([]interface{}, 0, len(cols))
		for _, col := range cols {
			colNames = append(colNames, col.Name)
			args = appendQueryArgs(args, col)
		}
	}
	return
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

// placeHolder returns a string separated by comma
// n must be greater or equal than 1, or the function will panic
func placeHolder(n int) string {
	var builder strings.Builder
	builder.Grow((n-1)*2 + 1)
	for i := 0; i < n; i++ {
		if i > 0 {
			builder.WriteString(",")
		}
		builder.WriteString("?")
	}
	return builder.String()
}
