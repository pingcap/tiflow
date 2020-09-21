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
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/filter"
	tifilter "github.com/pingcap/ticdc/pkg/filter"
	"github.com/pingcap/ticdc/pkg/notify"
	"github.com/pingcap/ticdc/pkg/quotes"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

const (
	defaultWorkerCount         = 16
	defaultMaxTxnRow           = 256
	defaultDMLMaxRetryTime     = 8
	defaultDDLMaxRetryTime     = 20
	defaultTiDBTxnMode         = "optimistic"
	defaultFlushInterval       = time.Millisecond * 50
	defaultBatchReplaceEnabled = true
	defaultBatchReplaceSize    = 20
	defaultReadTimeout         = "2m"
	defaultWriteTimeout        = "2m"
	defaultSafeMode            = true
)

var (
	validSchemes = map[string]bool{
		"mysql":     true,
		"mysql+ssl": true,
		"tidb":      true,
		"tidb+ssl":  true,
	}
)

type mysqlSink struct {
	db     *sql.DB
	params *sinkParams

	filter *filter.Filter
	cyclic *cyclic.Cyclic

	txnCache   *common.UnresolvedTxnCache
	workers    []*mysqlSinkWorker
	resolvedTs uint64

	execWaitNotifier *notify.Notifier
	resolvedNotifier *notify.Notifier
	errCh            chan error

	statistics *Statistics

	// metrics used by mysql sink only
	metricConflictDetectDurationHis prometheus.Observer
	metricBucketSizeCounters        []prometheus.Counter
}

func (s *mysqlSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	count := s.txnCache.Append(s.filter, rows...)
	s.statistics.AddRowsCount(count)
	return nil
}

func (s *mysqlSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) (uint64, error) {
	atomic.StoreUint64(&s.resolvedTs, resolvedTs)
	s.resolvedNotifier.Notify()

	// check and throw error
	select {
	case err := <-s.errCh:
		return 0, err
	default:
	}

	checkpointTs := resolvedTs
	for _, worker := range s.workers {
		workerCheckpointTs := atomic.LoadUint64(&worker.checkpointTs)
		if workerCheckpointTs < checkpointTs {
			checkpointTs = workerCheckpointTs
		}
	}
	return checkpointTs, nil
}

func (s *mysqlSink) flushRowChangedEvents(ctx context.Context) {
	receiver := s.resolvedNotifier.NewReceiver(50 * time.Millisecond)
	for {
		select {
		case <-ctx.Done():
			return
		case <-receiver.C:
		}
		resolvedTs := atomic.LoadUint64(&s.resolvedTs)
		resolvedTxnsMap := s.txnCache.Resolved(resolvedTs)
		if len(resolvedTxnsMap) == 0 {
			for _, worker := range s.workers {
				atomic.StoreUint64(&worker.checkpointTs, resolvedTs)
			}
			s.txnCache.UpdateCheckpoint(resolvedTs)
			continue
		}

		if s.cyclic != nil {
			// Filter rows if it is origined from downstream.
			skippedRowCount := cyclic.FilterAndReduceTxns(
				resolvedTxnsMap, s.cyclic.FilterReplicaID(), s.cyclic.ReplicaID())
			s.statistics.SubRowsCount(skippedRowCount)
		}
		s.dispatchAndExecTxns(ctx, resolvedTxnsMap)
		for _, worker := range s.workers {
			atomic.StoreUint64(&worker.checkpointTs, resolvedTs)
		}
		s.txnCache.UpdateCheckpoint(resolvedTs)
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
	err := s.execDDLWithMaxRetries(ctx, ddl, defaultDDLMaxRetryTime)
	return errors.Trace(err)
}

// Initialize is no-op for Mysql sink
func (s *mysqlSink) Initialize(ctx context.Context, tableInfo []*model.SimpleTableInfo) error {
	return nil
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
	shouldSwitchDB := len(ddl.TableInfo.Schema) > 0 && ddl.Type != timodel.ActionCreateSchema

	failpoint.Inject("MySQLSinkExecDDLDelay", func() {
		select {
		case <-ctx.Done():
			failpoint.Return(ctx.Err())
		case <-time.After(time.Hour):
		}
		failpoint.Return(nil)
	})

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.Schema)+";")
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				log.Error("Failed to rollback", zap.Error(err))
			}
			return cerror.WrapError(cerror.ErrMySQLTxnError, err)
		}
	}

	if _, err = tx.ExecContext(ctx, ddl.Query); err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			log.Error("Failed to rollback", zap.String("sql", ddl.Query), zap.Error(err))
		}
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	if err = tx.Commit(); err != nil {
		return cerror.WrapError(cerror.ErrMySQLTxnError, err)
	}

	log.Info("Exec DDL succeeded", zap.String("sql", ddl.Query))
	return nil
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
		return cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}

	newMode = cyclic.RelaxSQLMode(oldMode)
	rows, err := s.db.QueryContext(ctx, fmt.Sprintf("SET sql_mode = '%s';", newMode))
	if err != nil {
		return cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	err = rows.Close()
	return cerror.WrapError(cerror.ErrMySQLQueryError, err)
}

var _ Sink = &mysqlSink{}

type sinkParams struct {
	workerCount         int
	maxTxnRow           int
	tidbTxnMode         string
	changefeedID        string
	captureAddr         string
	batchReplaceEnabled bool
	batchReplaceSize    int
	readTimeout         string
	writeTimeout        string
	enableOldValue      bool
	safeMode            bool
}

func (s *sinkParams) Clone() *sinkParams {
	clone := *s
	return &clone
}

var defaultParams = &sinkParams{
	workerCount:         defaultWorkerCount,
	maxTxnRow:           defaultMaxTxnRow,
	tidbTxnMode:         defaultTiDBTxnMode,
	batchReplaceEnabled: defaultBatchReplaceEnabled,
	batchReplaceSize:    defaultBatchReplaceSize,
	readTimeout:         defaultReadTimeout,
	writeTimeout:        defaultWriteTimeout,
	safeMode:            defaultSafeMode,
}

func checkTiDBVariable(ctx context.Context, db *sql.DB, variableName, defaultValue string) (string, error) {
	var name string
	var value string
	querySQL := fmt.Sprintf("show session variables like '%s';", variableName)
	err := db.QueryRowContext(ctx, querySQL).Scan(&name, &value)
	if err != nil && err != sql.ErrNoRows {
		errMsg := "fail to query session variable " + variableName
		return "", errors.Annotate(cerror.WrapError(cerror.ErrMySQLQueryError, err), errMsg)
	}
	// session variable works, use given default value
	if err == nil {
		return defaultValue, nil
	}
	// session variable not exists, return "" to ignore it
	return "", nil
}

func configureSinkURI(
	ctx context.Context,
	dsnCfg *dmysql.Config,
	tz *time.Location,
	params *sinkParams,
	testDB *sql.DB,
) (string, error) {
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.DBName = ""
	dsnCfg.InterpolateParams = true
	dsnCfg.MultiStatements = true
	dsnCfg.Params["time_zone"] = fmt.Sprintf(`"%s"`, tz.String())
	dsnCfg.Params["readTimeout"] = params.readTimeout
	dsnCfg.Params["writeTimeout"] = params.writeTimeout

	autoRandom, err := checkTiDBVariable(ctx, testDB, "allow_auto_random_explicit_insert", "1")
	if err != nil {
		return "", err
	}
	if autoRandom != "" {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = autoRandom
	}

	txnMode, err := checkTiDBVariable(ctx, testDB, "tidb_txn_mode", params.tidbTxnMode)
	if err != nil {
		return "", err
	}
	if txnMode != "" {
		dsnCfg.Params["tidb_txn_mode"] = txnMode
	}

	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("format dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(
	ctx context.Context,
	changefeedID model.ChangeFeedID,
	sinkURI *url.URL,
	filter *tifilter.Filter,
	replicaConfig *config.ReplicaConfig,
	opts map[string]string,
) (Sink, error) {
	var db *sql.DB
	params := defaultParams.Clone()

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if caddr, ok := opts[OptCaptureAddr]; ok {
		params.captureAddr = caddr
	}
	tz := util.TimezoneFromCtx(ctx)

	if sinkURI == nil {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("fail to open MySQL sink, empty URL")
	}
	scheme := strings.ToLower(sinkURI.Scheme)
	if _, ok := validSchemes[scheme]; !ok {
		return nil, cerror.ErrMySQLConnectionError.GenWithStack("can't create mysql sink with unsupported scheme: %s", scheme)
	}
	s := sinkURI.Query().Get("worker-count")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		if c > 0 {
			params.workerCount = c
		}
	}
	s = sinkURI.Query().Get("max-txn-row")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.maxTxnRow = c
	}
	s = sinkURI.Query().Get("tidb-txn-mode")
	if s != "" {
		if s == "pessimistic" || s == "optimistic" {
			params.tidbTxnMode = s
		} else {
			log.Warn("invalid tidb-txn-mode, should be pessimistic or optimistic, use optimistic as default")
		}
	}
	var tlsParam string
	if sinkURI.Query().Get("ssl-ca") != "" {
		credential := security.Credential{
			CAPath:   sinkURI.Query().Get("ssl-ca"),
			CertPath: sinkURI.Query().Get("ssl-cert"),
			KeyPath:  sinkURI.Query().Get("ssl-key"),
		}
		tlsCfg, err := credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Annotate(err, "fail to open MySQL connection")
		}
		name := "cdc_mysql_tls" + changefeedID
		err = dmysql.RegisterTLSConfig(name, tlsCfg)
		if err != nil {
			return nil, errors.Annotate(
				cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection")
		}
		tlsParam = "?tls=" + name
	}

	s = sinkURI.Query().Get("batch-replace-enable")
	if s != "" {
		enable, err := strconv.ParseBool(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.batchReplaceEnabled = enable
	}
	if params.batchReplaceEnabled && sinkURI.Query().Get("batch-replace-size") != "" {
		size, err := strconv.Atoi(sinkURI.Query().Get("batch-replace-size"))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.batchReplaceSize = size
	}

	// TODO: force safe mode in startup phase
	s = sinkURI.Query().Get("safe-mode")
	if s != "" {
		safeModeEnabled, err := strconv.ParseBool(s)
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		params.safeMode = safeModeEnabled
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

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, sinkURI.Hostname(), port, tlsParam)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
	}

	// create test db used for parameter detection
	if dsn.Params == nil {
		dsn.Params = make(map[string]string, 1)
	}
	dsn.Params["time_zone"] = fmt.Sprintf(`"%s"`, tz.String())
	testDB, err := sql.Open("mysql", dsn.FormatDSN())
	if err != nil {
		return nil, errors.Annotate(
			cerror.WrapError(cerror.ErrMySQLConnectionError, err), "fail to open MySQL connection when configuring sink")
	}
	defer testDB.Close()

	dsnStr, err = configureSinkURI(ctx, dsn, tz, params, testDB)
	if err != nil {
		return nil, errors.Trace(err)
	}
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

	sink := &mysqlSink{
		db:                              db,
		params:                          params,
		filter:                          filter,
		txnCache:                        common.NewUnresolvedTxnCache(),
		statistics:                      NewStatistics(ctx, "mysql", opts),
		metricConflictDetectDurationHis: metricConflictDetectDurationHis,
		metricBucketSizeCounters:        metricBucketSizeCounters,
		errCh:                           make(chan error, 1),
	}

	if val, ok := opts[mark.OptCyclicConfig]; ok {
		cfg := new(config.CyclicConfig)
		err := cfg.Unmarshal([]byte(val))
		if err != nil {
			return nil, cerror.WrapError(cerror.ErrMySQLInvalidConfig, err)
		}
		sink.cyclic = cyclic.NewCyclic(cfg)

		err = sink.adjustSQLMode(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	sink.execWaitNotifier = new(notify.Notifier)
	sink.resolvedNotifier = new(notify.Notifier)
	sink.createSinkWorkers(ctx)

	go sink.flushRowChangedEvents(ctx)

	return sink, nil
}

func (s *mysqlSink) createSinkWorkers(ctx context.Context) {
	s.workers = make([]*mysqlSinkWorker, s.params.workerCount)
	for i := range s.workers {
		receiver := s.execWaitNotifier.NewReceiver(defaultFlushInterval)
		worker := newMySQLSinkWorker(
			s.params.maxTxnRow, i, s.metricBucketSizeCounters[i], receiver, s.execDMLs)
		s.workers[i] = worker
		go func() {
			err := worker.run(ctx)
			if err != nil && errors.Cause(err) != context.Canceled {
				select {
				case s.errCh <- err:
				default:
				}
			}
		}()
	}
}

func (s *mysqlSink) notifyAndWaitExec(ctx context.Context) {
	s.execWaitNotifier.Notify()
	done := make(chan struct{})
	go func() {
		for _, w := range s.workers {
			w.waitAllTxnsExecuted()
		}
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

type mysqlSinkWorker struct {
	txnCh            chan *model.SingleTableTxn
	txnWg            sync.WaitGroup
	maxTxnRow        int
	bucket           int
	execDMLs         func(context.Context, []*model.RowChangedEvent, uint64, int) error
	metricBucketSize prometheus.Counter
	receiver         *notify.Receiver
	checkpointTs     uint64
}

func newMySQLSinkWorker(
	maxTxnRow int,
	bucket int,
	metricBucketSize prometheus.Counter,
	receiver *notify.Receiver,
	execDMLs func(context.Context, []*model.RowChangedEvent, uint64, int) error,
) *mysqlSinkWorker {
	return &mysqlSinkWorker{
		txnCh:            make(chan *model.SingleTableTxn, 1024),
		maxTxnRow:        maxTxnRow,
		bucket:           bucket,
		metricBucketSize: metricBucketSize,
		execDMLs:         execDMLs,
		receiver:         receiver,
	}
}

func (w *mysqlSinkWorker) waitAllTxnsExecuted() {
	w.txnWg.Wait()
}

func (w *mysqlSinkWorker) appendTxn(ctx context.Context, txn *model.SingleTableTxn) {
	if txn == nil {
		return
	}
	w.txnWg.Add(1)
	select {
	case <-ctx.Done():
		w.txnWg.Done()
	case w.txnCh <- txn:
	}
}

func (w *mysqlSinkWorker) run(ctx context.Context) (err error) {
	var (
		toExecRows   []*model.RowChangedEvent
		replicaID    uint64
		txnNum       int
		lastCommitTs uint64
	)

	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			err = cerror.ErrMySQLWorkerPanic.GenWithStack("mysql sink concurrent execute panic, stack: %v", string(buf))
			log.Error("mysql sink worker panic", zap.Reflect("r", r), zap.Stack("stack trace"))
			w.txnWg.Add(-1 * txnNum)
		}
	}()

	flushRows := func() error {
		if len(toExecRows) == 0 {
			return nil
		}
		rows := make([]*model.RowChangedEvent, len(toExecRows))
		copy(rows, toExecRows)
		err := w.execDMLs(ctx, rows, replicaID, w.bucket)
		if err != nil {
			w.txnWg.Add(-1 * txnNum)
			txnNum = 0
			return err
		}
		atomic.StoreUint64(&w.checkpointTs, lastCommitTs)
		toExecRows = toExecRows[:0]
		w.metricBucketSize.Add(float64(txnNum))
		w.txnWg.Add(-1 * txnNum)
		txnNum = 0
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(flushRows())
		case txn := <-w.txnCh:
			if txn == nil {
				return errors.Trace(flushRows())
			}
			if txn.ReplicaID != replicaID || len(toExecRows)+len(txn.Rows) > w.maxTxnRow {
				if err := flushRows(); err != nil {
					return errors.Trace(err)
				}
			}
			replicaID = txn.ReplicaID
			toExecRows = append(toExecRows, txn.Rows...)
			lastCommitTs = txn.CommitTs
			txnNum++
		case <-w.receiver.C:
			if err := flushRows(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (s *mysqlSink) Close() error {
	s.execWaitNotifier.Close()
	s.resolvedNotifier.Close()
	err := s.db.Close()
	return cerror.WrapError(cerror.ErrMySQLConnectionError, err)
}

func (s *mysqlSink) execDMLWithMaxRetries(
	ctx context.Context, dmls *preparedDMLs, maxRetries uint64, bucket int,
) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Fatal("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}
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
			failpoint.Inject("MySQLSinkHangLongTime", func() {
				time.Sleep(time.Hour)
			})
			err := s.statistics.RecordBatchExecution(func() (int, error) {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					return 0, checkTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
				}
				for i, query := range dmls.sqls {
					args := dmls.values[i]
					log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						return 0, checkTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
					}
				}
				if len(dmls.markSQL) != 0 {
					log.Debug("exec row", zap.String("sql", dmls.markSQL))
					if _, err := tx.ExecContext(ctx, dmls.markSQL); err != nil {
						return 0, checkTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
					}
				}
				if err = tx.Commit(); err != nil {
					return 0, checkTxnErr(cerror.WrapError(cerror.ErrMySQLTxnError, err))
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
		},
	)
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

		// Translate to UPDATE if old value is enabled, not in safe mode and is update event
		if translateToInsert && len(row.PreColumns) != 0 && len(row.Columns) != 0 {
			flushCacheDMLs()
			query, args = prepareUpdate(quoteTable, row.PreColumns, row.Columns)
			if query != "" {
				sqls = append(sqls, query)
				values = append(values, args)
				rowCount++
			}
			continue
		}

		// Case for delete event or update event
		// If old value is enabled and not in safe mode,
		// update will be translated to DELETE + INSERT(or REPLACE) SQL.
		if len(row.PreColumns) != 0 {
			flushCacheDMLs()
			query, args = prepareDelete(quoteTable, row.PreColumns)
			if query != "" {
				sqls = append(sqls, query)
				values = append(values, args)
				rowCount++
			}
		}

		// Case for insert event or update event
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
	failpoint.Inject("MySQLSinkExecDMLError", func() {
		// Add a delay to ensure the sink worker with `MySQLSinkHangLongTime`
		// failpoint injected is executed first.
		time.Sleep(time.Second * 2)
		failpoint.Return(errors.Trace(dmysql.ErrInvalidConn))
	})
	dmls := s.prepareDMLs(rows, replicaID, bucket)
	log.Debug("prepare DMLs", zap.Any("rows", rows), zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))
	if err := s.execDMLWithMaxRetries(ctx, dmls, defaultDMLMaxRetryTime, bucket); err != nil {
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
			cacheCount += 1
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

func prepareUpdate(quoteTable string, preCols, cols []*model.Column) (string, []interface{}) {
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
			builder.WriteString("`" + model.EscapeName(column) + "`=?")
		} else {
			builder.WriteString("`" + model.EscapeName(column) + "`=?,")
		}
	}

	builder.WriteString(" WHERE ")
	colNames, wargs := whereSlice(preCols)
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

func prepareDelete(quoteTable string, cols []*model.Column) (string, []interface{}) {
	var builder strings.Builder
	builder.WriteString("DELETE FROM " + quoteTable + " WHERE ")

	colNames, wargs := whereSlice(cols)
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

func whereSlice(cols []*model.Column) (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for _, col := range cols {
		if col == nil || !col.Flag.IsHandleKey() {
			continue
		}
		colNames = append(colNames, col.Name)
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
