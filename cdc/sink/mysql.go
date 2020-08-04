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
	"github.com/pingcap/ticdc/cdc/sink/common"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
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
	"golang.org/x/sync/errgroup"
)

const (
	defaultWorkerCount     = 16
	defaultMaxTxnRow       = 256
	defaultDMLMaxRetryTime = 8
	defaultDDLMaxRetryTime = 20
	defaultTiDBTxnMode     = "optimistic"
	defaultFlushInterval   = time.Millisecond * 50
)

type mysqlSink struct {
	db     *sql.DB
	params *sinkParams

	filter *filter.Filter
	cyclic *cyclic.Cyclic

	txnCache *common.UnresolvedTxnCache
	workers  []*mysqlSinkWorker
	notifier *notify.Notifier
	errCh    chan error

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

func (s *mysqlSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	resolvedTxnsMap := s.txnCache.Resolved(resolvedTs)
	if len(resolvedTxnsMap) == 0 {
		s.txnCache.UpdateCheckpoint(resolvedTs)
		return nil
	}

	if s.cyclic != nil {
		// Filter rows if it is origined from downstream.
		cyclic.FilterAndReduceTxns(resolvedTxnsMap, s.cyclic.FilterReplicaID(), s.cyclic.ReplicaID())
	}

	if err := s.concurrentExec(ctx, resolvedTxnsMap); err != nil {
		return errors.Trace(err)
	}
	s.txnCache.UpdateCheckpoint(resolvedTs)
	return nil
}

func (s *mysqlSink) EmitCheckpointTs(ctx context.Context, ts uint64) error {
	// do nothing
	return nil
}

func (s *mysqlSink) EmitDDLEvent(ctx context.Context, ddl *model.DDLEvent) error {
	if s.filter.ShouldIgnoreDDLEvent(ddl.StartTs, ddl.TableInfo.Schema, ddl.TableInfo.Table) {
		log.Info(
			"DDL event ignored",
			zap.String("query", ddl.Query),
			zap.Uint64("startTs", ddl.StartTs),
			zap.Uint64("commitTs", ddl.CommitTs),
		)
		return errors.Trace(model.ErrorDDLEventIgnored)
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

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.TableInfo.Schema)+";")
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

type sinkParams struct {
	workerCount  int
	maxTxnRow    int
	tidbTxnMode  string
	changefeedID string
	captureAddr  string
}

var defaultParams = &sinkParams{
	workerCount: defaultWorkerCount,
	maxTxnRow:   defaultMaxTxnRow,
	tidbTxnMode: defaultTiDBTxnMode,
}

func configureSinkURI(ctx context.Context, dsnCfg *dmysql.Config, tz *time.Location, params *sinkParams) (string, error) {
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
	log.Debug("Opened connection to configure some tidb special parameters")

	var variableName string
	var autoRandomInsertEnabled string
	queryStr := "show session variables like 'allow_auto_random_explicit_insert';"
	err = testDB.QueryRowContext(ctx, queryStr).Scan(&variableName, &autoRandomInsertEnabled)
	if err != nil && err != sql.ErrNoRows {
		return "", errors.Annotate(err, "fail to query sink for support of auto-random")
	}
	if err == nil && (autoRandomInsertEnabled == "off" || autoRandomInsertEnabled == "0") {
		dsnCfg.Params["allow_auto_random_explicit_insert"] = "1"
		log.Debug("Set allow_auto_random_explicit_insert to 1")
	}

	var txnMode string
	queryStr = "show session variables like 'tidb_txn_mode';"
	err = testDB.QueryRowContext(ctx, queryStr).Scan(&variableName, &txnMode)
	if err != nil && err != sql.ErrNoRows {
		return "", errors.Annotate(err, "fail to query sink for txn mode")
	}
	if err == nil {
		dsnCfg.Params["tidb_txn_mode"] = params.tidbTxnMode
	}

	dsnClone := dsnCfg.Clone()
	dsnClone.Passwd = "******"
	log.Info("sink uri is configured", zap.String("format dsn", dsnClone.FormatDSN()))

	return dsnCfg.FormatDSN(), nil
}

// newMySQLSink creates a new MySQL sink using schema storage
func newMySQLSink(ctx context.Context, changefeedID model.ChangeFeedID, sinkURI *url.URL, filter *tifilter.Filter, opts map[string]string) (Sink, error) {
	var db *sql.DB
	params := defaultParams

	if cid, ok := opts[OptChangefeedID]; ok {
		params.changefeedID = cid
	}
	if caddr, ok := opts[OptCaptureAddr]; ok {
		params.captureAddr = caddr
	}
	tz := util.TimezoneFromCtx(ctx)

	if sinkURI == nil {
		return nil, errors.New("fail to open MySQL sink, empty URL")
	}
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
		if c > 0 {
			params.workerCount = c
		}
	}
	s = sinkURI.Query().Get("max-txn-row")
	if s != "" {
		c, err := strconv.Atoi(s)
		if err != nil {
			return nil, errors.Trace(err)
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
			return nil, errors.Annotate(err, "fail to open MySQL connection")
		}
		tlsParam = "?tls=" + name
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

	dsnStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, sinkURI.Hostname(), port, tlsParam)
	dsn, err := dmysql.ParseDSN(dsnStr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	dsnStr, err = configureSinkURI(ctx, dsn, tz, params)
	if err != nil {
		return nil, errors.Trace(err)
	}
	db, err = sql.Open("mysql", dsnStr)
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
			return nil, errors.Trace(err)
		}
		sink.cyclic = cyclic.NewCyclic(cfg)

		err = sink.adjustSQLMode(ctx)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}

	sink.notifier = new(notify.Notifier)
	sink.createSinkWorkers(ctx)

	return sink, nil
}

func (s *mysqlSink) createSinkWorkers(ctx context.Context) {
	s.workers = make([]*mysqlSinkWorker, s.params.workerCount)
	for i := range s.workers {
		receiver := s.notifier.NewReceiver(defaultFlushInterval)
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

func (s *mysqlSink) notifyAndWaitExec() {
	s.notifier.Notify()
	for _, w := range s.workers {
		w.waitAllTxnsExecuted()
	}
}

func (s *mysqlSink) concurrentExec(ctx context.Context, txnsGroup map[model.TableName][]*model.Txn) error {
	errg, ctx := errgroup.WithContext(ctx)
	ch := make(chan struct{}, 1)
	errg.Go(func() error {
		s.dispatchAndExecTxns(ctx, txnsGroup)
		ch <- struct{}{}
		return nil
	})
	errg.Go(func() error {
		select {
		case <-ctx.Done():
			return nil
		case <-ch:
			return nil
		case err := <-s.errCh:
			return err
		}
	})
	return errg.Wait()
}

func (s *mysqlSink) dispatchAndExecTxns(ctx context.Context, txnsGroup map[model.TableName][]*model.Txn) {
	nWorkers := s.params.workerCount
	causality := newCausality()
	rowsChIdx := 0

	sendFn := func(txn *model.Txn, idx int) {
		causality.add(txn.Keys, idx)
		s.workers[idx].appendTxn(ctx, txn)
	}
	resolveConflict := func(txn *model.Txn) {
		if conflict, idx := causality.detectConflict(txn.Keys); conflict {
			if idx >= 0 {
				sendFn(txn, idx)
				return
			}
			s.notifyAndWaitExec()
			causality.reset()
		}
		sendFn(txn, rowsChIdx)
		rowsChIdx++
		rowsChIdx = rowsChIdx % nWorkers
	}
	for _, txns := range txnsGroup {
		for _, txn := range txns {
			startTime := time.Now()
			resolveConflict(txn)
			s.metricConflictDetectDurationHis.Observe(time.Since(startTime).Seconds())
		}
	}
	s.notifyAndWaitExec()
}

type mysqlSinkWorker struct {
	txnCh            chan *model.Txn
	txnWg            sync.WaitGroup
	maxTxnRow        int
	bucket           int
	execDMLs         func(context.Context, []*model.RowChangedEvent, uint64, int) error
	metricBucketSize prometheus.Counter
	receiver         *notify.Receiver
}

func newMySQLSinkWorker(
	maxTxnRow int,
	bucket int,
	metricBucketSize prometheus.Counter,
	receiver *notify.Receiver,
	execDMLs func(context.Context, []*model.RowChangedEvent, uint64, int) error,
) *mysqlSinkWorker {
	return &mysqlSinkWorker{
		txnCh:            make(chan *model.Txn, 1024),
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

func (w *mysqlSinkWorker) appendTxn(ctx context.Context, txn *model.Txn) {
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
		toExecRows []*model.RowChangedEvent
		replicaID  uint64
		txnNum     int
	)

	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 4096)
			stackSize := runtime.Stack(buf, false)
			buf = buf[:stackSize]
			err = errors.Errorf("mysql sink concurrent execute panic, stack: %v", string(buf))
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
			txnNum++
		case <-w.receiver.C:
			if err := flushRows(); err != nil {
				return errors.Trace(err)
			}
		}
	}
}

func (s *mysqlSink) Close() error {
	s.notifier.Close()
	return s.db.Close()
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
			err := s.statistics.RecordBatchExecution(func() (int, error) {
				tx, err := s.db.BeginTx(ctx, nil)
				if err != nil {
					return 0, checkTxnErr(errors.Trace(err))
				}
				for i, query := range dmls.sqls {
					args := dmls.values[i]
					log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
					if _, err := tx.ExecContext(ctx, query, args...); err != nil {
						return 0, checkTxnErr(errors.Trace(err))
					}
				}
				if len(dmls.markSQL) != 0 {
					log.Debug("exec row", zap.String("sql", dmls.markSQL))
					if _, err := tx.ExecContext(ctx, dmls.markSQL); err != nil {
						return 0, checkTxnErr(errors.Trace(err))
					}
				}
				if err = tx.Commit(); err != nil {
					return 0, checkTxnErr(errors.Trace(err))
				}
				return len(dmls.sqls), nil
			})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("Exec Rows succeeded",
				zap.String("changefeed", s.params.changefeedID),
				zap.Int("num of Rows", len(dmls.sqls)),
				zap.Int("bucket", bucket))
			return nil
		},
	)
}

type preparedDMLs struct {
	sqls    []string
	values  [][]interface{}
	markSQL string
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent, replicaID uint64, bucket int) (*preparedDMLs, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		var query string
		var args []interface{}
		var err error
		// TODO(leoppro): using `UPDATE` instead of `REPLACE` if the old value is enabled
		if len(row.PreColumns) != 0 {
			query, args, err = prepareDelete(row.Table.Schema, row.Table.Table, row.PreColumns)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sqls = append(sqls, query)
			values = append(values, args)
		}
		if len(row.Columns) != 0 {
			query, args, err = prepareReplace(row.Table.Schema, row.Table.Table, row.Columns)
			if err != nil {
				return nil, errors.Trace(err)
			}
			sqls = append(sqls, query)
			values = append(values, args)
		}
	}
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
	}
	return dmls, nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent, replicaID uint64, bucket int) error {
	dmls, err := s.prepareDMLs(rows, replicaID, bucket)
	if err != nil {
		return errors.Trace(err)
	}
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

func prepareReplace(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for k, v := range cols {
		if v.Flag.IsGeneratedColumn() {
			continue
		}
		columnNames = append(columnNames, k)
		args = append(args, v.Value)
	}

	colList := "(" + buildColumnList(columnNames) + ")"
	tblName := quotes.QuoteSchema(schema, table)
	builder.WriteString("REPLACE INTO " + tblName + colList + " VALUES ")
	builder.WriteString("(" + model.HolderString(len(columnNames)) + ");")

	return builder.String(), args, nil
}

func prepareDelete(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	builder.WriteString("DELETE FROM " + quotes.QuoteSchema(schema, table) + " WHERE")

	colNames, wargs := whereSlice(cols)
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
	return sql, args, nil
}

func whereSlice(cols map[string]*model.Column) (colNames []string, args []interface{}) {
	// Try to use unique key values when available
	for colName, col := range cols {
		if !col.Flag.IsHandleKey() {
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
		b.WriteString(quotes.QuoteName(name))

	}

	return b.String()
}
