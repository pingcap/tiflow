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

	"github.com/cenkalti/backoff"
	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/parser/terror"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/config"
	"github.com/pingcap/ticdc/pkg/cyclic"
	"github.com/pingcap/ticdc/pkg/cyclic/mark"
	"github.com/pingcap/ticdc/pkg/filter"
	tifilter "github.com/pingcap/ticdc/pkg/filter"
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
)

type mysqlSink struct {
	db           *sql.DB
	checkpointTs uint64
	params       *sinkParams

	filter *filter.Filter
	cyclic *cyclic.Cyclic

	unresolvedTxnsMu sync.Mutex
	unresolvedTxns   map[model.TableName][]*model.Txn

	statistics *Statistics

	// metrics used by mysql sink only
	metricConflictDetectDurationHis prometheus.Observer
	metricBucketSizeCounters        []prometheus.Counter
}

func (s *mysqlSink) EmitRowChangedEvents(ctx context.Context, rows ...*model.RowChangedEvent) error {
	s.unresolvedTxnsMu.Lock()
	defer s.unresolvedTxnsMu.Unlock()
	for _, row := range rows {
		if s.filter.ShouldIgnoreDMLEvent(row.StartTs, row.Table.Schema, row.Table.Table) {
			log.Info("Row changed event ignored", zap.Uint64("start-ts", row.StartTs))
			continue
		}
		key := *row.Table
		txns := s.unresolvedTxns[key]
		if len(txns) == 0 || txns[len(txns)-1].StartTs != row.StartTs {
			// fail-fast check
			if len(txns) != 0 && txns[len(txns)-1].CommitTs > row.CommitTs {
				log.Fatal("the commitTs of the emit row is less than the received row",
					zap.Stringer("table", row.Table),
					zap.Uint64("emit row startTs", row.StartTs),
					zap.Uint64("emit row commitTs", row.CommitTs),
					zap.Uint64("last received row startTs", txns[len(txns)-1].StartTs),
					zap.Uint64("last received row commitTs", txns[len(txns)-1].CommitTs))
			}
			txns = append(txns, &model.Txn{
				StartTs:  row.StartTs,
				CommitTs: row.CommitTs,
			})
			s.unresolvedTxns[key] = txns
		}
		txns[len(txns)-1].Append(row)
	}
	return nil
}

func (s *mysqlSink) FlushRowChangedEvents(ctx context.Context, resolvedTs uint64) error {
	if resolvedTs <= atomic.LoadUint64(&s.checkpointTs) {
		return nil
	}

	defer s.statistics.PrintStatus()

	s.unresolvedTxnsMu.Lock()
	if len(s.unresolvedTxns) == 0 {
		atomic.StoreUint64(&s.checkpointTs, resolvedTs)
		s.unresolvedTxnsMu.Unlock()
		return nil
	}

	_, resolvedTxnsMap := splitResolvedTxn(resolvedTs, s.unresolvedTxns)
	s.unresolvedTxnsMu.Unlock()

	if len(resolvedTxnsMap) == 0 {
		atomic.StoreUint64(&s.checkpointTs, resolvedTs)
		return nil
	}

	if s.cyclic != nil {
		// Filter rows if it is origined from downstream.
		cyclic.FilterAndReduceTxns(resolvedTxnsMap, s.cyclic.FilterReplicaID(), s.cyclic.ReplicaID())
	}

	if err := s.concurrentExec(ctx, resolvedTxnsMap); err != nil {
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
		return errors.Trace(model.ErrorDDLEventIgnored)
	}
	err := s.execDDLWithMaxRetries(ctx, ddl, defaultDDLMaxRetryTime)
	return errors.Trace(err)
}

// Initialize is no-op for Mysql sink
func (s *mysqlSink) Initialize(ctx context.Context, tableInfo []*model.TableInfo) error {
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
	shouldSwitchDB := len(ddl.Schema) > 0 && ddl.Type != timodel.ActionCreateSchema

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Trace(err)
	}

	if shouldSwitchDB {
		_, err = tx.ExecContext(ctx, "USE "+quotes.QuoteName(ddl.Schema)+";")
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

func splitResolvedTxn(
	resolvedTs uint64, unresolvedTxns map[model.TableName][]*model.Txn,
) (minTs uint64, resolvedRowsMap map[model.TableName][]*model.Txn) {
	resolvedRowsMap = make(map[model.TableName][]*model.Txn, len(unresolvedTxns))
	minTs = resolvedTs
	for key, txns := range unresolvedTxns {
		i := sort.Search(len(txns), func(i int) bool {
			return txns[i].CommitTs > resolvedTs
		})
		if i == 0 {
			continue
		}
		var resolvedTxns []*model.Txn
		if i == len(txns) {
			resolvedTxns = txns
			delete(unresolvedTxns, key)
		} else {
			resolvedTxns = txns[:i]
			unresolvedTxns[key] = txns[i:]
		}
		resolvedRowsMap[key] = resolvedTxns

		if len(resolvedTxns) > 0 && resolvedTxns[0].CommitTs < minTs {
			minTs = resolvedTxns[0].CommitTs
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
	if err == nil && autoRandomInsertEnabled == "off" {
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
		unresolvedTxns:                  make(map[model.TableName][]*model.Txn),
		params:                          params,
		filter:                          filter,
		statistics:                      NewStatistics("mysql", opts),
		metricConflictDetectDurationHis: metricConflictDetectDurationHis,
		metricBucketSizeCounters:        metricBucketSizeCounters,
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

	return sink, nil
}

func (s *mysqlSink) concurrentExec(ctx context.Context, txnsGroup map[model.TableName][]*model.Txn) error {
	nWorkers := s.params.workerCount
	workers := make([]*mysqlSinkWorker, nWorkers)
	errg, ctx := errgroup.WithContext(ctx)
	for i := 0; i < nWorkers; i++ {
		i := i
		workers[i] = newMySQLSinkWorker(s.params.maxTxnRow, i, s.metricBucketSizeCounters[i], s.execDMLs)
		errg.Go(func() error {
			return workers[i].run(ctx)
		})
	}
	causality := newCausality()
	rowsChIdx := 0

	sendFn := func(txn *model.Txn, idx int) {
		causality.add(txn.Keys, idx)
		workers[idx].appendTxn(ctx, txn)
	}
	resolveConflict := func(txn *model.Txn) {
		if conflict, idx := causality.detectConflict(txn.Keys); conflict {
			if idx >= 0 {
				sendFn(txn, idx)
				return
			}
			for _, w := range workers {
				w.waitAllTxnsExecuted()
			}
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
	for _, w := range workers {
		w.waitAndClose()
	}
	return errg.Wait()
}

type mysqlSinkWorker struct {
	txnCh            chan *model.Txn
	txnWg            sync.WaitGroup
	maxTxnRow        int
	bucket           int
	execDMLs         func(context.Context, []*model.RowChangedEvent, uint64, int) error
	metricBucketSize prometheus.Counter
}

func newMySQLSinkWorker(
	maxTxnRow int,
	bucket int,
	metricBucketSize prometheus.Counter,
	execDMLs func(context.Context, []*model.RowChangedEvent, uint64, int) error,
) *mysqlSinkWorker {
	return &mysqlSinkWorker{
		txnCh:            make(chan *model.Txn, 1024),
		maxTxnRow:        maxTxnRow,
		bucket:           bucket,
		metricBucketSize: metricBucketSize,
		execDMLs:         execDMLs,
	}
}

func (w *mysqlSinkWorker) waitAllTxnsExecuted() {
	w.txnWg.Wait()
}

func (w *mysqlSinkWorker) waitAndClose() {
	w.waitAllTxnsExecuted()
	close(w.txnCh)
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

	lastExecTime := time.Now()
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
		lastExecTime = time.Now()
		return nil
	}

	t := time.NewTicker(50 * time.Millisecond)
	defer t.Stop()
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
		case <-t.C:
			if time.Since(lastExecTime) < time.Millisecond*100 {
				continue
			}
			if err := flushRows(); err != nil {
				return errors.Trace(err)
			}
		}
	}
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
func (s *mysqlSink) prepareDMLs(rows []*model.RowChangedEvent, replicaID uint64, bucket int) ([]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(rows))
	values := make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		var query string
		var args []interface{}
		var err error
		if row.Delete {
			query, args, err = prepareDelete(row.Table.Schema, row.Table.Table, row.Columns)
		} else {
			query, args, err = prepareReplace(row.Table.Schema, row.Table.Table, row.Columns)
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		sqls = append(sqls, query)
		values = append(values, args)
	}
	if s.cyclic != nil && len(rows) > 0 {
		// Write mark table with the current replica ID.
		row := rows[0]
		updateMark := s.cyclic.UdpateSourceTableCyclicMark(
			row.Table.Schema, row.Table.Table, uint64(bucket), replicaID, row.StartTs)
		sqls = append(sqls, updateMark)
		values = append(values, nil)
	}
	return sqls, values, nil
}

func (s *mysqlSink) execDMLs(ctx context.Context, rows []*model.RowChangedEvent, replicaID uint64, bucket int) error {
	sqls, values, err := s.prepareDMLs(rows, replicaID, bucket)
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

func prepareReplace(schema, table string, cols map[string]*model.Column) (string, []interface{}, error) {
	var builder strings.Builder
	columnNames := make([]string, 0, len(cols))
	args := make([]interface{}, 0, len(cols))
	for k, v := range cols {
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
		b.WriteString(quotes.QuoteName(name))

	}

	return b.String()
}
