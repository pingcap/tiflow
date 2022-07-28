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
	"net"
	"net/url"
	"strconv"
	"strings"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/parser/charset"
	"github.com/pingcap/tidb/parser/mysql"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sinkv2/eventsink"
	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
	dmutils "github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/quotes"
	"github.com/pingcap/tiflow/pkg/retry"
)

const (
	backoffBaseDelayInMs = 500       // 500ms
	backoffMaxDelayInMs  = 60 * 1000 // 60s

	// Max interval for flushing transactions to the downstream.
	maxFlushInterval = time.Millisecond * time.Duration(100)
)

var (
	defaultDMLMaxRetry uint64 = 8
	defaultDDLMaxRetry uint64 = 20
)

type mysqlSink struct {
	changefeedID   model.ChangeFeedID
	db             *sql.DB
	params         *sinkParams
	forceReplicate bool
	enableOldValue bool
	statistics     *metrics.Statistics

	events []*eventsink.TxnCallbackableEvent
	rows   int
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
	db.SetMaxIdleConns(params.workerCount)
	db.SetMaxOpenConns(params.workerCount)

	sink := &mysqlSink{
		changefeedID:   changefeedID,
		db:             db,
		params:         params,
		forceReplicate: replicaConfig.ForceReplicate,
		enableOldValue: replicaConfig.EnableOldValue,
		statistics:     metrics.NewStatistics(ctx, metrics.SinkTypeDB),
	}

	log.Info("mysql backend is created",
		zap.String("changefeedID", fmt.Sprintf("%s.%s", changefeedID.Namespace, changefeedID.ID)),
		zap.Bool("forceReplicate", sink.forceReplicate),
		zap.Bool("enableOldValue", sink.enableOldValue))
	return sink, nil
}

func (s *mysqlSink) OnTxnEvent(event *eventsink.TxnCallbackableEvent) (needFlush bool) {
	if event.Event == nil {
		return true
	}
	s.events = append(s.events, event)
	s.rows += len(event.Event.Rows)
	return event.Event.FinishWg != nil || s.rows >= s.params.maxTxnRow
}

func (s *mysqlSink) Flush(ctx context.Context) (err error) {
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

	for _, event := range s.events {
		s.statistics.ObserveRows(event.Event.Rows...)
	}

	dmls := s.prepareDMLs()
	log.Debug("prepare DMLs", zap.Any("rows", s.rows),
		zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))

	if err := s.execDMLWithMaxRetries(ctx, dmls); err != nil {
		log.Error("execute DMLs failed", zap.String("err", err.Error()))
		return errors.Trace(err)
	}

	s.events = s.events[:0]
	s.rows = 0
	return
}

func (s *mysqlSink) Close() (err error) {
	if s.db != nil {
		err = s.db.Close()
		s.db = nil
	}
	return
}

func (s *mysqlSink) MaxFlushInterval() time.Duration {
	return maxFlushInterval
}

func querySQLMode(ctx context.Context, db *sql.DB) (sqlMode string, err error) {
	row := db.QueryRowContext(ctx, "SELECT @@SESSION.sql_mode;")
	err = row.Scan(&sqlMode)
	if err != nil {
		err = cerror.WrapError(cerror.ErrMySQLQueryError, err)
	}
	return
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

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *mysqlSink) prepareDMLs() *preparedDMLs {
	startTsSet := make(map[uint64]struct{}, s.rows)
	sqls := make([]string, 0, s.rows)
	values := make([][]interface{}, 0, s.rows)
	callbacks := make([]eventsink.CallbackFunc, 0, len(s.events))
	replaces := make(map[string][][]interface{})

	// flush cached batch replace or insert, to keep the sequence of DMLs
	flushCacheDMLs := func() {
		if s.params.batchReplaceEnabled && len(replaces) > 0 {
			replaceSqls, replaceValues := reduceReplace(replaces, s.params.batchReplaceSize)
			sqls = append(sqls, replaceSqls...)
			values = append(values, replaceValues...)
			replaces = make(map[string][][]interface{})
		}
	}

	// translateToInsert control the update and insert behavior
	translateToInsert := s.enableOldValue && !s.params.safeMode
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
			var query string
			var args []interface{}
			quoteTable := quotes.QuoteSchema(row.Table.Schema, row.Table.Table)

			startTsSet[row.StartTs] = struct{}{}

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

	startTs := make([]uint64, 0, len(startTsSet))
	for k := range startTsSet {
		startTs = append(startTs, k)
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

func logDMLTxnErr(
	err error, start time.Time, changefeed model.ChangeFeedID,
	query string, count int, startTs []model.Ts,
) error {
	if isRetryableDMLError(err) {
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

func (s *mysqlSink) execDMLWithMaxRetries(ctx context.Context, dmls *preparedDMLs) error {
	if len(dmls.sqls) != len(dmls.values) {
		log.Panic("unexpected number of sqls and values",
			zap.Strings("sqls", dmls.sqls),
			zap.Any("values", dmls.values))
	}

	start := time.Now()
	return retry.Do(ctx, func() error {
		failpoint.Inject("MySQLSinkTxnRandomError", func() {
			failpoint.Return(
				logDMLTxnErr(
					errors.Trace(driver.ErrBadConn),
					start, s.changefeedID, "failpoint", 0, nil))
		})
		failpoint.Inject("MySQLSinkHangLongTime", func() {
			time.Sleep(time.Hour)
		})

		err := s.statistics.RecordBatchExecution(func() (int, error) {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				return 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeedID, "BEGIN", dmls.rowCount, dmls.startTs)
			}

			for i, query := range dmls.sqls {
				args := dmls.values[i]
				log.Debug("exec row", zap.String("sql", query), zap.Any("args", args))
				if _, err := tx.ExecContext(ctx, query, args...); err != nil {
					err := logDMLTxnErr(
						cerror.WrapError(cerror.ErrMySQLTxnError, err),
						start, s.changefeedID, query, dmls.rowCount, dmls.startTs)
					if rbErr := tx.Rollback(); rbErr != nil {
						log.Warn("failed to rollback txn", zap.Error(rbErr))
					}
					return 0, err
				}
			}

			if err = tx.Commit(); err != nil {
				return 0, logDMLTxnErr(
					cerror.WrapError(cerror.ErrMySQLTxnError, err),
					start, s.changefeedID, "COMMIT", dmls.rowCount, dmls.startTs)
			}

			for _, callback := range dmls.callbacks {
				callback()
			}
			return dmls.rowCount, nil
		})
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("Exec Rows succeeded",
			zap.String("namespace", s.changefeedID.Namespace),
			zap.String("changefeed", s.changefeedID.ID),
			zap.Int("numOfRows", dmls.rowCount))
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithMaxTries(defaultDMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}

type preparedDMLs struct {
	startTs   []model.Ts
	sqls      []string
	values    [][]interface{}
	callbacks []eventsink.CallbackFunc
	rowCount  int
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
