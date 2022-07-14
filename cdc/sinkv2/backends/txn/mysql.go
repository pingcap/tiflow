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

package txn

import (
	"database/sql"

	"github.com/pingcap/tiflow/cdc/sinkv2/metrics"
)

type preparedDMLs struct {
	sqls     []string
	values   [][]interface{}
	markSQL  string
	rowCount int
}

type sinkParams struct {
	workerCount         int
	maxTxnRow           int
	tidbTxnMode         string
	// changefeedID        model.ChangeFeedID
	batchReplaceEnabled bool
	batchReplaceSize    int
	readTimeout         string
	writeTimeout        string
	dialTimeout         string
	enableOldValue      bool
	safeMode            bool
	timezone            string
	tls                 string
}

type backend struct {
	db     *sql.DB
	params *sinkParams
    statistics *metrics.Statistics
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
	s.statistics.ObserveRows(rows...)
	dmls := s.prepareDMLs(rows, replicaID, bucket)
	log.Debug("prepare DMLs", zap.Any("rows", rows), zap.Strings("sqls", dmls.sqls), zap.Any("values", dmls.values))
	if err := s.execDMLWithMaxRetries(ctx, dmls, bucket); err != nil {
		log.Error("execute DMLs failed", zap.String("err", err.Error()))
		return errors.Trace(err)
	}
	return nil
}

// prepareDMLs converts model.RowChangedEvent list to query string list and args list
func (s *backend) prepareDMLs(rows []*model.RowChangedEvent, replicaID uint64, bucket int) *preparedDMLs {
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
	dmls.rowCount = rowCount
	return dmls
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
			zap.String("namespace", s.params.changefeedID.Namespace),
			zap.String("changefeed", s.params.changefeedID.ID),
			zap.Int("numOfRows", dmls.rowCount),
			zap.Int("bucket", bucket))
		return nil
	}, retry.WithBackoffBaseDelay(backoffBaseDelayInMs),
		retry.WithBackoffMaxDelay(backoffMaxDelayInMs),
		retry.WithMaxTries(defaultDMLMaxRetry),
		retry.WithIsRetryableErr(isRetryableDMLError))
}
