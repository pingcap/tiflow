// Copyright 2019 PingCAP, Inc.
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

package loader

import (
	"database/sql"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	tmysql "github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/dm/pkg/terror"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"go.uber.org/zap"
)

// DBConn represents a live DB connection
// it's not thread-safe.
type DBConn struct {
	name     string
	sourceID string
	baseConn *conn.BaseConn

	// generate new BaseConn and close old one
	resetBaseConnFn func(*tcontext.Context, *conn.BaseConn) (*conn.BaseConn, error)
}

func (conn *DBConn) querySQL(ctx *tcontext.Context, query string, args ...interface{}) (*sql.Rows, error) {
	if conn == nil || conn.baseConn == nil {
		return nil, terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: time.Second,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(retryTime int, err error) bool {
			if retry.IsConnectionError(err) {
				err = conn.resetConn(ctx)
				if err != nil {
					ctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("query", utils.TruncateInterface(query, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				return true
			}
			if dbutil.IsRetryableError(err) {
				ctx.L().Warn("query statement", zap.Int("retry", retryTime),
					zap.String("query", utils.TruncateString(query, -1)),
					zap.String("argument", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				return true
			}
			return false
		},
	}

	ret, _, err := conn.baseConn.ApplyRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			ret, err := conn.baseConn.QuerySQL(ctx, query, args...)
			if err == nil {
				if ret.Err() != nil {
					return ret, ret.Err()
				}
				cost := time.Since(startTime)
				// duration seconds
				ds := cost.Seconds()
				queryHistogram.WithLabelValues(conn.name, conn.sourceID).Observe(ds)
				if ds > 1 {
					ctx.L().Warn("query statement too slow",
						zap.Duration("cost time", cost),
						zap.String("query", utils.TruncateString(query, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)))
				}
			}
			return ret, err
		})
	if err != nil {
		ctx.L().ErrorFilterContextCanceled("query statement failed after retry",
			zap.String("query", utils.TruncateString(query, -1)),
			zap.String("argument", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
		return nil, err
	}
	return ret.(*sql.Rows), nil
}

func (conn *DBConn) executeSQL(ctx *tcontext.Context, queries []string, args ...[]interface{}) error {
	if len(queries) == 0 {
		return nil
	}

	if conn == nil || conn.baseConn == nil {
		return terror.ErrDBUnExpect.Generate("database connection not valid")
	}

	params := retry.Params{
		RetryCount:         10,
		FirstRetryDuration: 2 * time.Second,
		BackoffStrategy:    retry.LinearIncrease,
		IsRetryableFn: func(retryTime int, err error) bool {
			tidbExecutionErrorCounter.WithLabelValues(conn.name, conn.sourceID).Inc()
			if retry.IsConnectionError(err) {
				err = conn.resetConn(ctx)
				if err != nil {
					ctx.L().Error("reset connection failed", zap.Int("retry", retryTime),
						zap.String("queries", utils.TruncateInterface(queries, -1)),
						zap.String("arguments", utils.TruncateInterface(args, -1)),
						log.ShortError(err))
					return false
				}
				return true
			}
			if dbutil.IsRetryableError(err) {
				ctx.L().Warn("execute statements", zap.Int("retry", retryTime),
					zap.String("queries", utils.TruncateInterface(queries, -1)),
					zap.String("arguments", utils.TruncateInterface(args, -1)),
					log.ShortError(err))
				return true
			}
			return false
		},
	}

	_, _, err := conn.baseConn.ApplyRetryStrategy(
		ctx,
		params,
		func(ctx *tcontext.Context) (interface{}, error) {
			startTime := time.Now()
			_, err := conn.baseConn.ExecuteSQL(ctx, stmtHistogram, conn.name, queries, args...)
			failpoint.Inject("LoadExecCreateTableFailed", func(val failpoint.Value) {
				errCode, err1 := strconv.ParseUint(val.(string), 10, 16)
				if err1 != nil {
					ctx.L().Fatal("failpoint LoadExecCreateTableFailed's value is invalid", zap.String("val", val.(string)))
				}

				if len(queries) == 1 && strings.Contains(queries[0], "CREATE TABLE") {
					err = &mysql.MySQLError{Number: uint16(errCode), Message: ""}
					ctx.L().Warn("executeSQL failed", zap.String("failpoint", "LoadExecCreateTableFailed"), zap.Error(err))
				}
			})
			if err == nil {
				cost := time.Since(startTime)
				// duration seconds
				ds := cost.Seconds()
				if ds > 1 {
					ctx.L().Warn("execute transaction too slow",
						zap.Duration("cost time", cost),
						zap.String("query", utils.TruncateInterface(queries, -1)),
						zap.String("argument", utils.TruncateInterface(args, -1)))
				}
			}
			return nil, err
		})
	if err != nil {
		ctx.L().ErrorFilterContextCanceled("execute statements failed after retry",
			zap.String("queries", utils.TruncateInterface(queries, -1)),
			zap.String("arguments", utils.TruncateInterface(args, -1)),
			log.ShortError(err))
	}

	return err
}

// resetConn reset one worker connection from specify *BaseDB.
func (conn *DBConn) resetConn(tctx *tcontext.Context) error {
	baseConn, err := conn.resetBaseConnFn(tctx, conn.baseConn)
	if err != nil {
		return err
	}
	conn.baseConn = baseConn
	return nil
}

func createConns(tctx *tcontext.Context, cfg *config.SubTaskConfig,
	name, sourceID string,
	workerCount int,
) (*conn.BaseDB, []*DBConn, error) {
	baseDB, err := conn.DefaultDBProvider.Apply(&cfg.To)
	if err != nil {
		return nil, nil, terror.WithScope(err, terror.ScopeDownstream)
	}
	conns := make([]*DBConn, 0, workerCount)
	for i := 0; i < workerCount; i++ {
		baseConn, err := baseDB.GetBaseConn(tctx.Context())
		if err != nil {
			terr := baseDB.Close()
			if terr != nil {
				tctx.L().Error("failed to close baseDB", zap.Error(terr))
			}
			return nil, nil, terror.WithScope(err, terror.ScopeDownstream)
		}
		resetBaseConnFn := func(tctx *tcontext.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error) {
			err := baseDB.CloseBaseConn(baseConn)
			if err != nil {
				tctx.L().Warn("failed to close baseConn in reset")
			}
			return baseDB.GetBaseConn(tctx.Context())
		}
		conns = append(conns, &DBConn{baseConn: baseConn, name: name, sourceID: sourceID, resetBaseConnFn: resetBaseConnFn})
	}
	return baseDB, conns, nil
}

func isErrDBExists(err error) bool {
	return utils.IsMySQLError(err, tmysql.ErrDBCreateExists)
}

func isErrTableExists(err error) bool {
	return utils.IsMySQLError(err, tmysql.ErrTableExists)
}

func isErrDupEntry(err error) bool {
	return utils.IsMySQLError(err, tmysql.ErrDupEntry)
}
