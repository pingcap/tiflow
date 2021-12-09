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

package main

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/errno"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/dm/pkg/conn"
	tcontext "github.com/pingcap/ticdc/dm/pkg/context"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"github.com/pingcap/ticdc/dm/pkg/retry"
)

// dbConn holds a connection to a database and supports to reset the connection.
type dbConn struct {
	baseConn  *conn.BaseConn
	currDB    string // current database (will `USE` it when reseting the connection).
	resetFunc func(ctx context.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error)
}

// createDBConn creates a dbConn instance.
func createDBConn(ctx context.Context, db *conn.BaseDB, currDB string) (*dbConn, error) {
	c, err := db.GetBaseConn(ctx)
	if err != nil {
		return nil, err
	}

	return &dbConn{
		baseConn: c,
		currDB:   currDB,
		resetFunc: func(ctx context.Context, baseConn *conn.BaseConn) (*conn.BaseConn, error) {
			err2 := db.CloseBaseConn(baseConn)
			if err2 != nil {
				log.L().Warn("fail to close connection", zap.Error(err2))
			}
			return db.GetBaseConn(ctx)
		},
	}, nil
}

// resetConn resets the underlying connection.
func (c *dbConn) resetConn(ctx context.Context) error {
	baseConn, err := c.resetFunc(ctx, c.baseConn)
	if err != nil {
		return err
	}

	_, err = baseConn.ExecuteSQL(tcontext.NewContext(ctx, log.L()), nil, "chaos-cases", []string{fmt.Sprintf("USE %s", c.currDB)})
	if err != nil {
		return err
	}

	c.baseConn = baseConn
	return nil
}

// execSQLs executes SQL queries.
func (c *dbConn) execSQLs(ctx context.Context, queries ...string) error {
	params := retry.Params{
		RetryCount:         3,
		FirstRetryDuration: time.Second,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(_ int, err error) bool {
			if retry.IsConnectionError(err) || forceIgnoreExecSQLError(err) {
				// HACK: for some errors like `invalid connection`, `sql: connection is already closed`, we can ignore them just for testing.
				err = c.resetConn(ctx)
				return err == nil
			}
			return false
		},
	}

	_, _, err := c.baseConn.ApplyRetryStrategy(tcontext.NewContext(ctx, log.L()), params,
		func(tctx *tcontext.Context) (interface{}, error) {
			ret, err2 := c.baseConn.ExecuteSQLWithIgnoreError(tctx, nil, "chaos-cases", ignoreExecSQLError, queries)
			return ret, err2
		})
	return err
}

// execSQLs executes DDL queries.
func (c *dbConn) execDDLs(ctx context.Context, queries ...string) error {
	return c.execSQLs(ctx, queries...)
}

// dropDatabase drops the database if exists.
func dropDatabase(ctx context.Context, conn2 *dbConn, name string) error {
	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbutil.ColumnName(name))
	return conn2.execSQLs(ctx, query)
}

// createDatabase creates a database if not exists.
func createDatabase(ctx context.Context, conn2 *dbConn, name string) error {
	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbutil.ColumnName(name))
	return conn2.execSQLs(ctx, query)
}

func ignoreExecSQLError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrParse: // HACK: the query generated by `go-sqlsmith` may be invalid, so we just ignore them.
		return true
	case errno.ErrDupEntry: // HACK: we tolerate `invalid connection`, then `Duplicate entry` may be reported.
		return true
	case errno.ErrTooBigRowsize: // HACK: we tolerate `Error 1118: Row size too large. The maximum row size for the used table type, not counting BLOBs, is 65535`
		return true
	case errno.ErrCantDropFieldOrKey: // HACK: ignore error `Can't DROP '.*'; check that column/key exists`
		return true
	default:
		return false
	}
}

// forceIgnoreExecSQLError returns true for some errors which can be ignored ONLY in these tests.
func forceIgnoreExecSQLError(err error) bool {
	err = errors.Cause(err)
	switch err {
	case mysql.ErrInvalidConn:
		return true
	case sql.ErrConnDone:
		return true
	}
	return false
}
