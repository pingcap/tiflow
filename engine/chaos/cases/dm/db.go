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

package dm

import (
	"context"
	"fmt"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/pingcap/tidb/pkg/errno"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/dm/pkg/retry"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

type dbConn struct {
	db     *conn.BaseDB
	con    *conn.BaseConn
	currDB string
}

func newDBConn(ctx context.Context, cfg conn.ScopedDBConfig, currDB string) (*dbConn, error) {
	db, err := conn.DefaultDBProvider.Apply(cfg)
	if err != nil {
		return nil, err
	}
	con, err := db.GetBaseConn(ctx)
	if err != nil {
		return nil, err
	}

	return &dbConn{
		db:     db,
		con:    con,
		currDB: currDB,
	}, nil
}

func (dc *dbConn) resetConn(ctx context.Context) error {
	err := dc.db.ForceCloseConn(dc.con)
	if err != nil {
		log.L().Warn("fail to close connection", zap.Error(err))
	}
	dc.con, err = dc.db.GetBaseConn(ctx)
	if err != nil {
		return err
	}
	_, err = dc.con.ExecuteSQL(tcontext.NewContext(ctx, log.L()), nil, "chaos-cases", []string{fmt.Sprintf("USE %s", dc.currDB)})
	return err
}

func ignoreExecSQLError(err error) bool {
	err = errors.Cause(err) // check the original error
	mysqlErr, ok := err.(*mysql.MySQLError)
	if !ok {
		return false
	}

	switch mysqlErr.Number {
	case errno.ErrDupEntry: // HACK: we tolerate `invalid connection`, then `Duplicate entry` may be reported.
		return true
	default:
		return false
	}
}

func (dc *dbConn) ExecuteSQLs(queries ...string) (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	params := retry.Params{
		RetryCount:         3,
		FirstRetryDuration: time.Second,
		BackoffStrategy:    retry.Stable,
		IsRetryableFn: func(_ int, err error) bool {
			if retry.IsConnectionError(err) {
				// HACK: for some errors like `invalid connection`, `sql: connection is already closed`, we can ignore them just for testing.
				err = dc.resetConn(ctx)
				return err == nil
			}
			return false
		},
	}

	ret, _, err := dc.con.ApplyRetryStrategy(tcontext.NewContext(ctx, log.L()), params,
		func(tctx *tcontext.Context) (interface{}, error) {
			ret, err2 := dc.con.ExecuteSQLWithIgnoreError(tctx, nil, "chaos-cases", ignoreExecSQLError, queries)
			return ret, err2
		})
	return ret.(int), err
}
