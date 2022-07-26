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

package checker

import (
	"context"
	"database/sql"

	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type connAmountChecker struct {
	toCheckDB *conn.BaseDB
	stCfgs    []*config.SubTaskConfig

	getConfigConn func(stCfgs []*config.SubTaskConfig) int
	workerName    string
	unlimitedConn bool
}

func newConnAmountChecker(toCheckDB *conn.BaseDB, stCfgs []*config.SubTaskConfig, fn func(stCfgs []*config.SubTaskConfig) int, workerName string) connAmountChecker {
	return connAmountChecker{
		toCheckDB:     toCheckDB,
		stCfgs:        stCfgs,
		getConfigConn: fn,
		workerName:    workerName,
	}
}

func (c *connAmountChecker) check(ctx context.Context, checkerName string) *Result {
	result := &Result{
		Name:  checkerName,
		Desc:  "check if user-specified number of connection exceeds database's maximum",
		State: StateFailure,
	}
	baseConn, err := c.toCheckDB.GetBaseConn(ctx)
	//nolint:errcheck
	defer c.toCheckDB.CloseBaseConn(baseConn)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	var rows, processRows *sql.Rows
	rows, err = baseConn.QuerySQL(tcontext.NewContext(ctx, log.L()), "SHOW GLOBAL VARIABLES LIKE 'max_connections'")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	defer rows.Close()
	var (
		maxConn  int
		variable string
	)
	for rows.Next() {
		err = rows.Scan(&variable, &maxConn)
		if err != nil {
			markCheckError(result, err)
			return result
		}
	}
	if maxConn == 0 {
		c.unlimitedConn = true
	}
	processRows, err = baseConn.QuerySQL(tcontext.NewContext(ctx, log.L()), "SHOW PROCESSLIST")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	defer processRows.Close()
	usedConn := 0
	for processRows.Next() {
		usedConn++
	}
	usedConn -= 1 // exclude the connection used for show processlist
	log.L().Debug("connection checker", zap.Int("max_connections", maxConn), zap.Int("used_connections", usedConn))
	neededConn := c.getConfigConn(c.stCfgs)
	switch {
	case c.unlimitedConn:
		// zero max_connections means unlimited connections
		// we shouldn't report a warning.
		c.unlimitedConn = true
		result.State = StateSuccess
	case neededConn > maxConn:
		// nonzero max_connections and needed connections exceed max_connections
		// FYI: https://github.com/pingcap/tidb/pull/35453
		// currently, TiDB's max_connections is set to 0 representing unlimited connections,
		// while for MySQL, 0 is not a legal value (never retrieve from it).
		result.Errors = append(
			result.Errors,
			NewError(
				"database's max_connections: %d is less than the number %s needs: %d",
				maxConn,
				c.workerName,
				neededConn,
			),
		)
		result.Instruction = "set larger max_connections or reduce the pool size of dm"
		result.State = StateFailure
	default:
		// nonzero max_connections and needed connections are less than or equal to max_connections
		result.State = StateSuccess
		if maxConn-usedConn < neededConn {
			result.State = StateWarning
			result.Errors = append(
				result.Errors,
				NewError(
					"database's max_connections: %d, used_connections: %d, available_connections: %d is less than %s needs: %d",
					maxConn,
					usedConn,
					maxConn-usedConn,
					c.workerName,
					neededConn,
				),
			)
		}
	}
	return result
}

type LoaderConnAmountChecker struct {
	connAmountChecker
}

func NewLoaderConnAmountChecker(targetDB *conn.BaseDB, stCfgs []*config.SubTaskConfig) RealChecker {
	return &LoaderConnAmountChecker{
		connAmountChecker: newConnAmountChecker(targetDB, stCfgs, func(stCfgs []*config.SubTaskConfig) int {
			loaderConn := 0
			for _, stCfg := range stCfgs {
				// loader's worker and checkpoint (always keeps one db connection)
				loaderConn += stCfg.LoaderConfig.PoolSize + 1
			}
			return loaderConn
		}, "loader"),
	}
}

func (l *LoaderConnAmountChecker) Name() string {
	return "loader_conn_amount_checker"
}

func (l *LoaderConnAmountChecker) Check(ctx context.Context) *Result {
	result := l.check(ctx, l.Name())
	if !l.unlimitedConn && result.State == StateFailure {
		// if the max_connections is set as a specific number
		// and we failed because of the number connecions needed is smaller than max_connections
		for _, stCfg := range l.stCfgs {
			if stCfg.NeedUseLightning() {
				// if we're using lightning, this error should be omitted
				// because lightning doesn't need to keep connections while restoring.
				result.Errors = append(
					result.Errors,
					NewWarn("task precheck cannot accurately check the number of connection needed for Lightning, please set a sufficiently large connections for TiDB"),
				)
				result.State = StateWarning
				break
			}
		}
	}
	return result
}

func NewDumperConnAmountChecker(sourceDB *conn.BaseDB, dumperThreads int) RealChecker {
	return &DumperConnAmountChecker{
		connAmountChecker: newConnAmountChecker(sourceDB, nil, func(_ []*config.SubTaskConfig) int {
			// one for generating SQL, another for consistency control
			return dumperThreads + 2
		}, "dumper"),
	}
}

type DumperConnAmountChecker struct {
	connAmountChecker
}

func (d *DumperConnAmountChecker) Check(ctx context.Context) *Result {
	return d.check(ctx, d.Name())
}

func (d *DumperConnAmountChecker) Name() string {
	return "dumper_conn_amount_checker"
}
