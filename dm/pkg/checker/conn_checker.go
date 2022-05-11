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

	"github.com/pingcap/tiflow/dm/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
)

type connAmountChecker struct {
	targetDB *conn.BaseDB
	stCfgs   []*config.SubTaskConfig

	getConfigConn func(stCfgs []*config.SubTaskConfig) int
	workerName    string
}

func newConnAmountChecker(targetDB *conn.BaseDB, stCfgs []*config.SubTaskConfig, fn func(stCfgs []*config.SubTaskConfig) int, workerName string) connAmountChecker {
	return connAmountChecker{
		targetDB:      targetDB,
		stCfgs:        stCfgs,
		getConfigConn: fn,
		workerName:    workerName,
	}
}

func (c *connAmountChecker) check(ctx context.Context, checkerName string) *Result {
	result := &Result{
		Name:  checkerName,
		Desc:  "check if user-specified amount of connection exceeds downstream's maximum",
		State: StateFailure,
	}
	baseConn, err := c.targetDB.GetBaseConn(ctx)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	var rows *sql.Rows
	rows, err = baseConn.QuerySQL(tcontext.NewContext(ctx, log.L()), "SHOW GLOBAL VARIABLES LIKE 'max_connections'")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	defer rows.Close()
	var (
		maxConn   int
		_variable string
	)
	for rows.Next() {
		err = rows.Scan(&_variable, &maxConn)
		if err != nil {
			markCheckError(result, err)
			return result
		}
	}
	neededConn := c.getConfigConn(c.stCfgs)
	if neededConn > maxConn {
		result.Errors = append(
			result.Errors,
			NewError("downstream's max_connections: %d is less than the amount %s needs: %d", maxConn, c.workerName, neededConn),
		)
		result.Instruction = "set larger max_connections or reduce the pool size of dm"
		result.State = StateFailure
	} else {
		result.State = StateSuccess
	}
	return result
}

type SyncerConnAmountChecker struct {
	connAmountChecker
}

func NewSyncerConnAmountCheker(targetDB *conn.BaseDB, stCfgs []*config.SubTaskConfig) RealChecker {
	return &SyncerConnAmountChecker{
		connAmountChecker: newConnAmountChecker(targetDB, stCfgs, func(stCfgs []*config.SubTaskConfig) int {
			syncerConn := 0
			for _, stCfg := range stCfgs {
				// 1. worker count
				// 2. checkpoint
				// 3. ddl connection
				// 4. downstream tracker
				// 5. shard group keeper
				// 6. online ddl
				syncerConn += stCfg.SyncerConfig.WorkerCount + 5
			}
			return syncerConn
		}, "syncer"),
	}
}

func (s *SyncerConnAmountChecker) Name() string {
	return "syncer_conn_amount_checker"
}

func (s *SyncerConnAmountChecker) Check(ctx context.Context) *Result {
	return s.check(ctx, s.Name())
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
	return l.check(ctx, l.Name())
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
