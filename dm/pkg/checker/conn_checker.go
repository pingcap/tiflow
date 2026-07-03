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

	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/config"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	tcontext "github.com/pingcap/tiflow/dm/pkg/context"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type connNumberChecker struct {
	toCheckDB *conn.BaseDB
	stCfgs    []*config.SubTaskConfig

	getConfigConn func(stCfgs []*config.SubTaskConfig) int
	workerName    string
	unlimitedConn bool
}

func newConnNumberChecker(toCheckDB *conn.BaseDB, stCfgs []*config.SubTaskConfig, fn func(stCfgs []*config.SubTaskConfig) int, workerName string) connNumberChecker {
	return connNumberChecker{
		toCheckDB:     toCheckDB,
		stCfgs:        stCfgs,
		getConfigConn: fn,
		workerName:    workerName,
	}
}

func (c *connNumberChecker) check(ctx context.Context, checkerName string, neededPriv map[mysql.PrivilegeType]priv) *Result {
	result := &Result{
		Name:  checkerName,
		Desc:  "check if connetion concurrency exceeds database's maximum connection limit",
		State: StateSuccess,
	}
	baseConn, err := c.toCheckDB.GetBaseConn(ctx)
	if err != nil {
		markCheckError(result, err)
		return result
	}
	defer c.toCheckDB.ForceCloseConnWithoutErr(baseConn)
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
	defer func() {
		_ = rows.Close()
		_ = rows.Err()
	}()
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
	rows.Close()
	if maxConn == 0 {
		// state = success
		c.unlimitedConn = true
		return result
	}
	// check super privilege for SHOW PROCESSLIST
	usedConn := 0
	grants, err := dbutil.ShowGrants(ctx, c.toCheckDB.DB, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	err2 := verifyPrivilegesWithResult(result, grants, neededPriv)
	if err2 != nil {
		// no enough privilege to check the user's connection number
		result.State = StateWarning
		result.Errors = append(result.Errors, NewWarn(err2.ShortErr))
		result.Instruction = err2.Instruction
	} else {
		processRows, err = baseConn.QuerySQL(tcontext.NewContext(ctx, log.L()), "SHOW PROCESSLIST")
		if err != nil {
			markCheckError(result, err)
			return result
		}
		defer func() {
			_ = processRows.Close()
			_ = processRows.Err()
		}()
		for processRows.Next() {
			usedConn++
		}
		usedConn -= 1 // exclude the connection used for show processlist
	}
	log.L().Debug("connection checker", zap.Int("maxConnections", maxConn), zap.Int("usedConnections", usedConn))
	neededConn := c.getConfigConn(c.stCfgs)
	if neededConn > maxConn {
		// nonzero max_connections and needed connections exceed max_connections
		// FYI: https://github.com/pingcap/tidb/pull/35453
		// currently, TiDB's max_connections is set to 0 representing unlimited connections,
		// while for MySQL, 0 is not a legal value (never retrieve from it).
		result.Errors = append(
			result.Errors,
			NewError(
				"checked database's max_connections: %d is less than the number %s needs: %d",
				maxConn,
				c.workerName,
				neededConn,
			),
		)
		result.Instruction = "You need to set a larger max_connection, or adjust the configuration of DM such as reducing the worker count of sycner and reducing the pool size of the dumper and loader."
		result.State = StateFailure
	} else if maxConn-usedConn < neededConn {
		// if we don't have enough privilege to check the user's connection number,
		// usedConn is 0
		result.State = StateWarning
		result.Instruction = "You need to set a larger max_connection, or adjust the configuration of DM such as reducing the worker count of sycner and reducing the pool size of the dumper and loader."
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
	return result
}

type LoaderConnNumberChecker struct {
	connNumberChecker
}

func NewLoaderConnNumberChecker(targetDB *conn.BaseDB, stCfgs []*config.SubTaskConfig) RealChecker {
	return &LoaderConnNumberChecker{
		connNumberChecker: newConnNumberChecker(targetDB, stCfgs, func(stCfgs []*config.SubTaskConfig) int {
			loaderConn := 0
			for _, stCfg := range stCfgs {
				// loader's worker and checkpoint (always keeps one db connection)
				loaderConn += stCfg.LoaderConfig.PoolSize + 1
			}
			return loaderConn
		}, "loader"),
	}
}

func (l *LoaderConnNumberChecker) Name() string {
	return "loader_conn_number_checker"
}

func (l *LoaderConnNumberChecker) Check(ctx context.Context) *Result {
	result := l.check(ctx, l.Name(), map[mysql.PrivilegeType]priv{
		mysql.SuperPriv: {needGlobal: true},
	})
	if !l.unlimitedConn && result.State == StateFailure {
		// if we're using lightning, this error should be omitted
		// because lightning doesn't need to keep connections while restoring.
		result.Errors = append(
			result.Errors,
			NewWarn("task precheck cannot accurately check the number of connection needed for Lightning."),
		)
		result.State = StateWarning
		result.Instruction = "You need to set a larger connection for TiDB."
	}
	return result
}

func NewDumperConnNumberChecker(sourceDB *conn.BaseDB, dumperThreads int) RealChecker {
	return &DumperConnNumberChecker{
		connNumberChecker: newConnNumberChecker(sourceDB, nil, func(_ []*config.SubTaskConfig) int {
			// one for generating SQL, another for consistency control
			return dumperThreads + 2
		}, "dumper"),
	}
}

type DumperConnNumberChecker struct {
	connNumberChecker
}

// Mariadb (process priv): https://mariadb.com/kb/en/show-processlist/
// MySQL(process priv): https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html
// Aurora (process priv): https://aws.amazon.com/cn/premiumsupport/knowledge-center/rds-mysql-running-queries/
func (d *DumperConnNumberChecker) Check(ctx context.Context) *Result {
	return d.check(ctx, d.Name(), map[mysql.PrivilegeType]priv{
		mysql.ProcessPriv: {needGlobal: true},
	})
}

func (d *DumperConnNumberChecker) Name() string {
	return "dumper_conn_number_checker"
}
