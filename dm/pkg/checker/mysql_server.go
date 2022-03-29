// Copyright 2021 PingCAP, Inc.
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
	"fmt"

	toolsutils "github.com/pingcap/tidb-tools/pkg/utils"
	"github.com/pingcap/tidb/util/dbutil"

	"github.com/pingcap/tiflow/dm/pkg/utils"
)

// MySQLVersionChecker checks mysql/mariadb/rds,... version.
type MySQLVersionChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLVersionChecker returns a RealChecker.
func NewMySQLVersionChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &MySQLVersionChecker{db: db, dbinfo: dbinfo}
}

// SupportedVersion defines the MySQL/MariaDB version that DM/syncer supports
// * 5.6.0 <= MySQL Version < 8.0.0.
var SupportedVersion = map[string]struct {
	Min MySQLVersion
	Max MySQLVersion
}{
	"mysql": {
		MySQLVersion{5, 6, 0},
		MySQLVersion{8, 0, 0},
	},
}

// Check implements the RealChecker interface.
// we only support version >= 5.6.
func (pc *MySQLVersionChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql version is satisfied",
		State: StateWarning,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	value, err := dbutil.ShowVersion(ctx, pc.db)
	if err != nil {
		markCheckError(result, err)
		return result
	}

	err2 := pc.checkVersion(value, result)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
	}
	return result
}

func (pc *MySQLVersionChecker) checkVersion(value string, result *Result) *Error {
	needVersion := SupportedVersion["mysql"]
	if utils.IsMariaDB(value) {
		return NewWarn("Migrating from MariaDB is experimentally supported. If you must use DM to migrate data from MariaDB, we suggest make your MariaDB >= 10.1.2")
	}
	if IsTiDBFromVersion(value) {
		return NewWarn("Not support migrate from TiDB")
	}

	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return nil
	}

	if !version.Ge(needVersion.Min) {
		return NewWarn("version suggested at least %v but got %v", needVersion.Min, version)
	}

	if !version.Lt(needVersion.Max) {
		return NewWarn("version suggested less than %v but got %v", needVersion.Max, version)
	}

	result.State = StateSuccess
	return nil
}

// Name implements the RealChecker interface.
func (pc *MySQLVersionChecker) Name() string {
	return "mysql_version"
}

/*****************************************************/

// MySQLServerIDChecker checks mysql/mariadb server ID.
type MySQLServerIDChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewMySQLServerIDChecker returns a RealChecker.
func NewMySQLServerIDChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &MySQLServerIDChecker{db: db, dbinfo: dbinfo}
}

// Check implements the RealChecker interface.
func (pc *MySQLServerIDChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check whether mysql server_id has been greater than 0",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	serverID, err := dbutil.ShowServerID(ctx, pc.db)
	if err != nil {
		if toolsutils.OriginError(err) != sql.ErrNoRows {
			markCheckError(result, err)
			return result
		}
		result.Errors = append(result.Errors, NewError("server_id not set"))
		result.Instruction = "please set server_id in your database"
		return result
	}

	if serverID == 0 {
		result.Errors = append(result.Errors, NewError("server_id is 0"))
		result.Instruction = "please set server_id greater than 0"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (pc *MySQLServerIDChecker) Name() string {
	return "mysql_server_id"
}
