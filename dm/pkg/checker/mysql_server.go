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

	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tiflow/dm/pkg/conn"
	"github.com/pingcap/tiflow/pkg/errors"
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
		result.Instruction = err2.Instruction
		err2.Instruction = ""
		result.Errors = append(result.Errors, err2)
	}
	return result
}

func (pc *MySQLVersionChecker) checkVersion(value string, result *Result) *Error {
	needVersion := SupportedVersion["mysql"]
	if conn.IsMariaDB(value) {
		err := NewWarn("Migrating from MariaDB is still experimental.")
		err.Instruction = "It is recommended that you upgrade MariaDB to 10.1.2 or a later version."
		return err
	}
	if IsTiDBFromVersion(value) {
		err := NewWarn("migration from TiDB not supported")
		err.Instruction = "TiDB is not supported as an upstream database."
		return err
	}

	version, err := toMySQLVersion(value)
	if err != nil {
		markCheckError(result, err)
		return nil
	}

	if !version.Ge(needVersion.Min) {
		err := NewWarn("version suggested at least %v but got %v", needVersion.Min, version)
		err.Instruction = "It is recommended that you upgrade the database to the required version before performing data migration. Otherwise data inconsistency or task exceptions might occur."
		return err
	}

	if !version.Lt(needVersion.Max) {
		err := NewWarn("version suggested earlier than %v but got %v", needVersion.Max, version)
		err.Instruction = "It is recommended that you select a database version that meets the requirements before performing data migration. Otherwise data inconsistency or task exceptions might occur."
		return err
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
		State: StateWarning,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	serverID, err := dbutil.ShowServerID(ctx, pc.db)
	if err != nil {
		if errors.OriginError(err) != sql.ErrNoRows {
			markCheckError(result, err)
			return result
		}
		result.Errors = append(result.Errors, NewError("server_id not set"))
		result.Instruction = "Set server_id in your database, or errors might happen in master/slave switchover"
		return result
	}

	if serverID == 0 {
		result.Errors = append(result.Errors, NewError("server_id is 0"))
		result.Instruction = "Set server_id greater than 0, or errors might happen in master/slave switchover"
		return result
	}
	result.State = StateSuccess
	return result
}

// Name implements the RealChecker interface.
func (pc *MySQLServerIDChecker) Name() string {
	return "mysql_server_id"
}
