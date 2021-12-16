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
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"go.uber.org/zap"
)

var (
	dumpPrivileges = map[mysql.PrivilegeType]struct{}{
		// mysql.ReloadPriv:            {},
		mysql.SelectPriv:            {},
		mysql.ReplicationClientPriv: {},
		// mysql.LockTablesPriv:        {},
		// mysql.ProcessPriv:           {},
	}
	replicationPrivileges = map[mysql.PrivilegeType]struct{}{
		mysql.ReplicationClientPriv: {},
		mysql.ReplicationSlavePriv:  {},
	}
)

/*****************************************************/

// SourceDumpPrivilegeChecker checks dump privileges of source DB.
type SourceDumpPrivilegeChecker struct {
	db          *sql.DB
	dbinfo      *dbutil.DBConfig
	checkTables map[string][]string
	consistency string
}

// NewSourceDumpPrivilegeChecker returns a RealChecker.
func NewSourceDumpPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig, checkTables map[string][]string, consistency string) RealChecker {
	return &SourceDumpPrivilegeChecker{
		db:          db,
		dbinfo:      dbinfo,
		checkTables: checkTables,
		consistency: consistency,
	}
}

// Check implements the RealChecker interface.
// We only check RELOAD, SELECT privileges.
func (pc *SourceDumpPrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check dump privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	lackGrants := genDumpGrants(pc.checkTables)
	verifyPrivileges(result, grants, lackGrants)
	return result
}

// Name implements the RealChecker interface.
func (pc *SourceDumpPrivilegeChecker) Name() string {
	return "source db dump privilege checker"
}

/*****************************************************/

// SourceReplicatePrivilegeChecker checks replication privileges of source DB.
type SourceReplicatePrivilegeChecker struct {
	db     *sql.DB
	dbinfo *dbutil.DBConfig
}

// NewSourceReplicationPrivilegeChecker returns a RealChecker.
func NewSourceReplicationPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig) RealChecker {
	return &SourceReplicatePrivilegeChecker{db: db, dbinfo: dbinfo}
}

// Check implements the RealChecker interface.
// We only check REPLICATION SLAVE, REPLICATION CLIENT privileges.
func (pc *SourceReplicatePrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  pc.Name(),
		Desc:  "check replication privileges of source DB",
		State: StateFailure,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	lackGrants := genReplicationGrants()
	verifyPrivileges(result, grants, lackGrants)
	return result
}

// Name implements the RealChecker interface.
func (pc *SourceReplicatePrivilegeChecker) Name() string {
	return "source db replication privilege checker"
}

// TODO: if we add more privilege in future, we might add special checks (globally granted?) for that new privilege.
func verifyPrivileges(result *Result, grants []string, lackGrants map[mysql.PrivilegeType]map[string]map[string]struct{}) {
	result.State = StateFailure
	if len(grants) == 0 {
		result.Errors = append(result.Errors, NewError("there is no such grant defined for current user on host '%%'"))
		return
	}

	var user string

	p := parser.New()
	for i, grant := range grants {
		// get username and hostname
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			result.Errors = append(result.Errors, NewError(errors.Annotatef(err, "grant %s, grant after replace %s", grants[i], grant).Error()))
			return
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt:
				continue
			default:
				result.Errors = append(result.Errors, NewError("%s is not grant statement", grants[i]))
				return
			}
		}

		if len(grantStmt.Users) == 0 {
			result.Errors = append(result.Errors, NewError("grant has no user %s", grantStmt.Text()))
			return
		} else if user == "" {
			// show grants will only output grants for requested user
			user = grantStmt.Users[0].User.Username
		}

		dbName := grantStmt.Level.DBName
		tableName := grantStmt.Level.TableName
		log.Info("privilege", zap.String("grant", grant), zap.Int("level", int(grantStmt.Level.Level)), zap.String("db name", dbName), zap.String("table name", tableName))
		switch grantStmt.Level.Level {
		case ast.GrantLevelGlobal:
			for _, privElem := range grantStmt.Privs {
				switch privElem.Priv {
				case mysql.AllPriv:
					result.State = StateSuccess
					return
				// some privileges are only effective on global level. in other words, GRANT ALL ON test.* is not enough for them
				// mysql.ReloadPriv, mysql.ReplicationClientPriv, mysql.ReplicationSlavePriv
				// https://dev.mysql.com/doc/refman/5.7/en/grant.html#grant-global-privileges
				case mysql.ReloadPriv, mysql.ReplicationClientPriv, mysql.ReplicationSlavePriv, mysql.SelectPriv,
					mysql.LockTablesPriv, mysql.ProcessPriv:
					if _, ok := lackGrants[privElem.Priv]; !ok {
						continue
					}
					delete(lackGrants, privElem.Priv)
				}
			}
		case ast.GrantLevelDB:
			for _, privElem := range grantStmt.Privs {
				switch privElem.Priv {
				case mysql.SelectPriv, mysql.LockTablesPriv:
					if _, ok := lackGrants[privElem.Priv]; !ok {
						continue
					}
					if _, ok := lackGrants[privElem.Priv][dbName]; !ok {
						continue
					}
					// currently, only SELECT privilege goes here. we didn't require SELECT to be granted globally,
					// dumpling could report error if an allow-list table is lack of privilege.
					// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
					if len(privElem.Cols) != 0 {
						continue
					}
					delete(lackGrants[privElem.Priv], dbName)
					if len(lackGrants[privElem.Priv]) == 0 {
						delete(lackGrants, privElem.Priv)
					}
				}
			}
		case ast.GrantLevelTable:
			for _, privElem := range grantStmt.Privs {
				switch privElem.Priv {
				case mysql.SelectPriv, mysql.LockTablesPriv:
					if _, ok := lackGrants[privElem.Priv]; !ok {
						continue
					}
					if _, ok := lackGrants[privElem.Priv][dbName]; !ok {
						continue
					}
					if _, ok := lackGrants[privElem.Priv][dbName][tableName]; !ok {
						continue
					}
					// currently, only SELECT privilege goes here. we didn't require SELECT to be granted globally,
					// dumpling could report error if an allow-list table is lack of privilege.
					// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
					if len(privElem.Cols) != 0 {
						continue
					}
					delete(lackGrants[privElem.Priv][dbName], tableName)
					if len(lackGrants[privElem.Priv][dbName]) == 0 {
						delete(lackGrants[privElem.Priv], dbName)
					}
					if len(lackGrants[privElem.Priv]) == 0 {
						delete(lackGrants, privElem.Priv)
					}
				}
			}
		}
	}

	if len(lackGrants) != 0 {
		lackGrantsStr := make([]string, 0, len(lackGrants))
		for g, tableMap := range lackGrants {
			lackGrantsStr = append(lackGrantsStr, fmt.Sprintf("lack of %s privilege", mysql.Priv2Str[g]))
			lackGrantsStr[len(lackGrantsStr)-1] += ":"
			for schema, tables := range tableMap {
				lackGrantsStr[len(lackGrantsStr)-1] += "{schema(" + schema + "), table("
				for table := range tables {
					lackGrantsStr[len(lackGrantsStr)-1] += table + ","
				}
				lackGrantsStr[len(lackGrantsStr)-1] += ")}, "
			}
		}
		privileges := strings.Join(lackGrantsStr, ",")
		result.Errors = append(result.Errors, NewError(privileges))
		result.Instruction = "You need grant related privileges."
		return
	}

	result.State = StateSuccess
}
