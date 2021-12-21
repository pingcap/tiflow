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
	"bytes"
	"context"
	"database/sql"
	"fmt"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb-tools/pkg/dbutil"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"go.uber.org/zap"
)

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
// We check RELOAD, SELECT, LOCK TABLES privileges according to consistency.
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

	dumpPrivileges := map[mysql.PrivilegeType]struct{}{
		mysql.SelectPriv: {},
	}

	switch pc.consistency {
	case "auto", "flush":
		dumpPrivileges[mysql.ReloadPriv] = struct{}{}
		dumpPrivileges[mysql.LockTablesPriv] = struct{}{}
	case "lock":
		dumpPrivileges[mysql.LockTablesPriv] = struct{}{}
	}

	lackGrants := genDumpGrants(dumpPrivileges, pc.checkTables)
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
		State: StateSuccess,
		Extra: fmt.Sprintf("address of db instance - %s:%d", pc.dbinfo.Host, pc.dbinfo.Port),
	}

	grants, err := dbutil.ShowGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	replicationPrivileges := map[mysql.PrivilegeType]struct{}{
		mysql.ReplicationClientPriv: {},
		mysql.ReplicationSlavePriv:  {},
	}
	lackGrants := genReplicationGrants(replicationPrivileges)
	err2 := verifyPrivileges(result, grants, lackGrants)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
		result.State = StateFailure
	}
	return result
}

// Name implements the RealChecker interface.
func (pc *SourceReplicatePrivilegeChecker) Name() string {
	return "source db replication privilege checker"
}

func verifyPrivileges(result *Result, grants []string, lackGrants map[mysql.PrivilegeType]map[string]map[string]struct{}) *Error {
	if len(grants) == 0 {
		return NewError("there is no such grant defined for current user on host '%%'")
	}

	var user string

	p := parser.New()
	for i, grant := range grants {
		if len(lackGrants) == 0 {
			break
		}
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			return NewError(errors.Annotatef(err, "grant %s, grant after replace %s", grants[i], grant).Error())
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt:
				continue
			default:
				return NewError("%s is not grant statement", grants[i])
			}
		}

		if len(grantStmt.Users) == 0 {
			return NewError("grant has no user %s", grantStmt.Text())
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
					return nil
				default:
					// some privileges are only effective on global level. in other words, GRANT ALL ON test.* is not enough for them
					// https://dev.mysql.com/doc/refman/5.7/en/grant.html#grant-global-privileges
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
					// currently, only SELECT/LOCK TABLES privilege goes here. we didn't require SELECT/LOCK TABLES to be granted globally,
					// dumpling could report error if an allow-list table is lack of privilege.
					// we only check that SELECT/LOCK TABLES is granted on all columns, otherwise we can't SHOW CREATE TABLE
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
					// currently, only SELECT/LOCK TABLES privilege goes here. we didn't require SELECT/LOCK TABLES to be granted globally,
					// dumpling could report error if an allow-list table is lack of privilege.
					// we only check that SELECT/LOCK TABLES is granted on all columns, otherwise we can't SHOW CREATE TABLE
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
		var buffer bytes.Buffer
		for g, tableMap := range lackGrants {
			buffer.WriteString("lack of ")
			buffer.WriteString(mysql.Priv2Str[g])
			buffer.WriteString(" privilege")
			i := 0
			for schema, tables := range tableMap {
				if i == 0 {
					buffer.WriteString(": {")
				}
				j := 0
				for table := range tables {
					buffer.WriteString(dbutil.TableName(schema, table))
					if j != len(tables)-1 {
						buffer.WriteString(", ")
					}
					j++
				}
				if i == len(tableMap)-1 {
					buffer.WriteString("}")
				}
				i++
			}
			buffer.WriteString(";")
		}
		privileges := buffer.String()
		result.Instruction = "You need grant related privileges."
		return NewError(privileges)
	}
	return nil
}
