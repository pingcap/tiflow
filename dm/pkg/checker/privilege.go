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

	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/parser/ast"
	"github.com/pingcap/tidb/parser/mysql"
	_ "github.com/pingcap/tidb/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/util/dbutil"
	"github.com/pingcap/tidb/util/filter"
	"github.com/pingcap/tidb/util/stringutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// some privileges are only effective on global level. in other words, GRANT ALL ON test.* is not enough for them
// https://dev.mysql.com/doc/refman/5.7/en/grant.html#grant-global-privileges
var privNeedGlobal = map[mysql.PrivilegeType]struct{}{
	mysql.ReloadPriv:            {},
	mysql.ReplicationClientPriv: {},
	mysql.ReplicationSlavePriv:  {},
}

// SourceDumpPrivilegeChecker checks dump privileges of source DB.
type SourceDumpPrivilegeChecker struct {
	db          *sql.DB
	dbinfo      *dbutil.DBConfig
	checkTables []*filter.Table
	consistency string
}

// NewSourceDumpPrivilegeChecker returns a RealChecker.
func NewSourceDumpPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig, checkTables []*filter.Table, consistency string) RealChecker {
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
	case "lock":
		dumpPrivileges[mysql.LockTablesPriv] = struct{}{}
	}

	lackPriv := genDumpPriv(dumpPrivileges, pc.checkTables)
	err2 := verifyPrivileges(result, grants, lackPriv)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
	} else {
		result.State = StateSuccess
	}
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
	lackPriv := genReplicPriv(replicationPrivileges)
	err2 := verifyPrivileges(result, grants, lackPriv)
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

func verifyPrivileges(result *Result, grants []string, lackPriv map[mysql.PrivilegeType]map[string]map[string]struct{}) *Error {
	if len(grants) == 0 {
		return NewError("there is no such grant defined for current user on host '%%'")
	}

	p := parser.New()
	for _, grant := range grants {
		if len(lackPriv) == 0 {
			break
		}
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			return NewError(err.Error())
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt:
				continue
			default:
				return NewError("%s is not grant statement", grant)
			}
		}

		if len(grantStmt.Users) == 0 {
			return NewError("grant has no user %s", grant)
		}

		dbPatChar, dbPatType := stringutil.CompilePattern(grantStmt.Level.DBName, '\\')
		tableName := grantStmt.Level.TableName
		switch grantStmt.Level.Level {
		case ast.GrantLevelGlobal:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					if _, ok := lackPriv[mysql.GrantPriv]; ok {
						lackPriv = make(map[mysql.PrivilegeType]map[string]map[string]struct{})
						lackPriv[mysql.GrantPriv] = make(map[string]map[string]struct{})
						continue
					}
					return nil
				}
				// mysql> show master status;
				// ERROR 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
				if privElem.Priv == mysql.SuperPriv {
					delete(lackPriv, mysql.ReplicationClientPriv)
				}
				delete(lackPriv, privElem.Priv)
			}
		case ast.GrantLevelDB:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for priv := range lackPriv {
						if priv == mysql.GrantPriv {
							continue
						}
						// in this old release branch, we don't have a flag that mark a privilege is global level.
						// we use `deleted` to avoid delete a global level privilege when processing AllPriv.
						deleted := false
						for dbName := range lackPriv[priv] {
							if stringutil.DoMatch(dbName, dbPatChar, dbPatType) {
								delete(lackPriv[priv], dbName)
							}
							deleted = true
						}
						if deleted && len(lackPriv[priv]) == 0 {
							delete(lackPriv, priv)
						}
					}
					continue
				}
				if _, ok := lackPriv[privElem.Priv]; !ok {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				for dbName := range lackPriv[privElem.Priv] {
					if stringutil.DoMatch(dbName, dbPatChar, dbPatType) {
						delete(lackPriv[privElem.Priv], dbName)
					}
				}
				if len(lackPriv[privElem.Priv]) == 0 {
					delete(lackPriv, privElem.Priv)
				}
			}
		case ast.GrantLevelTable:
			dbName := grantStmt.Level.DBName
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for priv := range lackPriv {
						if priv == mysql.GrantPriv {
							continue
						}
						if _, ok := lackPriv[priv][dbName]; !ok {
							continue
						}
						if _, ok := lackPriv[priv][dbName][tableName]; !ok {
							continue
						}
						delete(lackPriv[priv][dbName], tableName)
						if len(lackPriv[priv][dbName]) == 0 {
							delete(lackPriv[priv], dbName)
						}
						if len(lackPriv[priv]) == 0 {
							delete(lackPriv, priv)
						}
					}
					continue
				}
				if _, ok := lackPriv[privElem.Priv]; !ok {
					continue
				}
				if _, ok := lackPriv[privElem.Priv][dbName]; !ok {
					continue
				}
				if _, ok := lackPriv[privElem.Priv][dbName][tableName]; !ok {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				delete(lackPriv[privElem.Priv][dbName], tableName)
				if len(lackPriv[privElem.Priv][dbName]) == 0 {
					delete(lackPriv[privElem.Priv], dbName)
				}
				if len(lackPriv[privElem.Priv]) == 0 {
					delete(lackPriv, privElem.Priv)
				}
			}
		}
	}

	if len(lackPriv) == 0 {
		return nil
	}
	var b strings.Builder
	// generate error message, for example
	// lack of privilege1: {tableID1, tableID2, ...};lack of privilege2...
	for p, tableMap := range lackPriv {
		b.WriteString("lack of ")
		b.WriteString(mysql.Priv2Str[p])
		b.WriteString(" privilege")
		if len(tableMap) != 0 {
			b.WriteString(": {")
		}
		i := 0
		for schema, tables := range tableMap {
			if len(tables) == 0 {
				b.WriteString(dbutil.ColumnName(schema))
			}
			j := 0
			for table := range tables {
				b.WriteString(dbutil.TableName(schema, table))
				j++
				if j != len(tables) {
					b.WriteString(", ")
				}
			}
			i++
			if i != len(tableMap) {
				b.WriteString("; ")
			}
		}
		if len(tableMap) != 0 {
			b.WriteString("}")
		}
		b.WriteString("; ")
	}
	privileges := b.String()
	result.Instruction = "You need grant related privileges."
	log.L().Info("lack privilege", zap.String("err msg", privileges))
	return NewError(privileges)
}

// lackPriv map privilege => schema => table.
func genExpectPriv(privileges map[mysql.PrivilegeType]struct{}, checkTables []*filter.Table) map[mysql.PrivilegeType]map[string]map[string]struct{} {
	lackPriv := make(map[mysql.PrivilegeType]map[string]map[string]struct{}, len(privileges))
	for p := range privileges {
		if _, ok := privNeedGlobal[p]; ok {
			lackPriv[p] = make(map[string]map[string]struct{})
			continue
		}
		lackPriv[p] = make(map[string]map[string]struct{}, len(checkTables))
		for _, table := range checkTables {
			if _, ok := lackPriv[p][table.Schema]; !ok {
				lackPriv[p][table.Schema] = make(map[string]struct{})
			}
			lackPriv[p][table.Schema][table.Name] = struct{}{}
		}
	}
	return lackPriv
}

func genReplicPriv(replicationPrivileges map[mysql.PrivilegeType]struct{}) map[mysql.PrivilegeType]map[string]map[string]struct{} {
	// replication privilege only check replication client and replication slave which are global level privilege
	// so don't need check tables
	return genExpectPriv(replicationPrivileges, nil)
}

func genDumpPriv(dumpPrivileges map[mysql.PrivilegeType]struct{}, checkTables []*filter.Table) map[mysql.PrivilegeType]map[string]map[string]struct{} {
	// due to dump privilege checker need check db/table level privilege
	// so we need know the check tables
	return genExpectPriv(dumpPrivileges, checkTables)
}
