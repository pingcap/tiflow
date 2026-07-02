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
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	_ "github.com/pingcap/tidb/pkg/types/parser_driver" // for parser driver
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/pingcap/tidb/pkg/util/stringutil"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/pkg/container/sortmap"
	"go.uber.org/zap"
)

type tablePriv struct {
	wholeTable bool
	columns    map[string]struct{}
}

type dbPriv struct {
	wholeDB bool
	tables  map[string]tablePriv
}

type priv struct {
	needGlobal bool
	dbs        map[string]dbPriv
}

// SourceDumpPrivilegeChecker checks dump privileges of source DB.
type SourceDumpPrivilegeChecker struct {
	db                *sql.DB
	dbinfo            *dbutil.DBConfig
	checkTables       []filter.Table
	consistency       string
	dumpWholeInstance bool
	version           string
}

// NewSourceDumpPrivilegeChecker returns a RealChecker.
func NewSourceDumpPrivilegeChecker(
	db *sql.DB,
	dbinfo *dbutil.DBConfig,
	version string,
	checkTables []filter.Table,
	consistency string,
	dumpWholeInstance bool,
) RealChecker {
	return &SourceDumpPrivilegeChecker{
		db:                db,
		dbinfo:            dbinfo,
		checkTables:       checkTables,
		consistency:       consistency,
		dumpWholeInstance: dumpWholeInstance,
		version:           version,
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

	grants, err := showGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}

	dumpRequiredPrivs := make(map[mysql.PrivilegeType]priv)
	// add required SELECT privilege
	if pc.dumpWholeInstance {
		dumpRequiredPrivs[mysql.SelectPriv] = priv{needGlobal: true}
	} else {
		dumpRequiredPrivs[mysql.SelectPriv] = priv{
			needGlobal: false,
			dbs:        genTableLevelPrivs(pc.checkTables),
		}
	}

	switch pc.consistency {
	case "auto", "flush":
		dumpRequiredPrivs[mysql.ReloadPriv] = priv{needGlobal: true}
	case "lock":
		dumpRequiredPrivs[mysql.LockTablesPriv] = priv{needGlobal: true}
	}

	err2 := verifyPrivilegesWithResult(result, grants, dumpRequiredPrivs, pc.version)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
		result.Instruction = "Please grant the required privileges to the account."
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
	db      *sql.DB
	dbinfo  *dbutil.DBConfig
	version string
}

// NewSourceReplicationPrivilegeChecker returns a RealChecker.
func NewSourceReplicationPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig, version string) RealChecker {
	return &SourceReplicatePrivilegeChecker{db: db, dbinfo: dbinfo, version: version}
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

	grants, err := showGrants(ctx, pc.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	replRequiredPrivs := map[mysql.PrivilegeType]priv{
		mysql.ReplicationSlavePriv:  {needGlobal: true},
		mysql.ReplicationClientPriv: {needGlobal: true},
	}
	err2 := verifyPrivilegesWithResult(result, grants, replRequiredPrivs, pc.version)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
		result.State = StateFailure
		result.Instruction = "Grant the required privileges to the account."
	}
	return result
}

// Name implements the RealChecker interface.
func (pc *SourceReplicatePrivilegeChecker) Name() string {
	return "source db replication privilege checker"
}

type TargetPrivilegeChecker struct {
	db      *sql.DB
	dbinfo  *dbutil.DBConfig
	version string
}

func NewTargetPrivilegeChecker(db *sql.DB, dbinfo *dbutil.DBConfig, version string) RealChecker {
	return &TargetPrivilegeChecker{db: db, dbinfo: dbinfo, version: version}
}

func (t *TargetPrivilegeChecker) Name() string {
	return "target db privilege checker"
}

func (t *TargetPrivilegeChecker) Check(ctx context.Context) *Result {
	result := &Result{
		Name:  t.Name(),
		Desc:  "check privileges of target DB",
		State: StateSuccess,
		Extra: fmt.Sprintf("address of db instance - %s:%d", t.dbinfo.Host, t.dbinfo.Port),
	}
	grants, err := showGrants(ctx, t.db, "", "")
	if err != nil {
		markCheckError(result, err)
		return result
	}
	replRequiredPrivs := map[mysql.PrivilegeType]priv{
		mysql.CreatePriv: {needGlobal: true},
		mysql.SelectPriv: {needGlobal: true},
		mysql.InsertPriv: {needGlobal: true},
		mysql.UpdatePriv: {needGlobal: true},
		mysql.DeletePriv: {needGlobal: true},
		mysql.AlterPriv:  {needGlobal: true},
		mysql.DropPriv:   {needGlobal: true},
		mysql.IndexPriv:  {needGlobal: true},
	}
	err2 := verifyPrivilegesWithResult(result, grants, replRequiredPrivs, t.version)
	if err2 != nil {
		result.Errors = append(result.Errors, err2)
		// because we cannot be very precisely sure about which table
		// the binlog will write, so we only throw a warning here.
		result.State = StateWarning
	}
	return result
}

func verifyPrivilegesWithResult(
	result *Result,
	grants []string,
	requiredPriv map[mysql.PrivilegeType]priv,
	version string,
) *Error {
	lackedPriv, err := VerifyPrivileges(grants, requiredPriv, version)
	if err != nil {
		// nolint
		return NewError("%s", err.Error())
	}
	if len(lackedPriv) == 0 {
		return nil
	}

	lackedPrivStr := LackedPrivilegesAsStr(lackedPriv)
	result.Instruction = "You need grant related privileges."
	log.L().Info("lack privilege", zap.String("err msg", lackedPrivStr))
	// nolint
	return NewError("%s", lackedPrivStr)
}

// LackedPrivilegesAsStr format lacked privileges as string.
// lack of privilege1: {tableID1, tableID2, ...}; lack of privilege2...
func LackedPrivilegesAsStr(lackPriv map[mysql.PrivilegeType]priv) string {
	var b strings.Builder

	for _, pair := range sortmap.Sort(lackPriv) {
		b.WriteString("lack of ")
		b.WriteString(pair.Key.String())
		if pair.Value.needGlobal {
			b.WriteString(" global (*.*)")
		}
		b.WriteString(" privilege")
		if len(pair.Value.dbs) == 0 {
			b.WriteString("; ")
			continue
		}

		b.WriteString(": {")
		i := 0
		for _, pair2 := range sortmap.Sort(pair.Value.dbs) {
			if pair2.Value.wholeDB {
				b.WriteString(dbutil.ColumnName(pair2.Key))
				b.WriteString(".*; ")
				continue
			}

			j := 0
			for table := range pair2.Value.tables {
				b.WriteString(dbutil.TableName(pair2.Key, table))
				j++
				if j != len(pair2.Value.tables) {
					b.WriteString(", ")
				}
			}
			i++
			if i != len(pair.Value.dbs) {
				b.WriteString("; ")
			}
		}
		b.WriteString("}; ")
	}

	return b.String()
}

// VerifyPrivileges verify user privileges, returns lacked privileges. this function modifies lackPriv in place.
// we expose it so other component can reuse it.
func VerifyPrivileges(
	grants []string,
	lackPrivs map[mysql.PrivilegeType]priv,
	version string,
) (map[mysql.PrivilegeType]priv, error) {
	if len(grants) == 0 {
		return nil, errors.New("there is no such grant defined for current user on host '%%'")
	}

	p := parser.New()

	// Support for BINLOG MONITOR and other MariaDB things
	if strings.Contains(version, "MariaDB") {
		p.SetMariaDB(true)
	}

	for _, grant := range grants {
		if len(lackPrivs) == 0 {
			break
		}
		if shouldIgnoreGrant(grant) {
			continue
		}
		node, err := p.ParseOneStmt(grant, "", "")
		if err != nil {
			return nil, errors.New(err.Error())
		}
		grantStmt, ok := node.(*ast.GrantStmt)
		if !ok {
			switch node.(type) {
			case *ast.GrantProxyStmt, *ast.GrantRoleStmt, *ast.RevokeStmt:
				continue
			default:
				return nil, errors.Errorf("%s is not grant statement", grant)
			}
		}

		if len(grantStmt.Users) == 0 {
			return nil, errors.Errorf("grant has no user %s", grant)
		}

		dbPatChar, dbPatType := stringutil.CompilePattern(grantStmt.Level.DBName, '\\')
		tableName := grantStmt.Level.TableName
		switch grantStmt.Level.Level {
		case ast.GrantLevelGlobal:
			for _, privElem := range grantStmt.Privs {
				if privElem.Priv == mysql.ExtendedPriv {
					if strings.EqualFold(privElem.Name, "FLUSH_TABLES") {
						delete(lackPrivs, mysql.ReloadPriv)
					}
					continue
				}
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					if _, ok := lackPrivs[mysql.GrantPriv]; ok {
						lackPrivs = map[mysql.PrivilegeType]priv{
							mysql.GrantPriv: {needGlobal: true},
						}
						continue
					}
					return nil, nil
				}
				// mysql> show master status;
				// ERROR 1227 (42000): Access denied; you need (at least one of) the SUPER, REPLICATION CLIENT privilege(s) for this operation
				if privElem.Priv == mysql.SuperPriv {
					delete(lackPrivs, mysql.ReplicationClientPriv)
				}
				delete(lackPrivs, privElem.Priv)
			}
		case ast.GrantLevelDB:
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for _, privs := range lackPrivs {
						if privs.needGlobal {
							continue
						}
						for dbName := range privs.dbs {
							if stringutil.DoMatch(dbName, dbPatChar, dbPatType) {
								delete(privs.dbs, dbName)
							}
						}
					}
					continue
				}
				privs, ok := lackPrivs[privElem.Priv]
				if !ok || privs.needGlobal {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				for dbName := range privs.dbs {
					if stringutil.DoMatch(dbName, dbPatChar, dbPatType) {
						delete(privs.dbs, dbName)
					}
				}
			}
		case ast.GrantLevelTable:
			dbName := grantStmt.Level.DBName
			for _, privElem := range grantStmt.Privs {
				// all privileges available at a given privilege level (except GRANT OPTION)
				// from https://dev.mysql.com/doc/refman/5.7/en/privileges-provided.html#priv_all
				if privElem.Priv == mysql.AllPriv {
					for _, privs := range lackPrivs {
						if privs.needGlobal {
							continue
						}
						dbPrivs, ok := privs.dbs[dbName]
						if !ok || dbPrivs.wholeDB {
							continue
						}
						delete(dbPrivs.tables, tableName)
					}
					continue
				}
				privs, ok := lackPrivs[privElem.Priv]
				if !ok || privs.needGlobal {
					continue
				}
				dbPrivs, ok := privs.dbs[dbName]
				if !ok || dbPrivs.wholeDB {
					continue
				}
				// dumpling could report error if an allow-list table is lack of privilege.
				// we only check that SELECT is granted on all columns, otherwise we can't SHOW CREATE TABLE
				if privElem.Priv == mysql.SelectPriv && len(privElem.Cols) != 0 {
					continue
				}
				delete(dbPrivs.tables, tableName)
			}
		}
	}

	// purge empty leaves
	for privName, privs := range lackPrivs {
		for dbName, dbPrivs := range privs.dbs {
			for tableName, tablePrivs := range dbPrivs.tables {
				if !tablePrivs.wholeTable && len(tablePrivs.columns) == 0 {
					delete(dbPrivs.tables, tableName)
				}
			}
			if !dbPrivs.wholeDB && len(dbPrivs.tables) == 0 {
				delete(privs.dbs, dbName)
			}
		}
		if !privs.needGlobal && len(privs.dbs) == 0 {
			delete(lackPrivs, privName)
		}
	}

	return lackPrivs, nil
}

func showGrants(ctx context.Context, db dbutil.QueryExecutor, user, host string) ([]string, error) {
	if host == "" {
		host = "%"
	}

	query := "SHOW GRANTS FOR CURRENT_USER"
	if user != "" {
		query = fmt.Sprintf("SHOW GRANTS FOR '%s'@'%s'", user, host)
	}

	readGrants := func(query string) ([]string, error) {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			return nil, errors.Trace(err)
		}
		defer rows.Close()

		grants := make([]string, 0, 8)
		for rows.Next() {
			var grant string
			if err := rows.Scan(&grant); err != nil {
				return nil, errors.Trace(err)
			}

			// TiDB parser does not support parse `IDENTIFIED BY PASSWORD <secret>`,
			// but it may appear in some cases, ref: https://dev.mysql.com/doc/refman/5.6/en/show-grants.html.
			// We do not need the password in grant statement, so we can replace it.
			grant = strings.Replace(grant, "IDENTIFIED BY PASSWORD <secret>", "IDENTIFIED BY PASSWORD 'secret'", 1)

			// support parse `IDENTIFIED BY PASSWORD WITH {GRANT OPTION | resource_option} ...`
			grant = strings.Replace(grant, "IDENTIFIED BY PASSWORD WITH", "IDENTIFIED BY PASSWORD 'secret' WITH", 1)

			// support parse `IDENTIFIED BY PASSWORD`
			if strings.HasSuffix(grant, "IDENTIFIED BY PASSWORD") {
				grant = grant + " 'secret'"
			}

			grants = append(grants, grant)
		}
		if err := rows.Err(); err != nil {
			return nil, errors.Trace(err)
		}
		return grants, nil
	}

	grants, err := readGrants(query)
	if err != nil {
		return nil, err
	}

	// For MySQL 8.0, collect granted roles and read grants using those roles.
	// HeatWave SHOW GRANTS may append `WITH ADMIN OPTION` to role grants, which
	// TiDB parser cannot parse yet. Strip the suffix for role discovery only.
	var roles []string
	p := parser.New()
	for _, grant := range grants {
		grantForParse := grant
		if shouldIgnoreGrant(grant) {
			grantForParse = trimAdminOption(grant)
		}
		node, err := p.ParseOneStmt(grantForParse, "", "")
		if err != nil {
			return nil, errors.New(err.Error())
		}
		if grantRoleStmt, ok := node.(*ast.GrantRoleStmt); ok {
			for _, role := range grantRoleStmt.Roles {
				roles = append(roles, role.String())
			}
		}
	}
	if len(roles) == 0 {
		return grants, nil
	}

	var builder strings.Builder
	builder.WriteString(query)
	builder.WriteString(" USING ")
	for i, role := range roles {
		if i > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(role)
	}
	return readGrants(builder.String())
}

func shouldIgnoreGrant(grant string) bool {
	normalized := strings.ToUpper(strings.Join(strings.Fields(grant), " "))

	// MySQL 8.0 and HeatWave SHOW GRANTS may return role grants with
	// `WITH ADMIN OPTION`, while TiDB parser currently accepts GrantRoleStmt
	// without this suffix only. Role grants do not directly grant the source
	// privileges checked here, so ignore them just like parsed GrantRoleStmt.
	return strings.HasPrefix(normalized, "GRANT ") &&
		strings.Contains(normalized, " TO ") &&
		strings.Contains(normalized, " WITH ADMIN OPTION") &&
		!strings.Contains(normalized, " ON ")
}

func trimAdminOption(grant string) string {
	upperGrant := strings.ToUpper(grant)
	idx := strings.LastIndex(upperGrant, " WITH ADMIN OPTION")
	if idx < 0 {
		return grant
	}
	return strings.TrimSpace(grant[:idx])
}

func genTableLevelPrivs(tables []filter.Table) map[string]dbPriv {
	ret := make(map[string]dbPriv)
	for _, table := range tables {
		if _, ok := ret[table.Schema]; !ok {
			ret[table.Schema] = dbPriv{wholeDB: false, tables: make(map[string]tablePriv)}
		}
		ret[table.Schema].tables[table.Name] = tablePriv{wholeTable: true}
	}
	return ret
}
