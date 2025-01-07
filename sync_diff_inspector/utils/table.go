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

package utils

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/ddl"
	"github.com/pingcap/tidb/pkg/meta/metabuild"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/types"
	"github.com/pingcap/tidb/pkg/util/collate"
	"github.com/pingcap/tidb/pkg/util/dbutil"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tidb/pkg/util/mock"
)

const (
	annotationClusteredReplaceString    = "${1} /*T![clustered_index] CLUSTERED */${2}\n"
	annotationNonClusteredReplaceString = "${1} /*T![clustered_index] NONCLUSTERED */${2}\n"
)

func init() {
	collate.SetNewCollationEnabledForTest(false)
}

// addClusteredAnnotation add the `/*T![clustered_index] NONCLUSTERED */` for primary key of create table info
// In the older version, the create table info hasn't `/*T![clustered_index] NONCLUSTERED */`,
// which lead the issue https://github.com/pingcap/tidb-tools/issues/678
//
// Before Get Create Table Info:
// mysql> SHOW CREATE TABLE `test`.`itest`;
//
//	+-------+--------------------------------------------------------------------+
//	| Table | Create Table                                                                                                                              |
//	+-------+--------------------------------------------------------------------+
//	| itest | CREATE TABLE `itest` (
//		`id` int(11) DEFAULT NULL,
//		`name` varchar(24) DEFAULT NULL,
//		PRIMARY KEY (`id`)
//		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
//	+-------+--------------------------------------------------------------------+
//
// After Add the annotation:
//
//	+-------+--------------------------------------------------------------------+
//	| Table | Create Table                                                                                                                              |
//	+-------+--------------------------------------------------------------------+
//	| itest | CREATE TABLE `itest` (
//		`id` int(11) DEFAULT NULL,
//		`name` varchar(24) DEFAULT NULL,
//		PRIMARY KEY (`id`) /*T![clustered_index] CLUSTERED */
//		) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin |
//	+-------+--------------------------------------------------------------------+
func addClusteredAnnotationForPrimaryKey(raw string, replace string) (string, error) {
	reg, regErr := regexp.Compile(`(PRIMARY\sKEY.*\))(\s*,?)\s*\n`)
	if reg == nil || regErr != nil {
		return raw, errors.Annotate(regErr, "failed to compile regex for add clustered annotation, err: %s")
	}
	return reg.ReplaceAllString(raw, replace), nil
}

func getTableInfoBySQL(ctx *metabuild.Context, createTableSQL string, parser2 *parser.Parser) (table *model.TableInfo, err error) {
	stmt, err := parser2.ParseOneStmt(createTableSQL, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	s, ok := stmt.(*ast.CreateTableStmt)
	if ok {
		table, err := ddl.BuildTableInfoWithStmt(ctx, s, mysql.DefaultCharset, "", nil)
		if err != nil {
			return nil, errors.Trace(err)
		}

		// put primary key in indices
		if table.PKIsHandle {
			pkIndex := &model.IndexInfo{
				Name:    pmodel.NewCIStr("PRIMARY"),
				Primary: true,
				State:   model.StatePublic,
				Unique:  true,
				Tp:      pmodel.IndexTypeBtree,
				Columns: []*model.IndexColumn{
					{
						Name:   table.GetPkName(),
						Length: types.UnspecifiedLength,
					},
				},
			}

			table.Indices = append(table.Indices, pkIndex)
		}

		return table, nil
	}

	return nil, errors.Errorf("get table info from sql %s failed", createTableSQL)
}

func isPKISHandle(
	ctx context.Context,
	db dbutil.QueryExecutor,
	schemaName, tableName string,
) bool {
	query := fmt.Sprintf("SELECT _tidb_rowid FROM %s LIMIT 0;", dbutil.TableName(schemaName, tableName))
	rows, err := db.QueryContext(ctx, query)
	defer func() {
		if rows != nil {
			_ = rows.Err()
			rows.Close()
		}
	}()

	if err != nil && strings.Contains(err.Error(), "Unknown column") {
		return true
	}
	return false
}

// GetTableInfoWithVersion returns table info under given version.
func GetTableInfoWithVersion(
	ctx context.Context,
	db dbutil.QueryExecutor,
	schemaName, tableName string,
	version *semver.Version,
) (*model.TableInfo, error) {
	createTableSQL, err := dbutil.GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if version != nil && version.Major <= 4 {
		var replaceString string
		if isPKISHandle(ctx, db, schemaName, tableName) {
			replaceString = annotationClusteredReplaceString
		} else {
			replaceString = annotationNonClusteredReplaceString
		}
		createTableSQL, err = addClusteredAnnotationForPrimaryKey(createTableSQL, replaceString)
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	parser2, err := dbutil.GetParserForDB(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	sctx := mock.NewContext()
	// unify the timezone to UTC +0:00
	// TODO(joechenrh): the following code doesn't work on the latest version of tidb.
	sctx.GetSessionVars().TimeZone = time.UTC
	sctx.GetSessionVars().SQLMode = mysql.DelSQLMode(sctx.GetSessionVars().SQLMode, mysql.ModeStrictTransTables)
	sctx.GetSessionVars().SQLMode = mysql.DelSQLMode(sctx.GetSessionVars().SQLMode, mysql.ModeStrictAllTables)
	metaBuildCtx := ddl.NewMetaBuildContextWithSctx(sctx)
	return getTableInfoBySQL(metaBuildCtx, createTableSQL, parser2)
}

// GetTableInfo returns table information.
func GetTableInfo(
	ctx context.Context, db dbutil.QueryExecutor,
	schemaName, tableName string,
) (*model.TableInfo, error) {
	createTableSQL, err := dbutil.GetCreateTableSQL(ctx, db, schemaName, tableName)
	if err != nil {
		return nil, errors.Trace(err)
	}

	parser2, err := dbutil.GetParserForDB(ctx, db)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return dbutiltest.GetTableInfoBySQL(createTableSQL, parser2)
}
