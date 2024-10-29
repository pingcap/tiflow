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

package syncer

import (
	"database/sql"
	"fmt"
	"strconv"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/util/dbutil/dbutiltest"
	"github.com/pingcap/tidb/pkg/util/filter"
	"github.com/stretchr/testify/require"
)

func genValidateTableInfo(t *testing.T, creatSQL string) *model.TableInfo {
	t.Helper()
	var (
		err       error
		parser2   *parser.Parser
		tableInfo *model.TableInfo
	)
	parser2 = parser.New()
	require.NoError(t, err)
	tableInfo, err = dbutiltest.GetTableInfoBySQL(creatSQL, parser2)
	require.NoError(t, err)
	return tableInfo
}

func genValidationCond(t *testing.T, schemaName, tblName, creatSQL string, pkvs [][]string) *Cond {
	t.Helper()
	tbl := filter.Table{Schema: schemaName, Name: tblName}
	tblInfo := genValidateTableInfo(t, creatSQL)
	return &Cond{
		TargetTbl: tbl.String(),
		Columns:   tblInfo.Columns,
		PK:        tblInfo.Indices[0],
		PkValues:  pkvs,
	}
}

func TestValidatorCondSelectMultiKey(t *testing.T) {
	var res *sql.Rows
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	creatTbl := "create table if not exists `test_cond`.`test1`(" +
		"a int," +
		"b int," +
		"c int," +
		"primary key(a, b)" +
		");"
	// get table diff
	pkValues := make([][]string, 0)
	for i := 0; i < 3; i++ {
		// 3 primary key
		key1, key2 := strconv.Itoa(i+1), strconv.Itoa(i+2)
		pkValues = append(pkValues, []string{key1, key2})
	}
	cond := genValidationCond(t, "test_cond", "test1", creatTbl, pkValues)
	// format query string
	rowsQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE %s;", "`test_cond`.`test1`", cond.GetWhere())
	mock.ExpectQuery(
		"SELECT COUNT\\(\\*\\) FROM `test_cond`.`test1` WHERE \\(a,b\\) in \\(\\(\\?,\\?\\),\\(\\?,\\?\\),\\(\\?,\\?\\)\\);",
	).WithArgs(
		"1", "2", "2", "3", "3", "4",
	).WillReturnRows(mock.NewRows([]string{"COUNT(*)"}).AddRow("3"))
	require.NoError(t, err)
	res, err = db.Query(rowsQuery, cond.GetArgs()...)
	require.NoError(t, err)
	defer res.Close()
	var cnt int
	if res.Next() {
		err = res.Scan(&cnt)
	}
	require.NoError(t, err)
	require.Equal(t, 3, cnt)
	require.NoError(t, res.Err())
}

func TestValidatorCondGetWhereArgs(t *testing.T) {
	db, _, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()
	type testCase struct {
		creatTbl   string
		pks        [][]string
		tblName    string
		schemaName string
		args       []string
		where      string
	}
	cases := []testCase{
		{
			creatTbl: `create table if not exists test_cond.test2(
				a char(10),
				b int,
				c int,
				primary key(a)
				);`, // single primary key,
			pks: [][]string{
				{"10a0"}, {"200"}, {"abc"},
			},
			tblName:    "test2",
			schemaName: "test_cond",
			where:      "a in (?,?,?)",
			args: []string{
				"10a0", "200", "abc",
			},
		},
		{
			creatTbl: `create table if not exists test_cond.test3(
				a int,
				b char(10),
				c varchar(10),
				primary key(a, b, c)
				);`, // multi primary key
			pks: [][]string{
				{"10", "abc", "ef"},
				{"9897", "afdkiefkjg", "acdee"},
			},
			tblName:    "test3",
			schemaName: "test_cond",
			where:      "(a,b,c) in ((?,?,?),(?,?,?))",
			args: []string{
				"10", "abc", "ef", "9897", "afdkiefkjg", "acdee",
			},
		},
	}
	for i := 0; i < len(cases); i++ {
		cond := genValidationCond(t, cases[i].schemaName, cases[i].tblName, cases[i].creatTbl, cases[i].pks)
		require.Equal(t, cases[i].where, cond.GetWhere())
		rawArgs := cond.GetArgs()
		for j := 0; j < 3; j++ {
			curData := fmt.Sprintf("%v", rawArgs[j])
			require.Equal(t, cases[i].args[j], curData)
		}
	}
}
