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
	"testing"

	tiddl "github.com/pingcap/tidb/pkg/ddl"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/parser"
	"github.com/pingcap/tidb/pkg/parser/ast"
	timock "github.com/pingcap/tidb/pkg/util/mock"
	cdcmodel "github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sqlmodel"
	"github.com/stretchr/testify/require"
)

func mockTableInfo(t *testing.T, sql string) *timodel.TableInfo {
	t.Helper()

	p := parser.New()
	se := timock.NewContext()
	node, err := p.ParseOneStmt(sql, "", "")
	require.NoError(t, err)
	ti, err := tiddl.MockTableInfo(se, node.(*ast.CreateTableStmt), 1)
	require.NoError(t, err)
	return ti
}

func TestGenSQL(t *testing.T) {
	t.Parallel()

	source := &cdcmodel.TableName{Schema: "db", Table: "tb"}
	target := &cdcmodel.TableName{Schema: "targetSchema", Table: "targetTable"}
	createSQL := "create table db.tb(id int primary key, col1 int unique not null, col2 int unique, name varchar(24))"

	cases := []struct {
		preValues  []interface{}
		postValues []interface{}
		safeMode   bool

		expectedSQLs []string
		expectedArgs [][]interface{}
	}{
		{
			nil,
			[]interface{}{1, 2, 3, "haha"},
			false,

			[]string{"INSERT INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			nil,
			[]interface{}{1, 2, 3, "haha"},
			true,

			[]string{"REPLACE INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1, 2, 3, "haha"}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			nil,
			false,

			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{1}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			[]interface{}{4, 5, 6, "hihi"},
			false,

			[]string{"UPDATE `targetSchema`.`targetTable` SET `id` = ?, `col1` = ?, `col2` = ?, `name` = ? WHERE `id` = ? LIMIT 1"},
			[][]interface{}{{4, 5, 6, "hihi", 1}},
		},
		{
			[]interface{}{1, 2, 3, "haha"},
			[]interface{}{4, 5, 6, "hihi"},
			true,

			[]string{"DELETE FROM `targetSchema`.`targetTable` WHERE `id` = ? LIMIT 1", "REPLACE INTO `targetSchema`.`targetTable` (`id`,`col1`,`col2`,`name`) VALUES (?,?,?,?)"},
			[][]interface{}{{1}, {4, 5, 6, "hihi"}},
		},
	}

	worker := &DMLWorker{}

	for _, c := range cases {
		tableInfo := mockTableInfo(t, createSQL)
		change := sqlmodel.NewRowChange(source, target, c.preValues, c.postValues, tableInfo, nil, nil)
		testEC := ec
		if c.safeMode {
			testEC = ecWithSafeMode
		}
		dmlJob := newDMLJob(change, testEC)
		queries, args := worker.genSQLs([]*job{dmlJob})
		require.Equal(t, c.expectedSQLs, queries)
		require.Equal(t, c.expectedArgs, args)
	}
}

func TestJudgeKeyNotFound(t *testing.T) {
	dmlWorker := &DMLWorker{
		compact:      true,
		multipleRows: true,
	}
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	dmlWorker.compact = false
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	dmlWorker.multipleRows = false
	require.False(t, dmlWorker.judgeKeyNotFound(0, nil))
	jobs := []*job{{safeMode: false}, {safeMode: true}}
	require.False(t, dmlWorker.judgeKeyNotFound(0, jobs))
	jobs[1].safeMode = false
	require.True(t, dmlWorker.judgeKeyNotFound(0, jobs))
	require.False(t, dmlWorker.judgeKeyNotFound(2, jobs))
	require.False(t, dmlWorker.judgeKeyNotFound(4, jobs))
}
