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

package filter

import (
	"testing"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestShouldSkipDML(t *testing.T) {
	helper := NewFilterTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test;")

	type innerCase struct {
		schema string
		table  string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.Column
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.Column
		preRow  []interface{}
		row     []interface{}
		ignore  bool
	}

	type testCase struct {
		ddl   string
		cfg   *config.FilterConfig
		cases []innerCase
	}

	testCases := []testCase{
		{
			ddl: "create table test.student(id int primary key, name char(50), age int, gender char(10))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.student"},
						IgnoreInsertValueExpr:    "age >= 20 or gender = 'female'",
						IgnoreDeleteValueExpr:    "age >= 32 and age < 48",
						IgnoreUpdateOldValueExpr: "gender = 'male'",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
				},
			},
			cases: []innerCase{
				{ // table name does not configure in matcher, no rule to filter it
					schema: "test",
					table:  "teacher",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
				},
				{ // schema name does not configure in matcher, no rule to filter it
					schema: "no",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{888, "Li", 45, "male"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{2, "Rustin", 18, "male"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{3, "Susan", 3, "female"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{4, "Helen", 18, "female"},
					ignore: false,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{5, "Madonna", 32, "female"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{6, "Madison", 48, "male"},
					ignore: false,
				},
				{ // update, filler by new value
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{7, "Marry", 28, "female"},
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{7, "Marry", 32, "female"},
					ignore: true,
				},
				{ // update
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{8, "Marilyn", 18, "female"},
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{8, "Monroe", 22, "female"},
					ignore: false,
				},
				{ // update, filter by old value
					schema: "test",
					table:  "student",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{9, "Andreja", 25, "male"},
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{9, "Andreja", 25, "female"},
					ignore: true,
				},
			},
		},
		{
			ddl: "create table test.computer(id int primary key, brand char(50), price int)",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:               []string{"test.*"},
						IgnoreInsertValueExpr: "price > 10000",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "computer",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{1, "apple", 12888},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "computer",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{2, "microsoft", 5888},
					ignore: false,
				},
			},
		},
		{
			ddl: "create table test.poet(id int primary key, name char(50), works char(100))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:               []string{"*.*"},
						IgnoreInsertValueExpr: "id <= 1",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{1, "李白", "静夜思"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{2, "杜甫", "石壕吏"},
					ignore: false,
				},
			},
		},
		{
			ddl: "create table test.season(id int primary key, name char(50), start char(100), end char(100))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{ // do not ignore any event of test.season table
						// and ignore events of !test.season table by configure SQL expression.
						Matcher:                  []string{"*.*", "!test.season"},
						IgnoreInsertValueExpr:    "id >= 1",
						IgnoreUpdateNewValueExpr: "id >= 1",
					},
				},
			},
			cases: []innerCase{
				{ // do not ignore any event of test.season table
					schema: "test",
					table:  "season",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{1, "Spring", "January", "March"},
					ignore: false,
				},
				{ // do not ignore any event of test.season table
					schema: "test",
					table:  "season",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{2, "Summer", "April", "June"},
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{2, "Summer", "April", "July"},
					ignore: false,
				},
				{ // ignore insert event of test.autumn table
					schema: "test",
					table:  "autumn",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{3, "Autumn", "July", "September"},
					ignore: true,
				},
				{ // ignore update event of test.winter table
					schema: "test",
					table:  "winter",
					preColumns: []*model.Column{
						{Name: "none"},
					},
					preRow: []interface{}{4, "Winter", "October", "January"},
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{4, "Winter", "October", "December"},
					ignore: true,
				},
			},
		},
	}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": "System",
	})

	for _, tc := range testCases {
		tableInfo := helper.ExecDDL(tc.ddl)
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		for _, c := range tc.cases {
			rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
			require.Nil(t, err)
			preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
			require.Nil(t, err)
			row := &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
				RowChangedDatums: &model.RowChangedDatums{
					RowDatums:    rowDatums,
					PreRowDatums: preRowDatums,
				},
				Columns:    c.columns,
				PreColumns: c.preColumns,
			}
			ignore, err := f.shouldSkipDML(row, tableInfo)
			require.Nil(t, err)
			require.Equal(t, c.ignore, ignore, "case: %+v", c)
		}
	}
}

func TestShouldSkipDMLError(t *testing.T) {
	helper := NewFilterTestHelper(t)
	defer helper.Close()
	helper.Tk().MustExec("use test;")

	type innerCase struct {
		schema string
		table  string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.Column
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.Column
		preRow  []interface{}
		row     []interface{}
		ignore  bool
	}

	type testCase struct {
		ddl   string
		cfg   *config.FilterConfig
		cases []innerCase
	}

	testCases := []testCase{
		{
			ddl: "create table test.student(id int primary key, name char(50), age int, gender char(10))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.student"},
						IgnoreInsertValueExpr:    "age >= 20 or gender = 'female' or mather = 'dongdong'",
						IgnoreDeleteValueExpr:    "age >= 32 and and age < 48",
						IgnoreUpdateOldValueExpr: "gender = 'male' and error(age) > 20",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
				},
			},
			cases: []innerCase{
				{ // table name does not configure in matcher, no rule to filter it
					schema: "test",
					table:  "teacher",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
				},
				{ // schema name does not configure in matcher, no rule to filter it
					schema: "no",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{888, "Li", 45, "male"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.Column{
						{Name: "none"},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: true,
				},
			},
		},
	}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": "System",
	})

	for _, tc := range testCases {
		tableInfo := helper.ExecDDL(tc.ddl)
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		for _, c := range tc.cases {
			rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
			require.Nil(t, err)
			preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
			require.Nil(t, err)
			row := &model.RowChangedEvent{
				Table: &model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
				RowChangedDatums: &model.RowChangedDatums{
					RowDatums:    rowDatums,
					PreRowDatums: preRowDatums,
				},
				Columns:    c.columns,
				PreColumns: c.preColumns,
			}
			ignore, err := f.shouldSkipDML(row, tableInfo)
			require.Nil(t, err)
			require.Equal(t, c.ignore, ignore, "case: %+v", c)
		}
	}
}
