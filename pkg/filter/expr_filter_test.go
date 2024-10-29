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

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/pkg/util/dbterror/plannererrors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/dm/pkg/utils"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldSkipDMLBasic(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.close()
	helper.getTk().MustExec("use test;")

	type innerCase struct {
		schema string
		table  string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.ColumnData
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.ColumnData
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
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
				},
				{ // schema name does not configure in matcher, no rule to filter it
					schema: "no",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{888, "Li", 45, "male"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{2, "Rustin", 18, "male"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{3, "Susan", 3, "female"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{4, "Helen", 18, "female"},
					ignore: false,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{5, "Madonna", 32, "female"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{6, "Madison", 48, "male"},
					ignore: false,
				},
				{ // update, filler by new value
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{7, "Marry", 28, "female"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{7, "Marry", 32, "female"},
					ignore: true,
				},
				{ // update
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{8, "Marilyn", 18, "female"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{8, "Monroe", 22, "female"},
					ignore: false,
				},
				{ // update, filter by old value
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{9, "Andreja", 25, "male"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
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
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "apple", 12888},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "computer",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{2, "microsoft", 5888},
					ignore: false,
				},
			},
		},
		{ // test case for gbk charset
			ddl: "create table test.poet(id int primary key, name varchar(50) CHARACTER SET GBK COLLATE gbk_bin, works char(100))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:               []string{"*.*"},
						IgnoreInsertValueExpr: "id <= 1 or name='辛弃疾' or works='离骚'",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "李白", "静夜思"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{2, "杜甫", "石壕吏"},
					ignore: false,
				},
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{4, "屈原", "离骚"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "poet",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{3, "辛弃疾", "众里寻他千百度"},
					ignore: true,
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
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "Spring", "January", "March"},
					ignore: false,
				},
				{ // do not ignore any event of test.season table
					schema: "test",
					table:  "season",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{2, "Summer", "April", "June"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{2, "Summer", "April", "July"},
					ignore: false,
				},
				{ // ignore insert event of test.autumn table
					schema: "test",
					table:  "autumn",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{3, "Autumn", "July", "September"},
					ignore: true,
				},
				{ // ignore update event of test.winter table
					schema: "test",
					table:  "winter",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{4, "Winter", "October", "January"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{4, "Winter", "October", "December"},
					ignore: true,
				},
			},
		},
	}

	sessCtx := utils.ZeroSessionCtx

	for _, tc := range testCases {
		tableInfo := helper.execDDL(tc.ddl)
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		for _, c := range tc.cases {
			rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
			require.Nil(t, err)
			preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
			require.Nil(t, err)
			row := &model.RowChangedEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: c.schema,
						Table:  c.table,
					},
				},
				Columns:    c.columns,
				PreColumns: c.preColumns,
			}
			rawRow := model.RowChangedDatums{
				RowDatums:    rowDatums,
				PreRowDatums: preRowDatums,
			}
			ignore, err := f.shouldSkipDML(row, rawRow, tableInfo)
			require.Nil(t, err)
			require.Equal(t, c.ignore, ignore, "case: %+v", c, rowDatums)
		}
	}
}

// This test case is for testing when there are syntax error
// or unknown error in the expression the return error type and message
// are as expected.
func TestShouldSkipDMLError(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.close()
	helper.getTk().MustExec("use test;")

	type innerCase struct {
		schema string
		table  string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.ColumnData
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.ColumnData
		preRow  []interface{}
		row     []interface{}
		ignore  bool
		err     error
		errMsg  string
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
						IgnoreInsertValueExpr:    "age >= 20 or gender = 'female' and mather='a'",
						IgnoreDeleteValueExpr:    "age >= 32 and and age < 48",
						IgnoreUpdateOldValueExpr: "gender = 'male' and error(age) > 20",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
					err:    cerror.ErrExpressionColumnNotFound,
					errMsg: "Cannot find column 'mather' from table 'test.student' in",
				},
				{ // update
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: false,
					err:    cerror.ErrExpressionParseFailed,
					errMsg: "There is a syntax error in",
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female"},
					ignore: false,
					err:    cerror.ErrExpressionParseFailed,
					errMsg: "There is a syntax error in",
				},
			},
		},
	}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": "UTC",
	})

	for _, tc := range testCases {
		tableInfo := helper.execDDL(tc.ddl)
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		for _, c := range tc.cases {
			rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
			require.Nil(t, err)
			preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
			require.Nil(t, err)
			row := &model.RowChangedEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: c.schema,
						Table:  c.table,
					},
				},
				Columns:    c.columns,
				PreColumns: c.preColumns,
			}
			rawRow := model.RowChangedDatums{
				RowDatums:    rowDatums,
				PreRowDatums: preRowDatums,
			}
			ignore, err := f.shouldSkipDML(row, rawRow, tableInfo)
			require.True(t, errors.ErrorEqual(c.err, err), "case: %+v", c, err)
			require.Contains(t, err.Error(), c.errMsg)
			require.Equal(t, c.ignore, ignore)
		}
	}
}

// This test case is for testing when a table is updated,
// the filter will works as expected.
func TestShouldSkipDMLTableUpdated(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.close()
	helper.getTk().MustExec("use test;")

	type innerCase struct {
		schema    string
		table     string
		updateDDl string
		// set preColumns to non nil to indicate this case is for update
		preColumns []*model.ColumnData
		// set columns to non nil to indicate this case is for insert
		// set columns to nil to indicate this case is for delete
		columns []*model.ColumnData
		preRow  []interface{}
		row     []interface{}
		ignore  bool
		err     error
		errMsg  string
	}

	type testCase struct {
		ddl   string
		cfg   *config.FilterConfig
		cases []innerCase
	}

	testCases := []testCase{
		{ // add new column case.
			ddl: "create table test.student(id int primary key, name char(50), age int, gender char(10))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.student"},
						IgnoreInsertValueExpr:    "age >= 20 and mather = 'Marisa'",
						IgnoreDeleteValueExpr:    "age >= 32 and mather = 'Maria'",
						IgnoreUpdateOldValueExpr: "gender = 'female'",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "student",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
					err:    cerror.ErrExpressionColumnNotFound,
					errMsg: "Cannot find column 'mather' from table 'test.student' in",
				},
				{ // insert
					schema: "test",
					table:  "student",
					// we execute updateDDl to update the table info
					updateDDl: "ALTER TABLE student ADD COLUMN mather char(50)",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male", "Marry"},
					ignore: false,
				},
				{ // update
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "student",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female", "Maria"},
					ignore: true,
				},
			},
		},
		{ // drop column case
			ddl: "create table test.worker(id int primary key, name char(50), age int, gender char(10), company char(50))",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.worker"},
						IgnoreInsertValueExpr:    "age >= 20 and company = 'Apple'",
						IgnoreDeleteValueExpr:    "age >= 32 and company = 'Google'",
						IgnoreUpdateOldValueExpr: "gender = 'female'",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
				},
			},
			cases: []innerCase{
				{ // insert
					schema: "test",
					table:  "worker",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male", "Apple"},
					ignore: true,
				},
				{ // insert
					schema: "test",
					table:  "worker",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{11, "Tom", 21, "male", "FaceBook"},
					ignore: false,
				},
				{ // update
					schema: "test",
					table:  "worker",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female"},
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{1, "Dongmen", 20, "male"},
					ignore: true,
				},
				{ // delete
					schema: "test",
					table:  "worker",
					preColumns: []*model.ColumnData{
						{ColumnID: 0},
					},
					preRow: []interface{}{876, "Li", 45, "female", "Google"},
					ignore: true,
				},
				{ // insert
					schema:    "test",
					table:     "worker",
					updateDDl: "ALTER TABLE worker DROP COLUMN company",
					columns: []*model.ColumnData{
						{ColumnID: 0},
					},
					row:    []interface{}{999, "Will", 39, "male"},
					ignore: false,
					err:    cerror.ErrExpressionColumnNotFound,
					errMsg: "Cannot find column 'company' from table 'test.worker' in",
				},
			},
		},
	}

	sessCtx := utils.NewSessionCtx(map[string]string{
		"time_zone": "UTC",
	})

	for _, tc := range testCases {
		tableInfo := helper.execDDL(tc.ddl)
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		for _, c := range tc.cases {
			if c.updateDDl != "" {
				tableInfo = helper.execDDL(c.updateDDl)
			}
			rowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.row, tableInfo.Columns)
			require.Nil(t, err)
			preRowDatums, err := utils.AdjustBinaryProtocolForDatum(sessCtx, c.preRow, tableInfo.Columns)
			require.Nil(t, err)
			row := &model.RowChangedEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: c.schema,
						Table:  c.table,
					},
				},
				Columns:    c.columns,
				PreColumns: c.preColumns,
			}
			rawRow := model.RowChangedDatums{
				RowDatums:    rowDatums,
				PreRowDatums: preRowDatums,
			}
			ignore, err := f.shouldSkipDML(row, rawRow, tableInfo)
			require.True(t, errors.ErrorEqual(c.err, err), "case: %+v", c, err)
			if err != nil {
				require.Contains(t, err.Error(), c.errMsg)
			}
			require.Equal(t, c.ignore, ignore, "case: %+v", c)
		}
	}
}

func TestVerify(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.close()
	helper.getTk().MustExec("use test;")

	type testCase struct {
		ddls   []string
		cfg    *config.FilterConfig
		err    error
		errMsg string
	}

	testCases := []testCase{
		{
			ddls: []string{
				"create table test.worker(id int primary key, name char(50), age int, company char(50), gender char(50))",
				"create table test.student(id int primary key, name char(50), age int, school char(50))",
				"create table test.teacher(id int primary key, name char(50), age int, school char(50))",
				"create table test.parent(id int primary key, name char(50), age int, company char(50))",
			},
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:                  []string{"test.worker"},
						IgnoreInsertValueExpr:    "age >= 20 and company = 'Apple'",
						IgnoreDeleteValueExpr:    "age >= 32 and company = 'Google'",
						IgnoreUpdateOldValueExpr: "gender = 'female'",
						IgnoreUpdateNewValueExpr: "age > 28",
					},
					{
						Matcher:               []string{"test.student"},
						IgnoreInsertValueExpr: "age < 20 and school = 'guanghua'",
						IgnoreDeleteValueExpr: "age < 11 and school = 'dongfang'",
					},
					{
						Matcher:               []string{"test.nonExist"},
						IgnoreInsertValueExpr: "age < 20 or id > 100",
						IgnoreDeleteValueExpr: "age > 100 or id < 20",
					},
					{
						Matcher:                  []string{"test.parent"},
						IgnoreUpdateNewValueExpr: "company = 'Apple'",
					},
					{
						Matcher:                  []string{"*.*"},
						IgnoreUpdateNewValueExpr: "id <= 100",
					},
				},
			},
		},
		{
			ddls: []string{
				"create table test.child(id int primary key, name char(50), age int, parent_id int, school char(50))",
			},
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:               []string{"test.child"},
						IgnoreInsertValueExpr: "company = 'Apple'",
					},
				},
			},
			err:    cerror.ErrExpressionColumnNotFound,
			errMsg: "Cannot find column 'company' from table 'test.child' in",
		},
		{
			ddls: []string{
				"create table test.fruit(id int primary key, name char(50), price int)",
			},
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:               []string{"test.fruit"},
						IgnoreInsertValueExpr: "error(price) == null",
					},
				},
			},
			err:    cerror.ErrExpressionParseFailed,
			errMsg: "There is a syntax error in",
		},
	}

	for _, tc := range testCases {
		var tableInfos []*model.TableInfo
		for _, ddl := range tc.ddls {
			ti := helper.execDDL(ddl)
			tableInfos = append(tableInfos, ti)
		}
		f, err := newExprFilter("", tc.cfg)
		require.Nil(t, err)
		err = f.verify(tableInfos)
		require.True(t, errors.ErrorEqual(tc.err, err), "case: %+v", tc, err)
		if err != nil {
			require.Contains(t, err.Error(), tc.errMsg)
		}
	}
}

func TestGetColumnFromError(t *testing.T) {
	type testCase struct {
		err      error
		expected string
	}

	testCases := []testCase{
		{
			err:      plannererrors.ErrUnknownColumn.FastGenByArgs("mother", "expression"),
			expected: "mother",
		},
		{
			err:      plannererrors.ErrUnknownColumn.FastGenByArgs("company", "expression"),
			expected: "company",
		},
		{
			err:      errors.New("what ever"),
			expected: "what ever",
		},
	}

	for _, tc := range testCases {
		column := getColumnFromError(tc.err)
		require.Equal(t, tc.expected, column, "case: %+v", tc)
	}
}
