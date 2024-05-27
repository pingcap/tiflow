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
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldSkipDDL(t *testing.T) {
	t.Parallel()
	type innerCase struct {
		schema  string
		table   string
		query   string
		ddlType timodel.ActionType
		skip    bool
	}

	type testCase struct {
		cfg   *config.FilterConfig
		cases []innerCase
		err   error
	}

	// filter all ddl
	case1 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"test.t1"},
					IgnoreEvent: []bf.EventType{bf.AllDDL},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "create table t1(id int primary key)",
				ddlType: timodel.ActionCreateTable,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t2", // table name not match
				query:   "alter table t2 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
			{
				schema:  "test2", // schema name not match
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
		},
	}
	f, err := newSQLEventFilter(case1.cfg)
	require.True(t, errors.ErrorEqual(err, case1.err), "case: %+s", err)
	for _, c := range case1.cases {
		ddl := &model.DDLEvent{
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
			},
			Query: c.query,
			Type:  c.ddlType,
		}
		skip, err := f.shouldSkipDDL(ddl)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// filter some ddl
	case2 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"*.t1"},
					IgnoreEvent: []bf.EventType{bf.DropDatabase, bf.DropSchema},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 modify column age int",
				ddlType: timodel.ActionModifyColumn,
				skip:    false,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "alter table t1 drop column age",
				ddlType: timodel.ActionDropColumn,
				skip:    false,
			},
			{
				schema:  "test2",
				table:   "t1",
				query:   "drop database test2",
				ddlType: timodel.ActionDropSchema,
				skip:    true,
			},
			{
				schema:  "test3",
				table:   "t1",
				query:   "drop index i3 on t1",
				ddlType: timodel.ActionDropIndex,
				skip:    false,
			},
		},
	}
	f, err = newSQLEventFilter(case2.cfg)
	require.True(t, errors.ErrorEqual(err, case2.err), "case: %+s", err)
	for _, c := range case2.cases {
		ddl := &model.DDLEvent{
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
			},
			Query: c.query,
			Type:  c.ddlType,
		}
		skip, err := f.shouldSkipDDL(ddl)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// filter ddl by IgnoreSQL
	case3 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:   []string{"*.t1"},
					IgnoreSQL: []string{"MODIFY COLUMN", "DROP COLUMN", "^DROP DATABASE"},
				},
			},
		},
		cases: []innerCase{
			{
				schema:  "test",
				table:   "t1",
				query:   "ALTER TABLE t1 MODIFY COLUMN age int(11) NOT NULL",
				ddlType: timodel.ActionModifyColumn,
				skip:    true,
			},
			{
				schema:  "test",
				table:   "t1",
				query:   "ALTER TABLE t1 DROP COLUMN age",
				ddlType: timodel.ActionDropColumn,
				skip:    true,
			},
			{ // no table name
				schema:  "test2",
				query:   "DROP DATABASE test",
				ddlType: timodel.ActionDropSchema,
				skip:    true,
			},
			{
				schema:  "test3",
				table:   "t1",
				query:   "Drop Index i1 on test3.t1",
				ddlType: timodel.ActionDropIndex,
				skip:    false,
			},
		},
	}
	f, err = newSQLEventFilter(case3.cfg)
	require.True(t, errors.ErrorEqual(err, case3.err), "case: %+s", err)
	for _, c := range case3.cases {
		ddl := &model.DDLEvent{
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
			},
			Query: c.query,
			Type:  c.ddlType,
		}
		skip, err := f.shouldSkipDDL(ddl)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}

	// config error
	case4 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"*.t1"},
					IgnoreEvent: []bf.EventType{bf.EventType("aa")},
				},
			},
		},
		err: cerror.ErrInvalidIgnoreEventType,
	}
	_, err = newSQLEventFilter(case4.cfg)
	require.True(t, errors.ErrorEqual(err, case4.err), "case: %+s", err)

	// config error
	case5 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:   []string{"*.t1"},
					IgnoreSQL: []string{"--6"}, // this is a valid regx
				},
			},
		},
	}
	_, err = newSQLEventFilter(case5.cfg)
	require.True(t, errors.ErrorEqual(err, case5.err), "case: %+s", err)

	// cover all ddl event types
	allEventTypes := make([]bf.EventType, 0, len(ddlWhiteListMap))
	for _, et := range ddlWhiteListMap {
		allEventTypes = append(allEventTypes, et)
	}
	innerCases := make([]innerCase, 0, len(ddlWhiteListMap))
	for at := range ddlWhiteListMap {
		innerCases = append(innerCases, innerCase{
			schema:  "test",
			table:   "t1",
			query:   "no matter",
			ddlType: at,
			skip:    true,
		})
	}
	case6 := testCase{
		cfg: &config.FilterConfig{
			EventFilters: []*config.EventFilterRule{
				{
					Matcher:     []string{"*.t1"},
					IgnoreEvent: allEventTypes,
				},
			},
		},
		cases: innerCases,
	}
	f, err = newSQLEventFilter(case6.cfg)
	require.NoError(t, err)
	for _, c := range case6.cases {
		ddl := &model.DDLEvent{
			TableInfo: &model.TableInfo{
				TableName: model.TableName{
					Schema: c.schema,
					Table:  c.table,
				},
			},
			Query: c.query,
			Type:  c.ddlType,
		}
		skip, err := f.shouldSkipDDL(ddl)
		require.NoError(t, err)
		require.Equal(t, c.skip, skip, "case: %+v", c)
	}
}

func TestShouldSkipDML(t *testing.T) {
	t.Parallel()
	type innerCase struct {
		schema     string
		table      string
		preColumns string
		columns    string
		skip       bool
	}

	type testCase struct {
		name  string
		cfg   *config.FilterConfig
		cases []innerCase
	}

	testCases := []testCase{
		{
			name: "dml-filter-test",
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"test1.allDml"},
						IgnoreEvent: []bf.EventType{bf.AllDML},
					},
					{
						Matcher:     []string{"test2.insert"},
						IgnoreEvent: []bf.EventType{bf.InsertEvent},
					},
					{
						Matcher:     []string{"*.delete"},
						IgnoreEvent: []bf.EventType{bf.DeleteEvent},
					},
				},
			},
			cases: []innerCase{
				{ // match test1.allDML
					schema:  "test1",
					table:   "allDml",
					columns: "insert",
					skip:    true,
				},
				{
					schema:     "test1",
					table:      "allDml",
					preColumns: "delete",
					skip:       true,
				},
				{
					schema:     "test1",
					table:      "allDml",
					preColumns: "update",
					columns:    "update",
					skip:       true,
				},
				{ // not match
					schema:  "test",
					table:   "t1",
					columns: "insert",
					skip:    false,
				},
				{ // match test2.insert
					schema:  "test2",
					table:   "insert",
					columns: "insert",
					skip:    true,
				},
				{
					schema:     "test2",
					table:      "insert",
					preColumns: "delete",
					skip:       false,
				},
				{
					schema:     "test2",
					table:      "insert",
					preColumns: "update",
					columns:    "update",
					skip:       false,
				},
				{
					schema:  "noMatter",
					table:   "delete",
					columns: "insert",
					skip:    false,
				},
				{
					schema:     "noMatter",
					table:      "delete",
					preColumns: "update",
					columns:    "update",
					skip:       false,
				},
				{
					schema:     "noMatter",
					table:      "delete",
					preColumns: "delete",
					skip:       true,
				},
			},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			f, err := newSQLEventFilter(tc.cfg)
			require.NoError(t, err)
			for _, c := range tc.cases {
				event := &model.RowChangedEvent{
					TableInfo: &model.TableInfo{
						TableName: model.TableName{
							Schema: c.schema,
							Table:  c.table,
						},
					},
				}
				if c.columns != "" {
					event.Columns = []*model.ColumnData{{Value: c.columns}}
				}
				if c.preColumns != "" {
					event.PreColumns = []*model.ColumnData{{Value: c.preColumns}}
				}
				skip, err := f.shouldSkipDML(event)
				require.NoError(t, err)
				require.Equal(t, c.skip, skip, "case: %+v", c)
			}
		})
	}
}

func TestVerifyIgnoreEvents(t *testing.T) {
	t.Parallel()
	type testCase struct {
		ignoreEvent []bf.EventType
		err         error
	}

	cases := make([]testCase, len(SupportedEventTypes()))
	for i, eventType := range SupportedEventTypes() {
		cases[i] = testCase{
			ignoreEvent: []bf.EventType{eventType},
			err:         nil,
		}
	}

	cases = append(cases, testCase{
		ignoreEvent: []bf.EventType{bf.EventType("unknown")},
		err:         cerror.ErrInvalidIgnoreEventType,
	})

	cases = append(cases, testCase{
		ignoreEvent: []bf.EventType{bf.AlterTable},
		err:         nil,
	})

	for _, tc := range cases {
		require.True(t, errors.ErrorEqual(tc.err, verifyIgnoreEvents(tc.ignoreEvent)))
	}
}
