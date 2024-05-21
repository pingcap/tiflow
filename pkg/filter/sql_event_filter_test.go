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
	"github.com/pingcap/tiflow/cdc/model"
	bf "github.com/pingcap/tiflow/pkg/binlog-filter"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestShouldSkipDDL(t *testing.T) {
	t.Parallel()
	type innerCase struct {
		schema string
		table  string
		query  string
		skip   bool
	}

	type testCase struct {
		cfg   *config.FilterConfig
		cases []innerCase
		err   error
	}

	testCases := []testCase{
		{
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
					schema: "test",
					table:  "t1",
					query:  "alter table t1 modify column age int",
					skip:   true,
				},
				{
					schema: "test",
					table:  "t1",
					query:  "create table t1(id int primary key)",
					skip:   true,
				},
				{
					schema: "test",
					table:  "t2", // table name not match
					query:  "alter table t2 modify column age int",
					skip:   false,
				},
				{
					schema: "test2", // schema name not match
					table:  "t1",
					query:  "alter table t1 modify column age int",
					skip:   false,
				},
			},
		},
		{
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
					schema: "test",
					table:  "t1",
					query:  "alter table t1 modify column age int",
					skip:   false,
				},
				{
					schema: "test",
					table:  "t1",
					query:  "alter table t1 drop column age",
					skip:   false,
				},
				{
					schema: "test2",
					table:  "t1",
					query:  "drop database test2",
					skip:   true,
				},
				{
					schema: "test3",
					table:  "t1",
					query:  "drop index i3 on t1",
					skip:   false,
				},
			},
		},
		{
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
					schema: "test",
					table:  "t1",
					query:  "ALTER TABLE t1 MODIFY COLUMN age int(11) NOT NULL",
					skip:   true,
				},
				{
					schema: "test",
					table:  "t1",
					query:  "ALTER TABLE t1 DROP COLUMN age",
					skip:   true,
				},
				{ // no table name
					schema: "test2",
					query:  "DROP DATABASE test",
					skip:   true,
				},
				{
					schema: "test3",
					table:  "t1",
					query:  "Drop Index i1 on test3.t1",
					skip:   false,
				},
			},
		},
		{ // config error
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:     []string{"*.t1"},
						IgnoreEvent: []bf.EventType{bf.EventType("aa")},
					},
				},
			},
			err: cerror.ErrInvalidIgnoreEventType,
		},
		{
			cfg: &config.FilterConfig{
				EventFilters: []*config.EventFilterRule{
					{
						Matcher:   []string{"*.t1"},
						IgnoreSQL: []string{"--6"}, // this is a valid regx
					},
				},
			},
		},
	}

	for _, tc := range testCases {
		f, err := newSQLEventFilter(tc.cfg, config.GetDefaultReplicaConfig().SQLMode)
		require.True(t, errors.ErrorEqual(err, tc.err), "case: %+s", err)
		for _, c := range tc.cases {
			ddl := &model.DDLEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: c.schema,
						Table:  c.table,
					},
				},
				Query: c.query,
			}
			skip, err := f.shouldSkipDDL(ddl)
			require.NoError(t, err)
			require.Equal(t, c.skip, skip, "case: %+v", c)
		}
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
			f, err := newSQLEventFilter(tc.cfg, config.GetDefaultReplicaConfig().SQLMode)
			require.NoError(t, err)
			for _, c := range tc.cases {
				event := &model.RowChangedEvent{
					Table: &model.TableName{
						Schema: c.schema,
						Table:  c.table,
					},
				}
				if c.columns != "" {
					event.Columns = []*model.Column{{Value: c.columns}}
				}
				if c.preColumns != "" {
					event.PreColumns = []*model.Column{{Value: c.preColumns}}
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

	cases := make([]testCase, len(supportedEventTypes))
	for i, eventType := range supportedEventTypes {
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
