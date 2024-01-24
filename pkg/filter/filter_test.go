// Copyright 2020 PingCAP, Inc.
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

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestShouldUseDefaultRules(t *testing.T) {
	t.Parallel()

	filter, err := NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	require.True(t, filter.ShouldIgnoreTable("information_schema", ""))
	require.True(t, filter.ShouldIgnoreTable("information_schema", "statistics"))
	require.True(t, filter.ShouldIgnoreTable("performance_schema", ""))
	require.False(t, filter.ShouldIgnoreTable("metric_schema", "query_duration"))
	require.False(t, filter.ShouldIgnoreTable("sns", "user"))
	require.False(t, filter.ShouldIgnoreTable("tidb_cdc", "repl_mark_a_a"))
}

func TestShouldUseCustomRules(t *testing.T) {
	t.Parallel()

	filter, err := NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			Rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
	}, "")
	require.Nil(t, err)
	require.True(t, filter.ShouldIgnoreTable("other", ""))
	require.True(t, filter.ShouldIgnoreTable("other", "what"))
	require.False(t, filter.ShouldIgnoreTable("sns", ""))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order"))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order"))
	require.True(t, filter.ShouldIgnoreTable("ecom", "test"))
	require.True(t, filter.ShouldIgnoreTable("sns", "log"))
	require.True(t, filter.ShouldIgnoreTable("information_schema", ""))

	filter, err = NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			// 1. match all schema and table
			// 2. do not match test.season
			// 3. match all table of schema school
			// 4. do not match table school.teacher
			Rules: []string{"*.*", "!test.season", "school.*", "!school.teacher"},
		},
	}, "")
	require.True(t, filter.ShouldIgnoreTable("test", "season"))
	require.False(t, filter.ShouldIgnoreTable("other", ""))
	require.False(t, filter.ShouldIgnoreTable("school", "student"))
	require.True(t, filter.ShouldIgnoreTable("school", "teacher"))
	require.Nil(t, err)

	filter, err = NewFilter(&config.ReplicaConfig{
		Filter: &config.FilterConfig{
			// 1. match all schema and table
			// 2. do not match test.season
			// 3. match all table of schema school
			// 4. do not match table school.teacher
			Rules: []string{"*.*", "!test.t1", "!test.t2"},
		},
	}, "")
	require.False(t, filter.ShouldIgnoreTable("test", "season"))
	require.True(t, filter.ShouldIgnoreTable("test", "t1"))
	require.True(t, filter.ShouldIgnoreTable("test", "t2"))
	require.Nil(t, err)
}

func TestShouldIgnoreDMLEvent(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		cases []struct {
			schema string
			table  string
			ts     uint64
			ignore bool
		}
		ignoreTxnStartTs []uint64
		rules            []string
	}{
		{
			cases: []struct {
				schema string
				table  string
				ts     uint64
				ignore bool
			}{
				{"sns", "ttta", 1, true},
				{"ecom", "aabb", 2, false},
				{"sns", "log", 3, true},
				{"sns", "log", 4, true},
				{"ecom", "test", 5, true},
				{"test", "test", 6, true},
				{"ecom", "log", 6, false},
			},
			ignoreTxnStartTs: []uint64{1, 3},
			rules:            []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
		{
			cases: []struct {
				schema string
				table  string
				ts     uint64
				ignore bool
			}{
				{"S", "D1", 1, true},
				{"S", "Da", 1, false},
				{"S", "Db", 1, false},
				{"S", "Daa", 1, false},
			},
			ignoreTxnStartTs: []uint64{},
			rules:            []string{"*.*", "!S.D[!a-d]"},
		},
	}

	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				IgnoreTxnStartTs: ftc.ignoreTxnStartTs,
				Rules:            ftc.rules,
			},
		}, "")
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			dml := &model.RowChangedEvent{
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: tc.schema,
						Table:  tc.table,
					},
				},
				StartTs: tc.ts,
			}
			ignoreDML, err := filter.ShouldIgnoreDMLEvent(dml, model.RowChangedDatums{}, nil)
			require.Nil(t, err)
			require.Equal(t, ignoreDML, tc.ignore)
		}
	}
}

func TestShouldIgnoreDDL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		cases []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}
		rules        []string
		ignoredTs    []uint64
		eventFilters []*config.EventFilterRule
	}{
		{ // cases ignore by startTs
			cases: []struct {
				startTs uint64
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{1, "ts", "", "create database test", timodel.ActionCreateSchema, true},
				{2, "ts", "student", "drop database test2", timodel.ActionDropSchema, true},
				{
					3, "ts", "teacher", "ALTER DATABASE dbname CHARACTER SET utf8 COLLATE utf8_general_ci",
					timodel.ActionModifySchemaCharsetAndCollate, true,
				},
				{4, "ts", "man", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
				{5, "ts", "fruit", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
				{6, "ts", "insect", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
			},
			rules:     []string{"*.*"},
			ignoredTs: []uint64{1, 2, 3},
		},
		{ // cases ignore by ddl type.
			cases: []struct {
				startTs uint64
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{1, "event", "", "drop table t1", timodel.ActionDropTable, true},
				{1, "event", "January", "drop index i on t1", timodel.ActionDropIndex, true},
				{1, "event", "February", "drop index x2 on t2", timodel.ActionDropIndex, true},
				{1, "event", "March", "create table t2(age int)", timodel.ActionCreateTable, false},
				{1, "event", "April", "create table t2(age int)", timodel.ActionCreateTable, false},
				{1, "event", "May", "create table t2(age int)", timodel.ActionCreateTable, false},
			},
			rules: []string{"*.*"},
			eventFilters: []*config.EventFilterRule{
				{
					Matcher: []string{"event.*"},
					IgnoreEvent: []bf.EventType{
						bf.AlterTable, bf.DropTable,
					},
				},
			},
			ignoredTs: []uint64{},
		},
		{ // cases ignore by ddl query
			cases: []struct {
				startTs uint64
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{1, "sql_pattern", "t1", "CREATE DATABASE sql_pattern", timodel.ActionCreateSchema, false},
				{1, "sql_pattern", "t1", "DROP DATABASE sql_pattern", timodel.ActionDropSchema, true},
				{
					1, "sql_pattern", "t1",
					"ALTER DATABASE `test_db` CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'",
					timodel.ActionModifySchemaCharsetAndCollate,
					false,
				},
				{1, "sql_pattern", "t1", "CREATE TABLE t1(id int primary key)", timodel.ActionCreateTable, false},
				{1, "sql_pattern", "t1", "DROP TABLE t1", timodel.ActionDropTable, true},
				{1, "sql_pattern", "t1", "CREATE VIEW test.v AS SELECT * FROM t", timodel.ActionCreateView, true},
			},
			rules: []string{"*.*"},
			eventFilters: []*config.EventFilterRule{
				{
					Matcher:   []string{"sql_pattern.*"},
					IgnoreSQL: []string{"^DROP TABLE", "^CREATE VIEW", "^DROP DATABASE"},
				},
			},
			ignoredTs: []uint64{},
		},
	}

	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				Rules:            ftc.rules,
				EventFilters:     ftc.eventFilters,
				IgnoreTxnStartTs: ftc.ignoredTs,
			},
		}, "")
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			ddl := &model.DDLEvent{
				StartTs: tc.startTs,
				TableInfo: &model.TableInfo{
					TableName: model.TableName{
						Schema: tc.schema,
						Table:  tc.table,
					},
				},
				Query: tc.query,
				Type:  tc.ddlType,
			}
			ignore, err := filter.ShouldIgnoreDDLEvent(ddl)
			require.NoError(t, err)
			require.Equal(t, tc.ignore, ignore, "%#v", tc)
		}
	}
}

func TestShouldDiscardDDL(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		cases []struct {
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}
		rules        []string
		eventFilters []*config.EventFilterRule
	}{
		{
			// Discard by not allowed DDL type cases.
			cases: []struct {
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{"sns", "", "create database test", timodel.ActionCreateSchema, false},
				{"sns", "", "drop database test", timodel.ActionDropSchema, false},
				{"test", "", "create database test", timodel.ActionCreateSequence, true},
			},
			rules: []string{"*.*"},
		},
		{
			// Discard by table name cases.
			cases: []struct {
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{"sns", "", "create database test", timodel.ActionCreateSchema, false},
				{"sns", "", "drop database test", timodel.ActionDropSchema, false},
				{
					"sns", "", "ALTER DATABASE dbname CHARACTER SET utf8 COLLATE utf8_general_ci",
					timodel.ActionModifySchemaCharsetAndCollate, false,
				},
				{"ecom", "", "create database test", timodel.ActionCreateSchema, false},
				{"ecom", "aa", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
				{"ecom", "", "create database test", timodel.ActionCreateSchema, false},
				{"test", "", "create database test", timodel.ActionCreateSchema, true},
			},
			rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		},
		{
			// Discard by schema name cases.
			cases: []struct {
				schema  string
				table   string
				query   string
				ddlType timodel.ActionType
				ignore  bool
			}{
				{"schema", "C1", "create database test", timodel.ActionCreateSchema, false},
				{"test", "", "drop database test1", timodel.ActionDropSchema, true},
				{
					"dbname", "", "ALTER DATABASE dbname CHARACTER SET utf8 COLLATE utf8_general_ci",
					timodel.ActionModifySchemaCharsetAndCollate, true,
				},
				{"test", "aa", "create table test.t1(a int primary key)", timodel.ActionCreateTable, true},
				{"schema", "C1", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
				{"schema", "", "create table test.t1(a int primary key)", timodel.ActionCreateTable, true},
			},
			rules: []string{"schema.C1"},
		},
	}

	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				Rules:        ftc.rules,
				EventFilters: ftc.eventFilters,
			},
		}, "")
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			ignore := filter.ShouldDiscardDDL(tc.ddlType, tc.schema, tc.table)
			require.Equal(t, tc.ignore, ignore, "%#v", tc)
		}
	}
}

func TestIsAllowedDDL(t *testing.T) {
	type testCase struct {
		timodel.ActionType
		allowed bool
	}
	testCases := make([]testCase, 0, len(allowDDLList))
	for _, action := range allowDDLList {
		testCases = append(testCases, testCase{action, true})
	}
	testCases = append(testCases, testCase{timodel.ActionAddForeignKey, false})
	testCases = append(testCases, testCase{timodel.ActionDropForeignKey, false})
	testCases = append(testCases, testCase{timodel.ActionCreateSequence, false})
	testCases = append(testCases, testCase{timodel.ActionAlterSequence, false})
	testCases = append(testCases, testCase{timodel.ActionDropSequence, false})
	for _, tc := range testCases {
		require.Equal(t, tc.allowed, isAllowedDDL(tc.ActionType), "%#v", tc)
	}
}

func TestIsSchemaDDL(t *testing.T) {
	cases := []struct {
		actionType timodel.ActionType
		isSchema   bool
	}{
		{timodel.ActionCreateSchema, true},
		{timodel.ActionDropSchema, true},
		{timodel.ActionModifySchemaCharsetAndCollate, true},
		{timodel.ActionCreateTable, false},
		{timodel.ActionDropTable, false},
		{timodel.ActionTruncateTable, false},
		{timodel.ActionAddColumn, false},
	}
	for _, tc := range cases {
		require.Equal(t, tc.isSchema, IsSchemaDDL(tc.actionType), "%#v", tc)
	}
}
