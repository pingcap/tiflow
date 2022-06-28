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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"go.uber.org/zap"

	bf "github.com/pingcap/tidb-tools/pkg/binlog-filter"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/stretchr/testify/require"
)

func TestShouldUseDefaultRules(t *testing.T) {
	t.Parallel()

	filter, err := NewFilter(config.GetDefaultReplicaConfig())
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
	})
	require.Nil(t, err)
	require.True(t, filter.ShouldIgnoreTable("other", ""))
	require.True(t, filter.ShouldIgnoreTable("other", "what"))
	require.False(t, filter.ShouldIgnoreTable("sns", ""))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order"))
	require.False(t, filter.ShouldIgnoreTable("ecom", "order"))
	require.True(t, filter.ShouldIgnoreTable("ecom", "test"))
	require.True(t, filter.ShouldIgnoreTable("sns", "log"))
	require.True(t, filter.ShouldIgnoreTable("information_schema", ""))
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
		})
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			dml := &model.RowChangedEvent{
				Table:   &model.TableName{Table: tc.table, Schema: tc.schema},
				StartTs: tc.ts,
			}
			ignoreDML, err := filter.ShouldIgnoreDMLEvent(dml, nil)
			require.Nil(t, err)
			require.Equal(t, ignoreDML, tc.ignore)
		}
	}
}

func TestShouldDiscardDDL(t *testing.T) {
	t.Parallel()

	config := &config.ReplicaConfig{
		Filter: &config.FilterConfig{
			DDLAllowlist: []timodel.ActionType{timodel.ActionAddForeignKey},
		},
	}
	filter, err := NewFilter(config)
	require.Nil(t, err)
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddForeignKey))

	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropSchema))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionCreateTable))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropTable))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddColumn))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropColumn))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddIndex))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropIndex))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionTruncateTable))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionModifyColumn))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionRenameTable))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionRenameTables))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionSetDefaultValue))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionModifyTableComment))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionRenameIndex))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddTablePartition))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropTablePartition))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionCreateView))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionModifyTableCharsetAndCollate))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionTruncateTablePartition))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropView))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionRecoverTable))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionModifySchemaCharsetAndCollate))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddPrimaryKey))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropPrimaryKey))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionAddColumns))
	require.False(t, filter.ShouldDiscardDDL(timodel.ActionDropColumns))

	// Discard sequence DDL.
	require.True(t, filter.ShouldDiscardDDL(timodel.ActionCreateSequence))
	require.True(t, filter.ShouldDiscardDDL(timodel.ActionAlterSequence))
	require.True(t, filter.ShouldDiscardDDL(timodel.ActionDropSequence))
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
		rules      []string
		ignoredTs  []uint64
		eventRules []*bf.BinlogEventRule
	}{{
		// Ignore by table name cases.
		cases: []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{1, "sns", "", "create database test", timodel.ActionCreateSchema, false},
			{1, "sns", "", "drop database test", timodel.ActionDropSchema, false},
			{1, "sns", "", "", timodel.ActionModifySchemaCharsetAndCollate, false},
			{1, "ecom", "", "create database test", timodel.ActionCreateSchema, false},
			{1, "ecom", "aa", "create table test.t1(a int primary key)", timodel.ActionCreateTable, false},
			{1, "ecom", "", "create database test", timodel.ActionCreateSchema, false},
			{1, "test", "", "create database test", timodel.ActionCreateSchema, true},
		},
		rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
		// Ignore by schema name cases.
	}, {
		cases: []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{1, "schema", "", "", timodel.ActionCreateSchema, false},
			{1, "schema", "", "", timodel.ActionDropSchema, false},
			{1, "schema", "", "", timodel.ActionModifySchemaCharsetAndCollate, false},
			{1, "schema", "aa", "", timodel.ActionCreateTable, true},
			{1, "schema", "C1", "", timodel.ActionCreateTable, false},
			{1, "schema", "", "", timodel.ActionCreateTable, true},
		},
		rules: []string{"schema.C1"},
	}, { // cases ignore by startTs
		cases: []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{1, "ts", "", "", timodel.ActionCreateSchema, true},
			{2, "ts", "student", "", timodel.ActionDropSchema, true},
			{3, "ts", "teacher", "", timodel.ActionModifySchemaCharsetAndCollate, true},
			{4, "ts", "man", "", timodel.ActionCreateTable, false},
			{5, "ts", "fruit", "", timodel.ActionCreateTable, false},
			{6, "ts", "insect", "", timodel.ActionCreateTable, false},
		},
		rules:     []string{"*.*"},
		ignoredTs: []uint64{1, 2, 3},
	}, { // cases ignore by ddl type.
		cases: []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{1, "event", "", "", timodel.ActionDropTable, true},
			{1, "event", "January", "", timodel.ActionDropIndex, true},
			{1, "event", "February", "", timodel.ActionDropIndexes, true},
			{1, "event", "March", "", timodel.ActionCreateTable, false},
			{1, "event", "April", "", timodel.ActionCreateTable, false},
			{1, "event", "May", "", timodel.ActionCreateTable, false},
		},
		rules: []string{"*.*"},
		eventRules: []*bf.BinlogEventRule{
			{
				SchemaPattern: "event",
				TablePattern:  "*",
				Action:        bf.Ignore,
				Events:        []bf.EventType{bf.DropTable, bf.DropIndex},
			},
		},
	}, { // cases ignore by ddl query
		cases: []struct {
			startTs uint64
			schema  string
			table   string
			query   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{1, "sql_pattern", "t1", "CREATE DATABASE sql-pattern", timodel.ActionNone, false},
			{1, "sql_pattern", "t1", "DROP DATABASE sql-pattern", timodel.ActionNone, true},
			{
				1, "sql_pattern", "t1",
				"ALTER DATABASE `test_db` CHARACTER SET 'utf8' COLLATE 'utf8_general_ci'",
				timodel.ActionNone, false,
			},
			{1, "sql_pattern", "t1", "CREATE TABLE t1(id int primary key)", timodel.ActionNone, false},
			{1, "sql_pattern", "t1", "DROP TABLE t1", timodel.ActionNone, true},
			{1, "sql_pattern", "t1", "ADD VIEW view_t1", timodel.ActionNone, true},
		},
		rules: []string{"*.*"},
		eventRules: []*bf.BinlogEventRule{
			{
				SchemaPattern: "sql_pattern",
				TablePattern:  "*",
				Action:        bf.Ignore,
				SQLPattern:    []string{"^DROP TABLE", "^ADD VIEW", "^DROP DATABASE"},
			},
		},
	}}

	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				Rules:            ftc.rules,
				IgnoreTxnStartTs: ftc.ignoredTs,
				EventRules:       ftc.eventRules,
			},
		})
		if err != nil {
			log.Info("fizz", zap.Any("event roles", ftc.rules))
		}
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			tableInfo := &model.SimpleTableInfo{Schema: tc.schema, Table: tc.table}
			ddl := &model.DDLEvent{StartTs: tc.startTs, Type: tc.ddlType, TableInfo: tableInfo, Query: tc.query}
			ignore, err := filter.ShouldIgnoreDDLEvent(ddl)
			require.Nil(t, err)
			require.Equal(t, tc.ignore, ignore, "%#v", tc)
		}
	}
}
