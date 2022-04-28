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

	"github.com/pingcap/tiflow/pkg/config"

	"github.com/pingcap/tidb/parser/model"
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
		Cyclic: &config.CyclicConfig{Enable: true},
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
	require.False(t, filter.ShouldIgnoreTable("tidb_cdc", "repl_mark_a_a"))
}

func TestShouldIgnoreTxn(t *testing.T) {
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
			require.Equal(t, filter.ShouldIgnoreDMLEvent(tc.ts, tc.schema, tc.table), tc.ignore)
			require.Equal(t, filter.ShouldIgnoreDDLEvent(tc.ts, model.ActionCreateTable, tc.schema, tc.table), tc.ignore)
		}
	}
}

func TestShouldDiscardDDL(t *testing.T) {
	t.Parallel()

	config := &config.ReplicaConfig{
		Filter: &config.FilterConfig{
			DDLAllowlist: []model.ActionType{model.ActionAddForeignKey},
		},
	}
	filter, err := NewFilter(config)
	require.Nil(t, err)
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddForeignKey))

	require.False(t, filter.ShouldDiscardDDL(model.ActionDropSchema))
	require.False(t, filter.ShouldDiscardDDL(model.ActionCreateTable))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropTable))
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddColumn))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropColumn))
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddIndex))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropIndex))
	require.False(t, filter.ShouldDiscardDDL(model.ActionTruncateTable))
	require.False(t, filter.ShouldDiscardDDL(model.ActionModifyColumn))
	require.False(t, filter.ShouldDiscardDDL(model.ActionRenameTable))
	require.False(t, filter.ShouldDiscardDDL(model.ActionRenameTables))
	require.False(t, filter.ShouldDiscardDDL(model.ActionSetDefaultValue))
	require.False(t, filter.ShouldDiscardDDL(model.ActionModifyTableComment))
	require.False(t, filter.ShouldDiscardDDL(model.ActionRenameIndex))
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddTablePartition))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropTablePartition))
	require.False(t, filter.ShouldDiscardDDL(model.ActionCreateView))
	require.False(t, filter.ShouldDiscardDDL(model.ActionModifyTableCharsetAndCollate))
	require.False(t, filter.ShouldDiscardDDL(model.ActionTruncateTablePartition))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropView))
	require.False(t, filter.ShouldDiscardDDL(model.ActionRecoverTable))
	require.False(t, filter.ShouldDiscardDDL(model.ActionModifySchemaCharsetAndCollate))
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddPrimaryKey))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropPrimaryKey))
	require.False(t, filter.ShouldDiscardDDL(model.ActionAddColumns))
	require.False(t, filter.ShouldDiscardDDL(model.ActionDropColumns))

	// Discard sequence DDL.
	require.True(t, filter.ShouldDiscardDDL(model.ActionCreateSequence))
	require.True(t, filter.ShouldDiscardDDL(model.ActionAlterSequence))
	require.True(t, filter.ShouldDiscardDDL(model.ActionDropSequence))
}

func TestShouldIgnoreDDL(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		cases []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}
		rules []string
	}{{
		cases: []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}{
			{"sns", "", model.ActionCreateSchema, false},
			{"sns", "", model.ActionDropSchema, false},
			{"sns", "", model.ActionModifySchemaCharsetAndCollate, false},
			{"ecom", "", model.ActionCreateSchema, false},
			{"ecom", "aa", model.ActionCreateTable, false},
			{"ecom", "", model.ActionCreateSchema, false},
			{"test", "", model.ActionCreateSchema, true},
		},
		rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
	}, {
		cases: []struct {
			schema  string
			table   string
			ddlType model.ActionType
			ignore  bool
		}{
			{"sns", "", model.ActionCreateSchema, false},
			{"sns", "", model.ActionDropSchema, false},
			{"sns", "", model.ActionModifySchemaCharsetAndCollate, false},
			{"sns", "aa", model.ActionCreateTable, true},
			{"sns", "C1", model.ActionCreateTable, false},
			{"sns", "", model.ActionCreateTable, true},
		},
		rules: []string{"sns.C1"},
	}}
	for _, ftc := range testCases {
		filter, err := NewFilter(&config.ReplicaConfig{
			Filter: &config.FilterConfig{
				IgnoreTxnStartTs: []uint64{},
				Rules:            ftc.rules,
			},
		})
		require.Nil(t, err)
		for _, tc := range ftc.cases {
			require.Equal(t, filter.ShouldIgnoreDDLEvent(1, tc.ddlType, tc.schema, tc.table), tc.ignore, "%#v", tc)
		}
	}
}
