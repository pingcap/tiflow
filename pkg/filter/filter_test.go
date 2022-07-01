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

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"

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
			require.Equal(t, tc.ignore, filter.ShouldIgnoreDMLEvent(tc.ts, tc.schema, tc.table))
			ddl := &model.DDLEvent{
				StartTs: tc.ts, Type: timodel.ActionCreateTable,
				TableInfo: &model.SimpleTableInfo{Schema: tc.schema, Table: tc.table},
			}
			require.Equal(t, tc.ignore, filter.ShouldIgnoreDDLEvent(ddl), "%#v", tc)
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
			schema  string
			table   string
			ddlType timodel.ActionType
			ignore  bool
		}
		rules []string
	}{{
		cases: []struct {
			schema  string
			table   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{"sns", "", timodel.ActionCreateSchema, false},
			{"sns", "", timodel.ActionDropSchema, false},
			{"sns", "", timodel.ActionModifySchemaCharsetAndCollate, false},
			{"ecom", "", timodel.ActionCreateSchema, false},
			{"ecom", "aa", timodel.ActionCreateTable, false},
			{"ecom", "", timodel.ActionCreateSchema, false},
			{"test", "", timodel.ActionCreateSchema, true},
		},
		rules: []string{"sns.*", "ecom.*", "!sns.log", "!ecom.test"},
	}, {
		cases: []struct {
			schema  string
			table   string
			ddlType timodel.ActionType
			ignore  bool
		}{
			{"sns", "", timodel.ActionCreateSchema, false},
			{"sns", "", timodel.ActionDropSchema, false},
			{"sns", "", timodel.ActionModifySchemaCharsetAndCollate, false},
			{"sns", "aa", timodel.ActionCreateTable, true},
			{"sns", "C1", timodel.ActionCreateTable, false},
			{"sns", "", timodel.ActionCreateTable, true},
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
			ddl := &model.DDLEvent{
				StartTs: 1, Type: tc.ddlType,
				TableInfo: &model.SimpleTableInfo{Schema: tc.schema, Table: tc.table},
			}
			require.Equal(t, filter.ShouldIgnoreDDLEvent(ddl), tc.ignore, "%#v", tc)
		}
	}
}
