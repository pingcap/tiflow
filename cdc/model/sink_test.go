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

package model

import (
	"sort"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/meta/model"
	pmodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/sink/dispatcher"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSetFlag(t *testing.T) {
	t.Parallel()

	var flag ColumnFlagType
	flag.SetIsBinary()
	flag.SetIsGeneratedColumn()
	require.True(t, flag.IsBinary())
	require.False(t, flag.IsHandleKey())
	require.True(t, flag.IsGeneratedColumn())
	flag.UnsetIsBinary()
	require.False(t, flag.IsBinary())
	flag.SetIsMultipleKey()
	flag.SetIsUniqueKey()
	require.True(t, flag.IsMultipleKey() && flag.IsUniqueKey())
	flag.UnsetIsUniqueKey()
	flag.UnsetIsGeneratedColumn()
	flag.UnsetIsMultipleKey()
	require.False(t, flag.IsUniqueKey() || flag.IsGeneratedColumn() || flag.IsMultipleKey())

	flag = ColumnFlagType(0)
	flag.SetIsHandleKey()
	flag.SetIsPrimaryKey()
	flag.SetIsUnsigned()
	require.True(t, flag.IsHandleKey() && flag.IsPrimaryKey() && flag.IsUnsigned())
	flag.UnsetIsHandleKey()
	flag.UnsetIsPrimaryKey()
	flag.UnsetIsUnsigned()
	require.False(t, flag.IsHandleKey() || flag.IsPrimaryKey() || flag.IsUnsigned())
	flag.SetIsNullable()
	require.True(t, flag.IsNullable())
	flag.UnsetIsNullable()
	require.False(t, flag.IsNullable())
}

func TestFlagValue(t *testing.T) {
	t.Parallel()

	require.Equal(t, ColumnFlagType(0b1), BinaryFlag)
	require.Equal(t, ColumnFlagType(0b1), BinaryFlag)
	require.Equal(t, ColumnFlagType(0b10), HandleKeyFlag)
	require.Equal(t, ColumnFlagType(0b100), GeneratedColumnFlag)
	require.Equal(t, ColumnFlagType(0b1000), PrimaryKeyFlag)
	require.Equal(t, ColumnFlagType(0b10000), UniqueKeyFlag)
	require.Equal(t, ColumnFlagType(0b100000), MultipleKeyFlag)
	require.Equal(t, ColumnFlagType(0b1000000), NullableFlag)
}

func TestTableNameFuncs(t *testing.T) {
	t.Parallel()

	// Test without sink routing (TargetSchema not set)
	tbl := &TableName{
		Schema:  "test",
		Table:   "t1",
		TableID: 1071,
	}
	require.Equal(t, "test.t1", tbl.String())
	require.Equal(t, "`test`.`t1`", tbl.QuoteString())
	require.Equal(t, "`test`.`t1`", tbl.QuoteSinkString())
	require.Equal(t, "test", tbl.GetSchema())
	require.Equal(t, "t1", tbl.GetTable())
	require.Equal(t, int64(1071), tbl.GetTableID())

	// Test with sink routing (TargetSchema set)
	tblWithRouting := &TableName{
		Schema:       "uds_000",
		Table:        "TidbReplicationObject",
		TableID:      1072,
		TargetSchema: "tidb_failover_test",
	}
	require.Equal(t, "uds_000.TidbReplicationObject", tblWithRouting.String())
	// QuoteString should return source schema for logging
	require.Equal(t, "`uds_000`.`TidbReplicationObject`", tblWithRouting.QuoteString())
	// QuoteSinkString should return target schema for SQL generation
	require.Equal(t, "`tidb_failover_test`.`TidbReplicationObject`", tblWithRouting.QuoteSinkString())
	require.Equal(t, "uds_000", tblWithRouting.GetSchema())
	require.Equal(t, "TidbReplicationObject", tblWithRouting.GetTable())
	require.Equal(t, int64(1072), tblWithRouting.GetTableID())

	// Test with both schema and table routing (TargetSchema and TargetTable set)
	tblWithFullRouting := &TableName{
		Schema:       "uds_000",
		Table:        "TidbReplicationObject",
		TableID:      1073,
		TargetSchema: "tidb_failover_test",
		TargetTable:  "TidbReplicationObject_routed",
	}
	require.Equal(t, "uds_000.TidbReplicationObject", tblWithFullRouting.String())
	// QuoteString should return source schema/table for logging
	require.Equal(t, "`uds_000`.`TidbReplicationObject`", tblWithFullRouting.QuoteString())
	// QuoteSinkString should return target schema/table for SQL generation
	require.Equal(t, "`tidb_failover_test`.`TidbReplicationObject_routed`", tblWithFullRouting.QuoteSinkString())
	require.Equal(t, "uds_000", tblWithFullRouting.GetSchema())
	require.Equal(t, "TidbReplicationObject", tblWithFullRouting.GetTable())
	require.Equal(t, int64(1073), tblWithFullRouting.GetTableID())
}

func TestRowChangedEventFuncs(t *testing.T) {
	t.Parallel()
	deleteRow := &RowChangedEvent{
		TableInfo: &TableInfo{
			TableName: TableName{
				Schema: "test",
				Table:  "t1",
			},
		},
		PreColumns: []*ColumnData{
			{
				ColumnID: 1,
				Value:    1,
			}, {
				ColumnID: 2,
				Value:    2,
			},
		},
	}
	require.True(t, deleteRow.IsDelete())
}

func TestColumnValueString(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		val      interface{}
		expected string
	}{
		{interface{}(nil), "null"},
		{interface{}(true), "1"},
		{interface{}(false), "0"},
		{interface{}(123), "123"},
		{interface{}(int8(-123)), "-123"},
		{interface{}(int16(-123)), "-123"},
		{interface{}(int32(-123)), "-123"},
		{interface{}(int64(-123)), "-123"},
		{interface{}(uint8(123)), "123"},
		{interface{}(uint16(123)), "123"},
		{interface{}(uint32(123)), "123"},
		{interface{}(uint64(123)), "123"},
		{interface{}(float32(123.01)), "123.01"},
		{interface{}(float64(123.01)), "123.01"},
		{interface{}("123.01"), "123.01"},
		{interface{}([]byte("123.01")), "123.01"},
		{interface{}(complex(1, 2)), "(1+2i)"},
	}
	for _, tc := range testCases {
		s := ColumnValueString(tc.val)
		require.Equal(t, tc.expected, s)
	}
}

func TestDDLEventFromJob(t *testing.T) {
	t.Parallel()
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)
	job := &timodel.Job{
		ID:         1071,
		TableID:    49,
		SchemaName: "test",
		Type:       timodel.ActionAddColumn,
		StartTS:    420536581131337731,
		Query:      "alter table t1 add column a int",
		BinlogInfo: &timodel.HistoryInfo{
			TableInfo: &timodel.TableInfo{
				ID:   49,
				Name: pmodel.CIStr{O: "t1"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					{ID: 2, Name: pmodel.CIStr{O: "a"}, FieldType: types.FieldType{}, State: timodel.StatePublic},
				},
			},
			FinishedTS: 420536581196873729,
		},
	}
	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test",
			Table:   "t1",
			TableID: 49,
		},
		TableInfo: &timodel.TableInfo{
			ID:   49,
			Name: pmodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
			},
		},
	}
	tableInfo := WrapTableInfo(job.SchemaID, job.SchemaName, job.BinlogInfo.FinishedTS, job.BinlogInfo.TableInfo)
	event := &DDLEvent{}
	err := event.FromJob(job, preTableInfo, tableInfo, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(420536581131337731), event.StartTs)
	require.Equal(t, int64(49), event.TableInfo.TableName.TableID)
	require.Equal(t, 1, len(event.PreTableInfo.TableInfo.Columns))

	event = &DDLEvent{}
	err = event.FromJob(job, nil, nil, nil)
	require.NoError(t, err)
	require.Nil(t, event.PreTableInfo)
}

func TestRenameTables(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag | mysql.UniqueFlag)
	job := &timodel.Job{
		ID:         71,
		TableID:    69,
		SchemaName: "test1",
		Type:       timodel.ActionRenameTables,
		StartTS:    432853521879007233,
		Query:      "rename table test1.t1 to test1.t10, test1.t2 to test1.t20",
		BinlogInfo: &timodel.HistoryInfo{
			FinishedTS: 432853521879007238,
			MultipleTableInfos: []*timodel.TableInfo{
				{
					ID:   67,
					Name: pmodel.CIStr{O: "t10"},
					Columns: []*timodel.ColumnInfo{
						{
							ID:        1,
							Name:      pmodel.CIStr{O: "id"},
							FieldType: *ft,
							State:     timodel.StatePublic,
						},
					},
				},
				{
					ID:   69,
					Name: pmodel.CIStr{O: "t20"},
					Columns: []*timodel.ColumnInfo{
						{
							ID:        1,
							Name:      pmodel.CIStr{O: "id"},
							FieldType: *ft,
							State:     timodel.StatePublic,
						},
					},
				},
			},
		},
	}

	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t1",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: pmodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	tableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t10",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: pmodel.CIStr{O: "t10"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event := &DDLEvent{}
	err := event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1", nil)
	require.NoError(t, err)
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t1")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.TableInfo.TableName.Table, "t10")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t1` TO `test1`.`t10`")
	require.Equal(t, event.Type, timodel.ActionRenameTable)

	preTableInfo = &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t2",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: pmodel.CIStr{O: "t2"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	tableInfo = &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t20",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: pmodel.CIStr{O: "t20"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event = &DDLEvent{}
	err = event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1", nil)
	require.NoError(t, err)
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t2")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.TableInfo.TableName.Table, "t20")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.Query, "RENAME TABLE `test1`.`t2` TO `test1`.`t20`")
	require.Equal(t, event.Type, timodel.ActionRenameTable)
}

func TestExchangeTablePartition(t *testing.T) {
	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag | mysql.UniqueFlag)
	job := &timodel.Job{
		ID:         71,
		TableID:    69,
		SchemaName: "test1",
		Type:       timodel.ActionExchangeTablePartition,
		StartTS:    432853521879007233,
		Query:      "alter table t1 exchange partition p0 with table t2",
		BinlogInfo: &timodel.HistoryInfo{
			FinishedTS: 432853521879007238,
		},
	}

	// source table
	preTableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test2",
			Table:   "t2",
			TableID: 67,
		},
		TableInfo: &timodel.TableInfo{
			ID:   67,
			Name: pmodel.CIStr{O: "t1"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	// target table
	tableInfo := &TableInfo{
		TableName: TableName{
			Schema:  "test1",
			Table:   "t1",
			TableID: 69,
		},
		TableInfo: &timodel.TableInfo{
			ID:   69,
			Name: pmodel.CIStr{O: "t10"},
			Columns: []*timodel.ColumnInfo{
				{
					ID:        1,
					Name:      pmodel.CIStr{O: "id"},
					FieldType: *ft,
					State:     timodel.StatePublic,
				},
			},
		},
	}

	event := &DDLEvent{}
	err := event.FromJob(job, preTableInfo, tableInfo, nil)
	require.NoError(t, err)
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t2")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.TableInfo.TableName.Table, "t1")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, "ALTER TABLE `test1`.`t1` EXCHANGE PARTITION `p0` WITH TABLE `test2`.`t2`", event.Query)
	require.Equal(t, event.Type, timodel.ActionExchangeTablePartition)
}

// Helper function to create a SinkRouter for tests using dispatcher approach
func newTestSinkRouter(routes map[string]string) *dispatcher.SinkRouter {
	if len(routes) == 0 {
		return nil
	}

	// Create dispatch rules for sink routing
	dispatchRules := make([]*config.DispatchRule, 0, len(routes))
	for source, target := range routes {
		dispatchRules = append(dispatchRules, &config.DispatchRule{
			Matcher:    []string{source + ".*"},
			SchemaRule: target,
			TableRule:  dispatcher.TablePlaceholder,
		})
	}

	replicaConfig := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: dispatchRules,
		},
	}

	router, _ := dispatcher.NewSinkRouter(replicaConfig)
	return router
}

// tableRoute represents a full table routing configuration
type tableRoute struct {
	sourceSchema string
	sourceTable  string
	targetSchema string
	targetTable  string
}

// newTestSinkRouterWithTableOnlyRouting creates a SinkRouter that only routes the table name
// (schema stays the same via "{schema}" pattern, table changes to explicit name)
func newTestSinkRouterWithTableOnlyRouting(routes []tableRoute) *dispatcher.SinkRouter {
	if len(routes) == 0 {
		return nil
	}

	dispatchRules := make([]*config.DispatchRule, 0, len(routes))
	for _, route := range routes {
		dispatchRules = append(dispatchRules, &config.DispatchRule{
			Matcher:    []string{route.sourceSchema + "." + route.sourceTable},
			SchemaRule: dispatcher.SchemaPlaceholder, // Keep schema the same
			TableRule:  route.targetTable,
		})
	}

	replicaConfig := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: dispatchRules,
		},
	}

	router, _ := dispatcher.NewSinkRouter(replicaConfig)
	return router
}

// newTestSinkRouterWithSchemaAndTableRouting creates a SinkRouter with full routing (schema + table)
func newTestSinkRouterWithSchemaAndTableRouting(routes []tableRoute) *dispatcher.SinkRouter {
	if len(routes) == 0 {
		return nil
	}

	dispatchRules := make([]*config.DispatchRule, 0, len(routes))
	for _, route := range routes {
		dispatchRules = append(dispatchRules, &config.DispatchRule{
			Matcher:    []string{route.sourceSchema + "." + route.sourceTable},
			SchemaRule: route.targetSchema,
			TableRule:  route.targetTable,
		})
	}

	replicaConfig := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: dispatchRules,
		},
	}

	router, _ := dispatcher.NewSinkRouter(replicaConfig)
	return router
}

// TestDDLSinkRoutingSchemaAndTable tests routing where BOTH schema AND table change.
// Example: source_db.test_table -> target_db.test_table_routed
func TestDDLSinkRoutingSchemaAndTable(t *testing.T) {
	t.Parallel()

	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)

	// Create a router with full routing: source_db.test_table -> target_db.test_table_routed
	router := newTestSinkRouterWithSchemaAndTableRouting([]tableRoute{
		{
			sourceSchema: "source_db",
			sourceTable:  "test_table",
			targetSchema: "target_db",
			targetTable:  "test_table_routed",
		},
	})

	tests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
			expectedQuery: "CREATE TABLE `target_db`.`test_table_routed` (`id` INT PRIMARY KEY)",
		},
		{
			name:          "DROP TABLE",
			jobType:       timodel.ActionDropTable,
			query:         "DROP TABLE `source_db`.`test_table`",
			expectedQuery: "DROP TABLE `target_db`.`test_table_routed`",
		},
		{
			name:          "TRUNCATE TABLE",
			jobType:       timodel.ActionTruncateTable,
			query:         "TRUNCATE TABLE `source_db`.`test_table`",
			expectedQuery: "TRUNCATE TABLE `target_db`.`test_table_routed`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create a fresh TableInfo for each subtest to avoid data races
			tableInfo := &TableInfo{
				TableName: TableName{
					Schema:  "source_db",
					Table:   "test_table",
					TableID: 1,
				},
				TableInfo: &timodel.TableInfo{
					ID:   1,
					Name: pmodel.CIStr{O: "test_table"},
					Columns: []*timodel.ColumnInfo{
						{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					},
				},
			}

			job := &timodel.Job{
				ID:         1,
				TableID:    1,
				SchemaName: "source_db",
				Type:       tt.jobType,
				StartTS:    1,
				Query:      tt.query,
				BinlogInfo: &timodel.HistoryInfo{
					TableInfo:  tableInfo.TableInfo,
					FinishedTS: 2,
				},
			}

			event := &DDLEvent{}
			err := event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, event.Query, "Query mismatch for %s", tt.name)
			// Verify source schema/table are NOT mutated
			require.Equal(t, "source_db", event.TableInfo.TableName.Schema)
			require.Equal(t, "test_table", event.TableInfo.TableName.Table)
			// Verify target schema/table are set correctly
			require.Equal(t, "target_db", event.TableInfo.TableName.TargetSchema)
			require.Equal(t, "test_table_routed", event.TableInfo.TableName.TargetTable)
		})
	}
}

// TestDDLSinkRoutingSchemaOnly tests routing where ONLY the schema changes.
// The table name stays the same via TableRule="{table}".
// Example: source_db.test_table -> target_db.test_table
func TestDDLSinkRoutingSchemaOnly(t *testing.T) {
	t.Parallel()

	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)

	// newTestSinkRouter creates rules with TableRule="{table}" which preserves the table name
	router := newTestSinkRouter(map[string]string{
		"source_db": "target_db",
	})

	tests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
			expectedQuery: "CREATE TABLE `target_db`.`test_table` (`id` INT PRIMARY KEY)",
		},
		{
			name:          "CREATE TABLE IF NOT EXISTS",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE IF NOT EXISTS `source_db`.`test_table` (id INT PRIMARY KEY)",
			expectedQuery: "CREATE TABLE IF NOT EXISTS `target_db`.`test_table` (`id` INT PRIMARY KEY)",
		},
		{
			name:          "DROP TABLE",
			jobType:       timodel.ActionDropTable,
			query:         "DROP TABLE `source_db`.`test_table`",
			expectedQuery: "DROP TABLE `target_db`.`test_table`",
		},
		{
			name:          "TRUNCATE TABLE",
			jobType:       timodel.ActionTruncateTable,
			query:         "TRUNCATE TABLE `source_db`.`test_table`",
			expectedQuery: "TRUNCATE TABLE `target_db`.`test_table`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create a fresh TableInfo for each subtest to avoid data races
			tableInfo := &TableInfo{
				TableName: TableName{
					Schema:  "source_db",
					Table:   "test_table",
					TableID: 1,
				},
				TableInfo: &timodel.TableInfo{
					ID:   1,
					Name: pmodel.CIStr{O: "test_table"},
					Columns: []*timodel.ColumnInfo{
						{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					},
				},
			}

			job := &timodel.Job{
				ID:         1,
				TableID:    1,
				SchemaName: "source_db",
				Type:       tt.jobType,
				StartTS:    1,
				Query:      tt.query,
				BinlogInfo: &timodel.HistoryInfo{
					TableInfo:  tableInfo.TableInfo,
					FinishedTS: 2,
				},
			}

			event := &DDLEvent{}
			err := event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, event.Query, "Query mismatch for %s", tt.name)
			// Verify that the original Schema/Table are NOT mutated (pullers need to read from original)
			require.Equal(t, "source_db", event.TableInfo.TableName.Schema, "Schema should not be mutated for %s", tt.name)
			require.Equal(t, "test_table", event.TableInfo.TableName.Table, "Table should not be mutated for %s", tt.name)
			// Verify that TargetSchema is set correctly (sinks need to write to target)
			require.Equal(t, "target_db", event.TableInfo.TableName.TargetSchema, "TargetSchema should be set for %s", tt.name)
			// When TableRule is "{table}", TargetTable should be the same as the source table name
			require.Equal(t, "test_table", event.TableInfo.TableName.TargetTable, "TargetTable should be same as source table when using {table} pattern for %s", tt.name)
		})
	}
}

// TestDDLSinkRoutingTableOnly tests routing where ONLY the table name changes.
// The schema stays the same via SchemaRule="{schema}".
// Example: source_db.test_table -> source_db.test_table_routed
func TestDDLSinkRoutingTableOnly(t *testing.T) {
	t.Parallel()

	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)

	// Create a router with table-only routing: source_db.test_table -> source_db.test_table_routed
	router := newTestSinkRouterWithTableOnlyRouting([]tableRoute{
		{
			sourceSchema: "source_db",
			sourceTable:  "test_table",
			targetTable:  "test_table_routed",
		},
	})

	tests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		expectedQuery string
	}{
		{
			name:          "CREATE TABLE",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE `source_db`.`test_table` (id INT PRIMARY KEY)",
			expectedQuery: "CREATE TABLE `source_db`.`test_table_routed` (`id` INT PRIMARY KEY)",
		},
		{
			name:          "DROP TABLE",
			jobType:       timodel.ActionDropTable,
			query:         "DROP TABLE `source_db`.`test_table`",
			expectedQuery: "DROP TABLE `source_db`.`test_table_routed`",
		},
		{
			name:          "TRUNCATE TABLE",
			jobType:       timodel.ActionTruncateTable,
			query:         "TRUNCATE TABLE `source_db`.`test_table`",
			expectedQuery: "TRUNCATE TABLE `source_db`.`test_table_routed`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create a fresh TableInfo for each subtest to avoid data races
			tableInfo := &TableInfo{
				TableName: TableName{
					Schema:  "source_db",
					Table:   "test_table",
					TableID: 1,
				},
				TableInfo: &timodel.TableInfo{
					ID:   1,
					Name: pmodel.CIStr{O: "test_table"},
					Columns: []*timodel.ColumnInfo{
						{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
					},
				},
			}

			job := &timodel.Job{
				ID:         1,
				TableID:    1,
				SchemaName: "source_db",
				Type:       tt.jobType,
				StartTS:    1,
				Query:      tt.query,
				BinlogInfo: &timodel.HistoryInfo{
					TableInfo:  tableInfo.TableInfo,
					FinishedTS: 2,
				},
			}

			event := &DDLEvent{}
			err := event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
			require.NoError(t, err)
			require.Equal(t, tt.expectedQuery, event.Query, "Query mismatch for %s", tt.name)
			// Verify source schema/table are NOT mutated
			require.Equal(t, "source_db", event.TableInfo.TableName.Schema)
			require.Equal(t, "test_table", event.TableInfo.TableName.Table)
			// Verify target: schema stays the same, table changes
			require.Equal(t, "source_db", event.TableInfo.TableName.TargetSchema)
			require.Equal(t, "test_table_routed", event.TableInfo.TableName.TargetTable)
		})
	}
}

func TestDDLSinkRoutingOrdering(t *testing.T) {
	t.Parallel()

	// Create a sink router where one schema is mapped and one is not
	// This tests that FetchDDLTables and RenameDDLTable maintain the correct ordering
	router := newTestSinkRouter(map[string]string{
		"mapped_source_db": "mapped_target_db",
	})

	// Create TableInfo objects for different test scenarios - defined as factory data
	columns := []*timodel.ColumnInfo{
		{ID: 1, Name: pmodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
	}

	// Define test case specifications (which TableInfo to create for each test)
	type tableInfoSpec struct {
		schema string
		table  string
	}

	tests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		oldSchemaName string         // for RENAME TABLE
		newSchemaName string         // for RENAME TABLE
		preTableSpec  *tableInfoSpec // spec for creating preTableInfo
		tableSpec     *tableInfoSpec // spec for creating tableInfo
		expectedQuery string
		description   string
	}{
		{
			name:    "DROP TABLE - mapped schema",
			jobType: timodel.ActionDropTable,
			query:   "DROP TABLE `mapped_source_db`.`t1`",
			tableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "t1",
			},
			expectedQuery: "DROP TABLE `mapped_target_db`.`t1`",
			description:   "Tests DROP TABLE with mapped schema",
		},
		{
			name:    "DROP TABLE - unmapped schema",
			jobType: timodel.ActionDropTable,
			query:   "DROP TABLE `unmapped_db`.`t1`",
			tableSpec: &tableInfoSpec{
				schema: "unmapped_db",
				table:  "t1",
			},
			expectedQuery: "DROP TABLE `unmapped_db`.`t1`",
			description:   "Tests DROP TABLE with unmapped schema (identity mapping)",
		},
		{
			name:          "RENAME TABLE - mapped old, mapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `mapped_source_db`.`new_table`",
			oldSchemaName: "mapped_source_db",
			newSchemaName: "mapped_source_db",
			preTableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "old_table",
			},
			tableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "new_table",
			},
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE where both old and new schemas are mapped (same schema)",
		},
		{
			name:          "RENAME TABLE - mapped old, unmapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `unmapped_db`.`new_table`",
			oldSchemaName: "mapped_source_db",
			newSchemaName: "unmapped_db",
			preTableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "old_table",
			},
			tableSpec: &tableInfoSpec{
				schema: "unmapped_db",
				table:  "new_table",
			},
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `unmapped_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is mapped, new table is unmapped",
		},
		{
			name:          "RENAME TABLE - unmapped old, mapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_source_db`.`new_table`",
			oldSchemaName: "unmapped_db",
			newSchemaName: "mapped_source_db",
			preTableSpec: &tableInfoSpec{
				schema: "unmapped_db",
				table:  "old_table",
			},
			tableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "new_table",
			},
			expectedQuery: "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is unmapped, new table is mapped",
		},
		{
			name:    "CREATE TABLE LIKE - mapped source, unmapped template",
			jobType: timodel.ActionCreateTable,
			query:   "CREATE TABLE `mapped_source_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			tableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "new_table",
			},
			expectedQuery: "CREATE TABLE `mapped_target_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is mapped, template is unmapped",
		},
		{
			name:    "CREATE TABLE LIKE - unmapped source, mapped template",
			jobType: timodel.ActionCreateTable,
			query:   "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_source_db`.`template_table`",
			tableSpec: &tableInfoSpec{
				schema: "mapped_source_db",
				table:  "new_table",
			},
			expectedQuery: "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_target_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is unmapped, template is mapped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create fresh TableInfo objects for this subtest to avoid data races
			var testTableInfo *TableInfo
			var testPreTableInfo *TableInfo

			if tt.tableSpec != nil {
				testTableInfo = WrapTableInfo(1, tt.tableSpec.schema, 100, &timodel.TableInfo{
					ID:      1,
					Name:    pmodel.NewCIStr(tt.tableSpec.table),
					Columns: columns,
				})
			}

			if tt.preTableSpec != nil {
				testPreTableInfo = WrapTableInfo(1, tt.preTableSpec.schema, 100, &timodel.TableInfo{
					ID:      1,
					Name:    pmodel.NewCIStr(tt.preTableSpec.table),
					Columns: columns,
				})
			}

			job := &timodel.Job{
				ID:         1,
				TableID:    1,
				SchemaName: "mapped_source_db",
				Type:       tt.jobType,
				StartTS:    1,
				Query:      tt.query,
				BinlogInfo: &timodel.HistoryInfo{
					TableInfo:  testTableInfo.TableInfo,
					FinishedTS: 2,
				},
			}

			event := &DDLEvent{}
			err := event.FromJobWithArgs(job, testPreTableInfo, testTableInfo,
				tt.oldSchemaName, tt.newSchemaName, router)
			require.NoError(t, err)

			require.Equal(t, tt.expectedQuery, event.Query,
				"ORDERING TEST FAILED for %s\n%s\nExpected: %s\nActual:   %s",
				tt.name, tt.description, tt.expectedQuery, event.Query)
		})
	}

	// Test with full routing (schema + table name changes)
	tableRouter := newTestSinkRouterWithSchemaAndTableRouting([]tableRoute{
		{
			sourceSchema: "source_db",
			sourceTable:  "source_table",
			targetSchema: "target_db",
			targetTable:  "target_table_routed",
		},
		{
			sourceSchema: "source_db",
			sourceTable:  "old_table",
			targetSchema: "target_db",
			targetTable:  "old_table_routed",
		},
		{
			sourceSchema: "source_db",
			sourceTable:  "new_table",
			targetSchema: "target_db",
			targetTable:  "new_table_routed",
		},
		{
			sourceSchema: "source_db",
			sourceTable:  "template_table",
			targetSchema: "target_db",
			targetTable:  "template_table_routed",
		},
	})

	tableRoutingTests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		oldSchemaName string
		newSchemaName string
		preTableSpec  *tableInfoSpec // spec for creating preTableInfo
		tableSpec     *tableInfoSpec // spec for creating tableInfo
		expectedQuery string
		description   string
	}{
		{
			name:    "DROP TABLE - with table routing",
			jobType: timodel.ActionDropTable,
			query:   "DROP TABLE `source_db`.`source_table`",
			tableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "source_table",
			},
			expectedQuery: "DROP TABLE `target_db`.`target_table_routed`",
			description:   "Tests DROP TABLE with both schema and table routing",
		},
		{
			name:    "TRUNCATE TABLE - with table routing",
			jobType: timodel.ActionTruncateTable,
			query:   "TRUNCATE TABLE `source_db`.`source_table`",
			tableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "source_table",
			},
			expectedQuery: "TRUNCATE TABLE `target_db`.`target_table_routed`",
			description:   "Tests TRUNCATE TABLE with both schema and table routing",
		},
		{
			name:    "CREATE TABLE - with table routing",
			jobType: timodel.ActionCreateTable,
			query:   "CREATE TABLE `source_db`.`source_table` (id INT PRIMARY KEY)",
			tableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "source_table",
			},
			expectedQuery: "CREATE TABLE `target_db`.`target_table_routed` (`id` INT PRIMARY KEY)",
			description:   "Tests CREATE TABLE with both schema and table routing",
		},
		{
			name:          "RENAME TABLE - with table routing",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `source_db`.`old_table` TO `source_db`.`new_table`",
			oldSchemaName: "source_db",
			newSchemaName: "source_db",
			preTableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "old_table",
			},
			tableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "new_table",
			},
			expectedQuery: "RENAME TABLE `target_db`.`old_table_routed` TO `target_db`.`new_table_routed`",
			description:   "Tests RENAME TABLE with both schema and table routing on both old and new tables",
		},
		{
			name:    "CREATE TABLE LIKE - with table routing",
			jobType: timodel.ActionCreateTable,
			query:   "CREATE TABLE `source_db`.`new_table` LIKE `source_db`.`template_table`",
			tableSpec: &tableInfoSpec{
				schema: "source_db",
				table:  "new_table",
			},
			expectedQuery: "CREATE TABLE `target_db`.`new_table_routed` LIKE `target_db`.`template_table_routed`",
			description:   "Tests CREATE TABLE LIKE with both schema and table routing on both new and template tables",
		},
	}

	for _, tt := range tableRoutingTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create fresh TableInfo objects for this subtest to avoid data races
			var testTableInfo *TableInfo
			var testPreTableInfo *TableInfo

			if tt.tableSpec != nil {
				testTableInfo = WrapTableInfo(1, tt.tableSpec.schema, 100, &timodel.TableInfo{
					ID:      1,
					Name:    pmodel.NewCIStr(tt.tableSpec.table),
					Columns: columns,
				})
			}

			if tt.preTableSpec != nil {
				testPreTableInfo = WrapTableInfo(1, tt.preTableSpec.schema, 100, &timodel.TableInfo{
					ID:      1,
					Name:    pmodel.NewCIStr(tt.preTableSpec.table),
					Columns: columns,
				})
			}

			job := &timodel.Job{
				ID:         1,
				TableID:    1,
				SchemaName: "source_db",
				Type:       tt.jobType,
				StartTS:    1,
				Query:      tt.query,
				BinlogInfo: &timodel.HistoryInfo{
					TableInfo:  testTableInfo.TableInfo,
					FinishedTS: 2,
				},
			}

			event := &DDLEvent{}
			err := event.FromJobWithArgs(job, testPreTableInfo, testTableInfo,
				tt.oldSchemaName, tt.newSchemaName, tableRouter)
			require.NoError(t, err)

			require.Equal(t, tt.expectedQuery, event.Query,
				"TABLE ROUTING TEST FAILED for %s\n%s\nExpected: %s\nActual:   %s",
				tt.name, tt.description, tt.expectedQuery, event.Query)
		})
	}

	// Test scenarios with multiple schemas mapped to different targets
	// Scenario: db1.* -> target1, db2.* -> target2
	multiSchemaRouter := newTestSinkRouter(map[string]string{
		"db1": "target1",
		"db2": "target2",
	})

	t.Run("CREATE TABLE LIKE - same table name different schemas routed differently", func(t *testing.T) {
		t.Parallel()
		// CREATE TABLE db1.users LIKE db2.users
		// Both schemas are mapped but to different targets
		tableInfoDb1Users := WrapTableInfo(1, "db1", 100, &timodel.TableInfo{
			ID:      1,
			Name:    pmodel.NewCIStr("users"),
			Columns: columns,
		})

		job := &timodel.Job{
			ID:         1,
			TableID:    1,
			SchemaName: "db1",
			Type:       timodel.ActionCreateTable,
			StartTS:    1,
			Query:      "CREATE TABLE `db1`.`users` LIKE `db2`.`users`",
			BinlogInfo: &timodel.HistoryInfo{
				TableInfo:  tableInfoDb1Users.TableInfo,
				FinishedTS: 2,
			},
		}

		event := &DDLEvent{}
		err := event.FromJobWithArgs(job, nil, tableInfoDb1Users, "", "", multiSchemaRouter)
		require.NoError(t, err)

		expectedQuery := "CREATE TABLE `target1`.`users` LIKE `target2`.`users`"
		require.Equal(t, expectedQuery, event.Query,
			"Same table name in different schemas should route to their respective targets")
	})

	// Test scenario with multiple schemas consolidated to a single target
	// Scenario: db1.* -> consolidated, db2.* -> consolidated
	consolidatedRouter := newTestSinkRouter(map[string]string{
		"db1": "consolidated",
		"db2": "consolidated",
	})

	t.Run("CREATE TABLE LIKE - multiple schemas consolidated to single target", func(t *testing.T) {
		t.Parallel()
		// CREATE TABLE db1.t1 LIKE db2.t2
		// Both schemas map to the same target
		tableInfoDb1T1 := WrapTableInfo(1, "db1", 100, &timodel.TableInfo{
			ID:      1,
			Name:    pmodel.NewCIStr("t1"),
			Columns: columns,
		})

		job := &timodel.Job{
			ID:         1,
			TableID:    1,
			SchemaName: "db1",
			Type:       timodel.ActionCreateTable,
			StartTS:    1,
			Query:      "CREATE TABLE `db1`.`t1` LIKE `db2`.`t2`",
			BinlogInfo: &timodel.HistoryInfo{
				TableInfo:  tableInfoDb1T1.TableInfo,
				FinishedTS: 2,
			},
		}

		event := &DDLEvent{}
		err := event.FromJobWithArgs(job, nil, tableInfoDb1T1, "", "", consolidatedRouter)
		require.NoError(t, err)

		expectedQuery := "CREATE TABLE `consolidated`.`t1` LIKE `consolidated`.`t2`"
		require.Equal(t, expectedQuery, event.Query,
			"Multiple schemas consolidated to single target should route both tables to same target")
	})
}

func TestSinkRoutingError(t *testing.T) {
	t.Parallel()

	ft := types.NewFieldType(mysql.TypeUnspecified)

	// Create a router that routes source_db.* -> target_db.*
	cfg := &config.ReplicaConfig{
		Sink: &config.SinkConfig{
			DispatchRules: []*config.DispatchRule{
				{
					Matcher:    []string{"source_db.*"},
					SchemaRule: "target_db",
				},
			},
		},
	}
	router, err := dispatcher.NewSinkRouter(cfg)
	require.NoError(t, err)
	require.NotNil(t, router)

	t.Run("invalid DDL query returns error", func(t *testing.T) {
		t.Parallel()
		// Create a fresh TableInfo for each subtest to avoid data races
		tableInfo := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "test_table",
				TableID: 1,
			},
			TableInfo: &timodel.TableInfo{
				ID:   1,
				Name: pmodel.CIStr{O: "test_table"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
				},
			},
		}

		job := &timodel.Job{
			ID:         1,
			TableID:    1,
			SchemaName: "source_db",
			Type:       timodel.ActionCreateTable,
			StartTS:    1,
			Query:      "THIS IS NOT VALID SQL AT ALL!!!",
			BinlogInfo: &timodel.HistoryInfo{
				TableInfo:  tableInfo.TableInfo,
				FinishedTS: 2,
			},
		}

		event := &DDLEvent{}
		err := event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse DDL for sink routing")
	})

	t.Run("incomplete DDL query returns error", func(t *testing.T) {
		t.Parallel()
		// Create a fresh TableInfo for each subtest to avoid data races
		tableInfo := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "test_table",
				TableID: 1,
			},
			TableInfo: &timodel.TableInfo{
				ID:   1,
				Name: pmodel.CIStr{O: "test_table"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
				},
			},
		}

		job := &timodel.Job{
			ID:         1,
			TableID:    1,
			SchemaName: "source_db",
			Type:       timodel.ActionCreateTable,
			StartTS:    1,
			Query:      "CREATE TABLE", // incomplete DDL
			BinlogInfo: &timodel.HistoryInfo{
				TableInfo:  tableInfo.TableInfo,
				FinishedTS: 2,
			},
		}

		event := &DDLEvent{}
		err := event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse DDL for sink routing")
	})

	t.Run("no error when router is nil", func(t *testing.T) {
		t.Parallel()
		// Create a fresh TableInfo for each subtest to avoid data races
		tableInfo := &TableInfo{
			TableName: TableName{
				Schema:  "source_db",
				Table:   "test_table",
				TableID: 1,
			},
			TableInfo: &timodel.TableInfo{
				ID:   1,
				Name: pmodel.CIStr{O: "test_table"},
				Columns: []*timodel.ColumnInfo{
					{ID: 1, Name: pmodel.CIStr{O: "id"}, FieldType: *ft, State: timodel.StatePublic},
				},
			},
		}

		job := &timodel.Job{
			ID:         1,
			TableID:    1,
			SchemaName: "source_db",
			Type:       timodel.ActionCreateTable,
			StartTS:    1,
			Query:      "THIS IS NOT VALID SQL AT ALL!!!",
			BinlogInfo: &timodel.HistoryInfo{
				TableInfo:  tableInfo.TableInfo,
				FinishedTS: 2,
			},
		}

		event := &DDLEvent{}
		// When router is nil, applySinkRouting is not called, so no error
		err := event.FromJobWithArgs(job, nil, tableInfo, "", "", nil)
		require.NoError(t, err)
		// The query should remain unchanged (invalid but not routed)
		require.Equal(t, "THIS IS NOT VALID SQL AT ALL!!!", event.Query)
	})
}

func TestSortRowChangedEvent(t *testing.T) {
	events := []*RowChangedEvent{
		{
			PreColumns: []*ColumnData{{}},
			Columns:    []*ColumnData{{}},
		},
		{
			Columns: []*ColumnData{{}},
		},
		{
			PreColumns: []*ColumnData{{}},
		},
	}
	assert.True(t, events[0].IsUpdate())
	assert.True(t, events[1].IsInsert())
	assert.True(t, events[2].IsDelete())
	sort.Sort(txnRows(events))
	assert.True(t, events[0].IsDelete())
	assert.True(t, events[1].IsUpdate())
	assert.True(t, events[2].IsInsert())
}

func TestTrySplitAndSortUpdateEventNil(t *testing.T) {
	t.Parallel()

	events := []*RowChangedEvent{nil}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTrySplitAndSortUpdateEventEmpty(t *testing.T) {
	t.Parallel()

	events := []*RowChangedEvent{
		{
			StartTs:  1,
			CommitTs: 2,
		},
	}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 0, len(result))
}

func TestTrySplitAndSortUpdateEvent(t *testing.T) {
	t.Parallel()

	// Update primary key.
	tableInfoWithPrimaryKey := BuildTableInfo("test", "t", []*Column{
		{
			Name: "col1",
			Flag: BinaryFlag,
		},
		{
			Name: "col2",
			Flag: HandleKeyFlag | PrimaryKeyFlag,
		},
	}, [][]int{{1}})
	events := []*RowChangedEvent{
		{
			CommitTs:  1,
			TableInfo: tableInfoWithPrimaryKey,
			Columns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Flag:  BinaryFlag,
					Value: "col1-value-updated",
				},
				{
					Name:  "col2",
					Flag:  HandleKeyFlag | PrimaryKeyFlag,
					Value: "col2-value-updated",
				},
			}, tableInfoWithPrimaryKey),
			PreColumns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Value: "col1-value",
				},
				{
					Name:  "col2",
					Value: "col2-value",
				},
			}, tableInfoWithPrimaryKey),
		},
	}
	result, err := trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.True(t, result[0].IsDelete())
	require.True(t, result[1].IsInsert())

	// Update unique key.
	tableInfoWithUniqueKey := BuildTableInfo("test", "t", []*Column{
		{
			Name: "col1",
			Flag: BinaryFlag,
		},
		{
			Name: "col2",
			Flag: UniqueKeyFlag | NullableFlag,
		},
	}, [][]int{{1}})
	events = []*RowChangedEvent{
		{
			CommitTs:  1,
			TableInfo: tableInfoWithUniqueKey,
			Columns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Value: "col1-value-updated",
				},
				{
					Name:  "col2",
					Value: "col2-value-updated",
				},
			}, tableInfoWithUniqueKey),
			PreColumns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Value: "col1-value",
				},
				{
					Name:  "col2",
					Value: "col2-value",
				},
			}, tableInfoWithUniqueKey),
		},
	}
	result, err = trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 2, len(result))
	require.True(t, result[0].IsDelete())
	require.True(t, result[0].IsDelete())
	require.True(t, result[1].IsInsert())

	// Update non-handle key.
	events = []*RowChangedEvent{
		{
			CommitTs:  1,
			TableInfo: tableInfoWithPrimaryKey,
			Columns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Value: "col1-value-updated",
				},
				{
					Name:  "col2",
					Value: "col2-value",
				},
			}, tableInfoWithPrimaryKey),
			PreColumns: Columns2ColumnDatas([]*Column{
				{
					Name:  "col1",
					Value: "col1-value",
				},
				{
					Name:  "col2",
					Value: "col2-value",
				},
			}, tableInfoWithPrimaryKey),
		},
	}
	result, err = trySplitAndSortUpdateEvent(events)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
}

func TestTxnTrySplitAndSortUpdateEvent(t *testing.T) {
	columns := []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag | UniqueKeyFlag | PrimaryKeyFlag,
			Value: "col2-value-updated",
		},
	}
	preColumns := []*Column{
		{
			Name:  "col1",
			Flag:  BinaryFlag,
			Value: "col1-value",
		},
		{
			Name:  "col2",
			Flag:  HandleKeyFlag | UniqueKeyFlag | PrimaryKeyFlag,
			Value: "col2-value",
		},
	}
	tableInfo := BuildTableInfo("test", "t", columns, [][]int{{1}})
	ukUpdatedEvent := &RowChangedEvent{
		TableInfo:  tableInfo,
		PreColumns: Columns2ColumnDatas(preColumns, tableInfo),
		Columns:    Columns2ColumnDatas(columns, tableInfo),
	}
	txn := &SingleTableTxn{
		Rows: []*RowChangedEvent{ukUpdatedEvent},
	}

	outputRawChangeEvent := true
	notOutputRawChangeEvent := false
	err := txn.TrySplitAndSortUpdateEvent(sink.KafkaScheme, outputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 1)
	err = txn.TrySplitAndSortUpdateEvent(sink.KafkaScheme, notOutputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 2)

	txn = &SingleTableTxn{
		Rows: []*RowChangedEvent{ukUpdatedEvent},
	}
	err = txn.TrySplitAndSortUpdateEvent(sink.MySQLScheme, outputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 1)
	err = txn.TrySplitAndSortUpdateEvent(sink.MySQLScheme, notOutputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn.Rows, 1)

	txn2 := &SingleTableTxn{
		Rows: []*RowChangedEvent{ukUpdatedEvent, ukUpdatedEvent},
	}
	err = txn.TrySplitAndSortUpdateEvent(sink.MySQLScheme, outputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn2.Rows, 2)
	err = txn.TrySplitAndSortUpdateEvent(sink.MySQLScheme, notOutputRawChangeEvent)
	require.NoError(t, err)
	require.Len(t, txn2.Rows, 2)
}

func TestToRedoLog(t *testing.T) {
	cols := []*Column{
		{
			Name: "col1",
			Flag: BinaryFlag,
		},
		{
			Name: "col2",
			Flag: HandleKeyFlag | UniqueKeyFlag,
		},
	}
	tableInfo := BuildTableInfo("test", "t", cols, [][]int{{1}})
	event := &RowChangedEvent{
		StartTs:         100,
		CommitTs:        1000,
		PhysicalTableID: 1,
		TableInfo:       tableInfo,
		Columns: Columns2ColumnDatas([]*Column{
			{
				Name:  "col1",
				Value: "col1-value",
			},
			{
				Name:  "col2",
				Value: "col2-value-updated",
			},
		}, tableInfo),
	}
	eventInRedoLog := event.ToRedoLog()
	require.Equal(t, event.StartTs, eventInRedoLog.RedoRow.Row.StartTs)
	require.Equal(t, event.CommitTs, eventInRedoLog.RedoRow.Row.CommitTs)
	require.Equal(t, event.GetTableID(), eventInRedoLog.RedoRow.Row.Table.TableID)
	require.Equal(t, event.TableInfo.GetSchemaName(), eventInRedoLog.RedoRow.Row.Table.Schema)
	require.Equal(t, event.TableInfo.GetTableName(), eventInRedoLog.RedoRow.Row.Table.Table)
	require.Equal(t, event.Columns, Columns2ColumnDatas(eventInRedoLog.RedoRow.Row.Columns, tableInfo))
}
