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

	// Test without schema routing (TargetSchema not set)
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

	// Test with schema routing (TargetSchema set)
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
	event.FromJob(job, preTableInfo, tableInfo, nil)
	require.Equal(t, uint64(420536581131337731), event.StartTs)
	require.Equal(t, int64(49), event.TableInfo.TableName.TableID)
	require.Equal(t, 1, len(event.PreTableInfo.TableInfo.Columns))

	event = &DDLEvent{}
	event.FromJob(job, nil, nil, nil)
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
	event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1", nil)
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
	event.FromJobWithArgs(job, preTableInfo, tableInfo, "test1", "test1", nil)
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
	event.FromJob(job, preTableInfo, tableInfo, nil)
	require.Equal(t, event.PreTableInfo.TableName.TableID, int64(67))
	require.Equal(t, event.PreTableInfo.TableName.Table, "t2")
	require.Len(t, event.PreTableInfo.TableInfo.Columns, 1)
	require.Equal(t, event.TableInfo.TableName.TableID, int64(69))
	require.Equal(t, event.TableInfo.TableName.Table, "t1")
	require.Len(t, event.TableInfo.TableInfo.Columns, 1)
	require.Equal(t, "ALTER TABLE `test1`.`t1` EXCHANGE PARTITION `p0` WITH TABLE `test2`.`t2`", event.Query)
	require.Equal(t, event.Type, timodel.ActionExchangeTablePartition)
}

// mockSchemaRouter is a simple implementation of SchemaRouter for testing
// Helper function to create a SchemaRouter for tests
func newTestSchemaRouter(routes map[string]string) *config.SchemaRouter {
	if len(routes) == 0 {
		return nil
	}
	schemaRoutes := make(map[string]*config.SchemaRoute)
	schemaRouteRules := make([]string, 0, len(routes))

	for source, target := range routes {
		ruleName := source + "_rule"
		schemaRoutes[ruleName] = &config.SchemaRoute{
			SourceSchema: source,
			TargetSchema: target,
		}
		schemaRouteRules = append(schemaRouteRules, ruleName)
	}

	router, _ := config.BuildSchemaRouter(schemaRoutes, schemaRouteRules)
	return router
}

func TestDDLSchemaRouting(t *testing.T) {
	t.Parallel()

	ft := types.NewFieldType(mysql.TypeUnspecified)
	ft.SetFlag(mysql.PriKeyFlag)

	router := newTestSchemaRouter(map[string]string{
		"source_db": "target_db",
	})

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
			event.FromJobWithArgs(job, nil, tableInfo, "", "", router)
			require.Equal(t, tt.expectedQuery, event.Query, "Query mismatch for %s", tt.name)
			// Verify that the original Schema is NOT mutated (pullers need to read from original)
			require.Equal(t, "source_db", event.TableInfo.TableName.Schema, "Schema should not be mutated for %s", tt.name)
			// Verify that TargetSchema is set correctly (sinks need to write to target)
			require.Equal(t, "target_db", event.TableInfo.TableName.TargetSchema, "TargetSchema should be set for %s", tt.name)
		})
	}
}

func TestDDLSchemaRoutingOrdering(t *testing.T) {
	t.Parallel()

	// Create a schema router where one schema is mapped and one is not
	// This tests that FetchDDLTables and RenameDDLTable maintain the correct ordering
	router := newTestSchemaRouter(map[string]string{
		"mapped_source_db": "mapped_target_db",
	})

	// Create TableInfo objects for different test scenarios
	columns := []*timodel.ColumnInfo{
		{ID: 1, Name: pmodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
	}

	// For DROP TABLE tests
	tableInfoMappedT1 := WrapTableInfo(1, "mapped_source_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("t1"),
		Columns: columns,
	})
	tableInfoUnmappedT1 := WrapTableInfo(1, "unmapped_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("t1"),
		Columns: columns,
	})

	// For RENAME TABLE tests - PreTableInfo (old table)
	preTableInfoMappedOld := WrapTableInfo(1, "mapped_source_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("old_table"),
		Columns: columns,
	})
	preTableInfoUnmappedOld := WrapTableInfo(1, "unmapped_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("old_table"),
		Columns: columns,
	})

	// For RENAME TABLE tests - TableInfo (new table)
	tableInfoMappedNew := WrapTableInfo(1, "mapped_source_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("new_table"),
		Columns: columns,
	})
	tableInfoUnmappedNew := WrapTableInfo(1, "unmapped_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("new_table"),
		Columns: columns,
	})

	// For CREATE TABLE LIKE tests
	tableInfoMapped := WrapTableInfo(1, "mapped_source_db", 100, &timodel.TableInfo{
		ID:      1,
		Name:    pmodel.NewCIStr("new_table"),
		Columns: columns,
	})

	tests := []struct {
		name          string
		jobType       timodel.ActionType
		query         string
		oldSchemaName string // for RENAME TABLE
		newSchemaName string // for RENAME TABLE
		preTableInfo  *TableInfo
		tableInfo     *TableInfo
		expectedQuery string
		description   string
	}{
		{
			name:          "DROP TABLE - mapped schema",
			jobType:       timodel.ActionDropTable,
			query:         "DROP TABLE `mapped_source_db`.`t1`",
			tableInfo:     tableInfoMappedT1,
			expectedQuery: "DROP TABLE `mapped_target_db`.`t1`",
			description:   "Tests DROP TABLE with mapped schema",
		},
		{
			name:          "DROP TABLE - unmapped schema",
			jobType:       timodel.ActionDropTable,
			query:         "DROP TABLE `unmapped_db`.`t1`",
			tableInfo:     tableInfoUnmappedT1,
			expectedQuery: "DROP TABLE `unmapped_db`.`t1`",
			description:   "Tests DROP TABLE with unmapped schema (identity mapping)",
		},
		{
			name:          "RENAME TABLE - mapped old, mapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `mapped_source_db`.`new_table`",
			oldSchemaName: "mapped_source_db",
			newSchemaName: "mapped_source_db",
			preTableInfo:  preTableInfoMappedOld,
			tableInfo:     tableInfoMappedNew,
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE where both old and new schemas are mapped (same schema)",
		},
		{
			name:          "RENAME TABLE - mapped old, unmapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `mapped_source_db`.`old_table` TO `unmapped_db`.`new_table`",
			oldSchemaName: "mapped_source_db",
			newSchemaName: "unmapped_db",
			preTableInfo:  preTableInfoMappedOld,
			tableInfo:     tableInfoUnmappedNew,
			expectedQuery: "RENAME TABLE `mapped_target_db`.`old_table` TO `unmapped_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is mapped, new table is unmapped",
		},
		{
			name:          "RENAME TABLE - unmapped old, mapped new",
			jobType:       timodel.ActionRenameTables,
			query:         "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_source_db`.`new_table`",
			oldSchemaName: "unmapped_db",
			newSchemaName: "mapped_source_db",
			preTableInfo:  preTableInfoUnmappedOld,
			tableInfo:     tableInfoMappedNew,
			expectedQuery: "RENAME TABLE `unmapped_db`.`old_table` TO `mapped_target_db`.`new_table`",
			description:   "Tests RENAME TABLE ordering: old table is unmapped, new table is mapped",
		},
		{
			name:          "CREATE TABLE LIKE - mapped source, unmapped template",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE `mapped_source_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			tableInfo:     tableInfoMapped,
			expectedQuery: "CREATE TABLE `mapped_target_db`.`new_table` LIKE `unmapped_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is mapped, template is unmapped",
		},
		{
			name:          "CREATE TABLE LIKE - unmapped source, mapped template",
			jobType:       timodel.ActionCreateTable,
			query:         "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_source_db`.`template_table`",
			tableInfo:     tableInfoMapped,
			expectedQuery: "CREATE TABLE `unmapped_db`.`new_table` LIKE `mapped_target_db`.`template_table`",
			description:   "Tests CREATE LIKE ordering: new table is unmapped, template is mapped",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Use the tableInfo from the test case, or default to the mapped one
			testTableInfo := tt.tableInfo
			if testTableInfo == nil {
				testTableInfo = tableInfoMapped
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
			event.FromJobWithArgs(job, tt.preTableInfo, testTableInfo,
				tt.oldSchemaName, tt.newSchemaName, router)

			require.Equal(t, tt.expectedQuery, event.Query,
				"ORDERING TEST FAILED for %s\n%s\nExpected: %s\nActual:   %s",
				tt.name, tt.description, tt.expectedQuery, event.Query)
		})
	}
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
