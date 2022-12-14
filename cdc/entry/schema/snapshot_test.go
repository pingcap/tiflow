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

package schema

import (
	"fmt"
	"testing"

	timodel "github.com/pingcap/parser/model"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
)

func TestTablesInSchema(t *testing.T) {
	snap := NewEmptySnapshot(true)
	require.Nil(t, snap.inner.createSchema(newDBInfo(1), 100))
	var vname versionedEntityName

	vname = newVersionedEntityName(1, "tb1", negative(80))
	vname.target = 1
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	vname = newVersionedEntityName(1, "tb1", negative(90))
	vname.target = 2
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	vname = newVersionedEntityName(1, "tb1", negative(110))
	vname.target = 3
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	vname = newVersionedEntityName(1, "tb2", negative(100))
	vname.target = 4
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	vname = newVersionedEntityName(1, "tb3", negative(120))
	vname.target = 5
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	vname = newVersionedEntityName(2, "tb1", negative(80))
	vname.target = 6
	snap.inner.tableNameToID.ReplaceOrInsert(vname)

	require.Equal(t, []int64{2, 4}, snap.inner.tablesInSchema("DB_1"))

	vname = newVersionedEntityName(1, "tb1", negative(130))
	vname.target = -1
	snap.inner.tableNameToID.ReplaceOrInsert(vname)
	snap.inner.currentTs = 130
	require.Equal(t, []int64{4, 5}, snap.inner.tablesInSchema("DB_1"))
}

func TestIterSchemas(t *testing.T) {
	snap := NewEmptySnapshot(true)
	require.Nil(t, snap.inner.createSchema(newDBInfo(1), 90))
	require.Nil(t, snap.inner.replaceSchema(newDBInfo(1), 100))
	require.Nil(t, snap.inner.createSchema(newDBInfo(2), 110))
	require.Nil(t, snap.inner.createSchema(newDBInfo(3), 90))
	snap.inner.currentTs = 100

	var schemas []int64 = make([]int64, 0, 3)
	snap.IterSchemas(func(i *timodel.DBInfo) {
		schemas = append(schemas, i.ID)
	})
	require.Equal(t, []int64{1, 3}, schemas)
}

func TestSchema(t *testing.T) {
	snap := NewEmptySnapshot(true)

	// createSchema fails if the schema ID or name already exist.
	dbName := timodel.CIStr{O: "DB_1", L: "db_1"}
	require.Nil(t, snap.inner.createSchema(&timodel.DBInfo{ID: 1, Name: dbName}, 100))
	require.Nil(t, snap.inner.createTable(newTbInfo(1, "DB_1", 11), 100))
	require.Error(t, snap.inner.createSchema(&timodel.DBInfo{ID: 1}, 110))
	require.Error(t, snap.inner.createSchema(&timodel.DBInfo{ID: 2, Name: dbName}, 120))
	snap1 := snap.Copy()

	// replaceSchema only success if the schema ID exists.
	dbName = timodel.CIStr{O: "DB_2", L: "db_2"}
	require.Error(t, snap.inner.replaceSchema(&timodel.DBInfo{ID: 2}, 130))
	require.Nil(t, snap.inner.replaceSchema(&timodel.DBInfo{ID: 1, Name: dbName}, 140))
	snap2 := snap.Copy()

	// dropSchema only success if the schema ID exists.
	require.Error(t, snap.inner.dropSchema(2, 150))
	require.Nil(t, snap.inner.dropSchema(1, 170))
	snap3 := snap.Copy()

	var db *timodel.DBInfo
	var ok bool

	// The schema and table should be available based on snap1.
	db, ok = snap1.SchemaByID(1)
	require.True(t, ok)
	require.Equal(t, db.Name.O, "DB_1")
	_, ok = snap1.TableIDByName("DB_1", "TB_11")
	require.True(t, ok)
	_, ok = snap1.PhysicalTableByID(11)
	require.True(t, ok)

	// The schema and table should be available based on snap2, but with a different schema name.
	db, ok = snap2.SchemaByID(1)
	require.True(t, ok)
	require.Equal(t, db.Name.O, "DB_2")
	_, ok = snap2.TableIDByName("DB_2", "TB_11")
	require.True(t, ok)
	_, ok = snap2.PhysicalTableByID(11)
	require.True(t, ok)
	_, ok = snap2.TableIDByName("DB_1", "TB_11")
	require.False(t, ok)

	// The schema and table should be unavailable based on snap3.
	_, ok = snap3.SchemaByID(1)
	require.False(t, ok)
	_, ok = snap3.PhysicalTableByID(11)
	require.False(t, ok)
	_, ok = snap3.TableIDByName("DB_2", "TB_11")
	require.False(t, ok)
}

func TestTable(t *testing.T) {
	var ok bool
	for _, forceReplicate := range []bool{true, false} {
		snap := NewEmptySnapshot(forceReplicate)

		// createTable should check whether the schema or table exist or not.
		require.Error(t, snap.inner.createTable(newTbInfo(1, "DB_1", 11), 100))
		_ = snap.inner.createSchema(newDBInfo(1), 110)
		require.Nil(t, snap.inner.createTable(newTbInfo(1, "DB_1", 11), 120))
		require.Error(t, snap.inner.createTable(newTbInfo(1, "DB_1", 11), 130))
		_, ok = snap.PhysicalTableByID(11)
		require.True(t, ok)
		_, ok = snap.PhysicalTableByID(11 + 65536)
		require.True(t, ok)
		_, ok = snap.TableByName("DB_1", "TB_11")
		require.True(t, ok)
		if !forceReplicate {
			require.True(t, snap.IsIneligibleTableID(11))
			require.True(t, snap.IsIneligibleTableID(11+65536))
		}

		// replaceTable should check whether the schema or table exist or not.
		require.Error(t, snap.inner.replaceTable(newTbInfo(2, "DB_2", 11), 140))
		require.Error(t, snap.inner.replaceTable(newTbInfo(1, "DB_1", 12), 150))
		require.Nil(t, snap.inner.replaceTable(newTbInfo(1, "DB_1", 11), 160))
		_, ok = snap.PhysicalTableByID(11)
		require.True(t, ok)
		_, ok = snap.PhysicalTableByID(11 + 65536)
		require.True(t, ok)
		_, ok = snap.TableByName("DB_1", "TB_11")
		require.True(t, ok)
		if !forceReplicate {
			require.True(t, snap.IsIneligibleTableID(11))
			require.True(t, snap.IsIneligibleTableID(11+65536))
		}

		// truncateTable should replace the old one.
		require.Error(t, snap.inner.truncateTable(12, newTbInfo(1, "DB_1", 13), 170))
		require.Nil(t, snap.inner.truncateTable(11, newTbInfo(1, "DB_1", 12), 180))
		_, ok = snap.PhysicalTableByID(11)
		require.False(t, ok)
		_, ok = snap.PhysicalTableByID(11 + 65536)
		require.False(t, ok)
		require.True(t, snap.IsTruncateTableID(11))
		_, ok = snap.PhysicalTableByID(12)
		require.True(t, ok)
		_, ok = snap.PhysicalTableByID(12 + 65536)
		require.True(t, ok)
		_, ok = snap.TableByName("DB_1", "TB_12")
		require.True(t, ok)
		if !forceReplicate {
			require.False(t, snap.IsIneligibleTableID(11))
			require.False(t, snap.IsIneligibleTableID(11+65536))
			require.True(t, snap.IsIneligibleTableID(12))
			require.True(t, snap.IsIneligibleTableID(12+65536))
		}

		// dropTable should check the table exists or not.
		require.Error(t, snap.inner.dropTable(11, 190))
		require.Nil(t, snap.inner.dropTable(12, 200))
		_, ok = snap.PhysicalTableByID(12)
		require.False(t, ok)
		_, ok = snap.PhysicalTableByID(12 + 65536)
		require.False(t, ok)
		_, ok = snap.TableByName("DB_1", "TB_12")
		require.False(t, ok)
		if !forceReplicate {
			require.False(t, snap.IsIneligibleTableID(12))
			require.False(t, snap.IsIneligibleTableID(12+65536))
		}

		// IterTables should get no available tables.
		require.Equal(t, snap.TableCount(true), 0)
	}
}

func TestUpdatePartition(t *testing.T) {
	var oldTb, newTb *model.TableInfo
	var snap1, snap2 *Snapshot
	var info *model.TableInfo
	var ok bool

	snap := NewEmptySnapshot(false)
	require.Nil(t, snap.inner.createSchema(newDBInfo(1), 100))

	// updatePartition fails if the old table is not partitioned.
	oldTb = newTbInfo(1, "DB_1", 11)
	oldTb.Partition = nil
	require.Nil(t, snap.inner.createTable(oldTb, 110))
	require.Error(t, snap.inner.updatePartition(newTbInfo(1, "DB_1", 11), 120))

	// updatePartition fails if the new table is not partitioned.
	require.Nil(t, snap.inner.dropTable(11, 130))
	require.Nil(t, snap.inner.createTable(newTbInfo(1, "DB_1", 11), 140))
	newTb = newTbInfo(1, "DB_1", 11)
	newTb.Partition = nil
	require.Error(t, snap.inner.updatePartition(newTb, 150))
	snap1 = snap.Copy()

	newTb = newTbInfo(1, "DB_1", 11)
	newTb.Partition.Definitions[0] = timodel.PartitionDefinition{ID: 11 + 65536*2}
	require.Nil(t, snap.inner.updatePartition(newTb, 160))
	snap2 = snap.Copy()

	info, _ = snap1.PhysicalTableByID(11)
	require.Equal(t, info.Partition.Definitions[0].ID, int64(11+65536))
	_, ok = snap1.PhysicalTableByID(11 + 65536)
	require.True(t, ok)
	require.True(t, snap1.IsIneligibleTableID(11+65536))
	_, ok = snap1.PhysicalTableByID(11 + 65536*2)
	require.False(t, ok)
	require.False(t, snap1.IsIneligibleTableID(11+65536*2))

	info, _ = snap2.PhysicalTableByID(11)
	require.Equal(t, info.Partition.Definitions[0].ID, int64(11+65536*2))
	_, ok = snap2.PhysicalTableByID(11 + 65536)
	require.False(t, ok)
	require.False(t, snap2.IsIneligibleTableID(11+65536))
	_, ok = snap2.PhysicalTableByID(11 + 65536*2)
	require.True(t, ok)
	require.True(t, snap2.IsIneligibleTableID(11+65536*2))
}

func TestDrop(t *testing.T) {
	snap := NewEmptySnapshot(false)

	require.Nil(t, snap.inner.createSchema(newDBInfo(1), 11))
	require.Nil(t, snap.inner.createSchema(newDBInfo(2), 12))
	require.Nil(t, snap.inner.replaceSchema(newDBInfo(2), 13))
	require.Nil(t, snap.inner.dropSchema(2, 14))

	require.Nil(t, snap.inner.createTable(newTbInfo(1, "DB_1", 3), 15))
	require.Nil(t, snap.inner.createTable(newTbInfo(1, "DB_1", 4), 16))
	require.Nil(t, snap.inner.replaceTable(newTbInfo(1, "DB_1", 4), 17))
	require.Nil(t, snap.inner.truncateTable(4, newTbInfo(1, "DB_1", 5), 18))
	require.Nil(t, snap.inner.dropTable(5, 19))
	snap.Drop()

	// After the latest snapshot is dropped, check schema and table count.
	require.Equal(t, 1, snap.inner.schemas.Len())
	require.Equal(t, 1, snap.inner.tables.Len())
	require.Equal(t, 1, snap.inner.schemaNameToID.Len())
	require.Equal(t, 1, snap.inner.tableNameToID.Len())
	require.Equal(t, 1, snap.inner.partitions.Len())
	require.Equal(t, 0, snap.inner.truncatedTables.Len())
	require.Equal(t, 2, snap.inner.ineligibleTables.Len())
}

func newDBInfo(id int64) *timodel.DBInfo {
	return &timodel.DBInfo{
		ID: id,
		Name: timodel.CIStr{
			O: fmt.Sprintf("DB_%d", id),
			L: fmt.Sprintf("db_%d", id),
		},
	}
}

// newTbInfo constructs a test TableInfo with a partition and a sequence.
// The partition ID will be tableID + 65536.
func newTbInfo(schemaID int64, schemaName string, tableID int64) *model.TableInfo {
	return &model.TableInfo{
		TableInfo: &timodel.TableInfo{
			ID: tableID,
			Name: timodel.CIStr{
				O: fmt.Sprintf("TB_%d", tableID),
				L: fmt.Sprintf("TB_%d", tableID),
			},
			Partition: &timodel.PartitionInfo{
				Enable:      true,
				Definitions: []timodel.PartitionDefinition{{ID: 65536 + tableID}},
			},
			Sequence: &timodel.SequenceInfo{Start: 0},
		},
		SchemaID: schemaID,
		TableName: model.TableName{
			Schema: schemaName,
			Table:  fmt.Sprintf("TB_%d", tableID),
		},
	}
}
