// Copyright 2021 PingCAP, Inc.
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

package entry

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tidb/pkg/parser/mysql"
	"github.com/pingcap/tidb/pkg/parser/types"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

func TestAllPhysicalTables(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	tableIDs, err := schema.AllPhysicalTables(context.Background(), ver.Ver)
	require.Nil(t, err)
	require.Len(t, tableIDs, 0)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1 := job.BinlogInfo.TableInfo.ID
	require.Nil(t, schema.HandleDDLJob(job))
	tableIDs, err = schema.AllPhysicalTables(context.Background(), job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.Equal(t, tableIDs, []model.TableID{tableIDT1})
	// add ineligible table
	job = helper.DDL2Job("create table test.t2(id int)")
	require.Nil(t, schema.HandleDDLJob(job))
	tableIDs, err = schema.AllPhysicalTables(context.Background(), job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.Equal(t, tableIDs, []model.TableID{tableIDT1})
	// add partition table
	job = helper.DDL2Job(`CREATE TABLE test.employees  (
			id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
			fname VARCHAR(25) NOT NULL,
			lname VARCHAR(25) NOT NULL,
			store_id INT NOT NULL,
			department_id INT NOT NULL
		)

		PARTITION BY RANGE(id)  (
			PARTITION p0 VALUES LESS THAN (5),
			PARTITION p1 VALUES LESS THAN (10),
			PARTITION p2 VALUES LESS THAN (15),
			PARTITION p3 VALUES LESS THAN (20)
		)`)
	require.Nil(t, schema.HandleDDLJob(job))
	expectedTableIDs := []model.TableID{tableIDT1}
	for _, p := range job.BinlogInfo.TableInfo.GetPartitionInfo().Definitions {
		expectedTableIDs = append(expectedTableIDs, p.ID)
	}
	sortTableIDs := func(tableIDs []model.TableID) {
		sort.Slice(tableIDs, func(i, j int) bool {
			return tableIDs[i] < tableIDs[j]
		})
	}
	sortTableIDs(expectedTableIDs)
	tableIDs, err = schema.AllPhysicalTables(context.Background(), job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	sortTableIDs(tableIDs)
	require.Equal(t, tableIDs, expectedTableIDs)
}

func TestAllTables(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	tableInfos, err := schema.AllTables(context.Background(), ver.Ver)
	require.Nil(t, err)
	require.Len(t, tableInfos, 0)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	require.Nil(t, schema.HandleDDLJob(job))
	tableInfos, err = schema.AllTables(context.Background(), job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.Len(t, tableInfos, 1)
	tableName := tableInfos[0].TableName
	require.Equal(t, model.TableName{
		Schema:  "test",
		Table:   "t1",
		TableID: 104,
	}, tableName)
	// add ineligible table
	job = helper.DDL2Job("create table test.t2(id int)")
	require.Nil(t, schema.HandleDDLJob(job))
	tableInfos, err = schema.AllTables(context.Background(), job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.Len(t, tableInfos, 1)
	tableName = tableInfos[0].TableName
	require.Equal(t, model.TableName{
		Schema:  "test",
		Table:   "t1",
		TableID: 104,
	}, tableName)
}

func TestIsIneligibleTableID(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1 := job.BinlogInfo.TableInfo.ID
	require.Nil(t, schema.HandleDDLJob(job))
	// add ineligible table
	job = helper.DDL2Job("create table test.t2(id int)")
	tableIDT2 := job.BinlogInfo.TableInfo.ID

	require.Nil(t, schema.HandleDDLJob(job))
	ctx := context.Background()
	ignore, err := schema.IsIneligibleTable(ctx, tableIDT1, job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.False(t, ignore)

	ignore, err = schema.IsIneligibleTable(ctx, tableIDT2, job.BinlogInfo.FinishedTS)
	require.Nil(t, err)
	require.True(t, ignore)

	// test we get the right snapshot to check ineligible table
	job = helper.DDL2Job("create table test.t3(id int)")
	tableIDT3 := job.BinlogInfo.TableInfo.ID
	snapshotTsWithoutPK := job.BinlogInfo.FinishedTS
	require.Nil(t, schema.HandleDDLJob(job))
	job = helper.DDL2Job("alter table test.t3 add primary key(id)")
	snapshotTsWithPK := job.BinlogInfo.FinishedTS
	require.Nil(t, schema.HandleDDLJob(job))
	// tableIDT3 is ineligible at snapshotTsWithoutPK
	ignore, err = schema.IsIneligibleTable(ctx, tableIDT3, snapshotTsWithoutPK)
	require.Nil(t, err)
	require.True(t, ignore)
	// tableIDT3 is eligible at snapshotTsWithPK
	ignore, err = schema.IsIneligibleTable(ctx, tableIDT3, snapshotTsWithPK)
	require.Nil(t, err)
	require.False(t, ignore)
}

func compareEvents(t *testing.T, e1, e2 *model.DDLEvent) {
	require.Equal(t, e1.StartTs, e2.StartTs)
	require.Equal(t, e1.CommitTs, e2.CommitTs)
	require.Equal(t, e1.Query, e2.Query)
	require.Equal(t, e1.TableInfo.TableName, e2.TableInfo.TableName)
	require.Equal(t, len(e1.TableInfo.TableInfo.Columns), len(e2.TableInfo.TableInfo.Columns))
	for idx, col := range e1.TableInfo.TableInfo.Columns {
		require.Equal(t, col.Name, e2.TableInfo.Columns[idx].Name)
		require.Equal(t, col.FieldType.GetType(), e2.TableInfo.Columns[idx].FieldType.GetType())
	}
}

func TestBuildDDLEventsFromSingleTableDDL(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	// add normal table
	ctx := context.Background()
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err := schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "create table test.t1(id int primary key)",
		Type:     timodel.ActionCreateTable,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t1",
				TableID: job.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
		PreTableInfo: nil,
	})
	require.Nil(t, schema.HandleDDLJob(job))
	job = helper.DDL2Job("ALTER TABLE test.t1 ADD COLUMN c1 CHAR(16) NOT NULL")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "ALTER TABLE test.t1 ADD COLUMN c1 CHAR(16) NOT NULL",
		Type:     timodel.ActionAddColumn,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t1",
				TableID: job.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
					{Name: timodel.NewCIStr("c1"), FieldType: *types.NewFieldType(mysql.TypeString)},
				},
			},
		},
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t1",
				TableID: job.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
	})
}

func TestBuildDDLEventsFromRenameTablesDDL(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	ctx := context.Background()
	job := helper.DDL2Job("create database test1")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err := schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))
	schemaID := job.SchemaID
	// add test.t1
	job = helper.DDL2Job("create table test1.t1(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))
	t1TableID := job.TableID

	// add test.t2
	job = helper.DDL2Job("create table test1.t2(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))
	t2TableID := job.TableID

	// rename test.t1 and test.t2
	job = helper.DDL2Job(
		"rename table test1.t1 to test1.t10, test1.t2 to test1.t20")
	oldSchemaIDs := []int64{schemaID, schemaID}
	oldTableIDs := []int64{t1TableID, t2TableID}
	newSchemaIDs := oldSchemaIDs
	oldSchemaNames := []timodel.CIStr{
		timodel.NewCIStr("test1"),
		timodel.NewCIStr("test1"),
	}
	newTableNames := []timodel.CIStr{
		timodel.NewCIStr("t10"),
		timodel.NewCIStr("t20"),
	}
	args := []interface{}{
		oldSchemaIDs, newSchemaIDs,
		newTableNames, oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.Nil(t, err)
	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	job.RawArgs = rawArgs
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 2)
	fmt.Printf("events[0]:%+v\n", events[0])
	fmt.Printf("events[1]:%+v\n", events[1])
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "RENAME TABLE `test1`.`t1` TO `test1`.`t10`",
		Type:     timodel.ActionRenameTable,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test1",
				Table:   "t10",
				TableID: t1TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test1",
				Table:   "t1",
				TableID: t1TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
	})
	compareEvents(t, events[1], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "RENAME TABLE `test1`.`t2` TO `test1`.`t20`",
		Type:     timodel.ActionRenameTable,
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test1",
				Table:   "t20",
				TableID: t2TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test1",
				Table:   "t2",
				TableID: t2TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
	})
}

func TestBuildDDLEventsFromDropTablesDDL(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	// add test.t1
	ctx := context.Background()
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err := schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	// add test.t2
	job = helper.DDL2Job("create table test.t2(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	jobs := helper.DDL2Jobs("drop table test.t1, test.t2", 2)
	t1DropJob := jobs[1]
	t2DropJob := jobs[0]
	schema.AdvanceResolvedTs(t1DropJob.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, t1DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(t1DropJob))
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  t1DropJob.StartTS,
		CommitTs: t1DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP TABLE `test`.`t1`",
		Type:     timodel.ActionDropTable,
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t1",
				TableID: t1DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t1",
				TableID: t1DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
	})
	schema.AdvanceResolvedTs(t2DropJob.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, t2DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(t2DropJob))
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  t2DropJob.StartTS,
		CommitTs: t2DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP TABLE `test`.`t2`",
		Type:     timodel.ActionDropTable,
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t2",
				TableID: t2DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "t2",
				TableID: t2DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeLong)},
				},
			},
		},
	})
}

func TestBuildDDLEventsFromDropViewsDDL(t *testing.T) {
	helper := NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schema, err := NewSchemaStorage(helper.Storage(), ver.Ver,
		false, dummyChangeFeedID, util.RoleTester, f)
	require.Nil(t, err)
	ctx := context.Background()
	// add test.tb1
	job := helper.DDL2Job("create table test.tb1(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err := schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	// add test.tb2
	job = helper.DDL2Job("create table test.tb2(id int primary key)")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	// add test.view1
	job = helper.DDL2Job(
		"create view test.view1 as select * from test.tb1 where id > 100")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	// add test.view2
	job = helper.DDL2Job(
		"create view test.view2 as select * from test.tb2 where id > 100")
	schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(job))

	jobs := helper.DDL2Jobs("drop view test.view1, test.view2", 2)
	view1DropJob := jobs[1]
	view2DropJob := jobs[0]
	schema.AdvanceResolvedTs(view1DropJob.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, view1DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(view1DropJob))
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  view1DropJob.StartTS,
		CommitTs: view1DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP VIEW `test`.`view1`",
		Type:     timodel.ActionDropView,
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "view1",
				TableID: view1DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeUnspecified)},
				},
			},
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "view1",
				TableID: view1DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeUnspecified)},
				},
			},
		},
	})
	schema.AdvanceResolvedTs(view2DropJob.BinlogInfo.FinishedTS - 1)
	events, err = schema.BuildDDLEvents(ctx, view2DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDLJob(view2DropJob))
	compareEvents(t, events[0], &model.DDLEvent{
		StartTs:  view2DropJob.StartTS,
		CommitTs: view2DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP VIEW `test`.`view2`",
		Type:     timodel.ActionDropView,
		PreTableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "view2",
				TableID: view2DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeUnspecified)},
				},
			},
		},
		TableInfo: &model.TableInfo{
			TableName: model.TableName{
				Schema:  "test",
				Table:   "view2",
				TableID: view2DropJob.TableID,
			},
			TableInfo: &timodel.TableInfo{
				Columns: []*timodel.ColumnInfo{
					{Name: timodel.NewCIStr("id"), FieldType: *types.NewFieldType(mysql.TypeUnspecified)},
				},
			},
		},
	})
}
