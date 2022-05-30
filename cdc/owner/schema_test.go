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

package owner

import (
	"encoding/json"
	"sort"
	"testing"

	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/parser/mysql"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
)

var dummyChangeFeedID = model.DefaultChangeFeedID("dummy_changefeed")

func TestAllPhysicalTables(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	require.Len(t, schema.AllPhysicalTables(), 0)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1 := job.BinlogInfo.TableInfo.ID
	require.Nil(t, schema.HandleDDL(job))
	require.Equal(t, schema.AllPhysicalTables(), []model.TableID{tableIDT1})
	// add ineligible table
	require.Nil(t, schema.HandleDDL(helper.DDL2Job("create table test.t2(id int)")))
	require.Equal(t, schema.AllPhysicalTables(), []model.TableID{tableIDT1})
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
	require.Nil(t, schema.HandleDDL(job))
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
	sortTableIDs(schema.AllPhysicalTables())
	require.Equal(t, schema.AllPhysicalTables(), expectedTableIDs)
}

func TestAllTableNames(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	require.Len(t, schema.AllTableNames(), 0)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	require.Nil(t, schema.HandleDDL(job))
	require.Equal(t, []model.TableName{{Schema: "test", Table: "t1"}}, schema.AllTableNames())
	// add ineligible table
	require.Nil(t, schema.HandleDDL(helper.DDL2Job("create table test.t2(id int)")))
	require.Equal(t, []model.TableName{{Schema: "test", Table: "t1"}}, schema.AllTableNames())
}

func TestIsIneligibleTableID(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1 := job.BinlogInfo.TableInfo.ID
	require.Nil(t, schema.HandleDDL(job))
	// add ineligible table
	job = helper.DDL2Job("create table test.t2(id int)")
	tableIDT2 := job.BinlogInfo.TableInfo.ID

	require.Nil(t, schema.HandleDDL(job))
	require.False(t, schema.IsIneligibleTableID(tableIDT1))
	require.True(t, schema.IsIneligibleTableID(tableIDT2))
}

func TestBuildDDLEventsFromSingleTableDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	events, err := schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "create table test.t1(id int primary key)",
		Type:     timodel.ActionCreateTable,
		TableInfo: &model.SimpleTableInfo{
			Schema:     "test",
			Table:      "t1",
			TableID:    job.TableID,
			ColumnInfo: []*model.ColumnInfo{{Name: "id", Type: mysql.TypeLong}},
		},
		PreTableInfo: nil,
	})
	require.Nil(t, schema.HandleDDL(job))
	job = helper.DDL2Job("ALTER TABLE test.t1 ADD COLUMN c1 CHAR(16) NOT NULL")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "ALTER TABLE test.t1 ADD COLUMN c1 CHAR(16) NOT NULL",
		Type:     timodel.ActionAddColumn,
		TableInfo: &model.SimpleTableInfo{
			Schema:     "test",
			Table:      "t1",
			TableID:    job.TableID,
			ColumnInfo: []*model.ColumnInfo{{Name: "id", Type: mysql.TypeLong}, {Name: "c1", Type: mysql.TypeString}},
		},
		PreTableInfo: &model.SimpleTableInfo{
			Schema:     "test",
			Table:      "t1",
			TableID:    job.TableID,
			ColumnInfo: []*model.ColumnInfo{{Name: "id", Type: mysql.TypeLong}},
		},
	})
}

func TestBuildDDLEventsFromRenameTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	job := helper.DDL2Job("create database test1")
	events, err := schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))
	schemaID := job.SchemaID
	// add test.t1
	job = helper.DDL2Job("create table test1.t1(id int primary key)")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))
	t1TableID := job.TableID

	// add test.t2
	job = helper.DDL2Job("create table test1.t2(id int primary key)")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))
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

	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 2)
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "RENAME TABLE `test1`.`t1` TO `test1`.`t10`",
		Type:     timodel.ActionRenameTable,
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test1",
			Table:   "t10",
			TableID: t1TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test1",
			Table:   "t1",
			TableID: t1TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
	})
	require.Equal(t, events[1], &model.DDLEvent{
		StartTs:  job.StartTS,
		CommitTs: job.BinlogInfo.FinishedTS,
		Query:    "RENAME TABLE `test1`.`t2` TO `test1`.`t20`",
		Type:     timodel.ActionRenameTable,
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test1",
			Table:   "t20",
			TableID: t2TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test1",
			Table:   "t2",
			TableID: t2TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
	})
}

func TestBuildDDLEventsFromDropTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	// add test.t1
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	events, err := schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	// add test.t2
	job = helper.DDL2Job("create table test.t2(id int primary key)")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	jobs := helper.DDL2Jobs("drop table test.t1, test.t2", 2)
	t1DropJob := jobs[1]
	t2DropJob := jobs[0]
	events, err = schema.BuildDDLEvents(t1DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(t1DropJob))
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  t1DropJob.StartTS,
		CommitTs: t1DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP TABLE `test`.`t1`",
		Type:     timodel.ActionDropTable,
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "t1",
			TableID: t1DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "t1",
			TableID: t1DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
	})

	events, err = schema.BuildDDLEvents(t2DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(t2DropJob))
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  t2DropJob.StartTS,
		CommitTs: t2DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP TABLE `test`.`t2`",
		Type:     timodel.ActionDropTable,
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "t2",
			TableID: t2DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "t2",
			TableID: t2DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeLong,
				},
			},
		},
	})
}

func TestBuildDDLEventsFromDropViewsDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()

	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver,
		config.GetDefaultReplicaConfig(), dummyChangeFeedID)
	require.Nil(t, err)
	// add test.tb1
	job := helper.DDL2Job("create table test.tb1(id int primary key)")
	events, err := schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	// add test.tb2
	job = helper.DDL2Job("create table test.tb2(id int primary key)")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	// add test.view1
	job = helper.DDL2Job(
		"create view test.view1 as select * from test.tb1 where id > 100")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	// add test.view2
	job = helper.DDL2Job(
		"create view test.view2 as select * from test.tb2 where id > 100")
	events, err = schema.BuildDDLEvents(job)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(job))

	jobs := helper.DDL2Jobs("drop view test.view1, test.view2", 2)
	view1DropJob := jobs[1]
	view2DropJob := jobs[0]
	events, err = schema.BuildDDLEvents(view1DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(view1DropJob))
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  view1DropJob.StartTS,
		CommitTs: view1DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP VIEW `test`.`view1`",
		Type:     timodel.ActionDropView,
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "view1",
			TableID: view1DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeUnspecified,
				},
			},
		},
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "view1",
			TableID: view1DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeUnspecified,
				},
			},
		},
	})

	events, err = schema.BuildDDLEvents(view2DropJob)
	require.Nil(t, err)
	require.Len(t, events, 1)
	require.Nil(t, schema.HandleDDL(view2DropJob))
	require.Equal(t, events[0], &model.DDLEvent{
		StartTs:  view2DropJob.StartTS,
		CommitTs: view2DropJob.BinlogInfo.FinishedTS,
		Query:    "DROP VIEW `test`.`view2`",
		Type:     timodel.ActionDropView,
		PreTableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "view2",
			TableID: view2DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeUnspecified,
				},
			},
		},
		TableInfo: &model.SimpleTableInfo{
			Schema:  "test",
			Table:   "view2",
			TableID: view2DropJob.TableID,
			ColumnInfo: []*model.ColumnInfo{
				{
					Name: "id",
					Type: mysql.TypeUnspecified,
				},
			},
		},
	})
}
