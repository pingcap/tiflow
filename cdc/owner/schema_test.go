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

func TestAllPhysicalTables(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver, config.GetDefaultReplicaConfig())
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

func TestIsIneligibleTableID(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver, config.GetDefaultReplicaConfig())
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

func TestBuildDDLEvent(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver, config.GetDefaultReplicaConfig())
	require.Nil(t, err)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	event, err := schema.BuildDDLEvent(job)
	require.Nil(t, err)
	require.Equal(t, event, &model.DDLEvent{
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
	event, err = schema.BuildDDLEvent(job)
	require.Nil(t, err)
	require.Equal(t, event, &model.DDLEvent{
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

func TestSinkTableInfos(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ver, err := helper.Storage().CurrentVersion(oracle.GlobalTxnScope)
	require.Nil(t, err)
	schema, err := newSchemaWrap4Owner(helper.Storage(), ver.Ver, config.GetDefaultReplicaConfig())
	require.Nil(t, err)
	// add normal table
	job := helper.DDL2Job("create table test.t1(id int primary key)")
	tableIDT1 := job.BinlogInfo.TableInfo.ID
	require.Nil(t, schema.HandleDDL(job))
	// add ineligible table
	job = helper.DDL2Job("create table test.t2(id int)")
	require.Nil(t, schema.HandleDDL(job))
	require.Equal(t, schema.SinkTableInfos(), []*model.SimpleTableInfo{
		{
			Schema:     "test",
			Table:      "t1",
			TableID:    tableIDT1,
			ColumnInfo: []*model.ColumnInfo{{Name: "id", Type: mysql.TypeLong}},
		},
	})
}
