// Copyright 2023 PingCAP, Inc.
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
	"fmt"
	"testing"

	timodel "github.com/pingcap/tidb/pkg/parser/model"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	config2 "github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func createDDLManagerForTest(t *testing.T) *ddlManager {
	startTs, checkpointTs := model.Ts(0), model.Ts(1)
	changefeedID := model.DefaultChangeFeedID("ddl-manager-test")
	ddlSink := &mockDDLSink{}
	ddlPuller := &mockDDLPuller{}
	cfg := config2.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(cfg, "")
	require.Nil(t, err)
	schema, err := entry.NewSchemaStorage(nil, startTs, cfg.ForceReplicate, changefeedID, util.RoleTester, f)
	require.Equal(t, nil, err)
	res := newDDLManager(
		changefeedID,
		startTs,
		checkpointTs,
		ddlSink,
		f,
		ddlPuller,
		schema,
		redo.NewDisabledDDLManager(),
		redo.NewDisabledMetaManager(),
		false)
	return res
}

func newFakeDDLEvent(
	tableID int64,
	tableName string,
	actionType timodel.ActionType,
	commitTs uint64,
) *model.DDLEvent {
	info := &model.TableInfo{
		TableName: model.TableName{Table: tableName, TableID: tableID},
	}
	info.TableInfo = &timodel.TableInfo{
		ID:   tableID,
		Name: timodel.NewCIStr(tableName),
	}
	return &model.DDLEvent{
		TableInfo: info,
		Type:      actionType,
		CommitTs:  commitTs,
	}
}

func TestGetNextDDL(t *testing.T) {
	dm := createDDLManagerForTest(t)
	dm.executingDDL = newFakeDDLEvent(1,
		"test_1", timodel.ActionDropColumn, 1)
	require.Equal(t, dm.executingDDL, dm.getNextDDL())

	dm.executingDDL = nil
	ddl1 := newFakeDDLEvent(1,
		"test_1", timodel.ActionDropColumn, 1)
	ddl2 := newFakeDDLEvent(2,
		"test_2", timodel.ActionDropColumn, 2)
	dm.pendingDDLs[ddl1.TableInfo.TableName] = append(dm.
		pendingDDLs[ddl1.TableInfo.TableName], ddl1)
	dm.pendingDDLs[ddl2.TableInfo.TableName] = append(dm.
		pendingDDLs[ddl2.TableInfo.TableName], ddl2)
	require.Equal(t, ddl1, dm.getNextDDL())
}

func TestBarriers(t *testing.T) {
	dm := createDDLManagerForTest(t)

	tableID1 := int64(1)
	tableName1 := model.TableName{Table: "test_1", TableID: tableID1}
	// this ddl commitTs will be minTableBarrierTs
	dm.justSentDDL = newFakeDDLEvent(tableID1,
		"test_1", timodel.ActionDropColumn, 1)
	dm.pendingDDLs[tableName1] = append(dm.pendingDDLs[tableName1],
		newFakeDDLEvent(tableID1, tableName1.Table, timodel.ActionAddColumn, 2))

	tableID2 := int64(2)
	tableName2 := model.TableName{Table: "test_2", TableID: tableID2}
	dm.pendingDDLs[tableName2] = append(dm.pendingDDLs[tableName2],
		// this ddl commitTs will become globalBarrierTs
		newFakeDDLEvent(tableID2, tableName2.Table, timodel.ActionCreateTable, 4))

	expectedMinTableBarrier := uint64(1)
	expectedBarrier := &schedulepb.Barrier{
		GlobalBarrierTs: 4,
		TableBarriers: []*schedulepb.TableBarrier{
			{
				TableID:   tableID1,
				BarrierTs: 1,
			},
		},
	}
	// advance the ddlResolvedTs
	dm.ddlResolvedTs = 6
	ddlBarrier := dm.barrier()
	minTableBarrierTs, barrier := ddlBarrier.MinTableBarrierTs, ddlBarrier.Barrier
	require.Equal(t, expectedMinTableBarrier, minTableBarrierTs)
	require.Equal(t, expectedBarrier, barrier)

	// test tableBarrier limit
	dm.pendingDDLs = make(map[model.TableName][]*model.DDLEvent)
	dm.ddlResolvedTs = 1024
	for i := 0; i < 512; i++ {
		tableID := int64(i)
		tableName := model.TableName{Table: fmt.Sprintf("test_%d", i), TableID: tableID}
		dm.pendingDDLs[tableName] = append(dm.pendingDDLs[tableName],
			newFakeDDLEvent(tableID, tableName.Table, timodel.ActionAddColumn, uint64(i)))
	}
	ddlBarrier = dm.barrier()
	minTableBarrierTs, barrier = ddlBarrier.MinTableBarrierTs, ddlBarrier.Barrier
	require.Equal(t, uint64(0), minTableBarrierTs)
	require.Equal(t, uint64(256), barrier.GlobalBarrierTs)
	require.Equal(t, 256, len(barrier.TableBarriers))
}

func TestGetSnapshotTs(t *testing.T) {
	dm := createDDLManagerForTest(t)
	dm.startTs = 0
	dm.checkpointTs = 1
	require.Equal(t, dm.startTs, dm.getSnapshotTs())

	dm.startTs = 1
	dm.checkpointTs = 10
	dm.BDRMode = true
	dm.ddlResolvedTs = 15
	require.Equal(t, dm.checkpointTs, dm.getSnapshotTs())

	dm.startTs = 1
	dm.checkpointTs = 10
	dm.BDRMode = false
	require.Equal(t, dm.checkpointTs, dm.getSnapshotTs())
}

func TestExecRenameTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	dm := createDDLManagerForTest(t)
	mockDDLSink := dm.ddlSink.(*mockDDLSink)

	var oldSchemaIDs, newSchemaIDs, oldTableIDs []int64
	var newTableNames, oldSchemaNames []timodel.CIStr

	execCreateStmt := func(tp, actualDDL, expectedDDL string) {
		mockDDLSink.ddlDone = false
		job := helper.DDL2Job(actualDDL)
		dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
		events, err := dm.schema.BuildDDLEvents(ctx, job)
		require.Nil(t, err)
		err = dm.schema.HandleDDLJob(job)
		require.Nil(t, err)

		for _, event := range events {
			done, err := dm.ddlSink.emitDDLEvent(ctx, event)
			if tp == "database" {
				oldSchemaIDs = append(oldSchemaIDs, job.SchemaID)
			} else {
				oldTableIDs = append(oldTableIDs, job.TableID)
			}
			require.Nil(t, err)
			require.Equal(t, false, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)

			mockDDLSink.ddlDone = true
			done, err = dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, true, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
		}
	}

	execCreateStmt("database", "create database test1",
		"create database test1")
	execCreateStmt("table", "create table test1.tb1(id int primary key)",
		"create table test1.tb1(id int primary key)")
	execCreateStmt("database", "create database test2",
		"create database test2")
	execCreateStmt("table", "create table test2.tb2(id int primary key)",
		"create table test2.tb2(id int primary key)")

	require.Len(t, oldSchemaIDs, 2)
	require.Len(t, oldTableIDs, 2)
	newSchemaIDs = []int64{oldSchemaIDs[1], oldSchemaIDs[0]}
	oldSchemaNames = []timodel.CIStr{
		timodel.NewCIStr("test1"),
		timodel.NewCIStr("test2"),
	}
	newTableNames = []timodel.CIStr{
		timodel.NewCIStr("tb20"),
		timodel.NewCIStr("tb10"),
	}
	require.Len(t, newSchemaIDs, 2)
	require.Len(t, oldSchemaNames, 2)
	require.Len(t, newTableNames, 2)
	args := []interface{}{
		oldSchemaIDs, newSchemaIDs, newTableNames,
		oldTableIDs, oldSchemaNames,
	}
	rawArgs, err := json.Marshal(args)
	require.Nil(t, err)
	job := helper.DDL2Job(
		"rename table test1.tb1 to test2.tb10, test2.tb2 to test1.tb20")
	// the RawArgs field in job fetched from tidb snapshot meta is incorrent,
	// so we manually construct `job.RawArgs` to do the workaround.
	job.RawArgs = rawArgs

	mockDDLSink.recordDDLHistory = true
	mockDDLSink.ddlDone = false
	dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
	events, err := dm.schema.BuildDDLEvents(ctx, job)
	require.Nil(t, err)
	for _, event := range events {
		done, err := dm.ddlSink.emitDDLEvent(ctx, event)
		require.Nil(t, err)
		require.Equal(t, false, done)

	}
	require.Len(t, mockDDLSink.ddlHistory, 2)
	require.Equal(t, "RENAME TABLE `test1`.`tb1` TO `test2`.`tb10`",
		mockDDLSink.ddlHistory[0])
	require.Equal(t, "RENAME TABLE `test2`.`tb2` TO `test1`.`tb20`",
		mockDDLSink.ddlHistory[1])

	// mock all rename table statements have been done
	mockDDLSink.resetDDLDone = false
	mockDDLSink.ddlDone = true
	for _, event := range events {
		done, err := dm.ddlSink.emitDDLEvent(ctx, event)
		require.Nil(t, err)
		require.Equal(t, true, done)
	}
}

func TestExecDropTablesDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	dm := createDDLManagerForTest(t)
	mockDDLSink := dm.ddlSink.(*mockDDLSink)

	execCreateStmt := func(actualDDL, expectedDDL string) {
		job := helper.DDL2Job(actualDDL)
		dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
		events, err := dm.schema.BuildDDLEvents(ctx, job)
		require.Nil(t, err)
		err = dm.schema.HandleDDLJob(job)
		require.Nil(t, err)
		mockDDLSink.ddlDone = false

		for _, event := range events {
			done, err := dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, false, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
			mockDDLSink.ddlDone = true
			done, err = dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, true, done)
		}
	}

	execCreateStmt("create database test1",
		"create database test1")
	execCreateStmt("create table test1.tb1(id int primary key)",
		"create table test1.tb1(id int primary key)")
	execCreateStmt("create table test1.tb2(id int primary key)",
		"create table test1.tb2(id int primary key)")

	// drop tables is different from rename tables, it will generate
	// multiple DDL jobs instead of one.
	jobs := helper.DDL2Jobs("drop table test1.tb1, test1.tb2", 2)
	require.Len(t, jobs, 2)

	execDropStmt := func(job *timodel.Job, expectedDDL string) {
		dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
		events, err := dm.schema.BuildDDLEvents(ctx, job)
		require.Nil(t, err)
		err = dm.schema.HandleDDLJob(job)
		require.Nil(t, err)
		mockDDLSink.ddlDone = false

		for _, event := range events {
			done, err := dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, false, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
			mockDDLSink.ddlDone = true
			done, err = dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, true, done)
		}
	}

	execDropStmt(jobs[0], "DROP TABLE `test1`.`tb2`")
	execDropStmt(jobs[1], "DROP TABLE `test1`.`tb1`")
}

func TestExecDropViewsDDL(t *testing.T) {
	helper := entry.NewSchemaTestHelper(t)
	defer helper.Close()
	ctx := cdcContext.NewBackendContext4Test(true)
	dm := createDDLManagerForTest(t)
	mockDDLSink := dm.ddlSink.(*mockDDLSink)

	execCreateStmt := func(actualDDL, expectedDDL string) {
		job := helper.DDL2Job(actualDDL)
		dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
		events, err := dm.schema.BuildDDLEvents(ctx, job)
		require.Nil(t, err)
		err = dm.schema.HandleDDLJob(job)
		require.Nil(t, err)
		mockDDLSink.ddlDone = false
		for _, event := range events {
			done, err := dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, false, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
			mockDDLSink.ddlDone = true
			done, err = dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, true, done)
		}
	}

	execCreateStmt("create database test1",
		"create database test1")
	execCreateStmt("create table test1.tb1(id int primary key)",
		"create table test1.tb1(id int primary key)")
	execCreateStmt("create view test1.view1 as "+
		"select * from test1.tb1 where id > 100",
		"create view test1.view1 as "+
			"select * from test1.tb1 where id > 100")
	execCreateStmt("create view test1.view2 as "+
		"select * from test1.tb1 where id > 200",
		"create view test1.view2 as "+
			"select * from test1.tb1 where id > 200")

	// drop views is similar to drop tables, it will also generate
	// multiple DDL jobs.
	jobs := helper.DDL2Jobs("drop view test1.view1, test1.view2", 2)
	require.Len(t, jobs, 2)

	execDropStmt := func(job *timodel.Job, expectedDDL string) {
		dm.schema.AdvanceResolvedTs(job.BinlogInfo.FinishedTS - 1)
		events, err := dm.schema.BuildDDLEvents(ctx, job)
		require.Nil(t, err)
		err = dm.schema.HandleDDLJob(job)
		require.Nil(t, err)
		mockDDLSink.ddlDone = false
		for _, event := range events {
			done, err := dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, false, done)
			require.Equal(t, expectedDDL, mockDDLSink.ddlExecuting.Query)
			mockDDLSink.ddlDone = true
			done, err = dm.ddlSink.emitDDLEvent(ctx, event)
			require.Nil(t, err)
			require.Equal(t, true, done)
		}
	}

	execDropStmt(jobs[0], "DROP VIEW `test1`.`view2`")
	execDropStmt(jobs[1], "DROP VIEW `test1`.`view1`")
}

func TestIsGlobalDDL(t *testing.T) {
	cases := []struct {
		ddl *model.DDLEvent
		ret bool
	}{
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionCreateSchema,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionDropSchema,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionCreateTable,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionRenameTables,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionRenameTable,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionExchangeTablePartition,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionModifySchemaCharsetAndCollate,
			},
			ret: true,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionTruncateTable,
			},
			ret: false,
		},
		{
			ddl: &model.DDLEvent{
				Type: timodel.ActionDropColumn,
			},
			ret: false,
		},
	}

	for _, c := range cases {
		require.Equal(t, c.ret, isGlobalDDL(c.ddl))
	}
}
