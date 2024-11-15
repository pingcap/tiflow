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

package puller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/pkg/meta/model"
	"github.com/pingcap/tidb/pkg/util/codec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/puller/memorysorter"
	"github.com/pingcap/tiflow/cdc/vars"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

func jonToRawKVEntry(t *testing.T, job *timodel.Job) *model.RawKVEntry {
	b, err := json.Marshal(job)
	require.Nil(t, err)
	ek := []byte("m")
	ek = codec.EncodeBytes(ek, []byte("DDLJobList"))
	ek = codec.EncodeUint(ek, uint64('l'))
	ek = codec.EncodeInt(ek, 1)
	return &model.RawKVEntry{
		OpType:  model.OpTypePut,
		Key:     ek,
		Value:   b,
		StartTs: job.StartTS,
		CRTs:    job.BinlogInfo.FinishedTS,
	}
}

func tsToRawKVEntry(_ *testing.T, ts model.Ts) *model.RawKVEntry {
	return &model.RawKVEntry{
		OpType:  model.OpTypeResolved,
		CRTs:    ts,
		StartTs: ts,
	}
}

func inputDDL(t *testing.T, puller *ddlJobPullerImpl, job *timodel.Job) {
	rawJob := jonToRawKVEntry(t, job)
	puller.Input(context.Background(), rawJob, []tablepb.Span{}, func(_ *model.RawKVEntry) bool { return false })
}

func inputTs(t *testing.T, puller *ddlJobPullerImpl, ts model.Ts) {
	rawTs := tsToRawKVEntry(t, ts)
	puller.Input(context.Background(), rawTs, []tablepb.Span{}, func(_ *model.RawKVEntry) bool { return false })
}

func waitResolvedTs(t *testing.T, p DDLJobPuller, targetTs model.Ts) {
	err := retry.Do(context.Background(), func() error {
		if p.(*ddlJobPullerImpl).getResolvedTs() < targetTs {
			return fmt.Errorf("resolvedTs %d < targetTs %d", p.(*ddlJobPullerImpl).getResolvedTs(), targetTs)
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(200))
	require.Nil(t, err)
}

func newMockDDLJobPuller(
	t *testing.T,
	needSchemaStorage bool,
) (DDLJobPuller, *entry.SchemaTestHelper) {
	res := &ddlJobPullerImpl{
		outputCh: make(
			chan *model.DDLJobEntry,
			defaultPullerOutputChanSize),
	}
	res.sorter = memorysorter.NewEntrySorter(model.ChangeFeedID4Test("puller", "test"))

	var helper *entry.SchemaTestHelper
	if needSchemaStorage {
		helper = entry.NewSchemaTestHelper(t)
		kvStorage := helper.Storage()
		f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
		require.Nil(t, err)
		schemaStorage, err := entry.NewSchemaStorage(
			kvStorage,
			0,
			false,
			model.DefaultChangeFeedID("test"),
			util.RoleTester,
			f)
		require.Nil(t, err)
		res.schemaStorage = schemaStorage
		res.kvStorage = kvStorage
	}
	return res, helper
}

func TestHandleRenameTable(t *testing.T) {
	ddlJobPuller, helper := newMockDDLJobPuller(t, true)
	defer helper.Close()

	startTs := uint64(10)
	ddlJobPullerImpl := ddlJobPuller.(*ddlJobPullerImpl)
	ddlJobPullerImpl.setResolvedTs(startTs)

	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{
		"test1.t1",
		"test1.t2",
		"test1.t4",
		"test1.t66",
		"test1.t99",
		"test1.t100",
		"test1.t20230808",
		"test1.t202308081",
		"test1.t202308082",

		"test2.t4",

		"Test3.t1",
		"Test3.t2",
	}
	f, err := filter.NewFilter(cfg, "")
	require.NoError(t, err)
	ddlJobPullerImpl.filter = f

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go ddlJobPuller.Run(ctx)
	go func() {
		select {
		case <-ctx.Done():
			return
		case <-ddlJobPuller.Output():
		}
	}()

	// table t3, t5 not found in snapshot, skip it.
	// only table t1 remain.
	{
		remainTables := make([]int64, 1)
		job := helper.DDL2Job("create database test1")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t1(id int primary key)")
		remainTables[0] = job.TableID
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t2(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t3(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t5(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create database ignore1")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table ignore1.a(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("rename table test1.t1 to test1.t11, test1.t3 to test1.t33, test1.t5 to test1.t55, ignore1.a to ignore1.b")
		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.NoError(t, err)
		require.False(t, skip)
		require.Len(t, job.BinlogInfo.MultipleTableInfos, 1)
		require.Equal(t, remainTables[0], job.BinlogInfo.MultipleTableInfos[0].ID)
	}

	{
		_ = helper.DDL2Job("create table test1.t6(id int primary key)")
		job := helper.DDL2Job("rename table test1.t2 to test1.t22, test1.t6 to test1.t66")
		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.Error(t, err)
		require.True(t, skip)
		require.Contains(t, err.Error(), fmt.Sprintf("table's old name is not in filter rule, and its new name in filter rule "+
			"table id '%d', ddl query: [%s], it's an unexpected behavior, "+
			"if you want to replicate this table, please add its old name to filter rule.", job.TableID, job.Query))
	}

	// all tables are filtered out
	{
		job := helper.DDL2Job("create database test2")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t1(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t2(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t3(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("rename table test2.t1 to test2.t11, test2.t2 to test2.t22, test2.t3 to test2.t33")
		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test uppercase db name
	{
		job := helper.DDL2Job("create database Test3")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table Test3.t1(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// skip this table
		job = helper.DDL2Job("create table Test3.t2(id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("rename table Test3.t1 to Test3.t11, Test3.t2 to Test3.t22")
		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.NoError(t, err)
		require.False(t, skip)
		require.Equal(t, 2, len(job.BinlogInfo.MultipleTableInfos))
		require.Equal(t, "t11", job.BinlogInfo.MultipleTableInfos[0].Name.O)
		require.Equal(t, "t22", job.BinlogInfo.MultipleTableInfos[1].Name.O)
	}

	// test rename table
	{
		job := helper.DDL2Job("create table test1.t99 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// this ddl should be skipped
		job = helper.DDL2Job("create table test1.t1000 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// this ddl should be skipped
		job = helper.DDL2Job("create table test1.t888 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t20230808 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t202308081 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t202308082 (id int primary key)")
		inputDDL(t, ddlJobPullerImpl, job)
		inputTs(t, ddlJobPullerImpl, job.BinlogInfo.FinishedTS+1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)
		// since test1.99 in filter rule, we replicate it
		job = helper.DDL2Job("rename table test1.t99 to test1.t999")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		// since test1.t100 is in filter rule, replicate it
		job = helper.DDL2Job("rename table test1.t1000 to test1.t100")
		_, err = ddlJobPullerImpl.handleJob(job)
		require.Error(t, err)
		require.Contains(t, err.Error(), fmt.Sprintf("table's old name is not in filter rule, and its new name in filter rule "+
			"table id '%d', ddl query: [%s], it's an unexpected behavior, "+
			"if you want to replicate this table, please add its old name to filter rule.", job.TableID, job.Query))

		// since test1.t888 and test1.t777 are not in filter rule, skip it
		job = helper.DDL2Job("rename table test1.t888 to test1.t777")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		// since test1.t20230808 is  in filter rule, replicate it
		// ref: https://github.com/pingcap/tiflow/issues/9488
		job = helper.DDL2Job("rename table test1.t20230808 to ignore1.ignore")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		// FIXME(dongmen): since test1.t202308081 and test1.t202308082 are in filter rule, it should be replicated
		// but now it will throw an error since schema ignore1 are not in schemaStorage
		// ref: https://github.com/pingcap/tiflow/issues/9488
		job = helper.DDL2Job("rename table test1.t202308081 to ignore1.ignore1, test1.t202308082 to ignore1.dongmen")
		_, err = ddlJobPullerImpl.handleJob(job)
		require.NotNil(t, err)
		require.Contains(t, err.Error(), "ErrSnapshotSchemaNotFound")
	}
}

func TestHandleJob(t *testing.T) {
	ddlJobPuller, helper := newMockDDLJobPuller(t, true)
	defer helper.Close()
	startTs := uint64(10)
	ddlJobPullerImpl := ddlJobPuller.(*ddlJobPullerImpl)
	ddlJobPullerImpl.setResolvedTs(startTs)
	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{
		"test1.t1",
		"test1.t2",
		"test1.testStartTs",
	}
	// test start ts filter
	cfg.Filter.IgnoreTxnStartTs = []uint64{1}
	// test event filter
	cfg.Filter.EventFilters = []*config.EventFilterRule{
		{
			Matcher:   []string{"test1.*"},
			IgnoreSQL: []string{"alter table test1.t1 add column c1 int"},
		},
	}

	f, err := filter.NewFilter(cfg, "")
	require.NoError(t, err)
	ddlJobPullerImpl.filter = f

	// test create database
	{
		job := helper.DDL2Job("create database test1")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create database test2")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		job = helper.DDL2Job("create database test3")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test drop databases
	{
		job := helper.DDL2Job("drop database test2")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test create table
	{
		job := helper.DDL2Job("create table test1.t1(id int primary key) partition by range(id) (partition p0 values less than (10))")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t1 add column c1 int")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create table test1.testStartTs(id int primary key)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.testStartTs add column c1 int")
		job.StartTS = 1
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create table test1.t2(id int primary key)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create table test1.t3(id int primary key)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		job = helper.DDL2Job("create table test1.t4(id int primary key) partition by range(id) (partition p0 values less than (10))")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		// make sure no schema not found error
		job = helper.DDL2Job("create table test3.t1(id int primary key) partition by range(id) (partition p0 values less than (10))")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test drop table
	{
		job := helper.DDL2Job("drop table test1.t2")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("drop table test1.t3")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test add column and drop column
	{
		job := helper.DDL2Job("alter table test1.t1 add column age int")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t4 add column age int")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test add index and drop index
	{
		job := helper.DDL2Job("alter table test1.t1 add index idx_age(age)")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t4 add index idx_age(age)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		job = helper.DDL2Job("alter table test1.t1 drop index idx_age")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t4 drop index idx_age")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test drop column
	{
		job := helper.DDL2Job("alter table test1.t1 drop column age")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t4 drop column age")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test truncate table
	{
		job := helper.DDL2Job("truncate table test1.t1")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("truncate table test1.t4")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test add table partition
	{
		job := helper.DDL2Job("alter table test1.t1 add partition (partition p1 values less than (100))")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("alter table test1.t4 add partition (partition p1 values less than (100))")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test flashback cluster
	{
		// mock a flashback job
		job := &timodel.Job{
			Type:       timodel.ActionFlashbackCluster,
			BinlogInfo: &timodel.HistoryInfo{},
		}
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}
}

func TestDDLPuller(t *testing.T) {
	startTs := uint64(10)

	_, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	up := upstream.NewUpstream4Test(nil)
	f, err := filter.NewFilter(changefeedInfo.Config, "")
	require.Nil(t, err)
	schemaStorage, err := entry.NewSchemaStorage(nil,
		startTs,
		changefeedInfo.Config.ForceReplicate,
		model.DefaultChangeFeedID(changefeedInfo.ID),
		util.RoleTester,
		f,
	)
	require.Nil(t, err)
	p := NewDDLPuller(up, startTs, model.DefaultChangeFeedID(changefeedInfo.ID), schemaStorage, f)
	p.(*ddlPullerImpl).ddlJobPuller, _ = newMockDDLJobPuller(t, false)
	ddlJobPullerImpl := p.(*ddlPullerImpl).ddlJobPuller.(*ddlJobPullerImpl)
	ddlJobPullerImpl.setResolvedTs(startTs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		require.True(t, errors.ErrorEqual(err, context.Canceled))
	}()
	defer wg.Wait()
	defer p.Close()

	resolvedTs, ddl := p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	// test send resolvedTs
	inputTs(t, ddlJobPullerImpl, 15)
	waitResolvedTsGrowing(t, p, 15)

	// test send ddl job out of order
	inputDDL(t, ddlJobPullerImpl, &timodel.Job{
		ID:         2,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, FinishedTS: 18},
		Query:      "create table test.t1(id int primary key)",
	})
	inputDDL(t, ddlJobPullerImpl, &timodel.Job{
		ID:         1,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, FinishedTS: 16},
		Query:      "create table t2(id int primary key)",
	})
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(15))
	require.Nil(t, ddl)

	inputTs(t, ddlJobPullerImpl, 20)
	waitResolvedTsGrowing(t, p, 16)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(16))
	require.Equal(t, ddl.ID, int64(1))

	// DDL could be processed with a delay, wait here for a pending DDL job is added
	waitResolvedTsGrowing(t, p, 18)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(18))
	require.Equal(t, ddl.ID, int64(2))

	// test add ddl job repeated
	inputDDL(t, ddlJobPullerImpl, &timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, FinishedTS: 25},
		Query:      "create table t3(id int primary key)",
	})

	inputDDL(t, ddlJobPullerImpl, &timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, FinishedTS: 25},
		Query:      "create table t3(id int primary key)",
	})

	inputTs(t, ddlJobPullerImpl, 30)
	waitResolvedTsGrowing(t, p, 25)

	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(25))
	require.Equal(t, ddl.ID, int64(3))
	_, ddl = p.PopFrontDDL()
	require.Nil(t, ddl)

	waitResolvedTsGrowing(t, p, 30)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(30))
	require.Nil(t, ddl)

	inputDDL(t, ddlJobPullerImpl, &timodel.Job{
		ID:         5,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateCancelled,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 6, FinishedTS: 36},
		Query:      "create table t4(id int primary key)",
	})

	inputTs(t, ddlJobPullerImpl, 40)
	waitResolvedTsGrowing(t, p, 40)
	resolvedTs, ddl = p.PopFrontDDL()
	// no ddl should be received
	require.Equal(t, resolvedTs, uint64(40))
	require.Nil(t, ddl)
}

func TestResolvedTsStuck(t *testing.T) {
	// For observing the logs
	zapcore, logs := observer.New(zap.WarnLevel)
	conf := &log.Config{Level: "warn", File: log.FileLogConfig{}}
	_, r, _ := log.InitLogger(conf)
	logger := zap.New(zapcore)
	restoreFn := log.ReplaceGlobals(logger, r)
	defer restoreFn()

	startTs := uint64(10)

	_, changefeedInfo := vars.NewGlobalVarsAndChangefeedInfo4Test()
	ctx := context.Background()
	up := upstream.NewUpstream4Test(nil)
	f, err := filter.NewFilter(config.GetDefaultReplicaConfig(), "")
	require.Nil(t, err)
	schemaStorage, err := entry.NewSchemaStorage(nil,
		startTs,
		changefeedInfo.Config.ForceReplicate,
		model.DefaultChangeFeedID(changefeedInfo.ID),
		util.RoleTester,
		f,
	)
	require.Nil(t, err)
	p := NewDDLPuller(up, startTs, model.DefaultChangeFeedID(changefeedInfo.ID), schemaStorage, f)

	p.(*ddlPullerImpl).ddlJobPuller, _ = newMockDDLJobPuller(t, false)
	ddlJobPullerImpl := p.(*ddlPullerImpl).ddlJobPuller.(*ddlJobPullerImpl)
	ddlJobPullerImpl.setResolvedTs(startTs)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		if errors.Cause(err) == context.Canceled {
			err = nil
		}
		require.Nil(t, err)
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	inputTs(t, ddlJobPullerImpl, 30)
	waitResolvedTsGrowing(t, p, 30)
	require.Equal(t, 0, logs.Len())

	inputTs(t, ddlJobPullerImpl, 40)
	waitResolvedTsGrowing(t, p, 40)
}

// waitResolvedTsGrowing can wait the first DDL reaches targetTs or if no pending
// DDL, DDL resolved ts reaches targetTs.
func waitResolvedTsGrowing(t *testing.T, p DDLPuller, targetTs model.Ts) {
	err := retry.Do(context.Background(), func() error {
		resolvedTs := p.ResolvedTs()
		if resolvedTs < targetTs {
			return errors.New("resolvedTs < targetTs")
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(200))
	require.Nil(t, err)
}

func TestCcheckIneligibleTableDDL(t *testing.T) {
	ddlJobPuller, helper := newMockDDLJobPuller(t, true)
	defer helper.Close()

	startTs := uint64(10)
	ddlJobPullerImpl := ddlJobPuller.(*ddlJobPullerImpl)
	ddlJobPullerImpl.setResolvedTs(startTs)

	cfg := config.GetDefaultReplicaConfig()
	f, err := filter.NewFilter(cfg, "")
	require.NoError(t, err)
	ddlJobPullerImpl.filter = f

	ddl := helper.DDL2Job("CREATE DATABASE test1")
	skip, err := ddlJobPullerImpl.handleJob(ddl)
	require.NoError(t, err)
	require.False(t, skip)

	// case 1: create a table only has a primary key and drop it, expect an error.
	// It is because the table is not eligible after the drop primary key DDL.
	ddl = helper.DDL2Job(`CREATE TABLE test1.t1 (
		id INT PRIMARY KEY /*T![clustered_index] NONCLUSTERED */,
		name VARCHAR(255),
		email VARCHAR(255) UNIQUE
		);`)
	skip, err = ddlJobPullerImpl.handleJob(ddl)
	require.NoError(t, err)
	require.False(t, skip)

	ddl = helper.DDL2Job("ALTER TABLE test1.t1 DROP PRIMARY KEY;")
	skip, err = ddlJobPullerImpl.handleJob(ddl)
	require.Error(t, err)
	require.False(t, skip)
	require.Contains(t, err.Error(), "An eligible table become ineligible after DDL")

	// case 2: create a table has a primary key and another not null unique key,
	// and drop the primary key, expect no error.
	// It is because the table is still eligible after the drop primary key DDL.
	ddl = helper.DDL2Job(`CREATE TABLE test1.t2 (
		id INT PRIMARY KEY /*T![clustered_index] NONCLUSTERED */,
		name VARCHAR(255),
		email VARCHAR(255) NOT NULL UNIQUE
		);`)
	skip, err = ddlJobPullerImpl.handleJob(ddl)
	require.NoError(t, err)
	require.False(t, skip)

	ddl = helper.DDL2Job("ALTER TABLE test1.t2 DROP PRIMARY KEY;")
	skip, err = ddlJobPullerImpl.handleJob(ddl)
	require.NoError(t, err)
	require.False(t, skip)

	// case 3: continue to drop the unique key, expect an error.
	// It is because the table is not eligible after the drop unique key DDL.
	ddl = helper.DDL2Job("ALTER TABLE test1.t2 DROP INDEX email;")
	skip, err = ddlJobPullerImpl.handleJob(ddl)
	require.Error(t, err)
	require.False(t, skip)
	require.Contains(t, err.Error(), "An eligible table become ineligible after DDL")
}
