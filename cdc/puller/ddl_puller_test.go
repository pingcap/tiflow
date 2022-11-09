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
	"sync/atomic"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	timodel "github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/util/codec"
	"github.com/pingcap/tiflow/cdc/entry"
	"github.com/pingcap/tiflow/cdc/entry/schema"
	"github.com/pingcap/tiflow/cdc/kv"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cdcContext "github.com/pingcap/tiflow/pkg/context"
	"github.com/pingcap/tiflow/pkg/filter"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/upstream"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

type mockPuller struct {
	t          *testing.T
	inCh       chan *model.RawKVEntry
	outCh      chan *model.RawKVEntry
	resolvedTs model.Ts
}

func (m *mockPuller) UnmarshalDDL(rawKV *model.RawKVEntry) (*timodel.Job, error) {
	return entry.ParseDDLJob(nil, rawKV, 0)
}

//nolint:unparam
func newMockPuller(t *testing.T, startTs model.Ts) *mockPuller {
	return &mockPuller{
		t:          t,
		inCh:       make(chan *model.RawKVEntry),
		outCh:      make(chan *model.RawKVEntry),
		resolvedTs: startTs - 1,
	}
}

func (m *mockPuller) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-m.inCh:
			m.outCh <- e
			atomic.StoreUint64(&m.resolvedTs, e.CRTs)
		}
	}
}

func (m *mockPuller) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&m.resolvedTs)
}

func (m *mockPuller) Output() <-chan *model.RawKVEntry {
	return m.outCh
}

func (m *mockPuller) Stats() Stats {
	return Stats{}
}

func (m *mockPuller) append(e *model.RawKVEntry) {
	m.inCh <- e
}

func (m *mockPuller) appendDDL(job *timodel.Job) {
	b, err := json.Marshal(job)
	require.Nil(m.t, err)
	ek := []byte("m")
	ek = codec.EncodeBytes(ek, []byte("DDLJobList"))
	ek = codec.EncodeUint(ek, uint64('l'))
	ek = codec.EncodeInt(ek, 1)
	m.append(&model.RawKVEntry{
		OpType:  model.OpTypePut,
		Key:     ek,
		Value:   b,
		StartTs: job.StartTS,
		CRTs:    job.BinlogInfo.FinishedTS,
	})
}

func (m *mockPuller) appendResolvedTs(ts model.Ts) {
	m.append(&model.RawKVEntry{
		OpType:  model.OpTypeResolved,
		CRTs:    ts,
		StartTs: ts,
	})
}

func newMockDDLJobPuller(t *testing.T, puller Puller, needSchemaSnap bool) (DDLJobPuller, *entry.SchemaTestHelper) {
	res := &ddlJobPullerImpl{
		puller: puller,
		outputCh: make(
			chan *model.DDLJobEntry,
			defaultPullerOutputChanSize),
		metricDiscardedDDLCounter: discardedDDLCounter.
			WithLabelValues("ddl", "test"),
	}
	var helper *entry.SchemaTestHelper
	if needSchemaSnap {
		helper = entry.NewSchemaTestHelper(t)
		kvStorage := helper.Storage()
		ts := helper.GetCurrentMeta().StartTS
		meta, err := kv.GetSnapshotMeta(kvStorage, ts)
		require.Nil(t, err)
		schemaSnap, err := schema.NewSingleSnapshotFromMeta(meta, ts, false)
		require.Nil(t, err)
		res.schemaSnapshot = schemaSnap
		res.kvStorage = kvStorage
	}
	return res, helper
}

func TestHandleRenameTable(t *testing.T) {
	startTs := uint64(10)
	mockPuller := newMockPuller(t, startTs)
	ddlJobPuller, helper := newMockDDLJobPuller(t, mockPuller, true)
	defer helper.Close()

	ddlJobPullerImpl := ddlJobPuller.(*ddlJobPullerImpl)
	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{
		"test1.t1",
		"test1.t2",
		"test1.t4",
		"test1.t66",
		"test1.t99",
		"test1.t100",

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
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t1(id int)")
		remainTables[0] = job.TableID
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t2(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t3(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test1.t5(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("rename table test1.t1 to test1.t11, test1.t3 to test1.t33, test1.t5 to test1.t55")

		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.NoError(t, err)
		require.False(t, skip)
		require.Len(t, job.BinlogInfo.MultipleTableInfos, 1)
		require.Equal(t, remainTables[0], job.BinlogInfo.MultipleTableInfos[0].ID)
	}

	{
		_ = helper.DDL2Job("create table test1.t6(id int)")
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
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t1(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t2(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table test2.t3(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("rename table test2.t1 to test2.t11, test2.t2 to test2.t22, test2.t3 to test2.t33")
		skip, err := ddlJobPullerImpl.handleRenameTables(job)
		require.NoError(t, err)
		require.True(t, skip)
	}

	// test uppercase db name
	{
		job := helper.DDL2Job("create database Test3")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		job = helper.DDL2Job("create table Test3.t1(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// skip this table
		job = helper.DDL2Job("create table Test3.t2(id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
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
		job := helper.DDL2Job("create table test1.t99 (id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// this ddl should be skipped
		job = helper.DDL2Job("create table test1.t1000 (id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// this ddl should be skipped
		job = helper.DDL2Job("create table test1.t888 (id int)")
		mockPuller.appendDDL(job)
		mockPuller.appendResolvedTs(job.BinlogInfo.FinishedTS + 1)
		waitResolvedTs(t, ddlJobPuller, job.BinlogInfo.FinishedTS+1)

		// since test1.99 in filter rule, we replicate it
		job = helper.DDL2Job("rename table test1.t99 to test1.t999")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		// since test1.t100 is in filter rule, replicate it
		job = helper.DDL2Job("rename table test1.t1000 to test1.t100")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.Error(t, err)
		require.True(t, skip)
		require.Contains(t, err.Error(), fmt.Sprintf("table's old name is not in filter rule, and its new name in filter rule "+
			"table id '%d', ddl query: [%s], it's an unexpected behavior, "+
			"if you want to replicate this table, please add its old name to filter rule.", job.TableID, job.Query))

		// since test1.t888 and test1.t777 are not in filter rule, skip it
		job = helper.DDL2Job("rename table test1.t888 to test1.t777")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)
	}
}

func TestHandleJob(t *testing.T) {
	startTs := uint64(10)
	mockPuller := newMockPuller(t, startTs)
	ddlJobPuller, helper := newMockDDLJobPuller(t, mockPuller, true)
	defer helper.Close()

	ddlJobPullerImpl := ddlJobPuller.(*ddlJobPullerImpl)
	cfg := config.GetDefaultReplicaConfig()
	cfg.Filter.Rules = []string{
		"test1.t1",
		"test1.t2",
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
		job := helper.DDL2Job("create table test1.t1(id int) partition by range(id) (partition p0 values less than (10))")
		skip, err := ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create table test1.t2(id int)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.False(t, skip)

		job = helper.DDL2Job("create table test1.t3(id int)")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		job = helper.DDL2Job("create table test1.t4(id int) partition by range(id) (partition p0 values less than (10))")
		skip, err = ddlJobPullerImpl.handleJob(job)
		require.NoError(t, err)
		require.True(t, skip)

		// make sure no schema not found error
		job = helper.DDL2Job("create table test3.t1(id int) partition by range(id) (partition p0 values less than (10))")
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

func TestDDLPuller(t *testing.T) {
	startTs := uint64(10)
	mockPuller := newMockPuller(t, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	up := upstream.NewUpstream4Test(nil)
	p, err := NewDDLPuller(
		ctx, ctx.ChangefeedVars().Info.Config, up, startTs, ctx.ChangefeedVars().ID)
	require.Nil(t, err)
	p.(*ddlPullerImpl).ddlJobPuller, _ = newMockDDLJobPuller(t, mockPuller, false)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := p.Run(ctx)
		require.True(t, errors.ErrorEqual(err, context.Canceled))
	}()
	defer wg.Wait()
	defer p.Close()

	// test initialize state
	resolvedTs, ddl := p.FrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	// test send resolvedTs
	mockPuller.appendResolvedTs(15)
	waitResolvedTsGrowing(t, p, 15)

	// test send ddl job out of order
	mockPuller.appendDDL(&timodel.Job{
		ID:         2,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 2, FinishedTS: 18},
		Query:      "create table test.t1(id int)",
	})
	mockPuller.appendDDL(&timodel.Job{
		ID:         1,
		Type:       timodel.ActionCreateTable,
		StartTS:    5,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 1, FinishedTS: 16},
		Query:      "create table t2(id int)",
	})
	resolvedTs, ddl = p.FrontDDL()
	require.Equal(t, resolvedTs, uint64(15))
	require.Nil(t, ddl)

	mockPuller.appendResolvedTs(20)
	waitResolvedTsGrowing(t, p, 16)
	resolvedTs, ddl = p.FrontDDL()
	require.Equal(t, resolvedTs, uint64(16))
	require.Equal(t, ddl.ID, int64(1))
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(16))
	require.Equal(t, ddl.ID, int64(1))

	// DDL could be processed with a delay, wait here for a pending DDL job is added
	waitResolvedTsGrowing(t, p, 18)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, uint64(18))
	require.Equal(t, ddl.ID, int64(2))

	// test add ddl job repeated
	mockPuller.appendDDL(&timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, FinishedTS: 25},
		Query:      "create table t3(id int)",
	})
	mockPuller.appendDDL(&timodel.Job{
		ID:         3,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateDone,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 4, FinishedTS: 25},
		Query:      "create table t3(id int)",
	})
	mockPuller.appendResolvedTs(30)
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

	mockPuller.appendDDL(&timodel.Job{
		ID:         5,
		Type:       timodel.ActionCreateTable,
		StartTS:    20,
		State:      timodel.JobStateCancelled,
		BinlogInfo: &timodel.HistoryInfo{SchemaVersion: 6, FinishedTS: 36},
		Query:      "create table t4(id int)",
	})
	mockPuller.appendResolvedTs(40)
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
	mockPuller := newMockPuller(t, startTs)
	ctx := cdcContext.NewBackendContext4Test(true)
	up := upstream.NewUpstream4Test(nil)
	p, err := NewDDLPuller(
		ctx, ctx.ChangefeedVars().Info.Config, up, startTs, ctx.ChangefeedVars().ID)
	require.Nil(t, err)

	mockClock := clock.NewMock()
	p.(*ddlPullerImpl).clock = mockClock

	p.(*ddlPullerImpl).ddlJobPuller, _ = newMockDDLJobPuller(t, mockPuller, false)
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
	resolvedTs, ddl := p.FrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)
	resolvedTs, ddl = p.PopFrontDDL()
	require.Equal(t, resolvedTs, startTs)
	require.Nil(t, ddl)

	mockPuller.appendResolvedTs(30)
	waitResolvedTsGrowing(t, p, 30)
	require.Equal(t, logs.Len(), 0)

	mockClock.Add(2 * ddlPullerStuckWarnDuration)
	for i := 0; i < 20; i++ {
		mockClock.Add(time.Second)
		if logs.Len() > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
		if i == 19 {
			t.Fatal("warning log not printed")
		}
	}

	mockPuller.appendResolvedTs(40)
	waitResolvedTsGrowing(t, p, 40)
}

// waitResolvedTsGrowing can wait the first DDL reaches targetTs or if no pending
// DDL, DDL resolved ts reaches targetTs.
func waitResolvedTsGrowing(t *testing.T, p DDLPuller, targetTs model.Ts) {
	err := retry.Do(context.Background(), func() error {
		resolvedTs, _ := p.FrontDDL()
		if resolvedTs < targetTs {
			return errors.New("resolvedTs < targetTs")
		}
		return nil
	}, retry.WithBackoffBaseDelay(20), retry.WithMaxTries(200))
	require.Nil(t, err)
}
