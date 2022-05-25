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

package redo

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestConsistentConfig(t *testing.T) {
	t.Parallel()
	levelCases := []struct {
		level string
		valid bool
	}{
		{"none", true},
		{"eventual", true},
		{"NONE", false},
		{"", false},
	}
	for _, lc := range levelCases {
		require.Equal(t, lc.valid, IsValidConsistentLevel(lc.level))
	}

	levelEnableCases := []struct {
		level      string
		consistent bool
	}{
		{"invalid-level", false},
		{"none", false},
		{"eventual", true},
	}
	for _, lc := range levelEnableCases {
		require.Equal(t, lc.consistent, IsConsistentEnabled(lc.level))
	}

	storageCases := []struct {
		storage string
		valid   bool
	}{
		{"local", true},
		{"nfs", true},
		{"s3", true},
		{"blackhole", true},
		{"Local", false},
		{"", false},
	}
	for _, sc := range storageCases {
		require.Equal(t, sc.valid, IsValidConsistentStorage(sc.storage))
	}

	s3StorageCases := []struct {
		storage   string
		s3Enabled bool
	}{
		{"local", false},
		{"nfs", false},
		{"s3", true},
		{"blackhole", false},
	}
	for _, sc := range s3StorageCases {
		require.Equal(t, sc.s3Enabled, IsS3StorageEnabled(sc.storage))
	}
}

// TestLogManagerInProcessor tests how redo log manager is used in processor,
// where the redo log manager needs to handle DMLs and redo log meta data
func TestLogManagerInProcessor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	checkResovledTs := func(mgr LogManager, expectedRts uint64) {
		time.Sleep(time.Millisecond*200 + updateRtsInterval)
		resolvedTs := mgr.GetMinResolvedTs()
		require.Equal(t, expectedRts, resolvedTs)
	}

	cfg := &config.ConsistentConfig{
		Level:   string(consistentLevelEventual),
		Storage: "blackhole://",
	}
	errCh := make(chan error, 1)
	opts := &ManagerOptions{
		EnableBgRunner: true,
		ErrCh:          errCh,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	require.Nil(t, err)

	// check emit row changed events can move forward resolved ts
	tables := []model.TableID{53, 55, 57, 59}
	startTs := uint64(100)
	for _, tableID := range tables {
		logMgr.AddTable(tableID, startTs)
	}
	testCases := []struct {
		tableID model.TableID
		rows    []*model.RowChangedEvent
	}{
		{
			tableID: 53,
			rows: []*model.RowChangedEvent{
				{CommitTs: 120, Table: &model.TableName{TableID: 53}},
				{CommitTs: 125, Table: &model.TableName{TableID: 53}},
				{CommitTs: 130, Table: &model.TableName{TableID: 53}},
			},
		},
		{
			tableID: 55,
			rows: []*model.RowChangedEvent{
				{CommitTs: 130, Table: &model.TableName{TableID: 55}},
				{CommitTs: 135, Table: &model.TableName{TableID: 55}},
			},
		},
		{
			tableID: 57,
			rows: []*model.RowChangedEvent{
				{CommitTs: 130, Table: &model.TableName{TableID: 57}},
			},
		},
		{
			tableID: 59,
			rows: []*model.RowChangedEvent{
				{CommitTs: 128, Table: &model.TableName{TableID: 59}},
				{CommitTs: 130, Table: &model.TableName{TableID: 59}},
				{CommitTs: 133, Table: &model.TableName{TableID: 59}},
			},
		},
	}
	for _, tc := range testCases {
		err := logMgr.EmitRowChangedEvents(ctx, tc.tableID, tc.rows...)
		require.Nil(t, err)
	}
	checkResovledTs(logMgr, uint64(130))

	// check FlushLog can move forward the resolved ts when there is not row event.
	flushResolvedTs := uint64(150)
	for _, tableID := range tables {
		err := logMgr.FlushLog(ctx, tableID, flushResolvedTs)
		require.Nil(t, err)
	}
	checkResovledTs(logMgr, flushResolvedTs)

	// check remove table can work normally
	removeTable := tables[len(tables)-1]
	tables = tables[:len(tables)-1]
	logMgr.RemoveTable(removeTable)
	flushResolvedTs = uint64(200)
	for _, tableID := range tables {
		err := logMgr.FlushLog(ctx, tableID, flushResolvedTs)
		require.Nil(t, err)
	}
	checkResovledTs(logMgr, flushResolvedTs)

	err = logMgr.FlushResolvedAndCheckpointTs(ctx, 200 /*resolvedTs*/, 120 /*CheckPointTs*/)
	require.Nil(t, err)
}

// TestUpdateResolvedTsWithDelayedTable tests redo manager doesn't move resolved
// ts forward if one or more tables resolved ts are not returned from underlying
// writer, this secenario happens when there is no data or resolved ts of this
// table sent to redo log writer yet.
func TestUpdateResolvedTsWithDelayedTable(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.ConsistentConfig{
		Level:   string(ConsistentLevelEventual),
		Storage: "blackhole://",
	}
	errCh := make(chan error, 1)
	opts := &ManagerOptions{
		EnableBgRunner: true,
		ErrCh:          errCh,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	require.Nil(t, err)

	var (
		table53 = int64(53)
		table55 = int64(55)
		table57 = int64(57)

		startTs   = uint64(100)
		table53Ts = uint64(125)
		table55Ts = uint64(120)
		table57Ts = uint64(110)
	)
	tables := []model.TableID{table53, table55, table57}
	for _, tableID := range tables {
		logMgr.AddTable(tableID, startTs)
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		logMgr.bgWriteLog(ctx, errCh)
	}()

	// table 53 has new data, resolved-ts moves forward to 125
	rows := []*model.RowChangedEvent{
		{CommitTs: table53Ts, Table: &model.TableName{TableID: table53}},
		{CommitTs: table53Ts, Table: &model.TableName{TableID: table53}},
	}
	err = logMgr.EmitRowChangedEvents(ctx, table53, rows...)
	require.Nil(t, err)
	require.Eventually(t, func() bool {
		tsMap, err := logMgr.writer.GetCurrentResolvedTs(ctx, []int64{table53})
		require.Nil(t, err)
		ts, ok := tsMap[table53]
		return ok && ts == table53Ts
	}, time.Second, time.Millisecond*10)

	// table 55 has no data, but receives resolved-ts event and moves forward to 120
	err = logMgr.FlushLog(ctx, table55, table55Ts)
	require.Nil(t, err)

	// get min resolved ts should take each table into consideration
	err = logMgr.updateTableResolvedTs(ctx)
	require.Nil(t, err)
	require.Equal(t, startTs, logMgr.GetMinResolvedTs())

	// table 57 moves forward, update table resolved ts and check again
	logMgr.FlushLog(ctx, table57, table57Ts)
	err = logMgr.updateTableResolvedTs(ctx)
	require.Nil(t, err)
	require.Equal(t, table57Ts, logMgr.GetMinResolvedTs())

	cancel()
	wg.Wait()
}

// TestLogManagerInOwner tests how redo log manager is used in owner,
// where the redo log manager needs to handle DDL event only.
func TestLogManagerInOwner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &config.ConsistentConfig{
		Level:   string(consistentLevelEventual),
		Storage: "blackhole://",
	}
	opts := &ManagerOptions{
		EnableBgRunner: false,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	require.Nil(t, err)

	ddl := &model.DDLEvent{StartTs: 100, CommitTs: 120, Query: "CREATE TABLE `TEST.T1`"}
	err = logMgr.EmitDDLEvent(ctx, ddl)
	require.Nil(t, err)

	err = logMgr.writer.DeleteAllLogs(ctx)
	require.Nil(t, err)
}
