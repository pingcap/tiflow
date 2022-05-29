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
<<<<<<< HEAD
=======
	"sync"
	"sync/atomic"
>>>>>>> 9b29eef20 (redo(ticdc): fix a bug that flush log executed before writing logs (#5621))
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

// TestWriteLogFlushLogSequence tests flush log must be executed after table's
// log has been written to writer.
func TestWriteLogFlushLogSequence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.ConsistentConfig{
		Level:   string(ConsistentLevelEventual),
		Storage: "blackhole://",
	}
	errCh := make(chan error, 1)
	opts := &ManagerOptions{
		EnableBgRunner: false,
		ErrCh:          errCh,
	}
	logMgr, err := NewManager(ctx, cfg, opts)
	require.Nil(t, err)

	var (
		wg sync.WaitGroup

		tableID    = int64(53)
		startTs    = uint64(100)
		resolvedTs = uint64(150)
	)
	logMgr.AddTable(tableID, startTs)

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			return
		case err := <-errCh:
			require.Nil(t, err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		// FlushLog blocks until bgWriteLog consumes data and close callback chan.
		err := logMgr.FlushLog(ctx, tableID, resolvedTs)
		require.Nil(t, err)
	}()

	// Sleep a short time to ensure `logMgr.FlushLog` is called
	time.Sleep(time.Millisecond * 100)
	// FlushLog is still ongoing
	require.Equal(t, int64(1), atomic.LoadInt64(&logMgr.flushing))
	err = logMgr.updateTableResolvedTs(ctx)
	require.Nil(t, err)
	require.Equal(t, startTs, logMgr.GetMinResolvedTs())

	wg.Add(1)
	go func() {
		defer wg.Done()
		logMgr.bgWriteLog(ctx, errCh)
	}()

	require.Eventually(t, func() bool {
		err = logMgr.updateTableResolvedTs(ctx)
		require.Nil(t, err)
		return logMgr.GetMinResolvedTs() == resolvedTs
	}, time.Second, time.Millisecond*20)

	cancel()
	wg.Wait()
}
