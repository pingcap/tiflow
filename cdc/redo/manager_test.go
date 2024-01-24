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

package redo

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

func checkResolvedTs(t *testing.T, mgr *logManager, expectedRts uint64) {
	time.Sleep(time.Duration(redo.MinFlushIntervalInMs+200) * time.Millisecond)
	resolvedTs := uint64(math.MaxUint64)
	mgr.rtsMap.Range(func(span tablepb.Span, value any) bool {
		v, ok := value.(*statefulRts)
		require.True(t, ok)
		ts := v.getFlushed()
		if ts < resolvedTs {
			resolvedTs = ts
		}
		return true
	})
	require.Equal(t, expectedRts, resolvedTs)
}

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
		require.Equal(t, lc.valid, redo.IsValidConsistentLevel(lc.level))
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
		require.Equal(t, lc.consistent, redo.IsConsistentEnabled(lc.level))
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
		require.Equal(t, sc.valid, redo.IsValidConsistentStorage(sc.storage))
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
		require.Equal(t, sc.s3Enabled, redo.IsExternalStorage(sc.storage))
	}
}

// TestLogManagerInProcessor tests how redo log manager is used in processor.
func TestLogManagerInProcessor(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWriteDMLs := func(storage string, useFileBackend bool) {
		ctx, cancel := context.WithCancel(ctx)
		cfg := &config.ConsistentConfig{
			Level:                 string(redo.ConsistentLevelEventual),
			MaxLogSize:            redo.DefaultMaxLogSize,
			Storage:               storage,
			FlushIntervalInMs:     redo.MinFlushIntervalInMs,
			MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
			EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
			FlushWorkerNum:        redo.DefaultFlushWorkerNum,
			UseFileBackend:        useFileBackend,
		}
		dmlMgr := NewDMLManager(model.DefaultChangeFeedID("test"), cfg)
		var eg errgroup.Group
		eg.Go(func() error {
			return dmlMgr.Run(ctx)
		})
		// check emit row changed events can move forward resolved ts
		spans := []tablepb.Span{
			spanz.TableIDToComparableSpan(53),
			spanz.TableIDToComparableSpan(55),
			spanz.TableIDToComparableSpan(57),
			spanz.TableIDToComparableSpan(59),
		}

		startTs := uint64(100)
		for _, span := range spans {
			dmlMgr.AddTable(span, startTs)
		}
		tableInfo := &model.TableInfo{
			TableName: model.TableName{Schema: "test", Table: "t"},
		}
		testCases := []struct {
			span tablepb.Span
			rows []*model.RowChangedEvent
		}{
			{
				span: spanz.TableIDToComparableSpan(53),
				rows: []*model.RowChangedEvent{
					{CommitTs: 120, PhysicalTableID: 53, TableInfo: tableInfo},
					{CommitTs: 125, PhysicalTableID: 53, TableInfo: tableInfo},
					{CommitTs: 130, PhysicalTableID: 53, TableInfo: tableInfo},
				},
			},
			{
				span: spanz.TableIDToComparableSpan(55),
				rows: []*model.RowChangedEvent{
					{CommitTs: 130, PhysicalTableID: 55, TableInfo: tableInfo},
					{CommitTs: 135, PhysicalTableID: 55, TableInfo: tableInfo},
				},
			},
			{
				span: spanz.TableIDToComparableSpan(57),
				rows: []*model.RowChangedEvent{
					{CommitTs: 130, PhysicalTableID: 57, TableInfo: tableInfo},
				},
			},
			{
				span: spanz.TableIDToComparableSpan(59),
				rows: []*model.RowChangedEvent{
					{CommitTs: 128, PhysicalTableID: 59, TableInfo: tableInfo},
					{CommitTs: 130, PhysicalTableID: 59, TableInfo: tableInfo},
					{CommitTs: 133, PhysicalTableID: 59, TableInfo: tableInfo},
				},
			},
		}
		for _, tc := range testCases {
			err := dmlMgr.EmitRowChangedEvents(ctx, tc.span, nil, tc.rows...)
			require.NoError(t, err)
		}

		// check UpdateResolvedTs can move forward the resolved ts when there is not row event.
		flushResolvedTs := uint64(150)
		for _, span := range spans {
			checkResolvedTs(t, dmlMgr.logManager, startTs)
			err := dmlMgr.UpdateResolvedTs(ctx, span, flushResolvedTs)
			require.NoError(t, err)
		}
		checkResolvedTs(t, dmlMgr.logManager, flushResolvedTs)

		// check remove table can work normally
		removeTable := spans[len(spans)-1]
		spans = spans[:len(spans)-1]
		dmlMgr.RemoveTable(removeTable)
		flushResolvedTs = uint64(200)
		for _, span := range spans {
			err := dmlMgr.UpdateResolvedTs(ctx, span, flushResolvedTs)
			require.NoError(t, err)
		}
		checkResolvedTs(t, dmlMgr.logManager, flushResolvedTs)

		cancel()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	}

	testWriteDMLs("blackhole://", true)
	storages := []string{
		fmt.Sprintf("file://%s", t.TempDir()),
		fmt.Sprintf("nfs://%s", t.TempDir()),
	}
	for _, storage := range storages {
		testWriteDMLs(storage, true)
		testWriteDMLs(storage, false)
	}
}

// TestLogManagerInOwner tests how redo log manager is used in owner,
// where the redo log manager needs to handle DDL event only.
func TestLogManagerInOwner(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	testWriteDDLs := func(storage string, useFileBackend bool) {
		ctx, cancel := context.WithCancel(ctx)
		cfg := &config.ConsistentConfig{
			Level:                 string(redo.ConsistentLevelEventual),
			MaxLogSize:            redo.DefaultMaxLogSize,
			Storage:               storage,
			FlushIntervalInMs:     redo.MinFlushIntervalInMs,
			MetaFlushIntervalInMs: redo.DefaultMetaFlushIntervalInMs,
			EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
			FlushWorkerNum:        redo.DefaultFlushWorkerNum,
			UseFileBackend:        useFileBackend,
		}
		startTs := model.Ts(10)
		ddlMgr := NewDDLManager(model.DefaultChangeFeedID("test"), cfg, startTs)

		var eg errgroup.Group
		eg.Go(func() error {
			return ddlMgr.Run(ctx)
		})

		require.Equal(t, startTs, ddlMgr.GetResolvedTs())
		ddl := &model.DDLEvent{StartTs: 100, CommitTs: 120, Query: "CREATE TABLE `TEST.T1`"}
		err := ddlMgr.EmitDDLEvent(ctx, ddl)
		require.NoError(t, err)
		require.Equal(t, startTs, ddlMgr.GetResolvedTs())

		ddlMgr.UpdateResolvedTs(ctx, ddl.CommitTs)
		checkResolvedTs(t, ddlMgr.logManager, ddl.CommitTs)

		cancel()
		require.ErrorIs(t, eg.Wait(), context.Canceled)
	}

	testWriteDDLs("blackhole://", true)
	storages := []string{
		fmt.Sprintf("file://%s", t.TempDir()),
		fmt.Sprintf("nfs://%s", t.TempDir()),
	}
	for _, storage := range storages {
		testWriteDDLs(storage, true)
		testWriteDDLs(storage, false)
	}
}

// TestManagerError tests whether internal error in bgUpdateLog could be managed correctly.
func TestLogManagerError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               "blackhole-invalid://",
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
	}
	logMgr := NewDMLManager(model.DefaultChangeFeedID("test"), cfg)
	var eg errgroup.Group
	eg.Go(func() error {
		return logMgr.Run(ctx)
	})

	tableInfo := &model.TableInfo{
		TableName: model.TableName{Schema: "test", Table: "t"},
	}
	testCases := []struct {
		span tablepb.Span
		rows []writer.RedoEvent
	}{
		{
			span: spanz.TableIDToComparableSpan(53),
			rows: []writer.RedoEvent{
				&model.RowChangedEvent{CommitTs: 120, PhysicalTableID: 53, TableInfo: tableInfo},
				&model.RowChangedEvent{CommitTs: 125, PhysicalTableID: 53, TableInfo: tableInfo},
				&model.RowChangedEvent{CommitTs: 130, PhysicalTableID: 53, TableInfo: tableInfo},
			},
		},
	}
	for _, tc := range testCases {
		err := logMgr.emitRedoEvents(ctx, tc.span, nil, tc.rows...)
		require.NoError(t, err)
	}

	err := eg.Wait()
	require.Regexp(t, ".*invalid black hole writer.*", err)
	require.Regexp(t, ".*WriteLog.*", err)
}

func BenchmarkBlackhole(b *testing.B) {
	runBenchTest(b, "blackhole://", false)
}

func BenchmarkMemoryWriter(b *testing.B) {
	storage := fmt.Sprintf("file://%s", b.TempDir())
	runBenchTest(b, storage, false)
}

func BenchmarkFileWriter(b *testing.B) {
	storage := fmt.Sprintf("file://%s", b.TempDir())
	runBenchTest(b, storage, true)
}

func runBenchTest(b *testing.B, storage string, useFileBackend bool) {
	ctx, cancel := context.WithCancel(context.Background())
	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               storage,
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     redo.DefaultEncodingWorkerNum,
		FlushWorkerNum:        redo.DefaultFlushWorkerNum,
		UseFileBackend:        useFileBackend,
	}
	dmlMgr := NewDMLManager(model.DefaultChangeFeedID("test"), cfg)
	var eg errgroup.Group
	eg.Go(func() error {
		return dmlMgr.Run(ctx)
	})

	// Init tables
	numOfTables := 200
	tables := make([]model.TableID, 0, numOfTables)
	maxTsMap := spanz.NewHashMap[*model.Ts]()
	startTs := uint64(100)
	for i := 0; i < numOfTables; i++ {
		tableID := model.TableID(i)
		tables = append(tables, tableID)
		span := spanz.TableIDToComparableSpan(tableID)
		ts := startTs
		maxTsMap.ReplaceOrInsert(span, &ts)
		dmlMgr.AddTable(span, startTs)
	}

	// write rows
	maxRowCount := 100000
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for _, tableID := range tables {
		wg.Add(1)
		tableInfo := &model.TableInfo{
			TableName: model.TableName{Schema: "test", Table: fmt.Sprintf("t_%d", tableID)},
		}
		go func(span tablepb.Span) {
			defer wg.Done()
			maxCommitTs := maxTsMap.GetV(span)
			var rows []*model.RowChangedEvent
			for i := 0; i < maxRowCount; i++ {
				if i%100 == 0 {
					// prepare new row change events
					b.StopTimer()
					*maxCommitTs += rand.Uint64() % 10
					rows = []*model.RowChangedEvent{
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
						{CommitTs: *maxCommitTs, PhysicalTableID: span.TableID, TableInfo: tableInfo},
					}

					b.StartTimer()
				}
				dmlMgr.EmitRowChangedEvents(ctx, span, nil, rows...)
				if i%100 == 0 {
					dmlMgr.UpdateResolvedTs(ctx, span, *maxCommitTs)
				}
			}
		}(spanz.TableIDToComparableSpan(tableID))
	}
	wg.Wait()

	// wait flushed
	for {
		ok := true
		maxTsMap.Range(func(span tablepb.Span, targetp *uint64) bool {
			flushed := dmlMgr.GetResolvedTs(span)
			if flushed != *targetp {
				ok = false
				log.Info("", zap.Uint64("targetTs", *targetp),
					zap.Uint64("flushed", flushed),
					zap.Any("tableID", span.TableID))
				return false
			}
			return true
		})
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	cancel()

	require.ErrorIs(b, eg.Wait(), context.Canceled)
}
