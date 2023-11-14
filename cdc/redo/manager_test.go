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
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

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
	maxTsMap := make(map[model.TableID]*model.Ts, numOfTables)
	startTs := uint64(100)
	for i := 0; i < numOfTables; i++ {
		tableID := model.TableID(i)
		tables = append(tables, tableID)
		ts := startTs
		maxTsMap[tableID] = &ts
		dmlMgr.AddTable(tableID, startTs)
	}

	// write rows
	maxRowCount := 100000
	wg := sync.WaitGroup{}
	b.ResetTimer()
	for _, tableID := range tables {
		wg.Add(1)
		go func(tableID model.TableID) {
			defer wg.Done()
			maxCommitTs := maxTsMap[tableID]
			rows := []*model.RowChangedEvent{}
			for i := 0; i < maxRowCount; i++ {
				if i%100 == 0 {
					// prepare new row change events
					b.StopTimer()
					*maxCommitTs += rand.Uint64() % 10
					rows = []*model.RowChangedEvent{
						{CommitTs: *maxCommitTs, Table: &model.TableName{TableID: tableID}},
						{CommitTs: *maxCommitTs, Table: &model.TableName{TableID: tableID}},
						{CommitTs: *maxCommitTs, Table: &model.TableName{TableID: tableID}},
					}

					b.StartTimer()
				}
				dmlMgr.EmitRowChangedEvents(ctx, tableID, nil, rows...)
				if i%100 == 0 {
					dmlMgr.UpdateResolvedTs(ctx, tableID, *maxCommitTs)
				}
			}
		}(tableID)
	}
	wg.Wait()

	// wait flushed
	for {
		ok := true
		for tableID, targetp := range maxTsMap {
			flushed := dmlMgr.GetResolvedTs(tableID)
			if flushed != *targetp {
				ok = false
				log.Info("", zap.Uint64("targetTs", *targetp),
					zap.Uint64("flushed", flushed),
					zap.Any("tableID", tableID))
			}
		}
		if ok {
			break
		}
		time.Sleep(time.Millisecond * 500)
	}
	cancel()

	require.ErrorIs(b, eg.Wait(), context.Canceled)
}
