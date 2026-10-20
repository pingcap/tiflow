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
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pingcap/log"
	mockstorage "github.com/pingcap/tidb/br/pkg/mock/storage"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/redo/writer"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/redo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

type closeErrorLogWriter struct {
	closeErr error
}

func (w *closeErrorLogWriter) WriteEvents(context.Context, ...writer.RedoEvent) error {
	return nil
}

func (w *closeErrorLogWriter) FlushLog(context.Context) error {
	return nil
}

func (w *closeErrorLogWriter) Close() error {
	return w.closeErr
}

func TestMergeRunAndCloseError(t *testing.T) {
	runErr := errors.New("run failed")
	closeErr := errors.New("close failed")

	require.Same(t, runErr, mergeRunAndCloseError(runErr, closeErr))
	require.Same(t, closeErr, mergeRunAndCloseError(nil, closeErr))
	require.Same(t, closeErr, mergeRunAndCloseError(context.Canceled, closeErr))
	require.Nil(t, mergeRunAndCloseError(nil, context.Canceled))
	require.ErrorIs(t, mergeRunAndCloseError(context.Canceled, context.Canceled), context.Canceled)
}

func TestLogManagerCloseReturnsWriterError(t *testing.T) {
	closeErr := errors.New("close failed")
	m := &logManager{
		cfg: &writer.LogWriterConfig{
			ChangeFeedID: model.DefaultChangeFeedID("test-changefeed"),
		},
		writer:    &closeErrorLogWriter{closeErr: closeErr},
		logBuffer: chann.NewDrainableChann[cacheEvents](),
	}

	require.Same(t, closeErr, m.close())
}

func TestLogManagerRunReturnsUploadError(t *testing.T) {
	ctrl := gomock.NewController(t)
	extStorage := mockstorage.NewMockExternalStorage(ctrl)
	uploadErr := errors.New("upload failed")
	extStorage.EXPECT().WriteFile(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(uploadErr)

	originalInitExternalStorage := redo.InitExternalStorage
	redo.InitExternalStorage = func(context.Context, url.URL) (storage.ExternalStorage, error) {
		return extStorage, nil
	}
	t.Cleanup(func() {
		redo.InitExternalStorage = originalInitExternalStorage
	})

	cfg := &config.ConsistentConfig{
		Level:                 string(redo.ConsistentLevelEventual),
		MaxLogSize:            redo.DefaultMaxLogSize,
		Storage:               "s3://bucket/prefix",
		FlushIntervalInMs:     redo.MinFlushIntervalInMs,
		MetaFlushIntervalInMs: redo.MinFlushIntervalInMs,
		EncodingWorkerNum:     1,
		FlushWorkerNum:        1,
		FlushConcurrency:      1,
	}
	m := NewDMLManager(model.DefaultChangeFeedID("test-changefeed"), cfg)
	m.AddTable(1, 0)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	runErrCh := make(chan error, 1)
	go func() {
		runErrCh <- m.Run(ctx)
	}()

	require.NoError(t, m.EmitRowChangedEvents(ctx, 1, nil, &model.RowChangedEvent{
		Table:    &model.TableName{TableID: 1},
		CommitTs: 1,
	}))

	select {
	case err := <-runErrCh:
		require.ErrorIs(t, err, uploadErr)
	case <-time.After(5 * time.Second):
		t.Fatal("redo manager did not return the upload error")
	}
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
