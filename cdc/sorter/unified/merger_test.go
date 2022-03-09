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

package unified

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/sync/errgroup"
)

type mockFlushTaskBuilder struct {
	task       *flushTask
	writer     backEndWriter
	totalCount int
}

var backEndCounterForTest int64

func newMockFlushTaskBuilder() *mockFlushTaskBuilder {
	backEnd := newMemoryBackEnd()
	atomic.AddInt64(&backEndCounterForTest, 1)

	task := &flushTask{
		backend:       backEnd,
		tsLowerBound:  0,
		maxResolvedTs: 0,
		finished:      make(chan error, 2),
	}

	task.dealloc = func() error {
		if task.backend != nil {
			atomic.AddInt64(&backEndCounterForTest, -1)
			task.backend = nil
			return backEnd.free()
		}
		return nil
	}

	writer, _ := backEnd.writer()

	return &mockFlushTaskBuilder{
		task:   task,
		writer: writer,
	}
}

func (b *mockFlushTaskBuilder) generateRowChanges(tsRangeBegin, tsRangeEnd uint64, count int) *mockFlushTaskBuilder {
	if b.task.tsLowerBound == 0 {
		b.task.tsLowerBound = tsRangeBegin
	}
	density := float64(tsRangeEnd-tsRangeBegin) / float64(count)
	for fTs := float64(tsRangeBegin); fTs < float64(tsRangeEnd); fTs += density {
		ts := uint64(fTs)
		kvEntry := generateMockRawKV(ts)
		_ = b.writer.writeNext(model.NewPolymorphicEvent(kvEntry))
		b.totalCount++
	}
	return b
}

func (b *mockFlushTaskBuilder) addResolved(ts uint64) *mockFlushTaskBuilder {
	_ = b.writer.writeNext(model.NewResolvedPolymorphicEvent(0, ts))
	b.task.maxResolvedTs = ts
	return b
}

func (b *mockFlushTaskBuilder) build() *flushTask {
	_ = b.writer.flushAndClose()
	return b.task
}

// TestMergerSingleHeap simulates a situation where there is only one data stream
// It tests the most basic scenario.
func TestMergerSingleHeap(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	totalCount := 0
	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task2 := builder.generateRowChanges(100002, 200000, 2048).addResolved(200001).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task3 := builder.generateRowChanges(200002, 300000, 2048).addResolved(300001).build()
	totalCount += builder.totalCount

	wg.Go(func() error {
		inChan <- task1
		close(task1.finished)
		inChan <- task2
		close(task2.finished)
		inChan <- task3
		close(task3.finished)

		return nil
	})

	wg.Go(func() error {
		count := 0
		lastTs := uint64(0)
		lastResolved := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-outChan:
				switch event.RawKV.OpType {
				case model.OpTypePut:
					count++
					require.GreaterOrEqual(t, event.CRTs, lastTs)
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					lastResolved = event.CRTs
				}
				if lastResolved >= 300001 {
					require.Equal(t, totalCount, count)
					cancel()
					return nil
				}
			}
		}
	})
	require.Regexp(t, ".*context canceled.*", wg.Wait())
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}

// TestMergerSingleHeapRetire simulates a situation where the resolved event is not the last event in a flushTask
func TestMergerSingleHeapRetire(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	totalCount := 0
	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task2 := builder.generateRowChanges(100002, 200000, 2048).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task3 := builder.generateRowChanges(200002, 300000, 2048).addResolved(300001).build()
	totalCount += builder.totalCount

	wg.Go(func() error {
		inChan <- task1
		close(task1.finished)
		inChan <- task2
		close(task2.finished)
		inChan <- task3
		close(task3.finished)

		return nil
	})

	wg.Go(func() error {
		count := 0
		lastTs := uint64(0)
		lastResolved := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-outChan:
				switch event.RawKV.OpType {
				case model.OpTypePut:
					count++
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					require.GreaterOrEqual(t, event.CRTs, lastTs)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					lastResolved = event.CRTs
				}
				if lastResolved >= 300001 {
					require.Equal(t, totalCount, count)
					cancel()
					return nil
				}
			}
		}
	})

	require.Regexp(t, ".*context canceled.*", wg.Wait())
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}

// TestMergerSortDelay simulates a situation where merging takes a long time.
// Expects intermediate resolved events to be generated, so that the sink would not get stuck in a real life situation.
func TestMergerSortDelay(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	require.Nil(t, err)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterMergeDelay", "sleep(5)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterMergeDelay")
	}()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	totalCount := 0
	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 1000000, 1024).addResolved(1000001).build()
	totalCount += builder.totalCount

	wg.Go(func() error {
		inChan <- task1
		close(task1.finished)
		return nil
	})

	wg.Go(func() error {
		var (
			count            int
			lastTs           uint64
			lastResolved     uint64
			lastResolvedTime time.Time
		)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-outChan:
				switch event.RawKV.OpType {
				case model.OpTypePut:
					count++
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					require.GreaterOrEqual(t, event.CRTs, lastTs)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					if !lastResolvedTime.IsZero() {
						require.LessOrEqual(t, time.Since(lastResolvedTime), 2*time.Second)
					}
					log.Debug("resolved event received", zap.Uint64("ts", event.CRTs))
					lastResolvedTime = time.Now()
					lastResolved = event.CRTs
				}
				if lastResolved >= 1000001 {
					require.Equal(t, totalCount, count)
					cancel()
					return nil
				}
			}
		}
	})

	require.Regexp(t, ".*context canceled.*", wg.Wait())
	close(inChan)
	mergerCleanUp(inChan)
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}

// TestMergerCancel simulates a situation where the merger is cancelled with pending data.
// Expects proper clean-up of the data.
func TestMergerCancel(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	require.Nil(t, err)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterMergeDelay", "sleep(10)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterMergeDelay")
	}()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()
	builder = newMockFlushTaskBuilder()
	task2 := builder.generateRowChanges(100002, 200000, 2048).addResolved(200001).build()
	builder = newMockFlushTaskBuilder()
	task3 := builder.generateRowChanges(200002, 300000, 2048).addResolved(300001).build()

	wg.Go(func() error {
		inChan <- task1
		close(task1.finished)
		inChan <- task2
		close(task2.finished)
		inChan <- task3
		close(task3.finished)
		return nil
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-outChan:
				// We just drain the data here. We don't care about it.
			}
		}
	})

	time.Sleep(5 * time.Second)
	cancel()
	require.Regexp(t, ".*context canceled.*", wg.Wait())
	close(inChan)
	mergerCleanUp(inChan)
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}

// TestMergerCancel simulates a situation where the merger is cancelled with pending data.
// Expects proper clean-up of the data.
func TestMergerCancelWithUnfinishedFlushTasks(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	require.Nil(t, err)

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()
	builder = newMockFlushTaskBuilder()
	task2 := builder.generateRowChanges(100002, 200000, 2048).addResolved(200001).build()
	builder = newMockFlushTaskBuilder()
	task3 := builder.generateRowChanges(200002, 300000, 2048).addResolved(300001).build()

	wg.Go(func() error {
		inChan <- task1
		inChan <- task2
		inChan <- task3
		close(task2.finished)
		close(task1.finished)
		time.Sleep(1 * time.Second)
		cancel()
		return nil
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-outChan:
				// We just drain the data here. We don't care about it.
			}
		}
	})

	require.Regexp(t, ".*context canceled.*", wg.Wait())
	close(inChan)
	mergerCleanUp(inChan)
	// Leaking one task is expected
	require.Equal(t, int64(1), atomic.LoadInt64(&backEndCounterForTest))
	atomic.StoreInt64(&backEndCounterForTest, 0)
}

// TestMergerCancel simulates a situation where the input channel is abruptly closed.
// There is expected to be NO fatal error.
func TestMergerCloseChannel(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	require.Nil(t, err)

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*15)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()

	inChan <- task1
	close(task1.finished)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	wg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-outChan:
				// We just drain the data here. We don't care about it.
			}
		}
	})

	time.Sleep(5 * time.Second)
	close(inChan)
	time.Sleep(5 * time.Second)
	cancel()
	require.Regexp(t, ".*context canceled.*", wg.Wait())
	mergerCleanUp(inChan)
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}

// TestMergerOutputBlocked simulates a situation where the output channel is blocked for
// a significant period of time.
func TestMergerOutputBlocked(t *testing.T) {
	err := failpoint.Enable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug", "return(true)")
	require.Nil(t, err)
	defer failpoint.Disable("github.com/pingcap/tiflow/cdc/sorter/unified/sorterDebug") //nolint:errcheck

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*25)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	// use unbuffered channel to make sure that the input has been processed
	inChan := make(chan *flushTask)
	// make a small channel to test blocking
	outChan := make(chan *model.PolymorphicEvent, 1)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {})
	})

	totalCount := 0
	builder := newMockFlushTaskBuilder()
	task1 := builder.generateRowChanges(1000, 100000, 2048).addResolved(100001).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task2 := builder.generateRowChanges(100002, 200000, 2048).addResolved(200001).build()
	totalCount += builder.totalCount
	builder = newMockFlushTaskBuilder()
	task3 := builder.generateRowChanges(200002, 300000, 2048).addResolved(300001).build()
	totalCount += builder.totalCount

	wg.Go(func() error {
		inChan <- task1
		close(task1.finished)
		inChan <- task2
		close(task2.finished)
		inChan <- task3
		close(task3.finished)

		return nil
	})

	wg.Go(func() error {
		time.Sleep(10 * time.Second)
		count := 0
		lastTs := uint64(0)
		lastResolved := uint64(0)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event := <-outChan:
				switch event.RawKV.OpType {
				case model.OpTypePut:
					count++
					require.GreaterOrEqual(t, event.CRTs, lastTs)
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					require.GreaterOrEqual(t, event.CRTs, lastResolved)
					lastResolved = event.CRTs
				}
				if lastResolved >= 300001 {
					require.Equal(t, totalCount, count)
					cancel()
					return nil
				}
			}
		}
	})
	require.Regexp(t, ".*context canceled.*", wg.Wait())
	require.Equal(t, int64(0), atomic.LoadInt64(&backEndCounterForTest))
}
