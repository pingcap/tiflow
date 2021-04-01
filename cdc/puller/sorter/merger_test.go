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

package sorter

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"

	"github.com/pingcap/check"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
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
func (s *sorterSuite) TestMergerSingleHeap(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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
					c.Assert(event.CRTs, check.GreaterEqual, lastTs)
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					lastResolved = event.CRTs
				}
				if lastResolved >= 300001 {
					c.Assert(count, check.Equals, totalCount)
					cancel()
					return nil
				}
			}
		}
	})
	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(0))
}

// TestMergerSingleHeapRetire simulates a situation where the resolved event is not the last event in a flushTask
func (s *sorterSuite) TestMergerSingleHeapRetire(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	if err != nil {
		log.Panic("Could not enable failpoint", zap.Error(err))
	}

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					c.Assert(event.CRTs, check.GreaterEqual, lastTs)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					lastResolved = event.CRTs
				}
				if lastResolved >= 300001 {
					c.Assert(count, check.Equals, totalCount)
					cancel()
					return nil
				}
			}
		}
	})

	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(0))
}

// TestMergerSortDelay simulates a situation where merging takes a long time.
// Expects intermediate resolved events to be generated, so that the sink would not get stuck in a real life situation.
func (s *sorterSuite) TestMergerSortDelay(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	c.Assert(err, check.IsNil)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterMergeDelay", "sleep(5)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterMergeDelay")
	}()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					c.Assert(event.CRTs, check.GreaterEqual, lastTs)
					lastTs = event.CRTs
				case model.OpTypeResolved:
					c.Assert(event.CRTs, check.GreaterEqual, lastResolved)
					if !lastResolvedTime.IsZero() {
						c.Assert(time.Since(lastResolvedTime), check.LessEqual, 2*time.Second)
					}
					log.Debug("resolved event received", zap.Uint64("ts", event.CRTs))
					lastResolvedTime = time.Now()
					lastResolved = event.CRTs
				}
				if lastResolved >= 1000001 {
					c.Assert(count, check.Equals, totalCount)
					cancel()
					return nil
				}
			}
		}
	})

	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	close(inChan)
	mergerCleanUp(inChan)
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(0))
}

// TestMergerCancel simulates a situation where the merger is cancelled with pending data.
// Expects proper clean-up of the data.
func (s *sorterSuite) TestMergerCancel(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	c.Assert(err, check.IsNil)

	// enable the failpoint to simulate delays
	err = failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterMergeDelay", "sleep(10)")
	c.Assert(err, check.IsNil)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterMergeDelay")
	}()

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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
	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	close(inChan)
	mergerCleanUp(inChan)
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(0))
}

// TestMergerCancel simulates a situation where the merger is cancelled with pending data.
// Expects proper clean-up of the data.
func (s *sorterSuite) TestMergerCancelWithUnfinishedFlushTasks(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	c.Assert(err, check.IsNil)

	log.SetLevel(zapcore.DebugLevel)
	defer log.SetLevel(zapcore.InfoLevel)

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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

	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	close(inChan)
	mergerCleanUp(inChan)
	// Leaking one task is expected
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(1))
	atomic.StoreInt64(&backEndCounterForTest, 0)
}

// TestMergerCancel simulates a situation where the input channel is abruptly closed.
// There is expected to be NO fatal error.
func (s *sorterSuite) TestMergerCloseChannel(c *check.C) {
	defer testleak.AfterTest(c)()
	err := failpoint.Enable("github.com/pingcap/ticdc/cdc/puller/sorter/sorterDebug", "return(true)")
	c.Assert(err, check.IsNil)

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
		return runMerger(ctx, 1, inChan, outChan, func() {}, nil)
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
	c.Assert(wg.Wait(), check.ErrorMatches, ".*context canceled.*")
	mergerCleanUp(inChan)
	c.Assert(atomic.LoadInt64(&backEndCounterForTest), check.Equals, int64(0))
}

// TestTaskBufferBasic tests the basic functionality of TaskBuffer
func (s *sorterSuite) TestTaskBufferBasic(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	var bufLen int64
	taskBuf := newTaskBuffer(&bufLen)

	// run producer
	errg.Go(func() error {
		for i := 0; i < 10000; i++ {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			default:
			}

			var dummyTask flushTask
			taskBuf.put(&dummyTask)
		}

		taskBuf.setClosed()
		return nil
	})

	// run consumer
	errg.Go(func() error {
		for i := 0; i < 10001; i++ {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			default:
			}

			task, err := taskBuf.get(ctx)
			c.Assert(err, check.IsNil)

			if i == 10000 {
				c.Assert(task, check.IsNil)
				taskBuf.close()
				return nil
			}

			c.Assert(task, check.NotNil)
		}
		c.Fail() // unreachable
		return nil
	})

	c.Assert(errg.Wait(), check.IsNil)
	c.Assert(bufLen, check.Equals, int64(0))
}

// TestTaskBufferBasic tests the situation where the taskBuffer's consumer is
// first starved and then exit due to taskBuf shutdown.
func (s *sorterSuite) TestTaskBufferStarveAndClose(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), 10*time.Second)
	defer cancel()
	errg, ctx := errgroup.WithContext(ctx)
	var bufLen int64
	taskBuf := newTaskBuffer(&bufLen)

	// run producer
	errg.Go(func() error {
		for i := 0; i < 1000; i++ {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			default:
			}

			var dummyTask flushTask
			taskBuf.put(&dummyTask)
		}

		// starve the consumer
		time.Sleep(3 * time.Second)
		taskBuf.setClosed()
		return nil
	})

	// run consumer
	errg.Go(func() error {
		for i := 0; i < 1001; i++ {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			default:
			}

			task, err := taskBuf.get(ctx)
			if i < 1000 {
				c.Assert(task, check.NotNil)
				c.Assert(err, check.IsNil)
			} else {
				c.Assert(task, check.IsNil)
				c.Assert(err, check.IsNil)
				taskBuf.close()
				return nil
			}
		}
		c.Fail() // unreachable
		return nil
	})

	c.Assert(errg.Wait(), check.IsNil)
	c.Assert(bufLen, check.Equals, int64(0))
}
