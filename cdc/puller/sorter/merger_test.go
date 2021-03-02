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
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"golang.org/x/sync/errgroup"
)

type mockFlushTaskBuilder struct {
	task       *flushTask
	writer     backEndWriter
	totalCount int
}

func newMockFlushTaskBuilder() *mockFlushTaskBuilder {
	backEnd := newMemoryBackEnd()
	dealloc := func() error {
		return backEnd.free()
	}

	task := &flushTask{
		backend:       backEnd,
		tsLowerBound:  0,
		maxResolvedTs: 0,
		finished:      make(chan error, 2),
		dealloc:       dealloc,
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
		return runMerger(ctx, 1, inChan, outChan)
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
}

// TestMergerSingleHeapRetire simulates a situation where the resolved event is not the last event in a flushTask
func (s *sorterSuite) TestMergerSingleHeapRetire(c *check.C) {
	defer testleak.AfterTest(c)()

	ctx, cancel := context.WithTimeout(context.TODO(), time.Second*10)
	defer cancel()
	wg, ctx := errgroup.WithContext(ctx)
	inChan := make(chan *flushTask, 1024)
	outChan := make(chan *model.PolymorphicEvent, 1024)

	wg.Go(func() error {
		return runMerger(ctx, 1, inChan, outChan)
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
}
