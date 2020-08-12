// Copyright 2020 PingCAP, Inc.
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
	"math"
	"sync/atomic"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"golang.org/x/sync/errgroup"
)

type rectifierSuite struct{}

var _ = check.Suite(&rectifierSuite{})

type mockSorter struct {
	outputCh chan *model.PolymorphicEvent
}

func newMockSorter() *mockSorter {
	return &mockSorter{
		outputCh: make(chan *model.PolymorphicEvent, 16),
	}
}

func (m *mockSorter) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (m *mockSorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	select {
	case <-ctx.Done():
		return
	case m.outputCh <- entry:
	}
}

func (m *mockSorter) Output() <-chan *model.PolymorphicEvent {
	return m.outputCh
}

func waitEntriesReceived(ctx context.Context, currentNum *int32, expectedNum int32) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			num := atomic.LoadInt32(currentNum)
			//log.Info("-", zap.Int32("-", num))
			if num >= expectedNum {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (s *rectifierSuite) TestRectifierSafeStop(c *check.C) {
	mockSorter := newMockSorter()
	r := NewRectifier(mockSorter, math.MaxUint64)
	ctx, cancel := context.WithCancel(context.Background())
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return r.Run(ctx)
	})
	entriesReceivedNum := int32(0)
	errg.Go(func() error {
		expected := []*model.PolymorphicEvent{
			{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
			{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
			{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		}
		i := int32(0)
		for e := range r.Output() {
			c.Assert(e, check.DeepEquals, expected[i])
			i++
			atomic.StoreInt32(&entriesReceivedNum, i)
		}

		return nil
	})
	entries := []*model.PolymorphicEvent{
		{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
	}
	for _, e := range entries {
		mockSorter.AddEntry(ctx, e)
	}
	waitEntriesReceived(ctx, &entriesReceivedNum, 6)
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusWorking)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(5))
	r.SafeStop()
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusStopping)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(5))

	entries = []*model.PolymorphicEvent{
		{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
	}
	for _, e := range entries {
		mockSorter.AddEntry(ctx, e)
	}
	waitEntriesReceived(ctx, &entriesReceivedNum, 9)
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusStopped)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(9))
	r.SafeStop()
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusStopped)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(9))
	cancel()
	c.Assert(errg.Wait(), check.IsNil)
}

func (s *rectifierSuite) TestRectifierFinished(c *check.C) {
	mockSorter := newMockSorter()
	r := NewRectifier(mockSorter, 7)
	ctx, cancel := context.WithCancel(context.Background())
	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		return r.Run(ctx)
	})
	entriesReceivedNum := int32(0)
	errg.Go(func() error {
		expected := []*model.PolymorphicEvent{
			{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
			{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
			{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
			{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved, CRTs: 7}},
		}
		i := int32(0)
		for e := range r.Output() {
			c.Assert(e, check.DeepEquals, expected[i])
			i++
			atomic.StoreInt32(&entriesReceivedNum, i)
		}

		return nil
	})
	entries := []*model.PolymorphicEvent{
		{CRTs: 1, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 2, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 3, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 4, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 5, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 6, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 7, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 8, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 9, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
		{CRTs: 10, RawKV: &model.RawKVEntry{OpType: model.OpTypePut}},
		{CRTs: 11, RawKV: &model.RawKVEntry{OpType: model.OpTypeResolved}},
	}
	for _, e := range entries {
		mockSorter.AddEntry(ctx, e)
	}
	waitEntriesReceived(ctx, &entriesReceivedNum, 8)
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusFinished)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(7))
	r.SafeStop()
	c.Assert(r.GetStatus(), check.Equals, model.SorterStatusFinished)
	c.Assert(r.GetMaxResolvedTs(), check.Equals, model.Ts(7))

	cancel()
	c.Assert(errg.Wait(), check.IsNil)
}
