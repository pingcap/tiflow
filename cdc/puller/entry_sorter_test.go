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
	"math/rand"
	"sync"
	"testing"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/ticdc/cdc/model"
)

type mockEntrySorterSuite struct{}

var _ = check.Suite(&mockEntrySorterSuite{})

func TestSuite(t *testing.T) {
	check.TestingT(t)
}

func (s *mockEntrySorterSuite) TestEntrySorter(c *check.C) {
	testCases := []struct {
		input      []*model.RawKVEntry
		resolvedTs uint64
		expect     []*model.RawKVEntry
	}{
		{
			input: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypeDelete}},
			resolvedTs: 0,
			expect: []*model.RawKVEntry{
				{CRTs: 0, OpType: model.OpTypeResolved}},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 5, OpType: model.OpTypePut}},
			resolvedTs: 3,
			expect: []*model.RawKVEntry{
				{CRTs: 1, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypeDelete},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 2, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypePut},
				{CRTs: 3, OpType: model.OpTypeResolved}},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 3,
			expect:     []*model.RawKVEntry{{CRTs: 3, OpType: model.OpTypeResolved}},
		},
		{
			input: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypePut}},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 4, OpType: model.OpTypeDelete},
				{CRTs: 5, OpType: model.OpTypePut},
				{CRTs: 6, OpType: model.OpTypeResolved}},
		},
		{
			input:      []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 6,
			expect: []*model.RawKVEntry{
				{CRTs: 6, OpType: model.OpTypeResolved}},
		},
		{
			input:      []*model.RawKVEntry{{CRTs: 7, OpType: model.OpTypeDelete}},
			resolvedTs: 8,
			expect: []*model.RawKVEntry{
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypeDelete},
				{CRTs: 7, OpType: model.OpTypePut},
				{CRTs: 8, OpType: model.OpTypeResolved}},
		},
		{
			input:      []*model.RawKVEntry{},
			resolvedTs: 15,
			expect: []*model.RawKVEntry{
				{CRTs: 15, OpType: model.OpTypeResolved}},
		},
	}
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()
	for _, tc := range testCases {
		for _, entry := range tc.input {
			es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(tc.resolvedTs))
		for i := 0; i < len(tc.expect); i++ {
			e := <-es.Output()
			c.Check(e.RawKV, check.DeepEquals, tc.expect[i])
		}
	}
	cancel()
	wg.Wait()
}

func (s *mockEntrySorterSuite) TestEntrySorterRandomly(c *check.C) {
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
	}()

	maxTs := uint64(1000000)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 1000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(resolvedTs) + rand.Int63n(int64(maxTs-resolvedTs))),
					OpType: opType,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(resolvedTs))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(maxTs))
	}()
	var lastTs uint64
	var resolvedTs uint64
	lastOpType := model.OpTypePut
	for entry := range es.Output() {
		c.Assert(entry.CRTs, check.GreaterEqual, lastTs)
		c.Assert(entry.CRTs, check.Greater, resolvedTs)
		if lastOpType == model.OpTypePut && entry.RawKV.OpType == model.OpTypeDelete {
			c.Assert(entry.CRTs, check.Greater, lastTs)
		}
		lastTs = entry.CRTs
		lastOpType = entry.RawKV.OpType
		if entry.RawKV.OpType == model.OpTypeResolved {
			resolvedTs = entry.CRTs
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}

func BenchmarkSorter(b *testing.B) {
	es := NewEntrySorter()
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := es.Run(ctx)
		if errors.Cause(err) != context.Canceled {
			panic(errors.Annotate(err, "unexpected error"))
		}
	}()

	maxTs := uint64(10000000)
	b.ResetTimer()
	wg.Add(1)
	go func() {
		defer wg.Done()
		for resolvedTs := uint64(1); resolvedTs <= maxTs; resolvedTs += 400 {
			var opType model.OpType
			if rand.Intn(2) == 0 {
				opType = model.OpTypePut
			} else {
				opType = model.OpTypeDelete
			}
			for i := 0; i < 100000; i++ {
				entry := &model.RawKVEntry{
					CRTs:   uint64(int64(resolvedTs) + rand.Int63n(1000)),
					OpType: opType,
				}
				es.AddEntry(ctx, model.NewPolymorphicEvent(entry))
			}
			es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(resolvedTs))
		}
		es.AddEntry(ctx, model.NewResolvedPolymorphicEvent(maxTs))
	}()
	var resolvedTs uint64
	for entry := range es.Output() {
		if entry.RawKV.OpType == model.OpTypeResolved {
			resolvedTs = entry.CRTs
		}
		if resolvedTs == maxTs {
			break
		}
	}
	cancel()
	wg.Wait()
}
