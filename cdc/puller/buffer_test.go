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
	"sync"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
)

type bufferSuite struct{}

var _ = check.Suite(&bufferSuite{})

func (bs *bufferSuite) TestCanAddAndReadEntriesInOrder(c *check.C) {
	b := makeChanBuffer()
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		first, err := b.Get(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(first.Val.CRTs, check.Equals, uint64(110))
		second, err := b.Get(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(second.Resolved.ResolvedTs, check.Equals, uint64(111))
	}()

	err := b.AddEntry(ctx, model.RegionFeedEvent{
		Val: &model.RawKVEntry{CRTs: 110},
	})
	c.Assert(err, check.IsNil)
	err = b.AddEntry(ctx, model.RegionFeedEvent{
		Resolved: &model.ResolvedSpan{
			Span:       regionspan.ComparableSpan{},
			ResolvedTs: 111,
		},
	})
	c.Assert(err, check.IsNil)

	wg.Wait()
}

func (bs *bufferSuite) TestWaitsCanBeCanceled(c *check.C) {
	b := makeChanBuffer()
	ctx := context.Background()

	timeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		for {
			err := b.AddEntry(timeout, model.RegionFeedEvent{
				Resolved: &model.ResolvedSpan{
					Span:       regionspan.ComparableSpan{},
					ResolvedTs: 111,
				},
			})
			if err == context.DeadlineExceeded {
				close(stopped)
				return
			}
			c.Assert(err, check.Equals, nil)
		}
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Millisecond):
		c.Fatal("AddEntry doesn't stop in time.")
	}
}

type memBufferSuite struct{}

var _ = check.Suite(&memBufferSuite{})

func (bs *memBufferSuite) TestMemBuffer(c *check.C) {
	limitter := NewBlurResourceLimmter(1024 * 1024)
	bf := makeMemBuffer(limitter)

	var err error
	var entries []model.RegionFeedEvent
	for {
		entry := model.RegionFeedEvent{
			Val: &model.RawKVEntry{
				Value: make([]byte, 1024),
			},
		}
		err = bf.AddEntry(context.Background(), entry)
		if err != nil {
			break
		}

		entries = append(entries, entry)
	}

	c.Assert(err, check.Equals, ErrReachLimit)
	num := float64(bf.mu.entries.Len())
	nearNum := 1024.0
	c.Assert(num >= nearNum*0.9, check.IsTrue)
	c.Assert(num <= nearNum*1.1, check.IsTrue)

	// Check can get back the entries.
	var getEntries []model.RegionFeedEvent
	for len(getEntries) < len(entries) {
		entry, err := bf.Get(context.Background())
		c.Assert(err, check.IsNil)
		getEntries = append(getEntries, entry)
	}
	c.Assert(getEntries, check.DeepEquals, entries)
}
