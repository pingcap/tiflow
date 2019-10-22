// Copyright 2019 PingCAP, Inc.
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

package cdc

import (
	"context"
	"sync"
	"time"

	"github.com/pingcap/tidb-cdc/pkg/util"

	"github.com/pingcap/check"
	"github.com/pingcap/tidb-cdc/cdc/kv"
)

type bufferSuite struct{}

var _ = check.Suite(&bufferSuite{})

func (bs *bufferSuite) TestCanAddAndReadEntriesInOrder(c *check.C) {
	b := MakeBuffer()
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		first, err := b.Get(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(first.KV.Ts, check.Equals, uint64(111))
		second, err := b.Get(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(second.Resolved.Timestamp, check.Equals, uint64(110))
		third, err := b.Get(ctx)
		c.Assert(err, check.IsNil)
		c.Assert(third.KV.Ts, check.Equals, uint64(112))
	}()

	err := b.AddKVEntry(ctx, &kv.RawKVEntry{Ts: 111})
	c.Assert(err, check.IsNil)
	err = b.AddResolved(ctx, util.Span{}, 110)
	c.Assert(err, check.IsNil)
	err = b.AddKVEntry(ctx, &kv.RawKVEntry{Ts: 112})
	c.Assert(err, check.IsNil)

	wg.Wait()
}

func (bs *bufferSuite) TestWaitsCanBeCanceled(c *check.C) {
	b := MakeBuffer()
	ctx := context.Background()

	timeout, cancel := context.WithTimeout(ctx, time.Millisecond)
	defer cancel()
	stopped := make(chan struct{})
	go func() {
		err := b.AddEntry(timeout, BufferEntry{KV: &kv.RawKVEntry{Ts: 111}})
		c.Assert(err, check.Equals, context.DeadlineExceeded)
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(10 * time.Millisecond):
		c.Fatal("AddEntry doesn't stop in time.")
	}
}
