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

package puller

import (
	"context"
	"errors"
	"fmt"

	"github.com/pingcap/check"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

type CollectRawTxnsSuite struct{}

type mockTracker struct {
	forwarded []bool
	cur       int
}

func (t *mockTracker) Forward(span util.Span, ts uint64) bool {
	if len(t.forwarded) > 0 {
		r := t.forwarded[t.cur]
		t.cur++
		return r
	}
	return true
}

func (t *mockTracker) Frontier() uint64 {
	return 1
}

var _ = check.Suite(&CollectRawTxnsSuite{})

func (cs *CollectRawTxnsSuite) TestShouldOutputTxnsInOrder(c *check.C) {
	var entries []model.KvOrResolved
	var startTs uint64 = 1024
	var i uint64
	for i = 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			e := model.KvOrResolved{
				KV: &model.RawKVEntry{
					OpType: model.OpTypePut,
					Key:    []byte(fmt.Sprintf("key-%d-%d", i, j)),
					Ts:     startTs + i,
				},
			}
			entries = append(entries, e)
		}
	}
	// Only add resolved entry for the first 2 transaction
	for i = 0; i < 2; i++ {
		e := model.KvOrResolved{
			Resolved: &model.ResolvedSpan{Timestamp: startTs + i},
		}
		entries = append(entries, e)
	}

	nRead := 0
	input := func(ctx context.Context) (model.KvOrResolved, error) {
		if nRead >= len(entries) {
			return model.KvOrResolved{}, errors.New("End")
		}
		e := entries[nRead]
		nRead++
		return e, nil
	}

	var rawTxns []model.RawTxn
	output := func(ctx context.Context, txn model.RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	err := collectRawTxns(ctx, input, output, &mockTracker{})
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, 2)
	c.Assert(rawTxns[0].Ts, check.Equals, startTs)
	for i, e := range rawTxns[0].Entries {
		c.Assert(e.Ts, check.Equals, startTs)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-0-%d", i))
	}
	c.Assert(rawTxns[1].Ts, check.Equals, startTs+1)
	for i, e := range rawTxns[1].Entries {
		c.Assert(e.Ts, check.Equals, startTs+1)
		c.Assert(string(e.Key), check.Equals, fmt.Sprintf("key-1-%d", i))
	}
}

func (cs *CollectRawTxnsSuite) TestShouldConsiderSpanResolvedTs(c *check.C) {
	var entries []model.KvOrResolved
	for _, v := range []struct {
		key          []byte
		ts           uint64
		isResolvedTs bool
	}{
		{key: []byte("key1-1"), ts: 1},
		{key: []byte("key2-1"), ts: 2},
		{key: []byte("key1-2"), ts: 1},
		{key: []byte("key1-3"), ts: 1},
		{ts: 1, isResolvedTs: true},
		{ts: 2, isResolvedTs: true},
		{key: []byte("key2-1"), ts: 2},
		{ts: 1, isResolvedTs: true},
	} {
		var e model.KvOrResolved
		if v.isResolvedTs {
			e = model.KvOrResolved{
				Resolved: &model.ResolvedSpan{Timestamp: v.ts},
			}
		} else {
			e = model.KvOrResolved{
				KV: &model.RawKVEntry{
					OpType: model.OpTypePut,
					Key:    v.key,
					Ts:     v.ts,
				},
			}
		}
		entries = append(entries, e)
	}

	cursor := 0
	input := func(ctx context.Context) (model.KvOrResolved, error) {
		if cursor >= len(entries) {
			return model.KvOrResolved{}, errors.New("End")
		}
		e := entries[cursor]
		cursor++
		return e, nil
	}

	var rawTxns []model.RawTxn
	output := func(ctx context.Context, txn model.RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	// Set up the tracker so that only the last resolve event forwards the global minimum Ts
	tracker := mockTracker{forwarded: []bool{false, false, true}}
	err := collectRawTxns(ctx, input, output, &tracker)
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, 1)
	txn := rawTxns[0]
	c.Assert(txn.Ts, check.Equals, uint64(1))
	c.Assert(txn.Entries, check.HasLen, 3)
	c.Assert(string(txn.Entries[0].Key), check.Equals, "key1-1")
	c.Assert(string(txn.Entries[1].Key), check.Equals, "key1-2")
	c.Assert(string(txn.Entries[2].Key), check.Equals, "key1-3")
}

func (cs *CollectRawTxnsSuite) TestShouldOutputBinlogEvenWhenThereIsNoRealEvent(c *check.C) {
	entries := []model.KvOrResolved{
		{Resolved: &model.ResolvedSpan{Timestamp: 1024}},
		{Resolved: &model.ResolvedSpan{Timestamp: 2000}},
	}

	cursor := 0
	input := func(ctx context.Context) (model.KvOrResolved, error) {
		if cursor >= len(entries) {
			return model.KvOrResolved{}, errors.New("End")
		}
		e := entries[cursor]
		cursor++
		return e, nil
	}

	var rawTxns []model.RawTxn
	output := func(ctx context.Context, txn model.RawTxn) error {
		rawTxns = append(rawTxns, txn)
		return nil
	}

	ctx := context.Background()
	tracker := mockTracker{forwarded: []bool{true, true}}
	err := collectRawTxns(ctx, input, output, &tracker)
	c.Assert(err, check.ErrorMatches, "End")

	c.Assert(rawTxns, check.HasLen, len(entries))
	for i, t := range rawTxns {
		c.Assert(t.Entries, check.HasLen, 0)
		c.Assert(t.Ts, check.Equals, entries[i].Resolved.Timestamp)
	}
}
