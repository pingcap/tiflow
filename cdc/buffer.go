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
	"github.com/pingcap/tidb-cdc/kv_entry"
)

// buffer entry from kv layer
type bufferEntry struct {
	kv       *kv_entry.RawKVEntry
	resolved *ResolvedSpan
}

type buffer struct {
	entriesCh chan bufferEntry
}

func makeBuffer() *buffer {
	return &buffer{
		entriesCh: make(chan bufferEntry),
	}
}

func (b *buffer) AddEntry(ctx context.Context, entry bufferEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- entry:
		return nil
	}
}

func (b *buffer) AddKVEntry(ctx context.Context, kv *kv_entry.RawKVEntry) error {
	return b.AddEntry(ctx, bufferEntry{kv: kv})
}

func (b *buffer) AddResolved(ctx context.Context, span Span, ts uint64) error {
	return b.AddEntry(ctx, bufferEntry{resolved: &ResolvedSpan{Span: span, Timestamp: ts}})
}

func (b *buffer) Get(ctx context.Context) (bufferEntry, error) {
	select {
	case <-ctx.Done():
		return bufferEntry{}, ctx.Err()
	case e := <-b.entriesCh:
		return e, nil
	}
}

// TODO limit memory buffer
