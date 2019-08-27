package cdc

import (
	"context"

	"github.com/pingcap/tidb-cdc/cdc/util"
)

// buffer entry from kv layer
type BufferEntry struct {
	KV       *KVEntry
	Resolved *ResolvedSpan
}

func (e *BufferEntry) GetValue() interface{} {
	if e.KV != nil {
		return e.KV
	} else if e.Resolved != nil {
		return e.Resolved
	} else {
		return nil
	}
}

// Buffer buffer kv entry
type Buffer struct {
	entriesCh chan BufferEntry
}

func MakeBuffer() *Buffer {
	return &Buffer{
		entriesCh: make(chan BufferEntry),
	}
}

func (b *Buffer) AddEntry(ctx context.Context, entry BufferEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b.entriesCh <- entry:
		return nil
	}
}

func (b *Buffer) AddKVEntry(ctx context.Context, kv *KVEntry) error {
	return b.AddEntry(ctx, BufferEntry{KV: kv})
}

func (b *Buffer) AddResolved(ctx context.Context, span util.Span, ts uint64) error {
	return b.AddEntry(ctx, BufferEntry{Resolved: &ResolvedSpan{Span: span, Timestamp: ts}})
}

func (b *Buffer) Get(ctx context.Context) (BufferEntry, error) {
	select {
	case <-ctx.Done():
		return BufferEntry{}, ctx.Err()
	case e := <-b.entriesCh:
		return e, nil
	}
}

// TODO limit memory buffer
