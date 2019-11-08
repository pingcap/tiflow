package puller

import (
	"context"

	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/pkg/util"
)

// buffer entry from kv layer
type BufferEntry = kv.KvOrResolved

// Buffer buffers kv entries
type Buffer chan BufferEntry

func MakeBuffer() Buffer {
	return make(Buffer)
}

func (b Buffer) AddEntry(ctx context.Context, entry BufferEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b <- entry:
		return nil
	}
}

func (b Buffer) AddKVEntry(ctx context.Context, kv *kv.RawKVEntry) error {
	return b.AddEntry(ctx, BufferEntry{KV: kv})
}

func (b Buffer) AddResolved(ctx context.Context, span util.Span, ts uint64) error {
	return b.AddEntry(ctx, BufferEntry{Resolved: &kv.ResolvedSpan{Span: span, Timestamp: ts}})
}

func (b Buffer) Get(ctx context.Context) (BufferEntry, error) {
	select {
	case <-ctx.Done():
		return BufferEntry{}, ctx.Err()
	case e := <-b:
		return e, nil
	}
}

// TODO limit memory buffer
