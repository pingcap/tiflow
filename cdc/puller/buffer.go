package puller

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

// BufferEntry is the type for buffer entry from kv layer
type BufferEntry = model.KvOrResolved

// Buffer buffers kv entries
type Buffer chan BufferEntry

func makeBuffer() Buffer {
	return make(Buffer, 8)
}

// AddEntry adds an entry to the buffer
func (b Buffer) AddEntry(ctx context.Context, entry BufferEntry) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b <- entry:
		return nil
	}
}

// AddKVEntry adds a kv entry to the buffer
func (b Buffer) AddKVEntry(ctx context.Context, kv *model.RawKVEntry) error {
	return b.AddEntry(ctx, BufferEntry{KV: kv})
}

// AddResolved adds a resolved entry to the buffer
func (b Buffer) AddResolved(ctx context.Context, span util.Span, ts uint64) error {
	return b.AddEntry(ctx, BufferEntry{Resolved: &model.ResolvedSpan{Span: span, Timestamp: ts}})
}

// Get waits for an entry from the input channel but will stop with respect to the context
func (b Buffer) Get(ctx context.Context) (BufferEntry, error) {
	select {
	case <-ctx.Done():
		return BufferEntry{}, ctx.Err()
	case e := <-b:
		return e, nil
	}
}

// TODO limit memory buffer
