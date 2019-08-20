package cdc

import "context"

// buffer entry from kv layer
type bufferEntry struct {
	kv       *KVEntry
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

func (b *buffer) AddKVEntry(ctx context.Context, kv *KVEntry) error {
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
