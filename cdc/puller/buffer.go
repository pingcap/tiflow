package puller

import (
	"context"

	"github.com/pingcap/ticdc/cdc/model"
)

// Buffer buffers kv entries
type Buffer chan model.RegionFeedEvent

func makeBuffer() Buffer {
	return make(Buffer, 8)
}

// AddEntry adds an entry to the buffer
func (b Buffer) AddEntry(ctx context.Context, entry model.RegionFeedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case b <- entry:
		return nil
	}
}

// Get waits for an entry from the input channel but will stop with respect to the context
func (b Buffer) Get(ctx context.Context) (model.RegionFeedEvent, error) {
	select {
	case <-ctx.Done():
		return model.RegionFeedEvent{}, ctx.Err()
	case e := <-b:
		return e, nil
	}
}

// TODO limit memory buffer
