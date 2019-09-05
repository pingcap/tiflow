package cdc

import (
	"context"
	"github.com/pingcap/tidb-cdc/kv_entry"
)

type Range struct {
	Start []byte
	End   []byte
}

type Puller interface {
	Pull(ranges []Range) <-chan kv_entry.RawKVEntry
}

// puller pull data from tikv and push changes into a buffer
type puller struct {
	checkpointTS uint64
	spans        []Span
	detail       ChangeFeedDetail
	buf          *buffer
}

func newPuller(
	checkpointTS uint64,
	spans []Span,
	detail ChangeFeedDetail,
	buf *buffer,
) *puller {
	p := &puller{
		checkpointTS: checkpointTS,
		spans:        spans,
		detail:       detail,
		buf:          buf,
	}

	return p
}

func (p *puller) Run(ctx context.Context) error {
	// TODO pull from tikv and push into buf
	// need buffer in memory first
	<-ctx.Done()
	return nil
}
