package cdc

import (
	"context"

	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/util"
	"golang.org/x/sync/errgroup"
)

// Puller pull data from tikv and push changes into a buffer
type Puller struct {
	pdCli        pd.Client
	checkpointTS uint64
	spans        []util.Span
	detail       ChangeFeedDetail
	buf          *Buffer
}

// NewPuller create a new Puller fetch event start from checkpointTS
// and put into buf.
func NewPuller(
	pdCli pd.Client,
	checkpointTS uint64,
	spans []util.Span,
	// useless now
	detail ChangeFeedDetail,
	buf *Buffer,
) *Puller {
	p := &Puller{
		pdCli:        pdCli,
		checkpointTS: checkpointTS,
		spans:        spans,
		detail:       detail,
		buf:          buf,
	}

	return p
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *Puller) Run(ctx context.Context) error {
	// TODO pull from tikv and push into buf
	// need buffer in memory first

	cli, err := kv.NewCDCClient(p.pdCli)
	if err != nil {
		return errors.Annotate(err, "create cdc client failed")
	}

	defer cli.Close()

	g, ctx := errgroup.WithContext(ctx)

	checkpointTS := p.checkpointTS
	eventCh := make(chan *kv.RegionFeedEvent, 128)

	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return cli.EventFeed(ctx, span, checkpointTS, eventCh)
		})
	}

	g.Go(func() error {
		for {
			select {
			case e := <-eventCh:
				if e.Val != nil {
					val := e.Val

					var opType OpType
					if val.OpType == kv.OpTypeDelete {
						opType = OpTypeDelete
					} else if val.OpType == kv.OpTypePut {
						opType = OpTypePut
					}

					kv := &KVEntry{
						OpType: opType,
						Key:    val.Key,
						Value:  val.Value,
						TS:     val.TS,
					}

					p.buf.AddKVEntry(ctx, kv)
				} else if e.Checkpoint != nil {
					cp := e.Checkpoint
					p.buf.AddResolved(ctx, cp.Span, cp.ResolvedTS)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return g.Wait()
}
