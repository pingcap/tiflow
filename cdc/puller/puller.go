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

	"github.com/pingcap/errors"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/txn"
	"github.com/pingcap/ticdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

// Puller pull data from tikv and push changes into a buffer
type Puller interface {
	// Run the puller, continually fetch event from TiKV and add event into buffer
	Run(ctx context.Context) error
	GetResolvedTs() uint64
	CollectRawTxns(ctx context.Context, outputFn func(context.Context, model.RawTxn) error) error
	Output() Buffer
}

type pullerImpl struct {
	pdCli        pd.Client
	checkpointTs uint64
	spans        []util.Span
	buf          Buffer
	tsTracker    txn.ResolveTsTracker
	// needEncode represents whether we need to encode a key when checking it is in span
	needEncode bool
}

// CancellablePuller is a puller that can be stopped with the Cancel function
type CancellablePuller struct {
	Puller

	Cancel context.CancelFunc
}

// NewPuller create a new Puller fetch event start from checkpointTs
// and put into buf.
func NewPuller(
	pdCli pd.Client,
	checkpointTs uint64,
	spans []util.Span,
	needEncode bool,
) *pullerImpl {
	p := &pullerImpl{
		pdCli:        pdCli,
		checkpointTs: checkpointTs,
		spans:        spans,
		buf:          makeBuffer(),
		tsTracker:    makeSpanFrontier(spans...),
		needEncode:   needEncode,
	}

	return p
}

func (p *pullerImpl) Output() Buffer {
	return p.buf
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	cli, err := kv.NewCDCClient(p.pdCli)
	if err != nil {
		return errors.Annotate(err, "create cdc client failed")
	}

	defer cli.Close()

	g, ctx := errgroup.WithContext(ctx)

	checkpointTs := p.checkpointTs
	eventCh := make(chan *model.RegionFeedEvent, 128)

	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return cli.EventFeed(ctx, span, checkpointTs, eventCh)
		})
	}

	g.Go(func() error {
		for {
			select {
			case e := <-eventCh:
				if e.Val != nil {
					val := e.Val

					// if a region with kv range [a, z)
					// and we only want the get [b, c) from this region,
					// tikv will return all key events in the region although we specified [b, c) int the request.
					// we can make tikv only return the events about the keys in the specified range.
					if !util.KeyInSpans(val.Key, p.spans, p.needEncode) {
						// log.Warn("key not in spans range", zap.Binary("key", val.Key), zap.Reflect("span", p.spans))
						continue
					}

					kv := &model.RawKVEntry{
						OpType: val.OpType,
						Key:    val.Key,
						Value:  val.Value,
						Ts:     val.Ts,
					}

					p.buf.AddKVEntry(ctx, kv)
				} else if e.Checkpoint != nil {
					cp := e.Checkpoint
					p.buf.AddResolved(ctx, cp.Span, cp.ResolvedTs)
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	return g.Wait()
}

func (p *pullerImpl) GetResolvedTs() uint64 {
	return p.tsTracker.Frontier()
}

func (p *pullerImpl) CollectRawTxns(ctx context.Context, outputFn func(context.Context, model.RawTxn) error) error {
	return txn.CollectRawTxns(ctx, p.buf.Get, outputFn, p.tsTracker)
}
