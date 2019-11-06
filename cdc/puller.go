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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/tidb-cdc/cdc/kv"
	"github.com/pingcap/tidb-cdc/cdc/txn"
	"github.com/pingcap/tidb-cdc/pkg/util"
	"golang.org/x/sync/errgroup"
)

// Puller pull data from tikv and push changes into a buffer
type Puller struct {
	pdCli        pd.Client
	checkpointTs uint64
	spans        []util.Span
	buf          Buffer
	tsTracker    txn.ResolveTsTracker
}

type CancellablePuller struct {
	*Puller

	Cancel context.CancelFunc
}

// NewPuller create a new Puller fetch event start from checkpointTs
// and put into buf.
func NewPuller(
	pdCli pd.Client,
	checkpointTs uint64,
	spans []util.Span,
) *Puller {
	p := &Puller{
		pdCli:        pdCli,
		checkpointTs: checkpointTs,
		spans:        spans,
		buf:          MakeBuffer(),
		tsTracker:    makeSpanFrontier(spans...),
	}

	return p
}

func (p *Puller) Output() Buffer {
	return p.buf
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

	checkpointTs := p.checkpointTs
	eventCh := make(chan *kv.RegionFeedEvent, 128)

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
					if !util.KeyInSpans(val.Key, p.spans) {
						log.Warn("key not in spans range")
						continue
					}

					kv := &kv.RawKVEntry{
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

func (p *Puller) GetResolvedTs() uint64 {
	return p.tsTracker.Frontier()
}

func (p *Puller) CollectRawTxns(ctx context.Context, outputFn func(context.Context, txn.RawTxn) error) error {
	return txn.CollectRawTxns(ctx, p.buf.Get, outputFn, p.tsTracker)
}
