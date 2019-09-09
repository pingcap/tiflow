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

					kv := &RawKVEntry{
						OpType: opType,
						Key:    val.Key,
						Value:  val.Value,
						Ts:     val.TS,
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

	if err := g.Wait(); err != nil {
		return err
	}

	return nil
}
