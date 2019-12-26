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
	"sort"

	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
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

// resolveTsTracker checks resolved event of spans and moves the global resolved ts ahead
type resolveTsTracker interface {
	Forward(span util.Span, ts uint64) bool
	Frontier() uint64
}

type pullerImpl struct {
	pdCli        pd.Client
	checkpointTs uint64
	spans        []util.Span
	buf          Buffer
	tsTracker    resolveTsTracker
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
		captureID := util.CaptureIDFromCtx(ctx)
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

					if err := p.buf.AddKVEntry(ctx, kv); err != nil {
						return errors.Trace(err)
					}
					eventCounter.WithLabelValues(captureID, "kv").Inc()
				} else if e.Checkpoint != nil {
					cp := e.Checkpoint
					if err := p.buf.AddResolved(ctx, cp.Span, cp.ResolvedTs); err != nil {
						return errors.Trace(err)
					}
					eventCounter.WithLabelValues(captureID, "resolved").Inc()
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
	return collectRawTxns(ctx, p.buf.Get, outputFn, p.tsTracker)
}

// collectRawTxns collects KV events from the inputFn,
// groups them by transactions and sends them to the outputFn.
func collectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (model.KvOrResolved, error),
	outputFn func(context.Context, model.RawTxn) error,
	tracker resolveTsTracker,
) error {
	entryGroups := make(map[uint64][]*model.RawKVEntry)
	for {
		be, err := inputFn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if be.KV != nil {
			entryGroups[be.KV.Ts] = append(entryGroups[be.KV.Ts], be.KV)
		} else if be.Resolved != nil {
			resolvedTs := be.Resolved.Timestamp
			// 1. Forward is called in a single thread
			// 2. The only way the global minimum resolved Ts can be forwarded is that
			// 	  the resolveTs we pass in replaces the original one
			// Thus, we can just use resolvedTs here as the new global minimum resolved Ts.
			forwarded := tracker.Forward(be.Resolved.Span, resolvedTs)
			if !forwarded {
				continue
			}
			var readyTxns []model.RawTxn
			for ts, entries := range entryGroups {
				if ts <= resolvedTs {
					readyTxns = append(readyTxns, model.RawTxn{Ts: ts, Entries: entries})
					delete(entryGroups, ts)
				}
			}
			sort.Slice(readyTxns, func(i, j int) bool {
				return readyTxns[i].Ts < readyTxns[j].Ts
			})
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return errors.Trace(err)
				}
			}
			resolvedTxnsBatchSize.Observe(float64(len(readyTxns)))
			if len(readyTxns) == 0 {
				log.Info("Forwarding fake txn", zap.Uint64("ts", resolvedTs))
				fakeTxn := model.RawTxn{
					Ts:      resolvedTs,
					Entries: nil,
				}
				err := outputFn(ctx, fakeTxn)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}
