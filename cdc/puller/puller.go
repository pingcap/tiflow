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
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPullerEventChanSize = 128000
)

// Puller pull data from tikv and push changes into a buffer
type Puller interface {
	// Run the puller, continually fetch event from TiKV and add event into buffer
	Run(ctx context.Context) error
	GetResolvedTs() uint64
	Output() ChanBuffer
	SortedOutput(ctx context.Context) <-chan *model.RawKVEntry
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
	buffer       *memBuffer
	chanBuffer   ChanBuffer
	tsTracker    resolveTsTracker
	resolvedTs   uint64
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
	limitter *BlurResourceLimitter,
) *pullerImpl {
	p := &pullerImpl{
		pdCli:        pdCli,
		checkpointTs: checkpointTs,
		spans:        spans,
		buffer:       makeMemBuffer(limitter),
		chanBuffer:   makeChanBuffer(),
		tsTracker:    makeSpanFrontier(spans...),
		needEncode:   needEncode,
	}

	return p
}

func (p *pullerImpl) Output() ChanBuffer {
	return p.chanBuffer
}

func (p *pullerImpl) SortedOutput(ctx context.Context) <-chan *model.RawKVEntry {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	sorter := NewEntrySorter()
	go func() {
		sorter.Run(ctx)
		defer close(sorter.resolvedCh)
		for {
			be, err := p.chanBuffer.Get(ctx)
			if err != nil {
				if errors.Cause(err) != context.Canceled {
					log.Error("error in puller", zap.Error(err))
				}
				break
			}
			if be.Val != nil {
				txnCollectCounter.WithLabelValues(captureID, changefeedID, "kv").Inc()
				sorter.AddEntry(be.Val)
			} else if be.Resolved != nil {
				txnCollectCounter.WithLabelValues(captureID, changefeedID, "resolved").Inc()
				resolvedTs := be.Resolved.ResolvedTs
				// 1. Forward is called in a single thread
				// 2. The only way the global minimum resolved Ts can be forwarded is that
				// 	  the resolveTs we pass in replaces the original one
				// Thus, we can just use resolvedTs here as the new global minimum resolved Ts.
				forwarded := p.tsTracker.Forward(be.Resolved.Span, resolvedTs)
				if !forwarded {
					continue
				}
				atomic.StoreUint64(&p.resolvedTs, resolvedTs)
				sorter.AddEntry(&model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved})
			}
		}
	}()
	return sorter.Output()
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
	eventCh := make(chan *model.RegionFeedEvent, defaultPullerEventChanSize)

	for _, span := range p.spans {
		span := span

		g.Go(func() error {
			return cli.EventFeed(ctx, span, checkpointTs, eventCh)
		})
	}

	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Minute):
				entryBufferSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(p.chanBuffer)))
				eventChanSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(eventCh)))
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case e := <-eventCh:
				if e.Val != nil {
					kvEventCounter.WithLabelValues(captureID, changefeedID, "kv").Inc()
					val := e.Val

					// if a region with kv range [a, z)
					// and we only want the get [b, c) from this region,
					// tikv will return all key events in the region although we specified [b, c) int the request.
					// we can make tikv only return the events about the keys in the specified range.
					if !util.KeyInSpans(val.Key, p.spans, p.needEncode) {
						// log.Warn("key not in spans range", zap.Binary("key", val.Key), zap.Reflect("span", p.spans))
						continue
					}

					if err := p.buffer.AddEntry(ctx, *e); err != nil {
						return errors.Trace(err)
					}
				} else if e.Resolved != nil {
					kvEventCounter.WithLabelValues(captureID, changefeedID, "resolved").Inc()
					if err := p.buffer.AddEntry(ctx, *e); err != nil {
						return errors.Trace(err)
					}
				}
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	g.Go(func() error {
		for {
			e, err := p.buffer.Get(ctx)
			if err != nil {
				return errors.Trace(err)
			}

			err = p.chanBuffer.AddEntry(ctx, e)
			if err != nil {
				return errors.Trace(err)
			}
		}
	})

	return g.Wait()
}

func (p *pullerImpl) GetResolvedTs() uint64 {
	return p.tsTracker.Frontier()
}

// TODO remove this function
// collectRawTxns collects KV events from the inputFn,
// groups them by transactions and sends them to the outputFn.
func collectRawTxns(
	ctx context.Context,
	inputFn func(context.Context) (model.RegionFeedEvent, error),
	outputFn func(context.Context, model.RawTxn) error,
	tracker resolveTsTracker,
) error {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	entryGroup := NewEntryGroup()
	for {
		be, err := inputFn(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		if be.Val != nil {
			txnCollectCounter.WithLabelValues(captureID, changefeedID, "kv").Inc()
			entryGroup.AddEntry(be.Val.Ts, be.Val)
		} else if be.Resolved != nil {
			txnCollectCounter.WithLabelValues(captureID, changefeedID, "resolved").Inc()
			resolvedTs := be.Resolved.ResolvedTs
			// 1. Forward is called in a single thread
			// 2. The only way the global minimum resolved Ts can be forwarded is that
			// 	  the resolveTs we pass in replaces the original one
			// Thus, we can just use resolvedTs here as the new global minimum resolved Ts.
			forwarded := tracker.Forward(be.Resolved.Span, resolvedTs)
			if !forwarded {
				continue
			}
			readyTxns := entryGroup.Consume(resolvedTs)
			for _, t := range readyTxns {
				err := outputFn(ctx, t)
				if err != nil {
					return errors.Trace(err)
				}
			}
			resolvedTxnsBatchSize.Observe(float64(len(readyTxns)))
			if len(readyTxns) == 0 {
				log.Debug("Forwarding fake txn", zap.Uint64("ts", resolvedTs))
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
