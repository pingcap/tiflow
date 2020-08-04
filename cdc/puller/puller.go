// Copyright 2020 PingCAP, Inc.
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
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/kv"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/cdc/puller/frontier"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPullerEventChanSize  = 128000
	defaultPullerOutputChanSize = 128000
)

// Puller pull data from tikv and push changes into a buffer
type Puller interface {
	// Run the puller, continually fetch event from TiKV and add event into buffer
	Run(ctx context.Context) error
	GetResolvedTs() uint64
	Output() <-chan *model.RawKVEntry
}

type pullerImpl struct {
	pdCli          pd.Client
	credential     *security.Credential
	kvStorage      tikv.Storage
	checkpointTs   uint64
	spans          []regionspan.ComparableSpan
	buffer         *memBuffer
	outputCh       chan *model.RawKVEntry
	tsTracker      frontier.Frontier
	resolvedTs     uint64
	enableOldValue bool
}

// NewPuller create a new Puller fetch event start from checkpointTs
// and put into buf.
func NewPuller(
	pdCli pd.Client,
	credential *security.Credential,
	kvStorage tidbkv.Storage,
	checkpointTs uint64,
	spans []regionspan.Span,
	limitter *BlurResourceLimitter,
	enableOldValue bool,
) Puller {
	tikvStorage, ok := kvStorage.(tikv.Storage)
	if !ok {
		log.Fatal("can't create puller for non-tikv storage")
	}
	comparableSpans := make([]regionspan.ComparableSpan, len(spans))
	for i := range spans {
		comparableSpans[i] = regionspan.ToComparableSpan(spans[i])
	}
	p := &pullerImpl{
		pdCli:          pdCli,
		credential:     credential,
		kvStorage:      tikvStorage,
		checkpointTs:   checkpointTs,
		spans:          comparableSpans,
		buffer:         makeMemBuffer(limitter),
		outputCh:       make(chan *model.RawKVEntry, defaultPullerOutputChanSize),
		tsTracker:      frontier.NewFrontier(checkpointTs, comparableSpans...),
		resolvedTs:     checkpointTs,
		enableOldValue: enableOldValue,
	}
	return p
}

func (p *pullerImpl) Output() <-chan *model.RawKVEntry {
	return p.outputCh
}

// Run the puller, continually fetch event from TiKV and add event into buffer
func (p *pullerImpl) Run(ctx context.Context) error {
	cli, err := kv.NewCDCClient(ctx, p.pdCli, p.kvStorage, p.credential)
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
			return cli.EventFeed(ctx, span, checkpointTs, p.enableOldValue, eventCh)
		})
	}

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	tableID, tableName := util.TableIDFromCtx(ctx)
	metricOutputChanSize := outputChanSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEventChanSize := eventChanSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricMemBufferSize := memBufferSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricPullerResolvedTs := pullerResolvedTsGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEventCounterKv := kvEventCounter.WithLabelValues(captureAddr, changefeedID, "kv")
	metricEventCounterResolved := kvEventCounter.WithLabelValues(captureAddr, changefeedID, "resolved")
	metricTxnCollectCounterKv := txnCollectCounter.WithLabelValues(captureAddr, changefeedID, tableName, "kv")
	metricTxnCollectCounterResolved := txnCollectCounter.WithLabelValues(captureAddr, changefeedID, tableName, "kv")

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(15 * time.Second):
				metricEventChanSize.Set(float64(len(eventCh)))
				metricMemBufferSize.Set(float64(p.buffer.Size()))
				metricOutputChanSize.Set(float64(len(p.outputCh)))
				metricPullerResolvedTs.Set(float64(oracle.ExtractPhysical(atomic.LoadUint64(&p.resolvedTs))))
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case e := <-eventCh:
				if e.Val != nil {
					metricEventCounterKv.Inc()
					val := e.Val
					// if a region with kv range [a, z)
					// and we only want the get [b, c) from this region,
					// tikv will return all key events in the region although we specified [b, c) int the request.
					// we can make tikv only return the events about the keys in the specified range.
					comparableKey := regionspan.ToComparableKey(val.Key)
					if !regionspan.KeyInSpans(comparableKey, p.spans) {
						// log.Warn("key not in spans range", zap.Binary("key", val.Key), zap.Stringer("span", p.spans))
						continue
					}

					if err := p.buffer.AddEntry(ctx, *e); err != nil {
						return errors.Trace(err)
					}
				} else if e.Resolved != nil {
					metricEventCounterResolved.Inc()
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
		output := func(raw *model.RawKVEntry) error {
			if raw.CRTs < p.resolvedTs || (raw.CRTs == p.resolvedTs && raw.OpType != model.OpTypeResolved) {
				log.Fatal("The CRTs must be greater than the resolvedTs",
					zap.Reflect("row", raw),
					zap.Uint64("CRTs", raw.CRTs),
					zap.Uint64("resolvedTs", p.resolvedTs),
					zap.Int64("tableID", tableID))
			}
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case p.outputCh <- raw:
			}
			return nil
		}
		var lastResolvedTs uint64
		for {
			e, err := p.buffer.Get(ctx)
			if err != nil {
				return errors.Trace(err)
			}
			if e.Val != nil {
				metricTxnCollectCounterKv.Inc()
				if err := output(e.Val); err != nil {
					return errors.Trace(err)
				}
			} else if e.Resolved != nil {
				metricTxnCollectCounterResolved.Inc()
				if !regionspan.IsSubSpan(e.Resolved.Span, p.spans...) {
					log.Fatal("the resolved span is not in the total span", zap.Reflect("resolved", e.Resolved), zap.Int64("tableID", tableID))
				}
				// Forward is called in a single thread
				p.tsTracker.Forward(e.Resolved.Span, e.Resolved.ResolvedTs)
				resolvedTs := p.tsTracker.Frontier()
				if resolvedTs == lastResolvedTs {
					continue
				}
				lastResolvedTs = resolvedTs
				err := output(&model.RawKVEntry{CRTs: resolvedTs, OpType: model.OpTypeResolved})
				if err != nil {
					return errors.Trace(err)
				}
				atomic.StoreUint64(&p.resolvedTs, resolvedTs)
			}
		}
	})
	return g.Wait()
}

func (p *pullerImpl) GetResolvedTs() uint64 {
	return atomic.LoadUint64(&p.resolvedTs)
}
