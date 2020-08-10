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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/pkg/notify"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/errors"
	"golang.org/x/sync/errgroup"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

// EntrySorter accepts out-of-order raw kv entries and output sorted entries
type EntrySorter struct {
	unsorted        []*model.PolymorphicEvent
	lock            sync.Mutex
	resolvedTsGroup []uint64
	closed          int32

	outputCh         chan *model.PolymorphicEvent
	resolvedNotifier *notify.Notifier
}

// NewEntrySorter creates a new EntrySorter
func NewEntrySorter() *EntrySorter {
	return &EntrySorter{
		resolvedNotifier: new(notify.Notifier),
		outputCh:         make(chan *model.PolymorphicEvent, 128000),
	}
}

// Run runs EntrySorter
func (es *EntrySorter) Run(ctx context.Context) error {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	_, tableName := util.TableIDFromCtx(ctx)
	metricEntrySorterResolvedChanSizeGuage := entrySorterResolvedChanSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEntrySorterOutputChanSizeGauge := entrySorterOutputChanSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEntryUnsortedSizeGauge := entrySorterUnsortedSizeGauge.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEntrySorterSortDuration := entrySorterSortDuration.WithLabelValues(captureAddr, changefeedID, tableName)
	metricEntrySorterMergeDuration := entrySorterMergeDuration.WithLabelValues(captureAddr, changefeedID, tableName)

	lessFunc := func(i *model.PolymorphicEvent, j *model.PolymorphicEvent) bool {
		if i.CRTs == j.CRTs {
			if i.RawKV.OpType == model.OpTypeDelete {
				return true
			}
			if j.RawKV.OpType == model.OpTypeResolved {
				return true
			}
		}
		return i.CRTs < j.CRTs
	}
	mergeFunc := func(kvsA []*model.PolymorphicEvent, kvsB []*model.PolymorphicEvent, output func(*model.PolymorphicEvent)) {
		var i, j int
		for i < len(kvsA) && j < len(kvsB) {
			if lessFunc(kvsA[i], kvsB[j]) {
				output(kvsA[i])
				i++
			} else {
				output(kvsB[j])
				j++
			}
		}
		for ; i < len(kvsA); i++ {
			output(kvsA[i])
		}
		for ; j < len(kvsB); j++ {
			output(kvsB[j])
		}
	}
	output := func(ctx context.Context, entry *model.PolymorphicEvent) {
		select {
		case <-ctx.Done():
			return
		case es.outputCh <- entry:
		}
	}

	errg, ctx := errgroup.WithContext(ctx)
	errg.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case <-time.After(defaultMetricInterval):
				metricEntrySorterOutputChanSizeGauge.Set(float64(len(es.outputCh)))
				es.lock.Lock()
				metricEntrySorterResolvedChanSizeGuage.Set(float64(len(es.resolvedTsGroup)))
				metricEntryUnsortedSizeGauge.Set(float64(len(es.unsorted)))
				es.lock.Unlock()
			}
		}
	})
	receiver := es.resolvedNotifier.NewReceiver(1000 * time.Millisecond)
	defer es.resolvedNotifier.Close()
	errg.Go(func() error {
		var sorted []*model.PolymorphicEvent
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&es.closed, 1)
				close(es.outputCh)
				return errors.Trace(ctx.Err())
			case <-receiver.C:
				es.lock.Lock()
				if len(es.resolvedTsGroup) == 0 {
					es.lock.Unlock()
					continue
				}
				resolvedTsGroup := es.resolvedTsGroup
				es.resolvedTsGroup = nil
				toSort := es.unsorted
				es.unsorted = nil
				es.lock.Unlock()

				resEvents := make([]*model.PolymorphicEvent, len(resolvedTsGroup))
				for i, rts := range resolvedTsGroup {
					resEvents[i] = model.NewResolvedPolymorphicEvent(rts)
				}
				toSort = append(toSort, resEvents...)
				startTime := time.Now()
				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})
				metricEntrySorterSortDuration.Observe(time.Since(startTime).Seconds())
				maxResolvedTs := resolvedTsGroup[len(resolvedTsGroup)-1]

				startTime = time.Now()
				var merged []*model.PolymorphicEvent
				mergeFunc(toSort, sorted, func(entry *model.PolymorphicEvent) {
					if entry.CRTs <= maxResolvedTs {
						output(ctx, entry)
					} else {
						merged = append(merged, entry)
					}
				})
				metricEntrySorterMergeDuration.Observe(time.Since(startTime).Seconds())
				sorted = merged
			}
		}
	})
	return errg.Wait()
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (es *EntrySorter) AddEntry(ctx context.Context, entry *model.PolymorphicEvent) {
	if atomic.LoadInt32(&es.closed) != 0 {
		return
	}
	es.lock.Lock()
	if entry.RawKV.OpType == model.OpTypeResolved {
		es.resolvedTsGroup = append(es.resolvedTsGroup, entry.CRTs)
		es.resolvedNotifier.Notify()
	} else {
		es.unsorted = append(es.unsorted, entry)
	}
	es.lock.Unlock()
}

// Output returns the sorted raw kv output channel
func (es *EntrySorter) Output() <-chan *model.PolymorphicEvent {
	return es.outputCh
}

// SortOutput receives a channel from a puller, then sort event and output to the channel returned.
func SortOutput(ctx context.Context, input <-chan *model.RawKVEntry) <-chan *model.RawKVEntry {
	ctx, cancel := context.WithCancel(ctx)
	sorter := NewEntrySorter()
	outputCh := make(chan *model.RawKVEntry, 128)
	output := func(rawKV *model.RawKVEntry) {
		select {
		case <-ctx.Done():
			if errors.Cause(ctx.Err()) != context.Canceled {
				log.Error("sorter exited with error", zap.Error(ctx.Err()))
			}
			return
		case outputCh <- rawKV:
		}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				if errors.Cause(ctx.Err()) != context.Canceled {
					log.Error("sorter exited with error", zap.Error(ctx.Err()))
				}
				return
			case rawKV := <-input:
				if rawKV == nil {
					continue
				}
				sorter.AddEntry(ctx, model.NewPolymorphicEvent(rawKV))
			case sorted := <-sorter.Output():
				if sorted != nil {
					output(sorted.RawKV)
				}
			}
		}
	}()
	go func() {
		if err := sorter.Run(ctx); err != nil {
			if errors.Cause(ctx.Err()) != context.Canceled {
				log.Error("sorter exited with error", zap.Error(ctx.Err()))
			}
		}
		cancel()
	}()
	return outputCh
}
