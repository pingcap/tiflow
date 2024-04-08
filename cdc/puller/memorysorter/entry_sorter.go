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

package memorysorter

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/notify"
	"golang.org/x/sync/errgroup"
)

// DDLPullerTableName is the fake table name for ddl puller
const DDLPullerTableName = "DDL_PULLER"

// EntrySorter accepts out-of-order raw kv entries and output sorted entries.
// For now, it only uses for DDL puller and test.
type EntrySorter struct {
	unsorted       []*model.PolymorphicEvent
	lock           sync.Mutex
	watermarkGroup []uint64
	closed         int32

	outputCh         chan *model.PolymorphicEvent
	resolvedNotifier *notify.Notifier
	changeFeedID     model.ChangeFeedID
}

// NewEntrySorter creates a new EntrySorter
func NewEntrySorter(changeFeedID model.ChangeFeedID) *EntrySorter {
	return &EntrySorter{
		resolvedNotifier: new(notify.Notifier),
		outputCh:         make(chan *model.PolymorphicEvent, 128000),
		changeFeedID:     changeFeedID,
	}
}

// Run runs EntrySorter
func (es *EntrySorter) Run(ctx context.Context) error {
	changefeedID := es.changeFeedID
	tableName := DDLPullerTableName
	metricEntrySorterResolvedChanSizeGauge := entrySorterResolvedChanSizeGauge.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, tableName)
	metricEntrySorterOutputChanSizeGauge := entrySorterOutputChanSizeGauge.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, tableName)
	metricEntryUnsortedSizeGauge := entrySorterUnsortedSizeGauge.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, tableName)
	metricEntrySorterSortDuration := entrySorterSortDuration.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, tableName)
	metricEntrySorterMergeDuration := entrySorterMergeDuration.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID, tableName)

	output := func(ctx context.Context, entry *model.PolymorphicEvent) {
		select {
		case <-ctx.Done():
			return
		case es.outputCh <- entry:
		}
	}

	errg, ctx := errgroup.WithContext(ctx)
	receiver, err := es.resolvedNotifier.NewReceiver(1000 * time.Millisecond)
	if err != nil {
		return err
	}
	defer es.resolvedNotifier.Close()
	errg.Go(func() error {
		var sorted []*model.PolymorphicEvent
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&es.closed, 1)
				close(es.outputCh)
				return errors.Trace(ctx.Err())
			case <-time.After(defaultMetricInterval):
				metricEntrySorterOutputChanSizeGauge.Set(float64(len(es.outputCh)))
				es.lock.Lock()
				metricEntrySorterResolvedChanSizeGauge.Set(float64(len(es.watermarkGroup)))
				metricEntryUnsortedSizeGauge.Set(float64(len(es.unsorted)))
				es.lock.Unlock()
			case <-receiver.C:
				es.lock.Lock()
				if len(es.watermarkGroup) == 0 {
					es.lock.Unlock()
					continue
				}
				watermarkGroup := es.watermarkGroup
				es.watermarkGroup = nil
				toSort := es.unsorted
				es.unsorted = nil
				es.lock.Unlock()

				resEvents := make([]*model.PolymorphicEvent, len(watermarkGroup))
				for i, rts := range watermarkGroup {
					// regionID = 0 means the event is produced by TiCDC
					resEvents[i] = model.NewWatermarkPolymorphicEvent(0, rts)
				}
				toSort = append(toSort, resEvents...)
				startTime := time.Now()
				sort.Slice(toSort, func(i, j int) bool {
					return eventLess(toSort[i], toSort[j])
				})
				metricEntrySorterSortDuration.Observe(time.Since(startTime).Seconds())
				maxWatermark := watermarkGroup[len(watermarkGroup)-1]

				startTime = time.Now()
				var merged []*model.PolymorphicEvent
				mergeEvents(toSort, sorted, func(entry *model.PolymorphicEvent) {
					if entry.CRTs <= maxWatermark {
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
func (es *EntrySorter) AddEntry(_ context.Context, entry *model.PolymorphicEvent) {
	if atomic.LoadInt32(&es.closed) != 0 {
		return
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	if entry.IsWatermark() {
		es.watermarkGroup = append(es.watermarkGroup, entry.CRTs)
		es.resolvedNotifier.Notify()
	} else {
		es.unsorted = append(es.unsorted, entry)
	}
}

// Output returns the sorted raw kv output channel
func (es *EntrySorter) Output() <-chan *model.PolymorphicEvent {
	return es.outputCh
}

func eventLess(i *model.PolymorphicEvent, j *model.PolymorphicEvent) bool {
	return model.ComparePolymorphicEvents(i, j)
}

func mergeEvents(kvsA []*model.PolymorphicEvent, kvsB []*model.PolymorphicEvent, output func(*model.PolymorphicEvent)) {
	var i, j int
	for i < len(kvsA) && j < len(kvsB) {
		if eventLess(kvsA[i], kvsB[j]) {
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
