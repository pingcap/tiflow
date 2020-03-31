package puller

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/util"
)

// EntrySorter accepts out-of-order raw kv entries and output sorted entries
type EntrySorter struct {
	unsorted        []*model.RawKVEntry
	lock            sync.Mutex
	resolvedTsGroup []uint64
	closed          int32

	outputCh       chan *model.RawKVEntry
	resolvedNotify chan struct{}
}

// NewEntrySorter creates a new EntrySorter
func NewEntrySorter() *EntrySorter {
	return &EntrySorter{
		resolvedNotify: make(chan struct{}, 128000),
		outputCh:       make(chan *model.RawKVEntry, 128000),
	}
}

// Run runs EntrySorter
func (es *EntrySorter) Run(ctx context.Context) {
	lessFunc := func(i *model.RawKVEntry, j *model.RawKVEntry) bool {
		if i.Ts == j.Ts {
			if i.OpType == model.OpTypeDelete {
				return true
			}
			if j.OpType == model.OpTypeResolved {
				return true
			}
		}
		return i.Ts < j.Ts
	}
	mergeFunc := func(kvsA []*model.RawKVEntry, kvsB []*model.RawKVEntry, output func(*model.RawKVEntry)) {
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
	output := func(ctx context.Context, entry *model.RawKVEntry) {
		select {
		case <-ctx.Done():
			return
		case es.outputCh <- entry:
		}
	}

	go func() {
		captureID := util.CaptureIDFromCtx(ctx)
		changefeedID := util.ChangefeedIDFromCtx(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				entrySorterResolvedChanSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(es.resolvedNotify)))
				entrySorterOutputChanSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(es.outputCh)))
			}
		}
	}()

	go func() {
		var sorted []*model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&es.closed, 1)
				close(es.outputCh)
				return
			case <-es.resolvedNotify:
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

				resEvents := make([]*model.RawKVEntry, len(resolvedTsGroup))
				for i, rts := range resolvedTsGroup {
					resEvents[i] = &model.RawKVEntry{Ts: rts, OpType: model.OpTypeResolved}
				}
				toSort = append(toSort, resEvents...)
				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})
				maxResolvedTs := resolvedTsGroup[len(resolvedTsGroup)-1]
				var merged []*model.RawKVEntry
				mergeFunc(toSort, sorted, func(entry *model.RawKVEntry) {
					if entry.Ts <= maxResolvedTs {
						output(ctx, entry)
					} else {
						merged = append(merged, entry)
					}
				})
				sorted = merged
			}
		}
	}()
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (es *EntrySorter) AddEntry(entry *model.RawKVEntry) {
	if atomic.LoadInt32(&es.closed) != 0 {
		return
	}
	es.lock.Lock()
	if entry.OpType == model.OpTypeResolved {
		es.resolvedTsGroup = append(es.resolvedTsGroup, entry.Ts)
	} else {
		es.unsorted = append(es.unsorted, entry)
	}
	es.lock.Unlock()
	if entry.OpType == model.OpTypeResolved {
		es.resolvedNotify <- struct{}{}
	}
}

// Output returns the sorted raw kv output channel
func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.outputCh
}
