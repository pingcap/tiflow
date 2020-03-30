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
	unsorted   []*model.RawKVEntry
	resolvedCh chan uint64
	lock       sync.Mutex
	resolvedTs uint64
	closed     int32

	output chan *model.RawKVEntry
}

// NewEntrySorter creates a new EntrySorter
func NewEntrySorter() *EntrySorter {
	return &EntrySorter{
		resolvedCh: make(chan uint64, 12800),
		output:     make(chan *model.RawKVEntry, 128000),
	}
}

// Run runs EntrySorter
func (es *EntrySorter) Run(ctx context.Context) {
	lessFunc := func(i *model.RawKVEntry, j *model.RawKVEntry) bool {
		if i.Ts == j.Ts {
			return i.OpType == model.OpTypeDelete
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

	go func() {
		captureID := util.CaptureIDFromCtx(ctx)
		changefeedID := util.ChangefeedIDFromCtx(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Minute):
				entrySorterResolvedChanSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(es.resolvedCh)))
				entrySorterOutputChanSizeGauge.WithLabelValues(captureID, changefeedID).Set(float64(len(es.output)))
			}
		}
	}()

	go func() {
		var sorted []*model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&es.closed, 1)
				close(es.output)
				return
			case resolvedTs := <-es.resolvedCh:
				es.lock.Lock()
				toSort := es.unsorted
				es.unsorted = nil
				es.lock.Unlock()

				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})

				var merged []*model.RawKVEntry
				mergeFunc(toSort, sorted, func(entry *model.RawKVEntry) {
					if entry.Ts <= resolvedTs {
						es.output <- entry
					} else {
						merged = append(merged, entry)
					}
				})
				es.output <- &model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved}
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
	if entry.OpType == model.OpTypeResolved {
		atomic.StoreUint64(&es.resolvedTs, entry.Ts)
		es.resolvedCh <- entry.Ts
		return
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	es.unsorted = append(es.unsorted, entry)

}

// Output returns the sorted raw kv output channel
func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.output
}
