package puller

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"

	"github.com/pingcap/ticdc/cdc/model"
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
		resolvedCh: make(chan uint64, 4096),
		output:     make(chan *model.RawKVEntry, 1024),
	}
}

// Run runs EntrySorter
func (es *EntrySorter) Run(ctx context.Context) {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	tableID := util.TableIDFromCtx(ctx)
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
		var sorted []*model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				atomic.StoreInt32(&es.closed, 1)
				close(es.output)
				close(es.resolvedCh)
				return
			case resolvedTs := <-es.resolvedCh:
				es.lock.Lock()
				toSort := es.unsorted
				es.unsorted = nil
				es.lock.Unlock()
				log.Info("print buffer size", zap.Int("resolvedCh", len(es.resolvedCh)), zap.Int("toSort", len(toSort)), zap.Int("sorted", len(sorted)))

				t1 := time.Now()
				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})
				log.Info("sort cost", zap.Duration("c", time.Now().Sub(t1)))

				t1 = time.Now()
				var merged []*model.RawKVEntry
				mergeFunc(toSort, sorted, func(entry *model.RawKVEntry) {
					if entry.Ts <= resolvedTs {
						es.output <- entry
					} else {
						merged = append(merged, entry)
					}
				})
				log.Info("merge cost", zap.Duration("c", time.Now().Sub(t1)))
				tableSortedResolvedTsGauge.WithLabelValues(changefeedID, captureID, strconv.FormatInt(tableID, 10)).Set(float64(oracle.ExtractPhysical(resolvedTs)))
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
