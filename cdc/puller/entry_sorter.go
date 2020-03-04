package puller

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	ee "github.com/pingcap/ticdc/cdc/entry"
	"github.com/pingcap/ticdc/cdc/model"
)

type EntrySorter struct {
	unsorted   []*model.RawKVEntry
	resolvedCh chan uint64
	lock       sync.Mutex
	resolvedTs uint64
	debug      bool
	closed     int32

	output chan *model.RawKVEntry
}

func NewEntrySorter(debug bool) *EntrySorter {
	return &EntrySorter{
		resolvedCh: make(chan uint64, 1024),
		output:     make(chan *model.RawKVEntry, 128),
		debug:      debug,
	}
}

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

				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})

				if es.debug {
					log.Info("sorter resolved", zap.Uint64("ts", resolvedTs))
					for _, entry := range toSort {
						job, _ := ee.UnmarshalDDL(entry, false)
						if job != nil {
							log.Info("toSort accept", zap.Reflect("job", job))
						}
					}
					for _, entry := range sorted {
						job, _ := ee.UnmarshalDDL(entry, false)
						if job != nil {
							log.Info("sorted accept", zap.Reflect("job", job))
						}
					}
				}

				var merged []*model.RawKVEntry
				mergeFunc(toSort, sorted, func(entry *model.RawKVEntry) {
					if entry.Ts <= resolvedTs {
						es.output <- entry
						job, _ := ee.UnmarshalDDL(entry, false)
						if job != nil {
							log.Info("sort output accept", zap.Reflect("job", job))
						}
					} else {
						merged = append(merged, entry)
						job, _ := ee.UnmarshalDDL(entry, false)
						if job != nil {
							log.Info("append merged accept", zap.Reflect("job", job))
						}
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
	if es.debug {
		job, _ := ee.UnmarshalDDL(entry, false)
		if job != nil {
			log.Info("sorter accept", zap.Reflect("job", job))
		}
	}
	rts := atomic.LoadUint64(&es.resolvedTs)
	if entry.Ts <= rts {
		job, _ := ee.UnmarshalDDL(entry, false)
		if job != nil {
			log.Info("sorter less rts", zap.Bool("eq", rts == entry.Ts), zap.Reflect("job", job))
		}
	}
	es.unsorted = append(es.unsorted, entry)

}

func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.output
}
