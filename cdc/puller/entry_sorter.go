package puller

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/log"

	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/tidb/store/tikv/oracle"

	"github.com/pingcap/ticdc/cdc/model"
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
		resolvedNotify: make(chan struct{}, 128),
		outputCh:       make(chan *model.RawKVEntry, 1024),
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
				close(es.outputCh)
				close(es.resolvedNotify)
				return
			case <-es.resolvedNotify:
				es.lock.Lock()
				toSort := es.unsorted
				es.unsorted = nil
				resolvedTsGroup := es.resolvedTsGroup
				es.resolvedTsGroup = nil
				es.lock.Unlock()
				log.Info("notf")
				if len(resolvedTsGroup) == 0 {
					continue
				}

				sort.Slice(toSort, func(i, j int) bool {
					return lessFunc(toSort[i], toSort[j])
				})
				log.Info("notf1")

				var merged []*model.RawKVEntry
				resolvedTsIndex := 0
				mergeFunc(toSort, sorted, func(entry *model.RawKVEntry) {
					if resolvedTsIndex >= len(resolvedTsGroup) {
						merged = append(merged, entry)
						return
					}
					var lastEvent *model.RawKVEntry
					for ; resolvedTsIndex < len(resolvedTsGroup); resolvedTsIndex++ {
						if entry.Ts <= resolvedTsGroup[resolvedTsIndex] {
							es.output(ctx, entry)
						} else {
							es.output(ctx, &model.RawKVEntry{Ts: resolvedTsGroup[resolvedTsIndex], OpType: model.OpTypeResolved})
							tableSortedResolvedTsGauge.WithLabelValues(changefeedID, captureID, strconv.FormatInt(tableID, 10)).Set(float64(oracle.ExtractPhysical(resolvedTsGroup[resolvedTsIndex])))
						}
					}
				})

				log.Info("notf2")
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
		es.resolvedNotify <- struct{}{}
	} else {
		es.unsorted = append(es.unsorted, entry)
	}
	es.lock.Unlock()
}

// Output returns the sorted raw kv output channel
func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.outputCh
}

func (es *EntrySorter) output(ctx context.Context, entry *model.RawKVEntry) {
	select {
	case <-ctx.Done():
		return
	case es.outputCh <- entry:
	}
}
