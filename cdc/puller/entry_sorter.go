package puller

import (
	"context"
	"sort"
	"sync"

	"github.com/pingcap/log"

	"github.com/pingcap/ticdc/cdc/model"
)

type EntrySorter struct {
	unsorted   []*model.RawKVEntry
	resolvedCh chan uint64
	lock       sync.Mutex

	output chan *model.RawKVEntry
}

func NewEntrySorter() *EntrySorter {
	return &EntrySorter{
		resolvedCh: make(chan uint64, 1024),
		output:     make(chan *model.RawKVEntry, 128),
	}
}

func (es *EntrySorter) Run(ctx context.Context) {
	lessFunc := func(i *model.RawKVEntry, j *model.RawKVEntry) bool {
		if i.Ts == j.Ts {
			return i.OpType == model.OpTypeDelete
		}
		return i.Ts < j.Ts
	}
	mergeFunc := func(kvsA []*model.RawKVEntry, kvsB []*model.RawKVEntry, i *int, j *int) (min *model.RawKVEntry) {
		if *i >= len(kvsA) && *j >= len(kvsB) {
			return nil
		}
		if *i >= len(kvsA) {
			min = kvsB[*j]
			*j += 1
			return
		}
		if *j >= len(kvsB) {
			min = kvsA[*i]
			*i += 1
			return
		}
		if lessFunc(kvsA[*i], kvsB[*j]) {
			min = kvsA[*i]
			*i += 1
			return
		} else {
			min = kvsB[*j]
			*j += 1
			return
		}
	}

	go func() {
		var sorted []*model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
				log.Info("EntrySorter quit")
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
				var i, j int
				var merged []*model.RawKVEntry
				resolvedSent := false
				for {
					minEvent := mergeFunc(toSort, sorted, &i, &j)
					if minEvent == nil {
						sorted = merged
						if !resolvedSent {
							es.output <- &model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved}
						}
						break
					}
					if minEvent.Ts > resolvedTs {
						if !resolvedSent {
							es.output <- &model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved}
							resolvedSent = true
						}
						if merged == nil {
							merged = make([]*model.RawKVEntry, 0, len(toSort)+len(sorted)-i-j+1)
						}
						merged = append(merged, minEvent)
						continue
					}
					es.output <- minEvent
				}
			}
		}
	}()
}

// AddEntry adds an RawKVEntry to the EntryGroup
func (es *EntrySorter) AddEntry(entry *model.RawKVEntry) {
	if entry.OpType == model.OpTypeResolved {
		es.resolvedCh <- entry.Ts
		return
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	es.unsorted = append(es.unsorted, entry)
}

func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.output
}
