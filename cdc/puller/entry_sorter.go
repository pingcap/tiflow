package puller

import (
	"context"
	"sort"
	"sync"

	"github.com/pingcap/ticdc/cdc/model"
)

// EntrySorter is used to sort entries outputted from puller
type EntrySorter struct {
	unsorted   []*model.RawKVEntry
	resolvedCh chan uint64
	lock       sync.Mutex

	output chan *model.RawKVEntry
}

// NewEntrySorter creates a new EntrySorter
func NewEntrySorter() *EntrySorter {
	return &EntrySorter{
		resolvedCh: make(chan uint64, 1024),
		output:     make(chan *model.RawKVEntry, 128),
	}
}

// Run runs the EntrySorter, sort the entries
func (es *EntrySorter) Run(ctx context.Context) {
	lessFunc := func(i *model.RawKVEntry, j *model.RawKVEntry) bool {
		if i.Ts == j.Ts {
			return i.OpType == model.OpTypeDelete && j.OpType == model.OpTypePut
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
		} else {
			min = kvsB[*j]
			*j += 1
		}
		return
	}

	go func() {
		var sorted []*model.RawKVEntry
		for {
			select {
			case <-ctx.Done():
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
				sendResolved := false
				for {
					minEvent := mergeFunc(toSort, sorted, &i, &j)
					if minEvent == nil {
						sorted = merged
						if !sendResolved {
							es.output <- &model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved}
						}
						break
					}
					if minEvent.Ts > resolvedTs {
						if !sendResolved {
							es.output <- &model.RawKVEntry{Ts: resolvedTs, OpType: model.OpTypeResolved}
							sendResolved = true
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

// AddEntry adds an RawKVEntry to the EntryGroup, this method *IS NOT* thread safe.
func (es *EntrySorter) AddEntry(entry *model.RawKVEntry) {
	if entry.OpType == model.OpTypeResolved {
		es.resolvedCh <- entry.Ts
		return
	}
	es.lock.Lock()
	defer es.lock.Unlock()
	es.unsorted = append(es.unsorted, entry)
}

// Output returns a channel with sorted entries
func (es *EntrySorter) Output() <-chan *model.RawKVEntry {
	return es.output
}
