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

package sink

import (
	"container/heap"

	"github.com/pingcap/tiflow/cdc/model"
)

type innerTxnsHeap []innerHeapEntry

type innerHeapEntry struct {
	ts     uint64
	bucket int
}

func (h innerTxnsHeap) Len() int           { return len(h) }
func (h innerTxnsHeap) Less(i, j int) bool { return h[i].ts < h[j].ts }
func (h innerTxnsHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *innerTxnsHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(innerHeapEntry))
}

func (h *innerTxnsHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type txnsHeap struct {
	inner     *innerTxnsHeap
	txnsGroup [][]*model.SingleTableTxn
}

func newTxnsHeap(txnsMap map[model.TableID][]*model.SingleTableTxn) *txnsHeap {
	txnsGroup := make([][]*model.SingleTableTxn, 0, len(txnsMap))
	for _, txns := range txnsMap {
		txnsGroup = append(txnsGroup, txns)
	}
	inner := make(innerTxnsHeap, 0, len(txnsGroup))
	heap.Init(&inner)
	for bucket, txns := range txnsGroup {
		if len(txns) == 0 {
			continue
		}
		entry := innerHeapEntry{ts: txns[0].CommitTs, bucket: bucket}
		heap.Push(&inner, entry)
	}
	return &txnsHeap{inner: &inner, txnsGroup: txnsGroup}
}

func (h *txnsHeap) iter(fn func(txn *model.SingleTableTxn)) {
	for {
		if h.inner.Len() == 0 {
			break
		}
		minEntry := heap.Pop(h.inner).(innerHeapEntry)
		bucket := minEntry.bucket
		fn(h.txnsGroup[bucket][0])
		h.txnsGroup[bucket] = h.txnsGroup[bucket][1:]
		if len(h.txnsGroup[bucket]) > 0 {
			heap.Push(h.inner, innerHeapEntry{
				ts:     h.txnsGroup[bucket][0].CommitTs,
				bucket: bucket,
			})
		}
	}
}
