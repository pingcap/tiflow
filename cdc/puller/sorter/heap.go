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

package sorter

import "github.com/pingcap/tiflow/cdc/model"

type sortItem struct {
	entry *model.PolymorphicEvent
	data  interface{}
}

type sortHeap []*sortItem

func (h sortHeap) Len() int { return len(h) }
func (h sortHeap) Less(i, j int) bool {
	if h[i].entry.CRTs == h[j].entry.CRTs {
		if h[j].entry.RawKV.OpType == model.OpTypeResolved && h[i].entry.RawKV.OpType != model.OpTypeResolved {
			return true
		}
		if h[i].entry.RawKV.OpType == model.OpTypeDelete && h[j].entry.RawKV.OpType != model.OpTypeDelete {
			return true
		}
	}
	return h[i].entry.CRTs < h[j].entry.CRTs
}
func (h sortHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *sortHeap) Push(x interface{}) {
	*h = append(*h, x.(*sortItem))
}

func (h *sortHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*h = old[0 : n-1]
	return x
}
