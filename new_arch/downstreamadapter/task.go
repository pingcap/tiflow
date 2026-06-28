// Copyright 2024 PingCAP, Inc.
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

package downstreamadapter

import "github.com/pingcap/tiflow/cdc/processor/tablepb"

type PullEventTask struct {
	span    *tablepb.Span
	StartTs uint64
	fn      func() (map[uint64]uint64, error)
}

// 实现 container/heap
type TaskHeap []*PullEventTask

func (th TaskHeap) Len() int {
	return len(th)
}

// 自定义比较顺序: 按照任务的timestamp降序
func (th TaskHeap) Less(i, j int) bool {
	return th[i].StartTs < th[j].StartTs
}

func (th TaskHeap) Swap(i, j int) {
	th[i], th[j] = th[j], th[i]
}

// Push 和 Pop 的实现 必须由 接口调用者来实现, heap 包并不提供这样的实现
func (th *TaskHeap) Push(x interface{}) {
	item := x.(*PullEventTask)
	*th = append(*th, item)
}

// 弹出最后一个元素
func (th *TaskHeap) Pop() interface{} {
	item := (*th)[len(*th)-1]
	*th = (*th)[:len(*th)-1]
	return item
}
