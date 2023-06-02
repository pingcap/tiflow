// Copyright 2022 PingCAP, Inc.
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

package state

import "sync/atomic"

// TableSinkState is state of the table sink.
type TableSinkState int32

// State for table sink
const (
	TableSinkStateUnknown TableSinkState = iota
	// TableSinkSinking indicate that the table is sinking.
	TableSinkSinking
	// TableSinkStopping means the table sink is stopping, but not guaranteed yet.
	TableSinkStopping
	// TableSinkStopped means sink stop all works.
	TableSinkStopped
)

var stateStringMap = map[TableSinkState]string{
	TableSinkStateUnknown: "Unknown",
	TableSinkSinking:      "Sinking",
	TableSinkStopping:     "Stopping",
	TableSinkStopped:      "Stopped",
}

func (s TableSinkState) String() string {
	return stateStringMap[s]
}

// Load state.
// This is THREAD-SAFE.
func (s *TableSinkState) Load() TableSinkState {
	return TableSinkState(atomic.LoadInt32((*int32)(s)))
}

// Store state.
// This is THREAD-SAFE.
func (s *TableSinkState) Store(new TableSinkState) {
	atomic.StoreInt32((*int32)(s), int32(new))
}

// CompareAndSwap is just like sync/atomic.Atomic*.CompareAndSwap.
func (s *TableSinkState) CompareAndSwap(old, new TableSinkState) bool {
	oldx := int32(old)
	newx := int32(new)
	return atomic.CompareAndSwapInt32((*int32)(s), oldx, newx)
}
