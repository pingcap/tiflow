// Copyright 2021 PingCAP, Inc.
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

package leveldb

import (
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sorter/leveldb/message"
	"go.uber.org/zap"
)

// outputBuffer a struct that facilitate leveldb table sorter.
type outputBuffer struct {
	// A slice of keys need to be deleted.
	deleteKeys []message.Key
	// A slice of resolved events that have the same commit ts.
	resolvedEvents []*model.PolymorphicEvent
	// partialReadTxn is set to true when a transaction is partially read.
	partialReadTxn bool

	advisedCapacity int
}

func newOutputBuffer(advisedCapacity int) *outputBuffer {
	return &outputBuffer{
		deleteKeys:      make([]message.Key, 0, advisedCapacity),
		resolvedEvents:  make([]*model.PolymorphicEvent, 0, advisedCapacity),
		advisedCapacity: advisedCapacity,
	}
}

// maybeShrink try to shrink slices to the advised capacity.
func (b *outputBuffer) maybeShrink() {
	if len(b.deleteKeys) < b.advisedCapacity {
		if cap(b.deleteKeys) > b.advisedCapacity {
			buf := make([]message.Key, 0, b.advisedCapacity)
			buf = append(buf, b.deleteKeys...)
			b.deleteKeys = buf
		}
	}
	if len(b.resolvedEvents) < b.advisedCapacity {
		if cap(b.resolvedEvents) > b.advisedCapacity {
			buf := make([]*model.PolymorphicEvent, 0, b.advisedCapacity)
			buf = append(buf, b.resolvedEvents...)
			b.resolvedEvents = buf
		}
	}
}

// In place left shift resolved events slice. After the call,
// `index` will become the first element in the slice
func (b *outputBuffer) shiftResolvedEvents(index int) {
	if index > len(b.resolvedEvents) {
		log.Panic("index out of range", zap.Int("len", len(b.resolvedEvents)))
	}
	if index != 0 {
		length := len(b.resolvedEvents)
		for left, right := 0, index; right < length; right++ {
			b.resolvedEvents[left] = b.resolvedEvents[right]
			// Set original element to nil to help GC.
			b.resolvedEvents[right] = nil
			left++
		}
		b.resolvedEvents = b.resolvedEvents[:length-index]
	}
}

// tryAppendResolvedEvent try to append resolved events to the buffer.
// Return false if the buffer is full and append fails.
func (b *outputBuffer) tryAppendResolvedEvent(event *model.PolymorphicEvent) bool {
	if len(b.resolvedEvents) > 0 {
		if b.resolvedEvents[0].CRTs != event.CRTs {
			log.Panic("commit ts must be equal",
				zap.Uint64("newCommitTs", event.CRTs),
				zap.Uint64("commitTs", b.resolvedEvents[0].CRTs))
		}
	} else if len(b.resolvedEvents) == 0 {
		// Reset if it appends a new transaction.
		b.partialReadTxn = false
	}
	if len(b.resolvedEvents) >= b.advisedCapacity {
		// buffer is full, and the commit ts of event is the same,
		// we must be in the middle of a transaction.
		b.partialReadTxn = true
		return false
	}
	b.resolvedEvents = append(b.resolvedEvents, event)
	return true
}

// appendDeleteKey appends to-be-deleted keys to the buffer.
func (b *outputBuffer) appendDeleteKey(key message.Key) {
	b.deleteKeys = append(b.deleteKeys, key)
}

// resetDeleteKey reset deleteKeys to a zero len slice.
func (b *outputBuffer) resetDeleteKey() {
	b.deleteKeys = b.deleteKeys[:0]
}

// len returns the length of resolvedEvents and delete keys.
func (b *outputBuffer) len() (int, int) {
	return len(b.resolvedEvents), len(b.deleteKeys)
}
