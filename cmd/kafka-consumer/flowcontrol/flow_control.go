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

package flowcontrol

import (
	"sync"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
)

// FlowController provides a convenient interface to control the memory consumption of a per table event stream
type FlowController struct {
	memoryQuota *MemoryQuota

	queueMu struct {
		sync.Mutex
		queue deque.Deque
	}
}

type entry struct {
	commitTs uint64
	size     uint64
}

// NewFlowController creates a new FlowController
func NewFlowController(quota uint64) *FlowController {
	return &FlowController{
		memoryQuota: newMemoryQuota(quota),
		queueMu: struct {
			sync.Mutex
			queue deque.Deque
		}{
			queue: deque.NewDeque(),
		},
	}
}

// Consume is called when an event has arrived for being processed by the sink.
// It will handle transaction boundaries automatically, and will not block intra-transaction.
func (c *FlowController) Consume(commitTs uint64, size uint64) error {
	err := c.memoryQuota.consumeWithBlocking(size)
	if err != nil {
		return errors.Trace(err)
	}

	c.enqueueSingleMsg(commitTs, size)

	return nil
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (c *FlowController) Release(resolvedTs uint64) {
	var nBytesToRelease uint64

	c.queueMu.Lock()
	for c.queueMu.queue.Len() > 0 {
		if peeked := c.queueMu.queue.Front().(*entry); peeked.commitTs <= resolvedTs {
			nBytesToRelease += peeked.size
			c.queueMu.queue.PopFront()
		} else {
			break
		}
	}
	c.queueMu.Unlock()

	c.memoryQuota.release(nBytesToRelease)
}

// Note that msgs received by enqueueSingleMsg must be sorted by commitTs_startTs order.
func (c *FlowController) enqueueSingleMsg(commitTs, size uint64) {
	c.queueMu.Lock()
	defer c.queueMu.Unlock()

	c.queueMu.queue.PushBack(&entry{
		commitTs: commitTs,
		size:     size,
	})
}

// Abort interrupts any ongoing Consume call
func (c *FlowController) Abort() {
	c.memoryQuota.abort()
}

// GetConsumption returns the current memory consumption
func (c *FlowController) GetConsumption() uint64 {
	return c.memoryQuota.getConsumption()
}
