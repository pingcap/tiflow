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

package common

import (
	"log"
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"go.uber.org/zap"
)

// TableMemorySizeController is designed to curb the total memory consumption of processing
// the event streams in a table.
// A higher-level controller more suitable for direct use by the processor is TableFlowController.
type TableMemorySizeController struct {
	Quota uint64 // should not be changed once intialized

	mu       sync.Mutex
	Consumed uint64

	cond *sync.Cond
}

// NewTableMemorySizeController creates a new TableMemorySizeController
// quota: max advised memory consumption in bytes.
func NewTableMemorySizeController(quota uint64) *TableMemorySizeController {
	ret := &TableMemorySizeController{
		Quota:    quota,
		mu:       sync.Mutex{},
		Consumed: 0,
	}

	ret.cond = sync.NewCond(&ret.mu)
	return ret
}

// ConsumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by Release.
// Should be used with care to prevent deadlock.
func (c *TableMemorySizeController) ConsumeWithBlocking(nBytes uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for c.Consumed+nBytes >= c.Quota {
		c.cond.Wait()
	}

	c.Consumed += nBytes
}

// ForceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (c *TableMemorySizeController) ForceConsume(nBytes uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.Consumed += nBytes
}

// Release is called when a chuck of memory is done being used.
func (c *TableMemorySizeController) Release(nBytes uint64) {
	c.mu.Lock()

	if c.Consumed < nBytes {
		defer c.mu.Unlock()
		log.Panic("TableMemorySizeController: releasing more than consumed, report a bug",
			zap.Uint64("consumed", c.Consumed),
			zap.Uint64("released", nBytes))
	}

	c.Consumed -= nBytes
	if c.Consumed < c.Quota {
		c.mu.Unlock()
		c.cond.Signal()
		return
	}

	c.mu.Unlock()
}

// TableFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableFlowController struct {
	memoryController *TableMemorySizeController

	mu    sync.Mutex
	queue deque.Deque

	lastCommitTs uint64
}

type commitTsSizeEntry struct {
	CommitTs uint64
	Size     uint64
}

// NewTableFlowController creates a new TableFlowController
func NewTableFlowController(quota uint64) *TableFlowController {
	return &TableFlowController{
		memoryController: NewTableMemorySizeController(quota),
		queue:            deque.NewDeque(),
	}
}

// Consume is called when an event has arrived for being processed by the sink.
// It will handle transaction boundaries automatically, and will not block intra-transaction.
func (c *TableFlowController) Consume(commitTs uint64, size uint64) {
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)

	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", c.lastCommitTs))
	}

	if commitTs > lastCommitTs {
		atomic.StoreUint64(&c.lastCommitTs, commitTs)
		c.memoryController.ConsumeWithBlocking(size)
	} else {
		// commitTs == lastCommitTs
		c.memoryController.ForceConsume(size)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue.PushBack(&commitTsSizeEntry{
		CommitTs: commitTs,
		Size:     size,
	})
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (c *TableFlowController) Release(resolvedTs uint64) {
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)
	if lastCommitTs < resolvedTs {
		log.Panic("lastCommitTs is less than resolvedTs",
			zap.Uint64("lastCommitTs", lastCommitTs),
			zap.Uint64("resolvedTs", resolvedTs))
	}

	var nBytesToRelease uint64

	c.mu.Lock()
	for c.queue.Len() > 0 {
		if peeked := c.queue.Front().(*commitTsSizeEntry); peeked.CommitTs <= resolvedTs {
			nBytesToRelease += peeked.Size
			c.queue.PopFront()
		} else {
			break
		}
	}
	c.mu.Unlock()

	c.memoryController.Release(nBytesToRelease)
}
