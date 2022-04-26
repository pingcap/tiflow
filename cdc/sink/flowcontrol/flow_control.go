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
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// TableFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableFlowController struct {
	memoryQuota *tableMemoryQuota

	queueMu struct {
		sync.Mutex
		queue deque.Deque
	}

	lastCommitTs uint64
}

type commitTsSizeEntry struct {
	commitTs uint64
	size     uint64
}

// NewTableFlowController creates a new TableFlowController
func NewTableFlowController(quota uint64) *TableFlowController {
	return &TableFlowController{
		memoryQuota: newTableMemoryQuota(quota),
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
func (c *TableFlowController) Consume(commitTs uint64, size uint64, blockCallBack func() error) error {
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)

	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", c.lastCommitTs))
	}

	if commitTs > lastCommitTs {
		atomic.StoreUint64(&c.lastCommitTs, commitTs)
		err := c.memoryQuota.consumeWithBlocking(size, blockCallBack)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// Here commitTs == lastCommitTs, which means that we are not crossing
		// a transaction boundary. In this situation, we use `forceConsume` because
		// blocking the event stream mid-transaction is highly likely to cause
		// a deadlock.
		// TODO fix this in the future, after we figure out how to elegantly support large txns.
		err := c.memoryQuota.forceConsume(size)
		if err != nil {
			return errors.Trace(err)
		}
	}

	c.queueMu.Lock()
	defer c.queueMu.Unlock()
	c.queueMu.queue.PushBack(&commitTsSizeEntry{
		commitTs: commitTs,
		size:     size,
	})

	return nil
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (c *TableFlowController) Release(resolvedTs uint64) {
	var nBytesToRelease uint64

	c.queueMu.Lock()
	for c.queueMu.queue.Len() > 0 {
		if peeked := c.queueMu.queue.Front().(*commitTsSizeEntry); peeked.commitTs <= resolvedTs {
			nBytesToRelease += peeked.size
			c.queueMu.queue.PopFront()
		} else {
			break
		}
	}
	c.queueMu.Unlock()

	c.memoryQuota.release(nBytesToRelease)
}

// Abort interrupts any ongoing Consume call
func (c *TableFlowController) Abort() {
	c.memoryQuota.abort()
}

// GetConsumption returns the current memory consumption
func (c *TableFlowController) GetConsumption() uint64 {
	return c.memoryQuota.getConsumption()
}
