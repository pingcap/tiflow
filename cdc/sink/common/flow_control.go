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
	"sync"
	"sync/atomic"

	"github.com/edwingeng/deque"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// TableMemoryQuota is designed to curb the total memory consumption of processing
// the event streams.
// TableMemoryQuota can be used to implement flow controller in different level.
// A higher-level controller more suitable for direct use by the pipeline is TableFlowController.
type TableMemoryQuota struct {
	Quota uint64 // should not be changed once initialized

	IsAborted uint32

	mu       sync.Mutex
	Consumed uint64

	cond *sync.Cond
}

// NewTableMemoryQuota creates a new TableMemoryQuota
// quota: max advised memory consumption in bytes.
func NewTableMemoryQuota(quota uint64) *TableMemoryQuota {
	ret := &TableMemoryQuota{
		Quota:    quota,
		mu:       sync.Mutex{},
		Consumed: 0,
	}

	ret.cond = sync.NewCond(&ret.mu)
	return ret
}

// ConsumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by Release.
// blockCallBack will be called if the function will block.
// Should be used with care to prevent deadlock.
func (m *TableMemoryQuota) ConsumeWithBlocking(nBytes uint64, blockCallBack func() error) error {
	if nBytes >= m.Quota {
		return cerrors.ErrFlowControllerEventLargerThanQuota.GenWithStackByArgs(nBytes, m.Quota)
	}

	m.mu.Lock()
	if m.Consumed+nBytes >= m.Quota {
		m.mu.Unlock()
		err := blockCallBack()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		m.mu.Unlock()
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for {
		if atomic.LoadUint32(&m.IsAborted) == 1 {
			return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
		}

		if m.Consumed+nBytes < m.Quota {
			break
		}
		m.cond.Wait()
	}

	m.Consumed += nBytes
	return nil
}

// ForceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (m *TableMemoryQuota) ForceConsume(nBytes uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if atomic.LoadUint32(&m.IsAborted) == 1 {
		return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
	}

	m.Consumed += nBytes
	return nil
}

// Release is called when a chuck of memory is done being used.
func (m *TableMemoryQuota) Release(nBytes uint64) {
	m.mu.Lock()

	if m.Consumed < nBytes {
		m.mu.Unlock()
		log.Panic("TableMemoryQuota: releasing more than consumed, report a bug",
			zap.Uint64("consumed", m.Consumed),
			zap.Uint64("released", nBytes))
	}

	m.Consumed -= nBytes
	if m.Consumed < m.Quota {
		m.mu.Unlock()
		m.cond.Signal()
		return
	}

	m.mu.Unlock()
}

// Abort interrupts any ongoing ConsumeWithBlocking call
func (m *TableMemoryQuota) Abort() {
	atomic.StoreUint32(&m.IsAborted, 1)
	m.cond.Signal()
}

// GetConsumption returns the current memory consumption
func (m *TableMemoryQuota) GetConsumption() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Consumed
}

// TableFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableFlowController struct {
	memoryQuota *TableMemoryQuota

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
		memoryQuota: NewTableMemoryQuota(quota),
		queue:       deque.NewDeque(),
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
		err := c.memoryQuota.ConsumeWithBlocking(size, blockCallBack)
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		// Here commitTs == lastCommitTs, which means that we are not crossing
		// a transaction boundary. In this situation, we use `ForceConsume` because
		// blocking the event stream mid-transaction is highly likely to cause
		// a deadlock.
		// TODO fix this in the future, after we figure out how to elegantly support large txns.
		err := c.memoryQuota.ForceConsume(size)
		if err != nil {
			return errors.Trace(err)
		}
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.queue.PushBack(&commitTsSizeEntry{
		CommitTs: commitTs,
		Size:     size,
	})

	return nil
}

// Release is called when all events committed before resolvedTs has been freed from memory.
func (c *TableFlowController) Release(resolvedTs uint64) {
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

	c.memoryQuota.Release(nBytesToRelease)
}

// Abort interrupts any ongoing Consume call
func (c *TableFlowController) Abort() {
	c.memoryQuota.Abort()
}

// GetConsumption returns the current memory consumption
func (c *TableFlowController) GetConsumption() uint64 {
	return c.memoryQuota.GetConsumption()
}
