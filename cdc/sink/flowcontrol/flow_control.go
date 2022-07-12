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
	"github.com/pingcap/tiflow/cdc/model"
	"go.uber.org/zap"
)

const (
	defaultRowsPerTxn = 1024
	defaultSizePerTxn = 1024 * 1024 /* 1MB */
	defaultBatchSize  = 100
)

// TableFlowController provides a convenient interface to control the memory consumption of a per table event stream
type TableFlowController struct {
	memoryQuota  *tableMemoryQuota
	lastCommitTs uint64

	queueMu struct {
		sync.Mutex
		queue deque.Deque
	}

	redoLogEnabled bool
	splitTxn       bool

	// batchGroupCount is the number of txnSizeEntries with same commitTs, which could be:
	// 1. Different txns with same commitTs but different startTs
	// 2. TxnSizeEntry split from the same txns which exceeds max rows or max size
	batchGroupCount uint64
	batchID         uint64

	batchSize     uint64
	maxRowsPerTxn uint64
	maxSizePerTxn uint64
}

type txnSizeEntry struct {
	// txn id
	startTs  uint64
	commitTs uint64

	size     uint64
	rowCount uint64
	batchID  uint64
}

// NewTableFlowController creates a new TableFlowController
func NewTableFlowController(quota uint64, redoLogEnabled bool, splitTxn bool) *TableFlowController {
	log.Info("create table flow controller",
		zap.Uint64("quota", quota),
		zap.Bool("redoLogEnabled", redoLogEnabled),
		zap.Bool("splitTxn", splitTxn))
	maxSizePerTxn := uint64(defaultSizePerTxn)
	if maxSizePerTxn > quota {
		maxSizePerTxn = quota
	}

	return &TableFlowController{
		memoryQuota: newTableMemoryQuota(quota),
		queueMu: struct {
			sync.Mutex
			queue deque.Deque
		}{
			queue: deque.NewDeque(),
		},
		redoLogEnabled: redoLogEnabled,
		splitTxn:       splitTxn,
		batchSize:      defaultBatchSize,
		maxRowsPerTxn:  defaultRowsPerTxn,
		maxSizePerTxn:  maxSizePerTxn,
	}
}

// Consume is called when an event has arrived for being processed by the sink.
// It will handle transaction boundaries automatically, and will not block intra-transaction.
func (c *TableFlowController) Consume(
	msg *model.PolymorphicEvent,
	size uint64,
	callBack func(batchID uint64) error,
) error {
	commitTs := msg.CRTs
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)
	blockingCallBack := func() (err error) {
		if commitTs > lastCommitTs || c.splitTxn {
			// Call `callback` in two condition:
			// 1. commitTs > lastCommitTs, handle new txn and send a normal resolved ts
			// 2. commitTs == lastCommitTs && splitTxn = true, split the same txn and
			// send a batch resolved ts
			err = callBack(c.batchID)
		}

		if commitTs == lastCommitTs {
			c.batchID++
			c.resetBatch(lastCommitTs, commitTs)
		}
		return err
	}

	if commitTs < lastCommitTs {
		log.Panic("commitTs regressed, report a bug",
			zap.Uint64("commitTs", commitTs),
			zap.Uint64("lastCommitTs", c.lastCommitTs))
	}

	if commitTs == lastCommitTs && (c.redoLogEnabled || !c.splitTxn) {
		// Here `commitTs == lastCommitTs` means we are not crossing transaction
		// boundaries, `c.redoLogEnabled || !c.splitTxn` means batch resolved mode
		// are not supported, hence we should use `forceConsume` to avoid deadlock.
		if err := c.memoryQuota.forceConsume(size); err != nil {
			return errors.Trace(err)
		}
	} else {
		if err := c.memoryQuota.consumeWithBlocking(size, blockingCallBack); err != nil {
			return errors.Trace(err)
		}
	}

	c.enqueueSingleMsg(msg, size, blockingCallBack)
	return nil
}

// Release releases the memory quota based on the given resolved timestamp.
func (c *TableFlowController) Release(resolved model.ResolvedTs) {
	var nBytesToRelease uint64

	c.queueMu.Lock()
	for c.queueMu.queue.Len() > 0 {
		peeked := c.queueMu.queue.Front().(*txnSizeEntry)
		if peeked.commitTs < resolved.Ts ||
			(peeked.commitTs == resolved.Ts && peeked.batchID <= resolved.BatchID) {
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
func (c *TableFlowController) enqueueSingleMsg(
	msg *model.PolymorphicEvent, size uint64, callback func() error,
) {
	commitTs := msg.CRTs
	lastCommitTs := atomic.LoadUint64(&c.lastCommitTs)

	c.queueMu.Lock()
	defer c.queueMu.Unlock()

	e := c.queueMu.queue.Back()
	// 1. Processing a new txn with different commitTs.
	if e == nil || lastCommitTs < commitTs {
		atomic.StoreUint64(&c.lastCommitTs, commitTs)
		c.resetBatch(lastCommitTs, commitTs)
		c.addEntry(msg, size)
		return
	}

	// Processing txns with the same commitTs.
	txnEntry := e.(*txnSizeEntry)
	if txnEntry.commitTs != lastCommitTs {
		log.Panic("got wrong commitTs from deque in flow control, report a bug",
			zap.Uint64("lastCommitTs", c.lastCommitTs),
			zap.Uint64("commitTsInDeque", txnEntry.commitTs))
	}

	// 2. Append row to current txn entry.
	if txnEntry.batchID == c.batchID && txnEntry.startTs == msg.Row.StartTs &&
		txnEntry.rowCount < c.maxRowsPerTxn && txnEntry.size < c.maxSizePerTxn {
		txnEntry.size += size
		txnEntry.rowCount++
		return
	}

	// 3. Split the txn or handle a new txn with the same commitTs.
	if c.batchGroupCount >= c.batchSize {
		_ = callback()
	}
	c.addEntry(msg, size)
}

// addEntry should be called only if c.queueMu is locked.
func (c *TableFlowController) addEntry(msg *model.PolymorphicEvent, size uint64) {
	c.batchGroupCount++
	c.queueMu.queue.PushBack(&txnSizeEntry{
		startTs:  msg.StartTs,
		commitTs: msg.CRTs,
		size:     size,
		rowCount: 1,
		batchID:  c.batchID,
	})
	if c.splitTxn {
		msg.Row.SplitTxn = true
	}
}

// resetBatch reset batchID and batchGroupCount if handling a new txn, Otherwise,
// just reset batchGroupCount.
func (c *TableFlowController) resetBatch(lastCommitTs, commitTs uint64) {
	if lastCommitTs < commitTs {
		// At least one batch for each txn.
		c.batchID = 1
	}
	c.batchGroupCount = 0
}

// Abort interrupts any ongoing Consume call
func (c *TableFlowController) Abort() {
	c.memoryQuota.abort()
}

// GetConsumption returns the current memory consumption
func (c *TableFlowController) GetConsumption() uint64 {
	return c.memoryQuota.getConsumption()
}
