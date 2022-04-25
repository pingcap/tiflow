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

package flowcontrol

import (
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	cerrors "github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// tableMemoryQuota is designed to curb the total memory consumption of processing
// the event streams in a table.
// A higher-level controller more suitable for direct use by the processor is TableFlowController.
type tableMemoryQuota struct {
	quota uint64 // should not be changed once initialized

	isAborted atomic.Bool

	consumed struct {
		sync.Mutex
		bytes uint64
	}

	consumedCond *sync.Cond
}

// newTableMemoryQuota creates a new tableMemoryQuota
// quota: max advised memory consumption in bytes.
func newTableMemoryQuota(quota uint64) *tableMemoryQuota {
	ret := &tableMemoryQuota{
		quota: quota,
	}

	ret.consumedCond = sync.NewCond(&ret.consumed)
	return ret
}

// consumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by release.
// blockCallBack will be called if the function will block.
// Should be used with care to prevent deadlock.
func (c *tableMemoryQuota) consumeWithBlocking(nBytes uint64, blockCallBack func() error) error {
	if nBytes >= c.quota {
		return cerrors.ErrFlowControllerEventLargerThanQuota.GenWithStackByArgs(nBytes, c.quota)
	}

	c.consumed.Lock()
	if c.consumed.bytes+nBytes >= c.quota {
		c.consumed.Unlock()
		err := blockCallBack()
		if err != nil {
			return errors.Trace(err)
		}
	} else {
		c.consumed.Unlock()
	}

	c.consumed.Lock()
	defer c.consumed.Unlock()

	for {
		if c.isAborted.Load() {
			return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
		}

		if c.consumed.bytes+nBytes < c.quota {
			break
		}
		c.consumedCond.Wait()
	}

	c.consumed.bytes += nBytes
	return nil
}

// forceConsume is called when blocking is not acceptable and the limit can be violated
// for the sake of avoid deadlock. It merely records the increased memory consumption.
func (c *tableMemoryQuota) forceConsume(nBytes uint64) error {
	c.consumed.Lock()
	defer c.consumed.Unlock()

	if c.isAborted.Load() {
		return cerrors.ErrFlowControllerAborted.GenWithStackByArgs()
	}

	c.consumed.bytes += nBytes
	return nil
}

// release is called when a chuck of memory is done being used.
func (c *tableMemoryQuota) release(nBytes uint64) {
	c.consumed.Lock()

	if c.consumed.bytes < nBytes {
		c.consumed.Unlock()
		log.Panic("tableMemoryQuota: releasing more than consumed, report a bug",
			zap.Uint64("consumed", c.consumed.bytes),
			zap.Uint64("released", nBytes))
	}

	c.consumed.bytes -= nBytes
	if c.consumed.bytes < c.quota {
		c.consumed.Unlock()
		c.consumedCond.Signal()
		return
	}

	c.consumed.Unlock()
}

// abort interrupts any ongoing consumeWithBlocking call
func (c *tableMemoryQuota) abort() {
	c.isAborted.Store(true)
	c.consumedCond.Signal()
}

// getConsumption returns the current memory consumption
func (c *tableMemoryQuota) getConsumption() uint64 {
	c.consumed.Lock()
	defer c.consumed.Unlock()

	return c.consumed.bytes
}
