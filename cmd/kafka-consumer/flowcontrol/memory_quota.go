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

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

var (
	errFlowControllerLargerThanQuota = errors.New("flow controller request memory larger than quota")
	ErrorFlowControllerAborted       = errors.New("flow controller aborted")
)

// MemoryQuota is designed to curb the total memory consumption of processing the events
// A higher-level controller more suitable for direct use by the processor is FlowController.
type MemoryQuota struct {
	quota uint64 // should not be changed once initialized

	isAborted atomic.Bool

	consumed struct {
		sync.Mutex
		bytes uint64
	}

	consumedCond *sync.Cond
}

// newMemoryQuota creates a new MemoryQuota
// quota: max advised memory consumption in bytes.
func newMemoryQuota(quota uint64) *MemoryQuota {
	ret := &MemoryQuota{
		quota: quota,
	}

	ret.consumedCond = sync.NewCond(&ret.consumed)
	return ret
}

// consumeWithBlocking is called when a hard-limit is needed. The method will
// block until enough memory has been freed up by release.
// blockCallBack will be called if the function will block.
// Should be used with care to prevent deadlock.
func (c *MemoryQuota) consumeWithBlocking(nBytes uint64) error {
	if nBytes >= c.quota {
		return errFlowControllerLargerThanQuota
	}

	c.consumed.Lock()
	defer c.consumed.Unlock()

	for {
		if c.isAborted.Load() {
			return ErrorFlowControllerAborted
		}

		if c.consumed.bytes+nBytes < c.quota {
			break
		}
		c.consumedCond.Wait()
	}

	c.consumed.bytes += nBytes
	return nil
}

// release is called when a chuck of memory is done being used.
func (c *MemoryQuota) release(nBytes uint64) {
	c.consumed.Lock()

	if c.consumed.bytes < nBytes {
		c.consumed.Unlock()
		log.Panic("MemoryQuota: releasing more than consumed, report a bug",
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
