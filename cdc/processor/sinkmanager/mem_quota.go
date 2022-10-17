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

package sinkmanager

import "github.com/pingcap/tiflow/cdc/model"

//nolint:varcheck,deadcode
const defaultMemoryUsage = 10 * 1024 * 1024 // 10MB

type memQuota interface {
	// TryAcquire try to acquire n bytes of memory.
	TryAcquire() bool
	// ForceAcquire force to acquire n bytes of memory.
	// It will exceed the total memory limit.
	ForceAcquire()
	// Release releases memory by resolved ts.
	Release(tableID model.TableID, resolved model.ResolvedTs) bool
	// Refund refunds unused memory.
	Refund(n uint64)
	// Record the memory usage.
	Record(tableID model.TableID, resolved model.ResolvedTs, size uint64)
	// IsExceed returns true if the memory quota is exceeded.
	IsExceed() bool
	// AllocateBatchID allocates a batch id for a batch of transactions.
	AllocateBatchID(tableID model.TableID) uint64
	// ResetBatchID resets the batch id for a table.
	ResetBatchID(tableID model.TableID)
}
