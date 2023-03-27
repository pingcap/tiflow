// Copyright 2023 PingCAP, Inc.
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

package util

import (
	"math"

	"github.com/pingcap/log"
	"github.com/pingcap/tidb/util/memory"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util/cgroup"
	"go.uber.org/zap"
)

const memoryMax uint64 = math.MaxUint64

// MemTotal returns the memory limit of current process based on cgroup.
var MemTotal func() (uint64, error) = GetMemoryLimit

// GetMemoryLimit gets the memory limit of current process based on cgroup.
// If the cgourp is not set or memory.max is set to max, returns the available
// memory of host.
func GetMemoryLimit() (uint64, error) {
	totalMemory, err := cgroup.FromCgroup()
	if err != nil || totalMemory == memoryMax {
		log.Info("no cgroup memory limit", zap.Error(err))
		totalMemory, err = memory.MemTotal()
		if err != nil {
			return 0, errors.Trace(err)
		}
	}
	return totalMemory, nil
}
