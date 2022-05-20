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

package model

import "github.com/pingcap/tiflow/engine/model"

// ResourceUnit is a type representing the value of
// resource used. Although currently it is a scalar,
// we expect expanding it to a vector, so that multiple
// scalars can be held together, such as (CPU time, memory).
type ResourceUnit = model.RescUnit

// ExecutorResourceStatus represents an overview of
// resource usage on a given executor.
type ExecutorResourceStatus struct {
	Capacity, Reserved, Used ResourceUnit
}

// Remaining calculates the available resource unit of given resource
func (s *ExecutorResourceStatus) Remaining() ResourceUnit {
	if s.Used > s.Reserved {
		return s.Capacity - s.Used
	}
	return s.Capacity - s.Reserved
}
