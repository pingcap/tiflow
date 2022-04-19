package model

import "github.com/hanfei1991/microcosm/model"

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

func (s *ExecutorResourceStatus) Remaining() ResourceUnit {
	if s.Used > s.Reserved {
		return s.Capacity - s.Used
	}
	return s.Capacity - s.Reserved
}
