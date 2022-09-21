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

package scheduler

import (
	"github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	"github.com/pingcap/tiflow/engine/pkg/rpcerror"
	"github.com/pingcap/tiflow/pkg/label"
)

// ErrResourceNotFound indicates that a given resource requirement
// (usually a local file requirement) cannot be satisfied because
// the given resource could not be found.
var ErrResourceNotFound = rpcerror.Normalize[ResourceNotFoundError](rpcerror.WithName("RequiredResourceNotFoundError"))

// ResourceNotFoundError provides the details of an ErrResourceNotFound.
type ResourceNotFoundError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.NotFound]

	ResourceID resModel.ResourceID
	Details    string
}

// ErrResourceConflict indicates that two resource requirements have
// conflicting executors (usually caused by requiring two local files
// on different executors).
var ErrResourceConflict = rpcerror.Normalize[ResourceConflictError]()

// ResourceConflictError provides details of ErrResourceConflict.
type ResourceConflictError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.FailedPrecondition]

	ConflictingResources [2]resModel.ResourceID
	AssignedExecutors    [2]model.ExecutorID
}

// ErrSelectorUnsatisfied indicates that a given selector could not
// be satisfied by the currently available executors.
var ErrSelectorUnsatisfied = rpcerror.Normalize[SelectorUnsatisfiedError]()

// SelectorUnsatisfiedError provides details of an ErrSelectorUnsatisfied.
type SelectorUnsatisfiedError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.ResourceExhausted]

	Selector *label.Selector
}

// ErrFilterNoResult indicates that a scheduler filter returns an
// empty set for potential executors.
var ErrFilterNoResult = rpcerror.Normalize[FilterNoResultError]()

// FilterNoResultError provides details of ErrFilterNoResult.
type FilterNoResultError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.ResourceExhausted]

	FilterName      string
	InputCandidates []model.ExecutorID
}

// ErrCapacityNotEnough indicates that no suitable executor with
// enough capacity can be found.
var ErrCapacityNotEnough = rpcerror.Normalize[CapacityNotEnoughError]()

// CapacityNotEnoughError provides details of an ErrCapacityNotEnough.
type CapacityNotEnoughError struct {
	rpcerror.Error[rpcerror.NotRetryable, rpcerror.ResourceExhausted]

	FinalCandidates []model.ExecutorID
}
