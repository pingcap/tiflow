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

import (
	stdErrors "errors"
	"fmt"

	"github.com/gogo/status"
	"google.golang.org/grpc/codes"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/model"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	derrors "github.com/pingcap/tiflow/pkg/errors"
)

// ResourceNotFoundError happens when the resource id doesn't equal to any record
// in metastore, it also contains detail cause by Inner error field.
type ResourceNotFoundError struct {
	ProblemResource resourcemeta.ResourceID
	Inner           error
}

// NewResourceNotFoundError creates a resource not found error
func NewResourceNotFoundError(
	resourceID resourcemeta.ResourceID, cause error,
) error {
	ret := &ResourceNotFoundError{
		ProblemResource: resourceID,
		Inner:           cause,
	}
	return errors.Trace(ret)
}

func (e *ResourceNotFoundError) Error() string {
	return fmt.Sprintf("Scheduler could not find resource %s, caused by %s",
		e.ProblemResource, e.Inner.Error())
}

// ResourceConflictError is raised when two resources are assigned to two executors.
type ResourceConflictError struct {
	ConflictingResources [2]resourcemeta.ResourceID
	AssignedExecutors    [2]model.ExecutorID
}

// NewResourceConflictError creates a new resource conflict error
func NewResourceConflictError(
	resourceA resourcemeta.ResourceID,
	executorA model.ExecutorID,
	resourceB resourcemeta.ResourceID,
	executorB model.ExecutorID,
) error {
	ret := &ResourceConflictError{
		ConflictingResources: [2]resourcemeta.ResourceID{resourceA, resourceB},
		AssignedExecutors:    [2]model.ExecutorID{executorA, executorB},
	}
	return errors.Trace(ret)
}

func (e *ResourceConflictError) Error() string {
	return fmt.Sprintf("Scheduler could not assign executor due to conflicting "+
		"requirements: resource %s needs executor %s, while resource %s needs executor %s",
		e.ConflictingResources[0], e.AssignedExecutors[0],
		e.ConflictingResources[1], e.AssignedExecutors[1])
}

// SchedulerErrorToGRPCError converts resource error to corresponding gRPC error
func SchedulerErrorToGRPCError(errIn error) error {
	if errIn == nil {
		log.L().Panic("Invalid input to SchedulerErrorToGRPCError")
	}

	var (
		conflictErr *ResourceConflictError
		notFoundErr *ResourceNotFoundError
	)
	switch {
	case stdErrors.As(errIn, &conflictErr):
		return status.Error(codes.FailedPrecondition, conflictErr.Error())
	case stdErrors.As(errIn, &notFoundErr):
		return status.Error(codes.NotFound, notFoundErr.Error())
	case derrors.ErrClusterResourceNotEnough.Equal(errIn):
		return status.Error(codes.ResourceExhausted, errIn.Error())
	default:
	}
	return errIn
}
