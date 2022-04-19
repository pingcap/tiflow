package model

import (
	"fmt"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta"
	"github.com/pingcap/errors"
)

type ResourceNotFoundError struct {
	ProblemResource resourcemeta.ResourceID
	Inner           error
}

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

type ResourceConflictError struct {
	ConflictingResources [2]resourcemeta.ResourceID
	AssignedExecutors    [2]model.ExecutorID
}

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
