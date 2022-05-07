package scheduler

import (
	"context"

	"github.com/hanfei1991/microcosm/model"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	resourcemeta "github.com/hanfei1991/microcosm/pkg/externalresource/resourcemeta/model"
)

// PlacementConstrainer describes an object that provides
// the placement constraint for an external resource.
type PlacementConstrainer interface {
	GetPlacementConstraint(
		ctx context.Context,
		id resourcemeta.ResourceID,
	) (resourcemeta.ExecutorID, bool, error)
}

type MockPlacementConstrainer struct {
	ResourceList map[resourcemeta.ResourceID]model.ExecutorID
}

func (c *MockPlacementConstrainer) GetPlacementConstraint(
	_ context.Context, id resourcemeta.ResourceID,
) (resourcemeta.ExecutorID, bool, error) {
	executorID, exists := c.ResourceList[id]
	if !exists {
		return "", false, derror.ErrResourceDoesNotExist.GenWithStackByArgs(id)
	}
	if executorID == "" {
		return "", false, nil
	}
	return executorID, true, nil
}
