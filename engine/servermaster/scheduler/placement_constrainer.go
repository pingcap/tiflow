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
	"context"

	"github.com/pingcap/tiflow/engine/model"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

// PlacementConstrainer describes an object that provides
// the placement constraint for an external resource.
type PlacementConstrainer interface {
	GetPlacementConstraint(
		ctx context.Context,
		resourceKey resourcemeta.ResourceKey,
	) (resourcemeta.ExecutorID, bool, error)
}

// MockPlacementConstrainer uses a resource executor binding map to implement PlacementConstrainer
type MockPlacementConstrainer struct {
	ResourceList map[resourcemeta.ResourceID]model.ExecutorID
}

// GetPlacementConstraint implements PlacementConstrainer.GetPlacementConstraint
func (c *MockPlacementConstrainer) GetPlacementConstraint(
	_ context.Context,
	resourceKey resourcemeta.ResourceKey,
) (resourcemeta.ExecutorID, bool, error) {
	executorID, exists := c.ResourceList[resourceKey.ID]
	if !exists {
		return "", false, errors.ErrResourceDoesNotExist.GenWithStackByArgs(resourceKey.ID)
	}
	if executorID == "" {
		return "", false, nil
	}
	return executorID, true, nil
}
