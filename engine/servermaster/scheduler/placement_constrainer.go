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
	derror "github.com/pingcap/tiflow/engine/pkg/errors"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
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
