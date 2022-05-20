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

package resourcemeta

import (
	"context"

	"go.uber.org/ratelimit"

	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
)

const (
	metadataQPSLimit = 1024
)

// MetadataAccessor manages access to framework metastore
type MetadataAccessor struct {
	// rl limits the frequency the metastore is written to.
	// It helps to prevent cascading failures after a fail-over
	// where a large number of resources are created.
	rl         ratelimit.Limiter
	metaclient pkgOrm.Client
}

// NewMetadataAccessor creates a new MetadataAccessor instance
func NewMetadataAccessor(client pkgOrm.Client) *MetadataAccessor {
	return &MetadataAccessor{
		rl:         ratelimit.New(metadataQPSLimit),
		metaclient: client,
	}
}

// GetResource queries resource by resource id
func (m *MetadataAccessor) GetResource(ctx context.Context, resourceID resModel.ResourceID) (*resModel.ResourceMeta, bool, error) {
	rec, err := m.metaclient.GetResourceByID(ctx, resourceID)
	if err == nil {
		return rec, true, nil
	}

	if pkgOrm.IsNotFoundError(err) {
		return nil, false, nil
	}

	return nil, false, err
}

// CreateResource creates a resource if it does not exist
// - If the resource with given resource id exits, return false and nil
// - Otherwise
//   - if create resource successfully, return true and nil
//   - Otherwise return false and error
func (m *MetadataAccessor) CreateResource(ctx context.Context, resource *resModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	if err == nil {
		// A duplicate exists
		return false, nil
	}
	if !pkgOrm.IsNotFoundError(err) {
		// An unexpected error
		return false, err
	}

	m.rl.Take()
	if err := m.metaclient.UpsertResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

// UpdateResource updates the content of a given resource if it exists
func (m *MetadataAccessor) UpdateResource(ctx context.Context, resource *resModel.ResourceMeta) (bool, error) {
	_, err := m.metaclient.GetResourceByID(ctx, resource.ID)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return false, nil
		}
		return false, err
	}

	m.rl.Take()
	if err := m.metaclient.UpdateResource(ctx, resource); err != nil {
		return false, err
	}

	return true, nil
}

// DeleteResource deletes a resource by its resource id
func (m *MetadataAccessor) DeleteResource(ctx context.Context, resourceID resModel.ResourceID) (bool, error) {
	res, err := m.metaclient.DeleteResource(ctx, resourceID)
	if err != nil {
		return false, err
	}
	if res.RowsAffected() == 0 {
		return false, nil
	}

	return true, nil
}

// GetAllResources returns all resources
func (m *MetadataAccessor) GetAllResources(ctx context.Context) ([]*resModel.ResourceMeta, error) {
	return m.metaclient.QueryResources(ctx)
}

// GetResourcesForExecutor queries all resources belong to the given executor
func (m *MetadataAccessor) GetResourcesForExecutor(
	ctx context.Context,
	executorID resModel.ExecutorID,
) ([]*resModel.ResourceMeta, error) {
	return m.metaclient.QueryResourcesByExecutorID(ctx, string(executorID))
}
