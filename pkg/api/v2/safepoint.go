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

package v2

import (
	"context"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// SafePointGetter has a method to return a StatusInterface.
type SafePointGetter interface {
	SafePoint() SafePointInterface
}

// SafePointInterface has methods to work with status api
type SafePointInterface interface {
	Query(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error)
	Set(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error)
	Delete(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error)
}

// SafePoint implements StatusGetter
type SafePoint struct {
	client rest.CDCRESTInterface
}

// newSafePoint returns SafePoint
func newSafePoint(c *APIV2Client) *SafePoint {
	return &SafePoint{
		client: c.RESTClient(),
	}
}

// Query returns the pd SafePoint
func (c *SafePoint) Query(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error) {
	result := new(v2.SafePoint)
	err := c.client.Get().
		WithURI("safepoint").
		WithBody(config).
		Do(ctx).
		Into(result)
	return result, err
}

// Set returns the pd SafePoint
func (c *SafePoint) Set(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error) {
	result := new(v2.SafePoint)
	err := c.client.Post().
		WithURI("safepoint").
		WithBody(config).
		Do(ctx).
		Into(result)
	return result, err
}

// Delete returns the pd SafePoint
func (c *SafePoint) Delete(ctx context.Context, config *v2.SafePointConfig) (*v2.SafePoint, error) {
	result := new(v2.SafePoint)
	err := c.client.Delete().
		WithURI("safepoint").
		WithBody(config).
		Do(ctx).
		Into(result)
	return result, err
}
