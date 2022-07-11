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

// UnsafeGetter has a method to return a UnsafeInterface.
type UnsafeGetter interface {
	Unsafe() UnsafeInterface
}

// UnsafeInterface has methods to work with unsafe api
type UnsafeInterface interface {
	Metadata(ctx context.Context) (*[]v2.EtcdData, error)
	ResolveLock(ctx context.Context, req *v2.ResolveLockReq) error
	DeleteServiceGcSafePoint(ctx context.Context, config *v2.UpstreamConfig) error
}

// unsafe implements UnsafeInterface
type unsafe struct {
	client rest.CDCRESTInterface
}

// newUnsafe returns unsafe
func newUnsafe(c *APIV2Client) *unsafe {
	return &unsafe{
		client: c.RESTClient(),
	}
}

// Metadata returns the etcd key value get from etcd
func (c *unsafe) Metadata(ctx context.Context) (*[]v2.EtcdData, error) {
	result := new([]v2.EtcdData)
	err := c.client.Get().
		WithURI("unsafe/metadata").
		Do(ctx).
		Into(result)
	return result, err
}

// ResolveLock resolves lock in region
func (c *unsafe) ResolveLock(ctx context.Context,
	req *v2.ResolveLockReq,
) error {
	return c.client.Post().
		WithURI("unsafe/resolve_lock").
		WithBody(req).
		Do(ctx).Error()
}

// DeleteServiceGcSafePoint delete service gc safe point in pd
func (c *unsafe) DeleteServiceGcSafePoint(ctx context.Context,
	config *v2.UpstreamConfig,
) error {
	return c.client.Delete().
		WithURI("unsafe/service_gc_safepoint").
		WithBody(config).
		Do(ctx).Error()
}
