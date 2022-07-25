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

package v1

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// StatusGetter has a method to return a StatusInterface.
type StatusGetter interface {
	Status() StatusInterface
}

// StatusInterface has methods to work with status api
type StatusInterface interface {
	Get(ctx context.Context) (*model.ServerStatus, error)
}

// status implements StatusGetter
type status struct {
	client rest.CDCRESTInterface
}

// newCaptures returns captures
func newStatus(c *APIV1Client) *status {
	return &status{
		client: c.RESTClient(),
	}
}

// Get returns the server status
func (c *status) Get(ctx context.Context) (*model.ServerStatus, error) {
	result := new(model.ServerStatus)
	err := c.client.Get().
		WithURI("status").
		Do(ctx).
		Into(result)
	return result, err
}
