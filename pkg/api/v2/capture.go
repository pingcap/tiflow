// Copyright 2023 PingCAP, Inc.
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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// CapturesGetter has a method to return a CaptureInterface.
type CapturesGetter interface {
	Captures() CaptureInterface
}

// CaptureInterface has methods to work with Capture items.
// We can also mock the capture operations by implement this interface.
type CaptureInterface interface {
	List(ctx context.Context) ([]model.Capture, error)
}

// captures implements CaptureInterface
type captures struct {
	client rest.CDCRESTInterface
}

// newCaptures returns captures
func newCaptures(c *APIV2Client) *captures {
	return &captures{
		client: c.RESTClient(),
	}
}

// List returns the list of captures
func (c *captures) List(ctx context.Context) ([]model.Capture, error) {
	result := &v2.ListResponse[model.Capture]{}
	err := c.client.Get().
		WithURI("captures").
		Do(ctx).
		Into(result)
	return result.Items, err
}
