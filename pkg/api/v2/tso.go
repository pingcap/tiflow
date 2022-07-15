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

// TsoGetter has a method to return a StatusInterface.
type TsoGetter interface {
	Tso() TsoInterface
}

// TsoInterface has methods to work with status api
type TsoInterface interface {
	Query(ctx context.Context, config *v2.UpstreamConfig) (*v2.Tso, error)
}

// tso implements StatusGetter
type tso struct {
	client rest.CDCRESTInterface
}

// newTso returns tso
func newTso(c *APIV2Client) *tso {
	return &tso{
		client: c.RESTClient(),
	}
}

// Query returns the pd tso
func (c *tso) Query(ctx context.Context, config *v2.UpstreamConfig) (*v2.Tso, error) {
	result := new(v2.Tso)
	err := c.client.Post().
		WithURI("tso").
		WithBody(config).
		Do(ctx).
		Into(result)
	return result, err
}
