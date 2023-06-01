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
	"fmt"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// ProcessorsGetter has a method to return a ProcessorInterface.
type ProcessorsGetter interface {
	Processors() ProcessorInterface
}

// ProcessorInterface has methods to work with Processor items.
// We can also mock the processor operations by implement this interface.
type ProcessorInterface interface {
	Get(ctx context.Context, namespace string, changefeedID, captureID string) (*v2.ProcessorDetail, error)
	List(ctx context.Context) ([]v2.ProcessorCommonInfo, error)
}

// processors implements ProcessorInterface.
type processors struct {
	client rest.CDCRESTInterface
}

// newProcessors returns processors.
func newProcessors(c *APIV2Client) *processors {
	return &processors{
		client: c.RESTClient(),
	}
}

// List returns the list of processors.
func (p *processors) List(ctx context.Context) ([]v2.ProcessorCommonInfo, error) {
	result := &v2.ListResponse[v2.ProcessorCommonInfo]{}
	err := p.client.Get().
		WithURI("processors").
		Do(ctx).
		Into(result)
	return result.Items, err
}

// Get gets the processor with given `changefeedID` and `captureID`.
func (p *processors) Get(
	ctx context.Context,
	namespace,
	changefeedID,
	captureID string,
) (*v2.ProcessorDetail, error) {
	result := &v2.ProcessorDetail{}
	u := fmt.Sprintf("processors/%s/%s?namespace=%s", changefeedID, captureID, namespace)
	err := p.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}
