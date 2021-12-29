// Copyright 2021 PingCAP, Inc.
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
	"fmt"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// ProcessorsGetter has a method to return a ProcessorInterface.
type ProcessorsGetter interface {
	Processors() ProcessorInterface
}

// ProcessorInterface has methods to work with Processor items.
// We can also mock the processor operations by implement this interface.
type ProcessorInterface interface {
	Get(ctx context.Context, changefeedID, captureID string) (*model.ProcessorDetail, error)
	List(ctx context.Context) (*[]model.ProcessorCommonInfo, error)
}

// processors implements ProcessorInterface
type processors struct {
	client rest.CDCRESTInterface
}

// newProcessors returns processors
func newProcessors(c *APIV1Client) *processors {
	return &processors{
		client: c.RESTClient(),
	}
}

// Get takes name of the processor, and returns the corresponding processor object,
// and an error if there is any.
func (c *processors) Get(ctx context.Context, changefeedID, captureID string) (*model.ProcessorDetail, error) {
	result := new(model.ProcessorDetail)
	u := fmt.Sprintf("processors/%s/%s", changefeedID, captureID)
	err := c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}

// List returns the list of processors
func (c *processors) List(ctx context.Context) (*[]model.ProcessorCommonInfo, error) {
	result := new([]model.ProcessorCommonInfo)
	err := c.client.Get().
		WithURI("processors").
		Do(ctx).
		Into(result)
	return result, err
}
