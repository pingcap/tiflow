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

// ChangefeedsGetter has a method to return a ChangefeedInterface.
type ChangefeedsGetter interface {
	Changefeeds() ChangefeedInterface
}

// ChangefeedInterface has methods to work with Changfeed items.
// We can also mock the changefeed operations by implement this interface.
type ChangefeedInterface interface {
	Get(ctx context.Context, name string) (*model.ChangefeedDetail, error)
	List(ctx context.Context, state string) (*[]model.ChangefeedCommonInfo, error)
	Delete(ctx context.Context, name string) error
	Pause(ctx context.Context, name string) error
	Resume(ctx context.Context, name string) error
}

// changefeeds implements ChangefeedInterface
type changefeeds struct {
	client rest.CDCRESTInterface
}

// newChangefeeds returns changefeeds
func newChangefeeds(c *APIV1Client) *changefeeds {
	return &changefeeds{
		client: c.RESTClient(),
	}
}

// Get takes name of the changefeed, and returns the corresponding changefeed object,
// and an error if there is any.
func (c *changefeeds) Get(ctx context.Context, name string) (*model.ChangefeedDetail, error) {
	result := new(model.ChangefeedDetail)
	u := fmt.Sprintf("changefeeds/%s", name)
	err := c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}

// List returns the list of changefeeds
func (c *changefeeds) List(ctx context.Context,
	state string,
) (*[]model.ChangefeedCommonInfo, error) {
	result := new([]model.ChangefeedCommonInfo)
	err := c.client.Get().
		WithURI("changefeeds").
		WithParam("state", state).
		Do(ctx).
		Into(result)
	return result, err
}

// Pause the changefeed
func (c *changefeeds) Pause(ctx context.Context, name string) error {
	u := fmt.Sprintf("changefeeds/%s/pause", name)
	return c.client.Post().
		WithURI(u).
		Do(ctx).Error()
}

// Resume a changefeed
func (c *changefeeds) Resume(ctx context.Context, name string) error {
	u := fmt.Sprintf("changefeeds/%s/resume", name)
	return c.client.Post().
		WithURI(u).
		Do(ctx).Error()
}

// Delete delete the changefeed
func (c *changefeeds) Delete(ctx context.Context, name string) error {
	u := fmt.Sprintf("changefeeds/%s", name)
	return c.client.Delete().
		WithURI(u).
		Do(ctx).Error()
}
