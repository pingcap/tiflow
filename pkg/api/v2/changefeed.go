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
	"fmt"

	v2 "github.com/pingcap/tiflow/cdc/api/v2"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/api/internal/rest"
)

// ChangefeedsGetter has a method to return a ChangefeedInterface.
type ChangefeedsGetter interface {
	Changefeeds() ChangefeedInterface
}

// ChangefeedInterface has methods to work with Changefeed items.
// We can also mock the changefeed operations by implement this interface.
type ChangefeedInterface interface {
	// Create creates a changefeed
	Create(ctx context.Context, cfg *v2.ChangefeedConfig) (*model.ChangeFeedInfo, error)
	// GetInfo gets a changefeed's info
	GetInfo(ctx context.Context, name string) (*model.ChangeFeedInfo, error)
	// VerifyTable verifies table for a changefeed
	VerifyTable(ctx context.Context, cfg *v2.VerifyTableConfig) (*v2.Tables, error)
	// Update updates a changefeed
	Update(ctx context.Context, cfg *v2.ChangefeedConfig,
		name string) (*model.ChangeFeedInfo, error)
}

// changefeeds implements ChangefeedInterface
type changefeeds struct {
	client rest.CDCRESTInterface
}

// newChangefeed returns changefeeds
func newChangefeeds(c *APIV2Client) *changefeeds {
	return &changefeeds{
		client: c.RESTClient(),
	}
}

func (c *changefeeds) Create(ctx context.Context,
	cfg *v2.ChangefeedConfig,
) (*model.ChangeFeedInfo, error) {
	result := &model.ChangeFeedInfo{}
	err := c.client.Post().
		WithURI("changefeeds").
		WithBody(cfg).
		Do(ctx).Into(result)
	return result, err
}

func (c *changefeeds) VerifyTable(ctx context.Context,
	cfg *v2.VerifyTableConfig,
) (*v2.Tables, error) {
	result := &v2.Tables{}
	err := c.client.Post().
		WithURI("verify_table").
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *changefeeds) GetInfo(ctx context.Context, name string) (*model.ChangeFeedInfo, error) {
	result := &model.ChangeFeedInfo{}
	u := fmt.Sprintf("changefeeds/%s/meta_info", name)
	err := c.client.Get().
		WithURI(u).
		Do(ctx).
		Into(result)
	return result, err
}

func (c *changefeeds) Update(ctx context.Context,
	cfg *v2.ChangefeedConfig, name string,
) (*model.ChangeFeedInfo, error) {
	result := &model.ChangeFeedInfo{}
	u := fmt.Sprintf("changefeeds/%s", name)
	err := c.client.Put().
		WithURI(u).
		WithBody(cfg).
		Do(ctx).
		Into(result)
	return result, err
}
