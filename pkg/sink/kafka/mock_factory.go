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

package kafka

import (
	"context"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/rcrowley/go-metrics"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	helper Factory
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(ctx context.Context, o *Options) (Factory, error) {
	// mock factory is used by some unit test based on sarama implementations,
	// so we adapt sarama factory to support the mock implementation temporarily.
	// todo: make unit test become implementation independent.
	helper, err := NewSaramaFactory(ctx, o)
	if err != nil {
		return nil, err
	}
	return &MockFactory{helper: helper}, nil
}

// AdminClient creates a cluster admin client
func (c *MockFactory) AdminClient() (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (c *MockFactory) SyncProducer() (SyncProducer, error) {
	return c.helper.SyncProducer()
}

// AsyncProducer creates an async producer
func (c *MockFactory) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	return c.helper.AsyncProducer(changefeedID, closedChan, failpointCh)
}

// MetricRegistry returns the metric registry
func (c *MockFactory) MetricRegistry() metrics.Registry {
	return metrics.DefaultRegistry
}

// Close closes the client
func (c *MockFactory) Close() error {
	return c.helper.Close()
}
