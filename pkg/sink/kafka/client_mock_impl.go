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
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
)

// ClientMockImpl is a mock implementation of Client interface.
type ClientMockImpl struct{}

// NewClientMockImpl creates a new ClientMockImpl instance.
func NewClientMockImpl() *ClientMockImpl {
	return &ClientMockImpl{}
}

// SyncProducer creates a sync producer
func (c *ClientMockImpl) SyncProducer() (SyncProducer, error) {
	return &saramaSyncProducer{}, nil
}

// AsyncProducer creates an async producer
func (c *ClientMockImpl) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	return &saramaAsyncProducer{}, nil
}

// MetricRegistry implement the MetricsCollector interface
func (c *ClientMockImpl) MetricRegistry() metrics.Registry {
	return nil
}

// MetricsCollector returns the metric collector
func (c *ClientMockImpl) MetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return nil
}

// Close closes the client
func (c *ClientMockImpl) Close() error {
	return nil
}
