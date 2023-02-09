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
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/rcrowley/go-metrics"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	// some unit test is based on sarama, so we set it as a helper
	helper Factory
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(ctx context.Context, o *Options) (Factory, error) {
	helper, err := NewSaramaFactory(ctx, o)
	if err != nil {
		return nil, err
	}

	return &MockFactory{helper: helper}, nil
}

func (f *MockFactory) AdminClient() (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (f *MockFactory) SyncProducer() (SyncProducer, error) {
	return f.helper.SyncProducer()
}

// AsyncProducer creates an async producer
func (f *MockFactory) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	return f.helper.AsyncProducer(changefeedID, closedChan, failpointCh)
}

// MetricRegistry implement the MetricsCollector interface
func (f *MockFactory) MetricRegistry() metrics.Registry {
	return metrics.DefaultRegistry
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector(
	changefeedID model.ChangeFeedID,
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return f.helper.MetricsCollector(changefeedID, role, adminClient)
}
