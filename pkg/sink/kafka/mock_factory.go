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
type MockFactory struct{}

func (c *MockFactory) AdminClient() (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (c *MockFactory) SyncProducer() (SyncProducer, error) {
	return &mockSyncProducer{}, nil
}

// AsyncProducer creates an async producer
func (c *MockFactory) AsyncProducer(
	changefeedID model.ChangeFeedID,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	return &mockAsyncProducer{}, nil
}

// MetricRegistry returns the metric registry
func (c *MockFactory) MetricRegistry() metrics.Registry {
	return metrics.DefaultRegistry
}

// Close closes the client
func (c *MockFactory) Close() error {
	return nil
}

type mockSyncProducer struct{}

func (p *mockSyncProducer) SendMessage(topic string, partitionNum int32, key []byte, value []byte) error {
	return nil
}

func (p *mockSyncProducer) SendMessages(topic string, partitionNum int32, key []byte, value []byte) error {
	return nil
}

func (p *mockSyncProducer) Close() error {
	return nil
}

type mockAsyncProducer struct{}

func (m *mockAsyncProducer) AsyncClose() {
	// TODO implement me
	panic("implement me")
}

func (m *mockAsyncProducer) Close() error {
	// TODO implement me
	panic("implement me")
}

func (m *mockAsyncProducer) AsyncSend(ctx context.Context, topic string, partition int32, key []byte, value []byte, callback func()) error {
	// TODO implement me
	panic("implement me")
}

func (m *mockAsyncProducer) AsyncRunCallback(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}
