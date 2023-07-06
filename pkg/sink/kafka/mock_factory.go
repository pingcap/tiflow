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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/Shopify/sarama/mocks"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/util"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	// some unit test is based on sarama, so we set it as a helper
	helper Factory

	t            *testing.T
	o            *Options
	changefeedID model.ChangeFeedID
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	t *testing.T,
	o *Options, changefeedID model.ChangeFeedID,
) (Factory, error) {
	helper, err := NewSaramaFactory(o, changefeedID)
	if err != nil {
		return nil, err
	}
	return &MockFactory{
		helper:       helper,
		t:            t,
		o:            o,
		changefeedID: changefeedID,
	}, nil
}

// AdminClient return a mocked admin client
func (f *MockFactory) AdminClient(_ context.Context) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}

// SyncProducer creates a sync producer
func (f *MockFactory) SyncProducer(ctx context.Context) (SyncProducer, error) {
	config, err := NewSaramaConfig(ctx, f.o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	syncProducer := mocks.NewSyncProducer(f.t, config)
	return &MockSaramaSyncProducer{
		Producer: syncProducer,
	}, nil
}

// AsyncProducer creates an async producer
func (f *MockFactory) AsyncProducer(
	ctx context.Context,
	closedChan chan struct{},
	failpointCh chan error,
) (AsyncProducer, error) {
	return f.helper.AsyncProducer(ctx, closedChan, failpointCh)
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector(
	role util.Role,
	adminClient ClusterAdminClient,
) MetricsCollector {
	return f.helper.MetricsCollector(role, adminClient)
}

type MockSaramaSyncProducer struct {
	Producer *mocks.SyncProducer
}

func (m *MockSaramaSyncProducer) SendMessage(
	ctx context.Context,
	topic string, partitionNum int32,
	key []byte, value []byte,
) error {
	_, _, err := m.Producer.SendMessage(&sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Partition: partitionNum,
	})
	return err
}

func (m *MockSaramaSyncProducer) SendMessages(ctx context.Context,
	topic string, partitionNum int32,
	key []byte, value []byte,
) error {
	msgs := make([]*sarama.ProducerMessage, partitionNum)
	for i := 0; i < int(partitionNum); i++ {
		msgs[i] = &sarama.ProducerMessage{
			Topic:     topic,
			Key:       sarama.ByteEncoder(key),
			Value:     sarama.ByteEncoder(value),
			Partition: int32(i),
		}
	}
	return m.Producer.SendMessages(msgs)
}

func (m *MockSaramaSyncProducer) Close() {
	m.Producer.Close()
}
