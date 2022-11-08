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

package dmlproducer

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

var _ DMLProducer = (*MockDMLProducer)(nil)

// MockDMLProducer is a mock producer for test.
type MockDMLProducer struct {
	mu     sync.Mutex
	events map[mqv1.TopicPartitionKey][]*common.Message
}

// NewDMLMockProducer creates a mock producer.
func NewDMLMockProducer(_ context.Context, _ sarama.Client,
	_ kafka.ClusterAdminClient, _ chan error,
) (DMLProducer, error) {
	return &MockDMLProducer{
		events: make(map[mqv1.TopicPartitionKey][]*common.Message),
	}, nil
}

// AsyncSendMessage appends a message to the mock producer.
func (m *MockDMLProducer) AsyncSendMessage(_ context.Context, topic string,
	partition int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := mqv1.TopicPartitionKey{
		Topic:     topic,
		Partition: partition,
	}
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	message.Callback()

	return nil
}

// Close do nothing.
func (m *MockDMLProducer) Close() {}

// GetAllEvents returns the events received by the mock producer.
func (m *MockDMLProducer) GetAllEvents() []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []*common.Message
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (m *MockDMLProducer) GetEvents(key mqv1.TopicPartitionKey) []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[key]
}
