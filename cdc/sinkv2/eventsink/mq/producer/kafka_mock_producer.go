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

package producer

import (
	"context"
	"sync"

	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
)

var _ Producer = (*MockProducer)(nil)

// MockProducer is a mock producer for test.
type MockProducer struct {
	mu     sync.Mutex
	events map[mqv1.TopicPartitionKey][]*codec.MQMessage
}

// NewMockProducer creates a mock producer.
func NewMockProducer() Producer {
	return &MockProducer{
		events: make(map[mqv1.TopicPartitionKey][]*codec.MQMessage),
	}
}

// AsyncSendMessage appends a message to the mock producer.
func (m *MockProducer) AsyncSendMessage(ctx context.Context, topic string,
	partition int32, message *codec.MQMessage,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := mqv1.TopicPartitionKey{
		Topic:     topic,
		Partition: partition,
	}
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*codec.MQMessage, 0)
	}
	m.events[key] = append(m.events[key], message)

	return nil
}

// Close do nothing.
func (m *MockProducer) Close() error {
	return nil
}

// GetEvents returns the events received by the mock producer.
func (m *MockProducer) GetEvents() []*codec.MQMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var events []*codec.MQMessage
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvent returns the event filtered by the key.
func (m *MockProducer) GetEvent(key mqv1.TopicPartitionKey) []*codec.MQMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.events[key]
}
