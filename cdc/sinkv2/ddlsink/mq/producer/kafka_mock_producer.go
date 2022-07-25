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

	"github.com/Shopify/sarama"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
)

var _ Producer = (*MockProducer)(nil)

// MockProducer is a mock producer for test.
type MockProducer struct {
	injectError error
	events      map[mqv1.TopicPartitionKey][]*codec.MQMessage
}

// NewMockProducer creates a mock producer.
func NewMockProducer(_ context.Context, _ sarama.Client) (Producer, error) {
	return &MockProducer{
		events: make(map[mqv1.TopicPartitionKey][]*codec.MQMessage),
	}, nil
}

// SyncBroadcastMessage stores a message to all partitions of the topic.
func (m *MockProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *codec.MQMessage,
) error {
	if m.injectError != nil {
		return m.injectError
	}
	for i := 0; i < int(totalPartitionsNum); i++ {
		key := mqv1.TopicPartitionKey{
			Topic:     topic,
			Partition: int32(i),
		}
		if _, ok := m.events[key]; !ok {
			m.events[key] = make([]*codec.MQMessage, 0)
		}
		m.events[key] = append(m.events[key], message)
	}

	return nil
}

// SyncSendMessage stores a message to a partition of the topic.
func (m *MockProducer) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *codec.MQMessage,
) error {
	if m.injectError != nil {
		return m.injectError
	}

	key := mqv1.TopicPartitionKey{
		Topic:     topic,
		Partition: partitionNum,
	}
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*codec.MQMessage, 0)
	}
	m.events[key] = append(m.events[key], message)

	return nil
}

// Close do nothing.
func (m *MockProducer) Close() {}

// GetEvents returns the event filtered by the key.
func (m *MockProducer) GetEvents(key mqv1.TopicPartitionKey) []*codec.MQMessage {
	return m.events[key]
}

// InjectError injects an error to the mock producer.
func (m *MockProducer) InjectError(err error) {
	m.injectError = err
}
