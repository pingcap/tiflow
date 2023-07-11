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

package ddlproducer

import (
	"context"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	mqv1 "github.com/pingcap/tiflow/cdc/sink/mq"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

var _ DDLProducer = (*MockDDLProducer)(nil)

// MockDDLProducer is a mock producer for test.
type MockDDLProducer struct {
	events map[mqv1.TopicPartitionKey][]*common.Message
}

// NewMockDDLProducer creates a mock producer.
func NewMockDDLProducer(_ context.Context, _ sarama.Client,
	_ kafka.ClusterAdminClient,
) (DDLProducer, error) {
	return &MockDDLProducer{
		events: make(map[mqv1.TopicPartitionKey][]*common.Message),
	}, nil
}

// SyncBroadcastMessage stores a message to all partitions of the topic.
func (m *MockDDLProducer) SyncBroadcastMessage(ctx context.Context, topic string,
	totalPartitionsNum int32, message *common.Message,
) error {
	for i := 0; i < int(totalPartitionsNum); i++ {
		key := mqv1.TopicPartitionKey{
			Topic:     topic,
			Partition: int32(i),
		}
		if _, ok := m.events[key]; !ok {
			m.events[key] = make([]*common.Message, 0)
		}
		m.events[key] = append(m.events[key], message)
	}

	return nil
}

// SyncSendMessage stores a message to a partition of the topic.
func (m *MockDDLProducer) SyncSendMessage(ctx context.Context, topic string,
	partitionNum int32, message *common.Message,
) error {
	key := mqv1.TopicPartitionKey{
		Topic:     topic,
		Partition: partitionNum,
	}
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	return nil
}

// Close do nothing.
func (m *MockDDLProducer) Close() {}

// GetAllEvents returns the events received by the mock producer.
func (m *MockDDLProducer) GetAllEvents() []*common.Message {
	var events []*common.Message
	for _, v := range m.events {
		events = append(events, v...)
	}
	return events
}

// GetEvents returns the event filtered by the key.
func (m *MockDDLProducer) GetEvents(key mqv1.TopicPartitionKey) []*common.Message {
	return m.events[key]
}
