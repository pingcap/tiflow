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
	"fmt"
	"sync"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
)

var _ DMLProducer = (*MockDMLProducer)(nil)

// MockDMLProducer is a mock producer for test.
type MockDMLProducer struct {
	mu     sync.Mutex
	events map[string][]*common.Message

	asyncProducer kafka.AsyncProducer
}

// NewDMLMockProducer creates a mock producer.
func NewDMLMockProducer(_ context.Context, _ model.ChangeFeedID, asyncProducer kafka.AsyncProducer,
	_ kafka.MetricsCollector,
	_ chan error,
	_ chan error,
) DMLProducer {
	return &MockDMLProducer{
		events:        make(map[string][]*common.Message),
		asyncProducer: asyncProducer,
	}
}

// AsyncSendMessage appends a message to the mock producer.
func (m *MockDMLProducer) AsyncSendMessage(_ context.Context, topic string,
	partition int32, message *common.Message,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	key := fmt.Sprintf("%s-%d", topic, partition)
	if _, ok := m.events[key]; !ok {
		m.events[key] = make([]*common.Message, 0)
	}
	m.events[key] = append(m.events[key], message)

	message.Callback()

	return nil
}

// Close do nothing.
func (m *MockDMLProducer) Close() {
	if m.asyncProducer != nil {
		m.asyncProducer.Close()
	}
}

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
func (m *MockDMLProducer) GetEvents(topic string, partition int32) []*common.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s-%d", topic, partition)
	return m.events[key]
}
