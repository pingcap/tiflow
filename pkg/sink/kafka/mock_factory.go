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

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	o            *Options
	changefeedID model.ChangeFeedID
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	o *Options, changefeedID model.ChangeFeedID,
) (Factory, error) {
	return &MockFactory{
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

	t := ctx.Value("testing.T").(*testing.T)
	syncProducer := mocks.NewSyncProducer(t, config)
	return &MockSaramaSyncProducer{
		Producer: syncProducer,
	}, nil
}

// AsyncProducer creates an async producer
func (f *MockFactory) AsyncProducer(
	ctx context.Context,
	failpointCh chan error,
) (AsyncProducer, error) {
	config, err := NewSaramaConfig(ctx, f.o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	t := ctx.Value("testing.T").(*testing.T)
	asyncProducer := mocks.NewAsyncProducer(t, config)
	return &MockSaramaAsyncProducer{
		AsyncProducer: asyncProducer,
		failpointCh:   failpointCh,
	}, nil
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector(
	_ util.Role, _ ClusterAdminClient,
) MetricsCollector {
	return &mockMetricsCollector{}
}

// MockSaramaSyncProducer is a mock implementation of SyncProducer interface.
type MockSaramaSyncProducer struct {
	Producer *mocks.SyncProducer
}

// SendMessage implement the SyncProducer interface.
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

// SendMessages implement the SyncProducer interface.
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

// Close implement the SyncProducer interface.
func (m *MockSaramaSyncProducer) Close() {
	m.Producer.Close()
}

// MockSaramaAsyncProducer is a mock implementation of AsyncProducer interface.
type MockSaramaAsyncProducer struct {
	AsyncProducer *mocks.AsyncProducer
	failpointCh   chan error

	closed bool
}

// AsyncRunCallback implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) AsyncRunCallback(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case err := <-p.failpointCh:
			return errors.Trace(err)
		case ack := <-p.AsyncProducer.Successes():
			if ack != nil {
				callback := ack.Metadata.(func())
				if callback != nil {
					callback()
				}
			}
		case err := <-p.AsyncProducer.Errors():
			// We should not wrap a nil pointer if the pointer
			// is of a subtype of `error` because Go would store the type info
			// and the resulted `error` variable would not be nil,
			// which will cause the pkg/error library to malfunction.
			// See: https://go.dev/doc/faq#nil_error
			if err == nil {
				return nil
			}
			return cerror.WrapError(cerror.ErrKafkaAsyncSendMessage, err)
		}
	}
}

// AsyncSend implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) AsyncSend(ctx context.Context, topic string,
	partition int32, key []byte, value []byte,
	callback func(),
) error {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: partition,
		Key:       sarama.StringEncoder(key),
		Value:     sarama.ByteEncoder(value),
		Metadata:  callback,
	}
	select {
	case <-ctx.Done():
		return errors.Trace(ctx.Err())
	case p.AsyncProducer.Input() <- msg:
	}
	return nil
}

// Close implement the AsyncProducer interface.
func (p *MockSaramaAsyncProducer) Close() {
	if p.closed {
		return
	}
	_ = p.AsyncProducer.Close()
	p.closed = true
}

type mockMetricsCollector struct{}

// Run implements the MetricsCollector interface.
func (m *mockMetricsCollector) Run(ctx context.Context) {
}
