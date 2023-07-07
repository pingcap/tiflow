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
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/util"
)

// MockFactory is a mock implementation of Factory interface.
type MockFactory struct {
	t            *testing.T
	o            *Options
	changefeedID model.ChangeFeedID
}

// NewMockFactory constructs a Factory with mock implementation.
func NewMockFactory(
	t *testing.T,
	o *Options, changefeedID model.ChangeFeedID,
) (Factory, error) {
	return &MockFactory{
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
	config, err := NewSaramaConfig(ctx, f.o)
	if err != nil {
		return nil, errors.Trace(err)
	}
	asyncProducer := mocks.NewAsyncProducer(f.t, config)
	return &MockSaramaAsyncProducer{
		AsyncProducer: asyncProducer,
		closedChan:    closedChan,
		failpointCh:   failpointCh,
	}, nil
}

// MetricsCollector returns the metric collector
func (f *MockFactory) MetricsCollector(
	_ util.Role, _ ClusterAdminClient,
) MetricsCollector {
	return &mockMetricsCollector{}
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

type MockSaramaAsyncProducer struct {
	AsyncProducer *mocks.AsyncProducer
	closedChan    chan struct{}
	failpointCh   chan error
}

func (p *MockSaramaAsyncProducer) AsyncRunCallback(
	ctx context.Context,
) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case <-p.closedChan:
			return nil
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
	case <-p.closedChan:
		return nil
	case p.AsyncProducer.Input() <- msg:
	}
	return nil
}

func (p *MockSaramaAsyncProducer) Close() {
	_ = p.AsyncProducer.Close()
}

type mockMetricsCollector struct{}

func (m *mockMetricsCollector) Run(ctx context.Context) {
}
