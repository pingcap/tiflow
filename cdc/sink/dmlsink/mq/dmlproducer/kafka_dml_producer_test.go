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
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/model"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func getOptions() *kafka.Options {
	options := kafka.NewOptions()
	options.Version = "0.9.0.0"
	options.ClientID = "test-client"
	options.PartitionNum = int32(2)
	options.AutoCreate = false
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	return options
}

func TestProducerAck(t *testing.T) {
	options := getOptions()
	options.MaxMessages = 1

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	config, err := kafka.NewSaramaConfig(ctx, options)
	require.Nil(t, err)
	require.Equal(t, 1, config.Producer.Flush.MaxMessages)

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	adminClient, err := factory.AdminClient(ctx)
	require.NoError(t, err)
	metricsCollector := factory.MetricsCollector(util.RoleTester, adminClient)

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	require.NoError(t, err)

	producer := NewKafkaDMLProducer(ctx, changefeed, asyncProducer, metricsCollector, errCh, failpointCh)
	require.NotNil(t, producer)

	messageCount := 20
	for i := 0; i < messageCount; i++ {
		asyncProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndSucceed()
	}

	count := atomic.NewInt64(0)
	for i := 0; i < 10; i++ {
		err = producer.AsyncSendMessage(ctx, kafka.DefaultMockTopicName, int32(0), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
			Callback: func() {
				count.Add(1)
			},
		})
		require.NoError(t, err)
		err = producer.AsyncSendMessage(ctx, kafka.DefaultMockTopicName, int32(1), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
			Callback: func() {
				count.Add(1)
			},
		})
		require.NoError(t, err)
	}
	// Test all messages are sent and callback is called.
	require.Eventuallyf(t, func() bool {
		return count.Load() == 20
	}, time.Second*5, time.Millisecond*10, "All msgs should be acked")

	// No error should be returned.
	select {
	case err := <-errCh:
		t.Fatalf("unexpected err: %s", err)
	default:
	}

	producer.Close()
	cancel()
	// check send messages when context is producer closed
	err = producer.AsyncSendMessage(ctx, kafka.DefaultMockTopicName, int32(0), &common.Message{
		Key:   []byte("cancel"),
		Value: nil,
	})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
}

func TestProducerSendMsgFailed(t *testing.T) {
	options := getOptions()
	errCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	_, err := kafka.NewSaramaConfig(ctx, options)
	require.Nil(t, err)
	options.MaxMessages = 1
	options.MaxMessageBytes = 1

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	adminClient, err := factory.AdminClient(ctx)
	require.NoError(t, err)
	metricsCollector := factory.MetricsCollector(util.RoleTester, adminClient)

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	require.NoError(t, err)

	producer := NewKafkaDMLProducer(ctx, changefeed, asyncProducer, metricsCollector, errCh, failpointCh)
	require.NotNil(t, producer)

	defer func() {
		producer.Close()

		// Close reentry.
		producer.Close()
	}()

	var wg sync.WaitGroup

	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()

		asyncProducer.(*kafka.MockSaramaAsyncProducer).AsyncProducer.ExpectInputAndFail(sarama.ErrMessageTooLarge)
		err = producer.AsyncSendMessage(ctx, kafka.DefaultMockTopicName, int32(0), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		})

		if err != nil {
			require.Condition(t, func() bool {
				return errors.Is(err, cerror.ErrKafkaProducerClosed) ||
					errors.Is(err, context.DeadlineExceeded)
			}, "should return error")
		}
	}(t)

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			t.Errorf("TestProducerSendMessageFailed timed out")
		case err := <-errCh:
			require.ErrorIs(t, err, sarama.ErrMessageTooLarge)
		}
	}()

	wg.Wait()
}

func TestProducerDoubleClose(t *testing.T) {
	options := getOptions()

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ctx = context.WithValue(ctx, "testing.T", t)
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	factory, err := kafka.NewMockFactory(options, changefeed)
	require.NoError(t, err)

	adminClient, err := factory.AdminClient(ctx)
	require.NoError(t, err)
	metricsCollector := factory.MetricsCollector(util.RoleTester, adminClient)

	failpointCh := make(chan error, 1)
	asyncProducer, err := factory.AsyncProducer(ctx, failpointCh)
	require.NoError(t, err)

	producer := NewKafkaDMLProducer(ctx, changefeed, asyncProducer, metricsCollector, errCh, failpointCh)
	require.NoError(t, err)
	require.NotNil(t, producer)

	producer.Close()
	producer.Close()
}
