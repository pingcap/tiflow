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

//go:build !race
// +build !race

package dmlproducer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	kafkav1 "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

//nolint:unused
func initBroker(t *testing.T, withProducerResponse bool) (*sarama.MockBroker, string) {
	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 2)
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0,
		leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1,
		leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)
	if withProducerResponse {
		prodSuccess := new(sarama.ProduceResponse)
		prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)
		prodSuccess.AddTopicPartition(topic, 1, sarama.ErrNoError)
		for i := 0; i < 20; i++ {
			leader.Returns(prodSuccess)
		}
	}
	return leader, topic
}

//nolint:unused
func getConfig(addr string) *kafkav1.Config {
	config := kafkav1.NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0.
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac
	// 030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(addr, ",")

	return config
}

func TestProducerAck(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, topic := initBroker(t, true)
	defer leader.Close()

	config := getConfig(leader.Addr())

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaDMLProducer(ctx, client, adminClient, errCh)
	require.Nil(t, err)
	require.NotNil(t, producer)

	count := atomic.NewInt64(0)
	for i := 0; i < 10; i++ {
		err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
			Callback: func() {
				count.Add(1)
			},
		})
		require.Nil(t, err)
		err = producer.AsyncSendMessage(ctx, topic, int32(1), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
			Callback: func() {
				count.Add(1)
			},
		})
		require.Nil(t, err)
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
	err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
		Key:   []byte("cancel"),
		Value: nil,
	})
	require.ErrorIs(t, err, cerror.ErrKafkaProducerClosed)
}

func TestProducerSendMsgFailed(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, topic := initBroker(t, false)
	defer leader.Close()

	config := getConfig(leader.Addr())
	errCh := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1
	saramaConfig.Producer.Retry.Max = 1
	// This will make the first send failed.
	saramaConfig.Producer.MaxMessageBytes = 8

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaDMLProducer(ctx, client, adminClient, errCh)
	defer func() {
		producer.Close()

		// Close reentry.
		producer.Close()
	}()
	require.Nil(t, err)
	require.NotNil(t, producer)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
				Key:   []byte("test-key-1"),
				Value: []byte("test-value"),
			})
			if err != nil {
				require.Condition(t, func() bool {
					return errors.Is(err, cerror.ErrKafkaProducerClosed) ||
						errors.Is(err, context.DeadlineExceeded)
				}, "should return error")
			}
		}
	}(t)

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			t.Errorf("TestProducerSendMessageFailed timed out")
		case err := <-errCh:
			require.Regexp(t, ".*too large.*", err)
		}
	}()

	wg.Wait()
}

func TestProducerDoubleClose(t *testing.T) {
	t.Skip("skip because of race introduced by #9026")
	t.Parallel()

	leader, _ := initBroker(t, false)
	defer leader.Close()

	config := getConfig(leader.Addr())

	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	saramaConfig, err := kafkav1.NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := kafka.NewMockAdminClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaDMLProducer(ctx, client, adminClient, errCh)
	require.Nil(t, err)
	require.NotNil(t, producer)

	producer.Close()
	producer.Close()
}
