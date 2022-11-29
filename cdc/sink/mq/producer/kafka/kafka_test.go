// Copyright 2020 PingCAP, Inc.
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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestClientID(t *testing.T) {
	testCases := []struct {
		role         string
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{
			"owner", "domain:1234", "123-121-121-121",
			"", false,
			"TiCDC_sarama_producer_owner_domain_1234_default_123-121-121-121",
		},
		{
			"owner", "127.0.0.1:1234", "123-121-121-121",
			"", false,
			"TiCDC_sarama_producer_owner_127.0.0.1_1234_default_123-121-121-121",
		},
		{
			"owner", "127.0.0.1:1234?:,\"", "123-121-121-121",
			"", false,
			"TiCDC_sarama_producer_owner_127.0.0.1_1234_____default_123-121-121-121",
		},
		{
			"owner", "中文", "123-121-121-121",
			"", true, "",
		},
		{
			"owner", "127.0.0.1:1234",
			"123-121-121-121", "cdc-changefeed-1", false,
			"cdc-changefeed-1",
		},
	}
	for _, tc := range testCases {
		id, err := kafkaClientID(tc.role, tc.addr,
			model.DefaultChangeFeedID(tc.changefeedID), tc.configuredID)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.Nil(t, err)
			require.Equal(t, tc.expected, id)
		}
	}
}

func TestNewSaramaProducer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(t, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	prodSuccess := new(sarama.ProduceResponse)
	prodSuccess.AddTopicPartition(topic, 0, sarama.ErrNoError)
	prodSuccess.AddTopicPartition(topic, 1, sarama.ErrNoError)
	// 200 async messages and 2 sync message, Kafka flush could be in batch,
	// we can set flush.max.messages to 1 to control message count exactly.
	for i := 0; i < 202; i++ {
		leader.Returns(prodSuccess)
	}

	errCh := make(chan error, 1)
	config := NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(ctx, config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		config,
		saramaConfig,
		errCh,
	)
	require.Nil(t, err)

	for i := 0; i < 100; i++ {
		err = producer.AsyncSendMessage(ctx, topic, int32(0), &codec.MQMessage{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		})
		require.Nil(t, err)
		err = producer.AsyncSendMessage(ctx, topic, int32(1), &codec.MQMessage{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		})
		require.Nil(t, err)
	}

	err = producer.Flush(ctx)
	require.Nil(t, err)
	select {
	case err := <-errCh:
		t.Fatalf("unexpected err: %s", err)
	default:
	}
	producer.mu.Lock()
	require.Equal(t, int64(0), producer.mu.inflight)
	producer.mu.Unlock()
	// check no events to flush
	err = producer.Flush(ctx)
	require.Nil(t, err)
	producer.mu.Lock()
	require.Equal(t, int64(0), producer.mu.inflight)
	producer.mu.Unlock()

	err = producer.SyncBroadcastMessage(ctx, topic, 2, &codec.MQMessage{
		Key:   []byte("test-broadcast"),
		Value: nil,
	})
	require.Nil(t, err)

	err = producer.Close()
	require.Nil(t, err)
	// check reentrant close
	err = producer.Close()
	require.Nil(t, err)
	cancel()

	// check send messages when context is canceled or producer closed
	err = producer.AsyncSendMessage(ctx, topic, int32(0), &codec.MQMessage{
		Key:   []byte("cancel"),
		Value: nil,
	})
	if err != nil {
		require.Equal(t, context.Canceled, err)
	}
	err = producer.SyncBroadcastMessage(ctx, topic, 2, &codec.MQMessage{
		Key:   []byte("cancel"),
		Value: nil,
	})
	if err != nil {
		require.Equal(t, context.Canceled, err)
	}
}

func TestAdjustConfigTopicNotExist(t *testing.T) {
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	config := NewConfig()
	config.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// When the topic does not exist, use the broker's configuration to create the topic.
	// topic not exist, `max-message-bytes` = `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, "create-random1")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes := config.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic not exist, `max-message-bytes` > `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	err = AdjustConfig(adminClient, config, saramaConfig, "create-random2")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic not exist, `max-message-bytes` < `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	err = AdjustConfig(adminClient, config, saramaConfig, "create-random3")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)
}

func TestAdjustConfigTopicExist(t *testing.T) {
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	config := NewConfig()
	config.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// topic exists, `max-message-bytes` = `max.message.bytes`.
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes := config.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic exists, `max-message-bytes` > `max.message.bytes`
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic exists, `max-message-bytes` < `max.message.bytes`
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic does not have `max.message.bytes`
	// create a topic without `max.message.bytes`
	topicName := "test-topic"
	detail := &sarama.TopicDetail{
		NumPartitions: 3,
		// Does not contain `max.message.bytes`.
		ConfigEntries: make(map[string]*string),
	}
	err = adminClient.CreateTopic(topicName, detail, false)
	require.Nil(t, err)

	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, topicName)
	require.Nil(t, err)

	// since `max.message.bytes` cannot found, use broker's `message.max.bytes` instead.
	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic doesn't have `max.message.bytes`
	// `max-message-bytes` > `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	err = AdjustConfig(adminClient, config, saramaConfig, topicName)
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)
}

func TestAdjustConfigMinInsyncReplicas(t *testing.T) {
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	config := NewConfig()
	config.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustConfig(adminClient, config, saramaConfig, "create-new-fail-invalid-min-insync-replicas")
	require.Regexp(
		t,
		".*`replication-factor` is smaller than the `min.insync.replicas` of broker.*",
		errors.Cause(err),
	)

	// topic not exist, and `min.insync.replicas` not found in broker's configuration
	adminClient.DropBrokerConfig(kafka.MinInsyncReplicasConfigName)
	topicName := "no-topic-no-min-insync-replicas"
	err = AdjustConfig(adminClient, config, saramaConfig, "no-topic-no-min-insync-replicas")
	require.Nil(t, err)
	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{ReplicationFactor: 1}, false)
	require.ErrorIs(t, err, sarama.ErrPolicyViolation)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)

	// topic exist, but `min.insync.replicas` not found in topic and broker configuration
	topicName = "topic-no-config-entry"
	err = adminClient.CreateTopic(topicName, &sarama.TopicDetail{
		ReplicationFactor: 3,
		NumPartitions:     3,
	}, false)
	require.Nil(t, err)
	err = AdjustConfig(adminClient, config, saramaConfig, topicName)
	require.Nil(t, err)

	// topic found, and have `min.insync.replicas`, but set to 2, larger than `replication-factor`.
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	require.Regexp(t,
		".*`replication-factor` is smaller than the `min.insync.replicas` of topic.*",
		errors.Cause(err),
	)
}

func TestCreateProducerFailed(t *testing.T) {
	config := NewConfig()
	config.Version = "invalid"
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	require.Nil(t, saramaConfig)
}

func TestProducerSendMessageFailed(t *testing.T) {
	topic := kafka.DefaultMockTopicName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(t, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	config := NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	errCh := make(chan error, 1)
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	saramaConfig.Producer.Flush.MaxMessages = 1
	saramaConfig.Producer.Retry.Max = 2
	saramaConfig.Producer.MaxMessageBytes = 8

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		config,
		saramaConfig,
		errCh,
	)
	defer func() {
		err := producer.Close()
		require.Nil(t, err)
	}()

	require.Nil(t, err)
	require.NotNil(t, producer)

	var wg sync.WaitGroup

	wg.Add(1)
	go func(t *testing.T) {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			err = producer.AsyncSendMessage(ctx, topic, int32(0), &codec.MQMessage{
				Key:   []byte("test-key-1"),
				Value: []byte("test-value"),
			})
			require.Nil(t, err)
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
	topic := kafka.DefaultMockTopicName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(t, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	// Response for `sarama.NewClient`
	leader.Returns(metadataResponse)

	config := NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	errCh := make(chan error, 1)
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	require.Nil(t, err)
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	require.Nil(t, err)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		config,
		saramaConfig,
		errCh,
	)
	defer func() {
		err := producer.Close()
		require.Nil(t, err)
	}()

	require.Nil(t, err)
	require.NotNil(t, producer)

	err = producer.Close()
	require.Nil(t, err)

	err = producer.Close()
	require.Nil(t, err)
}
