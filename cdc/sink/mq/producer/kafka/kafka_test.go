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
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/sink/codec/common"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
)

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
	options := kafka.NewOptions()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	options.Version = "0.9.0.0"
	options.PartitionNum = int32(2)
	options.AutoCreate = false
	options.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)
	options.MaxMessages = 1
	_, err := kafka.NewSaramaConfig(ctx, options)
	require.Nil(t, err)
	client, err := NewClientImpl(ctx, options)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(ctx, options)
	require.Nil(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		options,
		errCh,
		changefeedID,
	)
	require.Nil(t, err)

	for i := 0; i < 100; i++ {
		err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		})
		require.Nil(t, err)
		err = producer.AsyncSendMessage(ctx, topic, int32(1), &common.Message{
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

	err = producer.SyncBroadcastMessage(ctx, topic, 2, &common.Message{
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
	err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
		Key:   []byte("cancel"),
		Value: nil,
	})
	if err != nil {
		require.Equal(t, context.Canceled, err)
	}
	err = producer.SyncBroadcastMessage(ctx, topic, 2, &common.Message{
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

	options := kafka.NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// When the topic does not exist, use the broker's configuration to create the topic.
	// topic not exist, `max-message-bytes` = `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	saramaConfig, err := kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, "create-random1")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes := options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic not exist, `max-message-bytes` > `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)
	err = AdjustOptions(adminClient, options, "create-random2")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)

	// topic not exist, `max-message-bytes` < `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)
	err = AdjustOptions(adminClient, options, "create-random3")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)
}

func TestAdjustConfigTopicExist(t *testing.T) {
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	options := kafka.NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// topic exists, `max-message-bytes` = `max.message.bytes`.
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	saramaConfig, err := kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes := options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic exists, `max-message-bytes` > `max.message.bytes`
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() + 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)

	// topic exists, `max-message-bytes` < `max.message.bytes`
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() - 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic does not have `max.message.bytes`
	// create a topic without `max.message.bytes`
	topicName := "test-topic"
	detail := &kafka.TopicDetail{
		Name:          topicName,
		NumPartitions: 3,
		// Does not contain `max.message.bytes`.
		ConfigEntries: make(map[string]string),
	}
	err = adminClient.CreateTopic(context.Background(), detail, false)
	require.Nil(t, err)

	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, topicName)
	require.Nil(t, err)

	// since `max.message.bytes` cannot found, use broker's `message.max.bytes` instead.
	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic doesn't have `max.message.bytes`
	// `max-message-bytes` > `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)

	err = AdjustOptions(adminClient, options, topicName)
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)
}

func TestAdjustConfigMinInsyncReplicas(t *testing.T) {
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	options := kafka.NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	adminClient.SetMinInsyncReplicas("2")
	err := AdjustOptions(
		adminClient,
		options,
		"create-new-fail-invalid-min-insync-replicas",
	)
	require.Regexp(
		t,
		".*`replication-factor` is smaller than the `min.insync.replicas` of broker.*",
		errors.Cause(err),
	)

	// topic not exist, and `min.insync.replicas` not found in broker's configuration
	adminClient.DropBrokerConfig(kafka.MinInsyncReplicasConfigName)
	topicName := "no-topic-no-min-insync-replicas"
	err = AdjustOptions(adminClient, options, "no-topic-no-min-insync-replicas")
	require.Nil(t, err)
	err = adminClient.CreateTopic(context.Background(), &kafka.TopicDetail{
		Name:              topicName,
		ReplicationFactor: 1,
	}, false)
	require.ErrorIs(t, err, sarama.ErrPolicyViolation)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.

	// topic exist, but `min.insync.replicas` not found in topic and broker configuration
	topicName = "topic-no-options-entry"
	err = adminClient.CreateTopic(context.Background(), &kafka.TopicDetail{
		Name:              topicName,
		ReplicationFactor: 3,
		NumPartitions:     3,
	}, false)
	require.Nil(t, err)
	err = AdjustOptions(adminClient, options, topicName)
	require.Nil(t, err)

	// topic found, and have `min.insync.replicas`, but set to 2, larger than `replication-factor`.
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustOptions(adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Regexp(t,
		".*`replication-factor` is smaller than the `min.insync.replicas` of topic.*",
		errors.Cause(err),
	)
}

func TestCreateProducerFailed(t *testing.T) {
	options := kafka.NewOptions()
	options.Version = "invalid"
	saramaConfig, err := kafka.NewSaramaConfig(context.Background(), options)
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

	options := kafka.NewOptions()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	options.Version = "0.9.0.0"
	options.PartitionNum = int32(2)
	options.AutoCreate = false
	options.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	errCh := make(chan error, 1)
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)

	options.MaxMessages = 1
	options.MaxMessageBytes = 8
	saramaConfig, err := kafka.NewSaramaConfig(context.Background(), options)
	require.Nil(t, err)
	require.Equal(t, 1, saramaConfig.Producer.Flush.MaxMessages)
	require.Equal(t, 8, saramaConfig.Producer.MaxMessageBytes)
	require.Nil(t, err)
	client, err := NewClientImpl(ctx, options)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(ctx, options)
	require.Nil(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		options,
		errCh,
		changefeedID,
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
			err = producer.AsyncSendMessage(ctx, topic, int32(0), &common.Message{
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

	options := kafka.NewOptions()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	options.Version = "0.9.0.0"
	options.PartitionNum = int32(2)
	options.AutoCreate = false
	options.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	errCh := make(chan error, 1)
	ctx = contextutil.PutRoleInCtx(ctx, util.RoleTester)
	client, err := NewClientImpl(ctx, options)
	require.Nil(t, err)
	adminClient, err := NewAdminClientImpl(ctx, options)
	require.Nil(t, err)

	changefeedID := model.DefaultChangeFeedID("changefeed-test")
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		options,
		errCh,
		changefeedID,
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

func TestConfigurationCombinations(t *testing.T) {
	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	combinations := []struct {
		uriTemplate             string
		uriParams               []interface{}
		brokerMessageMaxBytes   string
		topicMaxMessageBytes    string
		expectedMaxMessageBytes string
	}{
		// topic not created,
		// `max-message-bytes` not set, `message.max.bytes` < `max-message-bytes`
		// expected = min(`max-message-bytes`, `message.max.bytes`) = `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"not-exist-topic"},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.BrokerMessageMaxBytes,
		},
		// topic not created,
		// `max-message-bytes` not set, `message.max.bytes` = `max-message-bytes`
		// expected = min(`max-message-bytes`, `message.max.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"not-exist-topic"},
			strconv.Itoa(config.DefaultMaxMessageBytes),
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// topic not created,
		// `max-message-bytes` not set, broker `message.max.bytes` > `max-message-bytes`
		// expected = min(`max-message-bytes`, `message.max.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},

		// topic not created
		// user set `max-message-bytes` < `message.max.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 - 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic not created
		// user set `max-message-bytes` < default `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic not created
		// `message.max.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 + 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.BrokerMessageMaxBytes,
		},
		// topic not created
		// `message.max.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.BrokerMessageMaxBytes,
		},
		// topic not created
		// default `max-message-bytes` < user set `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic not created
		// default `max-message-bytes` < `message.max.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},

		// topic created,
		// `max-message-bytes` not set, topic's `max.message.bytes` < `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.TopicMaxMessageBytes,
		},
		// `max-message-bytes` not set, topic created,
		// topic's `max.message.bytes` = `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// `max-message-bytes` not set, topic created,
		// topic's `max.message.bytes` > `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},

		// topic created
		// user set `max-message-bytes` < `max.message.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(1024*1024 - 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic created
		// user set `max-message-bytes` < default `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				kafka.DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic created
		// `max.message.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(1024*1024 + 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.TopicMaxMessageBytes,
		},
		// topic created
		// `max.message.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				kafka.DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.TopicMaxMessageBytes,
		},
		// topic created
		// default `max-message-bytes` < user set `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				kafka.DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic created
		// default `max-message-bytes` < `max.message.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				kafka.DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
	}

	for _, a := range combinations {
		kafka.BrokerMessageMaxBytes = a.brokerMessageMaxBytes
		kafka.TopicMaxMessageBytes = a.topicMaxMessageBytes

		uri := fmt.Sprintf(a.uriTemplate, a.uriParams...)
		sinkURI, err := url.Parse(uri)
		require.Nil(t, err)

		options := kafka.NewOptions()
		err = options.Apply(sinkURI)
		require.Nil(t, err)

		ctx := context.Background()
		saramaConfig, err := kafka.NewSaramaConfig(ctx, options)
		require.Nil(t, err)

		adminClient, err := NewAdminClientImpl(ctx, options)
		require.Nil(t, err)

		topic, ok := a.uriParams[0].(string)
		require.True(t, ok)
		require.NotEqual(t, "", topic)
		err = AdjustOptions(adminClient, options, topic)
		require.Nil(t, err)

		encoderConfig := common.NewConfig(config.ProtocolOpen)
		err = encoderConfig.Apply(sinkURI, &config.ReplicaConfig{})
		require.Nil(t, err)
		encoderConfig.WithMaxMessageBytes(saramaConfig.Producer.MaxMessageBytes)

		err = encoderConfig.Validate()
		require.Nil(t, err)

		// producer's `MaxMessageBytes` = encoder's `MaxMessageBytes`.
		require.Equal(t, encoderConfig.MaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

		expected, err := strconv.Atoi(a.expectedMaxMessageBytes)
		require.Nil(t, err)
		require.Equal(t, expected, options.MaxMessageBytes)

		_ = adminClient.Close()
	}
}
