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
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

type kafkaSuite struct{}

var _ = check.Suite(&kafkaSuite{})

func Test(t *testing.T) { check.TestingT(t) }

func (s *kafkaSuite) TestClientID(c *check.C) {
	defer testleak.AfterTest(c)()
	testCases := []struct {
		role         string
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{"owner", "domain:1234", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_domain_1234_123-121-121-121"},
		{"owner", "127.0.0.1:1234", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_127.0.0.1_1234_123-121-121-121"},
		{"owner", "127.0.0.1:1234?:,\"", "123-121-121-121", "", false, "TiCDC_sarama_producer_owner_127.0.0.1_1234_____123-121-121-121"},
		{"owner", "中文", "123-121-121-121", "", true, ""},
		{"owner", "127.0.0.1:1234", "123-121-121-121", "cdc-changefeed-1", false, "cdc-changefeed-1"},
	}
	for _, tc := range testCases {
		id, err := kafkaClientID(tc.role, tc.addr, tc.changefeedID, tc.configuredID)
		if tc.hasError {
			c.Assert(err, check.NotNil)
		} else {
			c.Assert(err, check.IsNil)
			c.Assert(id, check.Equals, tc.expected)
		}
	}
}

func (s *kafkaSuite) TestNewSaramaProducer(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := kafka.DefaultMockTopicName
	leader := sarama.NewMockBroker(c, 2)
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

	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(ctx, config)
	c.Assert(err, check.IsNil)
	saramaConfig.Producer.Flush.MaxMessages = 1
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		topic,
		config,
		saramaConfig,
		errCh,
	)
	c.Assert(err, check.IsNil)
	c.Assert(producer.GetPartitionNum(), check.Equals, int32(2))

	for i := 0; i < 100; i++ {
		err = producer.AsyncSendMessage(ctx, &codec.MQMessage{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		}, int32(0))
		c.Assert(err, check.IsNil)
		err = producer.AsyncSendMessage(ctx, &codec.MQMessage{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		}, int32(1))
		c.Assert(err, check.IsNil)
	}

	err = producer.Flush(ctx)
	c.Assert(err, check.IsNil)
	select {
	case err := <-errCh:
		c.Fatalf("unexpected err: %s", err)
	default:
	}
	producer.mu.Lock()
	c.Assert(producer.mu.inflight, check.Equals, int64(0))
	producer.mu.Unlock()
	// check no events to flush
	err = producer.Flush(ctx)
	c.Assert(err, check.IsNil)
	producer.mu.Lock()
	c.Assert(producer.mu.inflight, check.Equals, int64(0))
	producer.mu.Unlock()

	err = producer.SyncBroadcastMessage(ctx, &codec.MQMessage{
		Key:   []byte("test-broadcast"),
		Value: nil,
	})
	c.Assert(err, check.IsNil)

	err = producer.Close()
	c.Assert(err, check.IsNil)
	// check reentrant close
	err = producer.Close()
	c.Assert(err, check.IsNil)
	cancel()

	// check send messages when context is canceled or producer closed
	err = producer.AsyncSendMessage(ctx, &codec.MQMessage{
		Key:   []byte("cancel"),
		Value: nil,
	}, int32(0))
	if err != nil {
		c.Assert(err, check.Equals, context.Canceled)
	}
	err = producer.SyncBroadcastMessage(ctx, &codec.MQMessage{
		Key:   []byte("cancel"),
		Value: nil,
	})
	if err != nil {
		c.Assert(err, check.Equals, context.Canceled)
	}
}

func (s *kafkaSuite) TestAdjustConfigTopicNotExist(c *check.C) {
	defer testleak.AfterTest(c)()

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
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, "create-random1")
	c.Assert(err, check.IsNil)
	expectedSaramaMaxMessageBytes := config.MaxMessageBytes
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// topic not exist, `max-message-bytes` > `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	err = AdjustConfig(adminClient, config, saramaConfig, "create-random2")
	c.Assert(err, check.IsNil)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// topic not exist, `max-message-bytes` < `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	err = AdjustConfig(adminClient, config, saramaConfig, "create-random3")
	c.Assert(err, check.IsNil)
	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)
}

func (s *kafkaSuite) TestAdjustConfigTopicExist(c *check.C) {
	defer testleak.AfterTest(c)()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	config := NewConfig()
	config.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// topic exists, `max-message-bytes` = `max.message.bytes`.
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	c.Assert(err, check.IsNil)

	expectedSaramaMaxMessageBytes := config.MaxMessageBytes
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// topic exists, `max-message-bytes` > `max.message.bytes`
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	c.Assert(err, check.IsNil)

	expectedSaramaMaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// topic exists, `max-message-bytes` < `max.message.bytes`
	config.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	c.Assert(err, check.IsNil)

	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// When the topic exists, but the topic does not have `max.message.bytes`
	// create a topic without `max.message.bytes`
	topicName := "test-topic"
	detail := &sarama.TopicDetail{
		NumPartitions: 3,
		// Does not contain `max.message.bytes`.
		ConfigEntries: make(map[string]*string),
	}
	err = adminClient.CreateTopic(topicName, detail, false)
	c.Assert(err, check.IsNil)

	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, topicName)
	c.Assert(err, check.IsNil)

	// since `max.message.bytes` cannot found, use broker's `message.max.bytes` instead.
	expectedSaramaMaxMessageBytes = config.MaxMessageBytes
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)

	// When the topic exists, but the topic doesn't have `max.message.bytes`
	// `max-message-bytes` > `message.max.bytes`
	config.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)

	err = AdjustConfig(adminClient, config, saramaConfig, topicName)
	c.Assert(err, check.IsNil)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, expectedSaramaMaxMessageBytes)
}

func (s *kafkaSuite) TestAdjustConfigMinInsyncReplicas(c *check.C) {
	defer testleak.AfterTest(c)()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	config := NewConfig()
	config.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustConfig(adminClient, config, saramaConfig, "create-new-fail-invalid-min-insync-replicas")
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*`replication-factor` cannot be smaller than the `min.insync.replicas` of broker.*",
	)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.
	saramaConfig, err = NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustConfig(adminClient, config, saramaConfig, adminClient.GetDefaultMockTopicName())
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*`replication-factor` cannot be smaller than the `min.insync.replicas` of topic.*",
	)
}

func (s *kafkaSuite) TestCreateProducerFailed(c *check.C) {
	defer testleak.AfterTest(c)()
	config := NewConfig()
	config.Version = "invalid"
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	c.Assert(errors.Cause(err), check.ErrorMatches, "invalid version.*")
	c.Assert(saramaConfig, check.IsNil)
}

func (s *kafkaSuite) TestProducerSendMessageFailed(c *check.C) {
	defer testleak.AfterTest(c)()
	topic := kafka.DefaultMockTopicName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(c, 2)
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
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	saramaConfig.Producer.Flush.MaxMessages = 1
	saramaConfig.Producer.Retry.Max = 2
	saramaConfig.Producer.MaxMessageBytes = 8

	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		topic,
		config,
		saramaConfig,
		errCh,
	)
	defer func() {
		err := producer.Close()
		c.Assert(err, check.IsNil)
	}()

	c.Assert(err, check.IsNil)
	c.Assert(producer, check.NotNil)

	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			err = producer.AsyncSendMessage(ctx, &codec.MQMessage{
				Key:   []byte("test-key-1"),
				Value: []byte("test-value"),
			}, int32(0))
			c.Assert(err, check.IsNil)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		select {
		case <-ctx.Done():
			c.Fatal("TestProducerSendMessageFailed timed out")
		case err := <-errCh:
			c.Assert(err, check.ErrorMatches, ".*too large.*")
		}
	}()

	wg.Wait()
}

func (s *kafkaSuite) TestProducerDoubleClose(c *check.C) {
	defer testleak.AfterTest(c)()
	topic := kafka.DefaultMockTopicName
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(c, 2)
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
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	saramaConfig, err := NewSaramaConfig(context.Background(), config)
	c.Assert(err, check.IsNil)
	client, err := sarama.NewClient(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	adminClient, err := NewAdminClientImpl(config.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)
	producer, err := NewKafkaSaramaProducer(
		ctx,
		client,
		adminClient,
		topic,
		config,
		saramaConfig,
		errCh,
	)
	defer func() {
		err := producer.Close()
		c.Assert(err, check.IsNil)
	}()

	c.Assert(err, check.IsNil)
	c.Assert(producer, check.NotNil)

	err = producer.Close()
	c.Assert(err, check.IsNil)

	err = producer.Close()
	c.Assert(err, check.IsNil)
}
