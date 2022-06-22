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
	"strconv"
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

	newSaramaConfigImplBak := newSaramaConfigImpl
	newSaramaConfigImpl = func(ctx context.Context, config *Config) (*sarama.Config, error) {
		cfg, err := newSaramaConfigImplBak(ctx, config)
		c.Assert(err, check.IsNil)
		cfg.Producer.Flush.MaxMessages = 1
		return cfg, err
	}
	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	opts := make(map[string]string)
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	c.Assert(err, check.IsNil)
	c.Assert(producer.GetPartitionNum(), check.Equals, int32(2))
	c.Assert(opts, check.HasKey, "max-message-bytes")
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

	// In TiCDC logic, resolved ts event will always notify the flush loop. Here we
	// trigger the flushedNotifier periodically to prevent the flush loop block.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Millisecond * 100):
				producer.flushedNotifier.Notify()
			}
		}
	}()

	err = producer.Flush(ctx)
	c.Assert(err, check.IsNil)
	expected := []struct {
		flushed uint64
		sent    uint64
	}{
		{100, 100},
		{100, 100},
	}
	c.Assert(producer.partitionOffset, check.DeepEquals, expected)
	select {
	case err := <-errCh:
		c.Fatalf("unexpected err: %s", err)
	default:
	}
	// check no events to flush
	err = producer.Flush(ctx)
	c.Assert(err, check.IsNil)

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
	wg.Wait()

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

func (s *kafkaSuite) TestValidateAndCreateTopic(c *check.C) {
	defer testleak.AfterTest(c)
	config := NewConfig()
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func() {
		_ = adminClient.Close()
	}()

	// When topic exists and max message bytes is set correctly.
	config.MaxMessageBytes = adminClient.GetDefaultMaxMessageBytes()
	cfg, err := newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts := make(map[string]string)
	err = validateAndCreateTopic(adminClient, adminClient.GetDefaultMockTopicName(), config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// When topic exists and max message bytes is not set correctly.
	// use the smaller one.
	defaultMaxMessageBytes := adminClient.GetDefaultMaxMessageBytes()
	config.MaxMessageBytes = defaultMaxMessageBytes + 1
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, adminClient.GetDefaultMockTopicName(), config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, defaultMaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	config.MaxMessageBytes = defaultMaxMessageBytes - 1
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, adminClient.GetDefaultMockTopicName(), config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, config.MaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// When topic does not exist and auto-create is not enabled.
	config.AutoCreate = false
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "non-exist", config, cfg, opts)
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*auto-create-topic` is false, and topic not found.*",
	)

	// When the topic does not exist, use the broker's configuration to create the topic.
	// It is less than the value of broker.
	config.AutoCreate = true
	config.MaxMessageBytes = defaultMaxMessageBytes - 1
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "create-random1", config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, config.MaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// When the topic does not exist, use the broker's configuration to create the topic.
	// It is larger than the value of broker.
	config.MaxMessageBytes = defaultMaxMessageBytes + 1
	config.AutoCreate = true
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "create-random2", config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, defaultMaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// When the topic exists, but the topic does not store max message bytes info,
	// the check of parameter succeeds.
	// It is less than the value of broker.
	config.MaxMessageBytes = defaultMaxMessageBytes - 1
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	detail := &sarama.TopicDetail{
		NumPartitions: 3,
		// Does not contain max message bytes information.
		ConfigEntries: make(map[string]*string),
	}
	err = adminClient.CreateTopic("test-topic", detail, false)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "test-topic", config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, config.MaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// When the topic exists, but the topic does not store max message bytes info,
	// the check of parameter fails.
	// It is larger than the value of broker.
	config.MaxMessageBytes = defaultMaxMessageBytes + 1
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "test-topic", config, cfg, opts)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, defaultMaxMessageBytes)
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(cfg.Producer.MaxMessageBytes))

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	adminClient.SetMinInsyncReplicas("2")
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, "create-new-fail-invalid-min-insync-replicas", config, cfg, opts)
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*`replication-factor` cannot be smaller than the `min.insync.replicas` of broker.*",
	)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.
	cfg, err = newSaramaConfigImpl(context.Background(), config)
	c.Assert(err, check.IsNil)
	adminClient.SetMinInsyncReplicas("2")
	opts = make(map[string]string)
	err = validateAndCreateTopic(adminClient, adminClient.GetDefaultMockTopicName(), config, cfg, opts)
	c.Assert(
		errors.Cause(err),
		check.ErrorMatches,
		".*`replication-factor` cannot be smaller than the `min.insync.replicas` of topic.*",
	)
}

func (s *kafkaSuite) TestCreateProducerFailed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	errCh := make(chan error, 1)
	config := NewConfig()
	config.Version = "invalid"
	config.BrokerEndpoints = []string{"127.0.0.1:1111"}
	topic := "topic"
	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()
	opts := make(map[string]string)
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	_, err := NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	c.Assert(errors.Cause(err), check.ErrorMatches, "invalid version.*")
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

	newSaramaConfigImplBak := newSaramaConfigImpl
	newSaramaConfigImpl = func(ctx context.Context, config *Config) (*sarama.Config, error) {
		cfg, err := newSaramaConfigImplBak(ctx, config)
		c.Assert(err, check.IsNil)
		cfg.Producer.Flush.MaxMessages = 1
		cfg.Producer.Retry.Max = 2
		cfg.Producer.MaxMessageBytes = 8
		return cfg, err
	}
	defer func() {
		newSaramaConfigImpl = newSaramaConfigImplBak
	}()

	errCh := make(chan error, 1)
	opts := make(map[string]string)
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	c.Assert(opts, check.HasKey, "max-message-bytes")
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
	opts := make(map[string]string)
	ctx = util.PutRoleInCtx(ctx, util.RoleTester)
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	c.Assert(opts, check.HasKey, "max-message-bytes")
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
