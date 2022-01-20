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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/cdc/sink/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
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

func (s *kafkaSuite) TestInitializeConfig(c *check.C) {
	defer testleak.AfterTest(c)
	cfg := NewConfig()

	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)

	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	replicaConfig := config.GetDefaultReplicaConfig()

	opts := make(map[string]string)
	err = cfg.Initialize(sinkURI, replicaConfig, opts)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.PartitionNum, check.Equals, int32(1))
	c.Assert(cfg.ReplicationFactor, check.Equals, int16(3))
	c.Assert(cfg.Version, check.Equals, "2.6.0")
	c.Assert(cfg.MaxMessageBytes, check.Equals, 4096)

	expectedOpts := map[string]string{
		"max-message-bytes": maxMessageSize,
		"max-batch-size":    "5",
	}
	for k, v := range opts {
		c.Assert(v, check.Equals, expectedOpts[k])
	}

	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	err = cfg.Initialize(sinkURI, replicaConfig, opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid partition num.*")
}

func (s *kafkaSuite) TestSaramaProducer(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx, cancel := context.WithCancel(context.Background())

	topic := "unit_test_1"
	leader := sarama.NewMockBroker(c, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
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
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate", "return(true)"), check.IsNil)
	defer func() {
		newSaramaConfigImpl = newSaramaConfigImplBak
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate")
	}()
	opts := make(map[string]string)
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, opts, errCh)
	c.Assert(err, check.IsNil)
	c.Assert(producer.GetPartitionNum(), check.Equals, int32(2))
	c.Assert(opts, check.HasKey, "max-message-bytes")
	for i := 0; i < 100; i++ {
		err = producer.SendMessage(ctx, &codec.MQMessage{
			Key:   []byte("test-key-1"),
			Value: []byte("test-value"),
		}, int32(0))
		c.Assert(err, check.IsNil)
		err = producer.SendMessage(ctx, &codec.MQMessage{
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
	err = producer.SendMessage(ctx, &codec.MQMessage{
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

func (s *kafkaSuite) TestAdjustPartitionNum(c *check.C) {
	defer testleak.AfterTest(c)()
	config := NewConfig()
	err := config.adjustPartitionNum(2)
	c.Assert(err, check.IsNil)
	c.Assert(config.PartitionNum, check.Equals, int32(2))

	config.PartitionNum = 1
	err = config.adjustPartitionNum(2)
	c.Assert(err, check.IsNil)
	c.Assert(config.PartitionNum, check.Equals, int32(1))

	config.PartitionNum = 3
	err = config.adjustPartitionNum(2)
	c.Assert(cerror.ErrKafkaInvalidPartitionNum.Equal(err), check.IsTrue)
}

func (s *kafkaSuite) TestTopicPreProcess(c *check.C) {
	defer testleak.AfterTest(c)
	topic := "unit_test_2"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := sarama.NewMockBroker(c, 1)
	defer broker.Close()
	metaResponse := sarama.NewMockMetadataResponse(c).
		SetBroker(broker.Addr(), broker.BrokerID()).
		SetLeader(topic, 0, broker.BrokerID()).
		SetLeader(topic, 1, broker.BrokerID()).
		SetController(broker.BrokerID())
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"MetadataRequest":        metaResponse,
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(c),
	})
	config := NewConfig()
	config.PartitionNum = int32(0)
	config.BrokerEndpoints = strings.Split(broker.Addr(), ",")
	config.AutoCreate = false

	cfg, err := newSaramaConfigImpl(ctx, config)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.Producer.MaxMessageBytes, check.Equals, config.MaxMessageBytes)

	config.BrokerEndpoints = []string{""}
	cfg.Metadata.Retry.Max = 1

	err = topicPreProcess(topic, config, cfg)
	c.Assert(errors.Cause(err), check.Equals, sarama.ErrOutOfBrokers)
}

func (s *kafkaSuite) TestNewSaramaConfig(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	config := NewConfig()
	config.Version = "invalid"
	_, err := newSaramaConfigImpl(ctx, config)
	c.Assert(errors.Cause(err), check.ErrorMatches, "invalid version.*")

	ctx = util.SetOwnerInCtx(ctx)
	config.Version = "2.6.0"
	config.ClientID = "^invalid$"
	_, err = newSaramaConfigImpl(ctx, config)
	c.Assert(cerror.ErrKafkaInvalidClientID.Equal(err), check.IsTrue)

	config.ClientID = "test-kafka-client"
	compressionCases := []struct {
		algorithm string
		expected  sarama.CompressionCodec
	}{
		{"none", sarama.CompressionNone},
		{"gzip", sarama.CompressionGZIP},
		{"snappy", sarama.CompressionSnappy},
		{"lz4", sarama.CompressionLZ4},
		{"zstd", sarama.CompressionZSTD},
		{"others", sarama.CompressionNone},
	}
	for _, cc := range compressionCases {
		config.Compression = cc.algorithm
		cfg, err := newSaramaConfigImpl(ctx, config)
		c.Assert(err, check.IsNil)
		c.Assert(cfg.Producer.Compression, check.Equals, cc.expected)
	}

	config.Credential = &security.Credential{
		CAPath: "/invalid/ca/path",
	}
	_, err = newSaramaConfigImpl(ctx, config)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*no such file or directory")

	saslConfig := NewConfig()
	saslConfig.Version = "2.6.0"
	saslConfig.ClientID = "test-sasl-scram"
	saslConfig.SaslScram = &security.SaslScram{
		SaslUser:      "user",
		SaslPassword:  "password",
		SaslMechanism: sarama.SASLTypeSCRAMSHA256,
	}

	cfg, err := newSaramaConfigImpl(ctx, saslConfig)
	c.Assert(err, check.IsNil)
	c.Assert(cfg, check.NotNil)
	c.Assert(cfg.Net.SASL.User, check.Equals, "user")
	c.Assert(cfg.Net.SASL.Password, check.Equals, "password")
	c.Assert(cfg.Net.SASL.Mechanism, check.Equals, sarama.SASLMechanism("SCRAM-SHA-256"))
}

func (s *kafkaSuite) TestCreateProducerFailed(c *check.C) {
	defer testleak.AfterTest(c)()
	ctx := context.Background()
	errCh := make(chan error, 1)
	config := NewConfig()
	config.Version = "invalid"
	config.BrokerEndpoints = []string{"127.0.0.1:1111"}
	topic := "topic"
	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate", "return(true)"), check.IsNil)
	_, err := NewKafkaSaramaProducer(ctx, topic, config, map[string]string{}, errCh)
	c.Assert(errors.Cause(err), check.ErrorMatches, "invalid version.*")

	_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate")
}

func (s *kafkaSuite) TestProducerSendMessageFailed(c *check.C) {
	defer testleak.AfterTest(c)()
	topic := "unit_test_4"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(c, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	config := NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate", "return(true)"), check.IsNil)

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
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, map[string]string{}, errCh)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate")
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
			err = producer.SendMessage(ctx, &codec.MQMessage{
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
	topic := "unit_test_4"
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	leader := sarama.NewMockBroker(c, 2)
	defer leader.Close()
	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(leader.Addr(), leader.BrokerID())
	metadataResponse.AddTopicPartition(topic, 0, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	metadataResponse.AddTopicPartition(topic, 1, leader.BrokerID(), nil, nil, nil, sarama.ErrNoError)
	leader.Returns(metadataResponse)
	leader.Returns(metadataResponse)

	config := NewConfig()
	// Because the sarama mock broker is not compatible with version larger than 1.0.0
	// We use a smaller version in the following producer tests.
	// Ref: https://github.com/Shopify/sarama/blob/89707055369768913defac030c15cf08e9e57925/async_producer_test.go#L1445-L1447
	config.Version = "0.9.0.0"
	config.PartitionNum = int32(2)
	config.AutoCreate = false
	config.BrokerEndpoints = strings.Split(leader.Addr(), ",")

	c.Assert(failpoint.Enable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate", "return(true)"), check.IsNil)

	errCh := make(chan error, 1)
	producer, err := NewKafkaSaramaProducer(ctx, topic, config, map[string]string{}, errCh)
	defer func() {
		err := producer.Close()
		c.Assert(err, check.IsNil)
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/sink/producer/kafka/SkipTopicAutoCreate")
	}()

	c.Assert(err, check.IsNil)
	c.Assert(producer, check.NotNil)

	err = producer.Close()
	c.Assert(err, check.IsNil)

	err = producer.Close()
	c.Assert(err, check.IsNil)
}
