// Copyright 2021 PingCAP, Inc.
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

	"github.com/Shopify/sarama"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/util/testleak"
)

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

func (s *kafkaSuite) TestFillBySinkURI(c *check.C) {
	defer testleak.AfterTest(c)()

	uri := "kafka://127.0.0.1:9092/no-params"
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	producerConfig := NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(err, check.IsNil)

	c.Assert(len(producerConfig.BrokerEndpoints), check.Equals, 1)
	c.Assert(producerConfig.BrokerEndpoints[0], check.Equals, "127.0.0.1:9092")
	c.Assert(producerConfig.PartitionNum, check.Equals, int32(0))
	c.Assert(producerConfig.ReplicationFactor, check.Equals, int16(1))
	c.Assert(producerConfig.Version, check.Equals, defaultVersion)
	c.Assert(producerConfig.MaxMessageBytes, check.Equals, config.DefaultMaxMessageBytes)
	c.Assert(producerConfig.Compression, check.Equals, defaultCompression)
	c.Assert(producerConfig.ClientID, check.Equals, "")

	c.Assert(producerConfig.Credential.CAPath, check.Equals, "")
	c.Assert(producerConfig.Credential.KeyPath, check.Equals, "")
	c.Assert(producerConfig.Credential.CertPath, check.Equals, "")
	c.Assert(len(producerConfig.Credential.CertAllowedCN), check.Equals, 0)

	c.Assert(producerConfig.SaslScram.SaslMechanism, check.Equals, "")
	c.Assert(producerConfig.SaslScram.SaslUser, check.Equals, "")
	c.Assert(producerConfig.SaslScram.SaslPassword, check.Equals, "")

	c.Assert(producerConfig.AutoCreate, check.IsTrue)

	uri = "kafka://127.0.0.1:9092/full-params?partition-num=3&replication-factor=3" +
		"&kafka-version=2.6.0&kafka-client-id=kafka-ut&max-message-bytes=1048576&compression=snappy" +
		"&ca=123&cert=123&key=123&sasl-user=123&sasl-password=123&sasl-mechanism=123&auto-create-topic=false"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)

	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(err, check.IsNil)

	c.Assert(len(producerConfig.BrokerEndpoints), check.Equals, 1)
	c.Assert(producerConfig.BrokerEndpoints[0], check.Equals, "127.0.0.1:9092")
	c.Assert(producerConfig.PartitionNum, check.Equals, int32(3))
	c.Assert(producerConfig.ReplicationFactor, check.Equals, int16(3))
	c.Assert(producerConfig.Version, check.Equals, "2.6.0")
	c.Assert(producerConfig.ClientID, check.Equals, "kafka-ut")
	c.Assert(producerConfig.MaxMessageBytes, check.Equals, 1048576)
	c.Assert(producerConfig.Compression, check.Equals, "snappy")
	c.Assert(producerConfig.Credential.CAPath, check.Equals, "123")
	c.Assert(producerConfig.Credential.CertPath, check.Equals, "123")
	c.Assert(producerConfig.Credential.KeyPath, check.Equals, "123")

	c.Assert(producerConfig.SaslScram.SaslMechanism, check.Equals, "123")
	c.Assert(producerConfig.SaslScram.SaslUser, check.Equals, "123")
	c.Assert(producerConfig.SaslScram.SaslPassword, check.Equals, "123")

	c.Assert(producerConfig.AutoCreate, check.IsFalse)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal auto-create-topic
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&auto-create-topic=123"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	c.Assert(err, check.IsNil)
	producerConfig = NewConfig()
	err = producerConfig.fillBySinkURI(sinkURI)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid partition num.*")
}

func (s *kafkaSuite) TestCompleteOpts(c *check.C) {
	defer testleak.AfterTest(c)()

	uri := "kafka://127.0.0.1:9092/abc?enable-tidb-extension=true&protocol=canal-json&max-batch-size=1"
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	producerConfig := NewConfig()
	saramaConfig, err := newSaramaConfigImpl(context.Background(), producerConfig)
	c.Assert(err, check.IsNil)
	replicaConfig := config.GetDefaultReplicaConfig()
	err = replicaConfig.FillBySInkURI(sinkURI)
	c.Assert(err, check.IsNil)
	c.Assert(replicaConfig.Sink.Protocol, check.Equals, "canal-json")

	opts := make(map[string]string)
	err = completeOpts(sinkURI, opts, saramaConfig, replicaConfig)
	c.Assert(err, check.IsNil)
	c.Assert(opts["enable-tidb-extension"], check.Equals, "true")
	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(saramaConfig.Producer.MaxMessageBytes))
	c.Assert(opts["max-batch-size"], check.Equals, "1")

	// Illegal enable-tidb-extension.
	uri = "kafka://127.0.0.1:9092/abc?enable-tidb-extension=a&protocol=canal-json"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	replicaConfig = config.GetDefaultReplicaConfig()
	err = replicaConfig.FillBySInkURI(sinkURI)
	c.Assert(err, check.IsNil)
	c.Assert(replicaConfig.Sink.Protocol, check.Equals, "canal-json")
	opts = make(map[string]string)
	err = completeOpts(sinkURI, opts, saramaConfig, config.GetDefaultReplicaConfig())
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Use enable-tidb-extension on other protocols.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=1&enable-tidb-extension=true"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	replicaConfig = config.GetDefaultReplicaConfig()
	err = replicaConfig.FillBySInkURI(sinkURI)
	c.Assert(err, check.IsNil)
	c.Assert(replicaConfig.Sink.Protocol, check.Not(check.Equals), "canal-json")

	opts = make(map[string]string)
	err = completeOpts(sinkURI, opts, saramaConfig, config.GetDefaultReplicaConfig())
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*enable-tidb-extension only support canal-json protocol.*")
}

func (s *kafkaSuite) TestAdjustConfig(c *check.C) {
	defer testleak.AfterTest(c)()

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	producerConfig := NewConfig()
	saramaConfig, err := newSaramaConfigImpl(context.Background(), producerConfig)
	c.Assert(err, check.IsNil)

	adminClient, err := NewAdminClientImpl(producerConfig.BrokerEndpoints, saramaConfig)
	c.Assert(err, check.IsNil)

	// When the topic exists, but the topic does not store max message bytes info,
	// the check of parameter succeeds.
	detail := &sarama.TopicDetail{
		NumPartitions: 3,
		// Does not contain max message bytes information.
		ConfigEntries: make(map[string]*string),
	}
	err = adminClient.CreateTopic("test-topic", detail, false)
	c.Assert(err, check.IsNil)

	// It is less than the value of broker.
	producerConfig.MaxMessageBytes = config.DefaultMaxMessageBytes - 1
	saramaConfig, err = newSaramaConfigImpl(context.Background(), producerConfig)
	c.Assert(err, check.IsNil)

	err = adjustConfig(adminClient, "test-topic", producerConfig)
	c.Assert(err, check.IsNil)
	adjustSaramaConfig(saramaConfig, producerConfig)
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, producerConfig.MaxMessageBytes)

	// When the topic exists, but the topic does not store max message bytes info,
	// the check of parameter fails.
	// It is larger than the value of broker.
	producerConfig.MaxMessageBytes = config.DefaultMaxMessageBytes + 1
	saramaConfig, err = newSaramaConfigImpl(context.Background(), producerConfig)
	c.Assert(err, check.IsNil)
	err = adjustConfig(adminClient, "test-topic", producerConfig)
	c.Assert(err, check.IsNil)
	adjustSaramaConfig(saramaConfig, producerConfig)
	c.Assert(saramaConfig.Producer.MaxMessageBytes, check.Equals, producerConfig.MaxMessageBytes)

}

func (s *kafkaSuite) TestInitializeConfigurations(c *check.C) {
	defer testleak.AfterTest(c)()

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
		//`max-message-bytes` not set, topic not created, broker `message.max.bytes` is the default.
		{"kafka://127.0.0.1:9092/%s",
			[]interface{}{"no-params"},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultBrokerMessageMaxBytes,
		},
		// `max-message-bytes` not set, topic not created, broker `message.max.bytes` is set larger than the default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// `max-message-bytes` not set, topic not created, broker `message.max.bytes` is set less than the default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// `max-message-bytes` not set, topic created, topic's `max.message.bytes` is the default
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultTopicMaxMessageBytes,
		},
		// `max-message-bytes` not set, topic created, topic's `max.message.bytes` large than the default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.DefaultBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// `max-message-bytes` not set, topic created, topic's `max.message.bytes` small than the default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.DefaultBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic not created
		// user set `max-message-bytes` < `message.max.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 - 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic not created
		// user set `max-message-bytes` < default `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic not created
		// `message.max.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 + 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultBrokerMessageMaxBytes,
		},
		// topic not created
		// `message.max.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultBrokerMessageMaxBytes,
		},
		// topic not created
		// default `max-message-bytes` < user set `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic not created
		// default `max-message-bytes` < `message.max.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic created
		// user set `max-message-bytes` < `max.message.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(1024*1024 - 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic created
		// user set `max-message-bytes` < default `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic created
		// `max.message.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(1024*1024 + 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultTopicMaxMessageBytes,
		},
		// topic created
		// `max.message.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			kafka.DefaultTopicMaxMessageBytes,
			kafka.DefaultTopicMaxMessageBytes,
		},
		// topic created
		// default `max-message-bytes` < user set `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.DefaultBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic created
		// default `max-message-bytes` < `max.message.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			kafka.DefaultBrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
	}

	for _, a := range combinations {
		uri := fmt.Sprintf(a.uriTemplate, a.uriParams...)
		sinkURI, err := url.Parse(uri)
		c.Assert(err, check.IsNil)

		kafka.DefaultBrokerMessageMaxBytes = a.brokerMessageMaxBytes
		kafka.DefaultTopicMaxMessageBytes = a.topicMaxMessageBytes

		opts := make(map[string]string)
		replicaConfig := config.GetDefaultReplicaConfig()
		topic, ok := a.uriParams[0].(string)
		c.Assert(ok, check.IsTrue)
		producerConfig, saramaConfig, err := InitializeConfigurations(context.Background(), topic, sinkURI, replicaConfig, opts)
		c.Assert(err, check.IsNil)
		c.Assert(producerConfig.MaxMessageBytes, check.Equals, saramaConfig.Producer.MaxMessageBytes)
		c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(saramaConfig.Producer.MaxMessageBytes))

		c.Assert(strconv.Itoa(saramaConfig.Producer.MaxMessageBytes), check.Equals, a.expectedMaxMessageBytes)
	}
}

func (s *kafkaSuite) TestSetPartitionNum(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := NewConfig()
	err := cfg.setPartitionNum(2)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.PartitionNum, check.Equals, int32(2))

	cfg.PartitionNum = 1
	err = cfg.setPartitionNum(2)
	c.Assert(err, check.IsNil)
	c.Assert(cfg.PartitionNum, check.Equals, int32(1))

	cfg.PartitionNum = 3
	err = cfg.setPartitionNum(2)
	c.Assert(cerror.ErrKafkaInvalidPartitionNum.Equal(err), check.IsTrue)
}
