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
	defer testleak.AfterTest(c)

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
	defer testleak.AfterTest(c)

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

func (s *kafkaSuite) TestInitializeConfigurations(c *check.C) {
	defer testleak.AfterTest(c)

	NewAdminClientImpl = kafka.NewMockAdminClient
	defer func() {
		NewAdminClientImpl = kafka.NewSaramaAdminClient
	}()

	// most basic sink uri
	uriTemplate := "kafka://127.0.0.1:9092/%s"
	topic := "no-params"
	uri := fmt.Sprintf(uriTemplate, topic)
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)

	// topic not found
	opts := make(map[string]string)
	producerConfig, saramaConfig, err := InitializeConfigurations(context.Background(), topic, sinkURI, config.GetDefaultReplicaConfig(), opts)
	c.Assert(err, check.IsNil)
	c.Assert(producerConfig, check.NotNil)
	c.Assert(saramaConfig, check.NotNil)

	// Normal config.
	uriTemplate = "kafka://127.0.0.1:9092/%s?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	topic = "kafka-test"
	maxMessageSize := "4096" // 4kb
	uri = fmt.Sprintf(uriTemplate, topic, maxMessageSize)
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	opts = make(map[string]string)
	producerConfig, saramaConfig, err = InitializeConfigurations(context.Background(), topic, sinkURI, config.GetDefaultReplicaConfig(), opts)
	c.Assert(err, check.IsNil)
	c.Assert(producerConfig, check.NotNil)
	c.Assert(saramaConfig, check.NotNil)

	c.Assert(producerConfig.PartitionNum, check.Equals, int32(1))
	c.Assert(producerConfig.ReplicationFactor, check.Equals, int16(3))
	c.Assert(producerConfig.Version, check.Equals, "2.6.0")
	c.Assert(producerConfig.MaxMessageBytes, check.Equals, 4096)

	c.Assert(opts["max-message-bytes"], check.Equals, strconv.Itoa(producerConfig.MaxMessageBytes))
	c.Assert(opts["max-batch-size"], check.Equals, "5")

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
