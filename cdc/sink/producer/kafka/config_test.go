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
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
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

func (s *kafkaSuite) TestConfigTimeouts(c *check.C) {
	defer testleak.AfterTest(c)()

	cfg := NewConfig()
	c.Assert(cfg.DialTimeout, check.Equals, 10*time.Second)
	c.Assert(cfg.ReadTimeout, check.Equals, 10*time.Second)
	c.Assert(cfg.WriteTimeout, check.Equals, 10*time.Second)

	saramaConfig, err := newSaramaConfig(context.Background(), cfg)
	c.Assert(err, check.IsNil)
	c.Assert(saramaConfig.Net.DialTimeout, check.Equals, cfg.DialTimeout)
	c.Assert(saramaConfig.Net.WriteTimeout, check.Equals, cfg.WriteTimeout)
	c.Assert(saramaConfig.Net.ReadTimeout, check.Equals, cfg.ReadTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)
	opts := make(map[string]string)
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(err, check.IsNil)

	c.Assert(cfg.DialTimeout, check.Equals, 5*time.Second)
	c.Assert(cfg.ReadTimeout, check.Equals, 1000*time.Millisecond)
	c.Assert(cfg.WriteTimeout, check.Equals, 2*time.Minute)

	saramaConfig, err = newSaramaConfig(context.Background(), cfg)
	c.Assert(err, check.IsNil)
	c.Assert(saramaConfig.Net.DialTimeout, check.Equals, 5*time.Second)
	c.Assert(saramaConfig.Net.ReadTimeout, check.Equals, 1000*time.Millisecond)
	c.Assert(saramaConfig.Net.WriteTimeout, check.Equals, 2*time.Minute)
}

func (s *kafkaSuite) TestCompleteConfigByOpts(c *check.C) {
	defer testleak.AfterTest(c)()
	cfg := NewConfig()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	c.Assert(err, check.IsNil)
	opts := make(map[string]string)
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
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

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal enable-tidb-extension.
	uri = "kafka://127.0.0.1:9092/abc?enable-tidb-extension=a&protocol=canal-json"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid syntax.*")

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*invalid partition num.*")

	// Use enable-tidb-extension on other protocols.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=1&enable-tidb-extension=true"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(errors.Cause(err), check.ErrorMatches, ".*enable-tidb-extension only support canal-json protocol.*")

	// Test enable-tidb-extension.
	uri = "kafka://127.0.0.1:9092/abc?enable-tidb-extension=true&protocol=canal-json"
	sinkURI, err = url.Parse(uri)
	c.Assert(err, check.IsNil)
	cfg = NewConfig()
	opts = make(map[string]string)
	err = CompleteConfigsAndOpts(sinkURI, cfg, config.GetDefaultReplicaConfig(), opts)
	c.Assert(err, check.IsNil)
	expectedOpts = map[string]string{
		"enable-tidb-extension": "true",
	}
	for k, v := range opts {
		c.Assert(v, check.Equals, expectedOpts[k])
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
