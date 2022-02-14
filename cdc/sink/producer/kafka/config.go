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
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

// Config stores user specified Kafka producer configuration
type Config struct {
	BrokerEndpoints []string
	PartitionNum    int32

	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16

	Version         string
	MaxMessageBytes int
	Compression     string
	ClientID        string
	Credential      *security.Credential
	SaslScram       *security.SaslScram
	// control whether to create topic
	AutoCreate bool

	// Timeout for sarama `config.Net` configurations, default to `10s`
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// NewConfig returns a default Kafka configuration
func NewConfig() *Config {
	return &Config{
		Version: "2.4.0",
		// MaxMessageBytes will be used to initialize producer
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		ReplicationFactor: 1,
		Compression:       "none",
		Credential:        &security.Credential{},
		SaslScram:         &security.SaslScram{},
		AutoCreate:        true,
		DialTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadTimeout:       10 * time.Second,
	}
}

// set the partition-num by the topic's partition count.
func (c *Config) setPartitionNum(realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if c.PartitionNum == 0 {
		c.PartitionNum = realPartitionCount
		return nil
	}

	if c.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.Int32("sink-uri partitions", c.PartitionNum),
			zap.Int32("topic partitions", realPartitionCount))
		return nil
	}

	// Make sure that the user-specified `partition-num` is not greater than
	// the real partition count, since messages would be dispatched to different
	// partitions, this could prevent potential correctness problems.
	if c.PartitionNum > realPartitionCount {
		return cerror.ErrKafkaInvalidPartitionNum.GenWithStack(
			"the number of partition (%d) specified in sink-uri is more than that of actual topic (%d)",
			c.PartitionNum, realPartitionCount)
	}
	return nil
}

// CompleteConfigsAndOpts the kafka producer configuration, replication configuration and opts.
func CompleteConfigsAndOpts(sinkURI *url.URL, producerConfig *Config, replicaConfig *config.ReplicaConfig, opts map[string]string) error {
	producerConfig.BrokerEndpoints = strings.Split(sinkURI.Host, ",")
	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		producerConfig.PartitionNum = int32(a)
		if producerConfig.PartitionNum <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(producerConfig.PartitionNum)
		}
	}

	s = params.Get("replication-factor")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		producerConfig.ReplicationFactor = int16(a)
	}

	s = params.Get("kafka-version")
	if s != "" {
		producerConfig.Version = s
	}

	s = params.Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		producerConfig.MaxMessageBytes = a
		opts["max-message-bytes"] = s
	}

	s = params.Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}

	s = params.Get("compression")
	if s != "" {
		producerConfig.Compression = s
	}

	producerConfig.ClientID = params.Get("kafka-client-id")

	s = params.Get("ca")
	if s != "" {
		producerConfig.Credential.CAPath = s
	}

	s = params.Get("cert")
	if s != "" {
		producerConfig.Credential.CertPath = s
	}

	s = params.Get("key")
	if s != "" {
		producerConfig.Credential.KeyPath = s
	}

	s = params.Get("sasl-user")
	if s != "" {
		producerConfig.SaslScram.SaslUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		producerConfig.SaslScram.SaslPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		producerConfig.SaslScram.SaslMechanism = s
	}

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		producerConfig.AutoCreate = autoCreate
	}

	s = params.Get(config.ProtocolKey)
	if s != "" {
		replicaConfig.Sink.Protocol = s
	}

	s = params.Get("enable-tidb-extension")
	if s != "" {
		_, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		if replicaConfig.Sink.Protocol != "canal-json" {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, errors.New("enable-tidb-extension only support canal-json protocol"))
		}
		opts["enable-tidb-extension"] = s
	}

	s = params.Get("dial-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		producerConfig.DialTimeout = a
	}

	s = params.Get("write-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		producerConfig.WriteTimeout = a
	}

	s = params.Get("read-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		producerConfig.ReadTimeout = a
	}

	return nil
}

// newSaramaConfig return the default config and set the according version and metrics
func newSaramaConfig(ctx context.Context, c *Config) (*sarama.Config, error) {
	config := sarama.NewConfig()

	version, err := sarama.ParseKafkaVersion(c.Version)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrKafkaInvalidVersion, err)
	}
	var role string
	if util.IsOwnerFromCtx(ctx) {
		role = "owner"
	} else {
		role = "processor"
	}
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	config.ClientID, err = kafkaClientID(role, captureAddr, changefeedID, c.ClientID)
	if err != nil {
		return nil, errors.Trace(err)
	}
	config.Version = version

	// Producer fetch metadata from brokers frequently, if metadata cannot be
	// refreshed easily, this would indicate the network condition between the
	// capture server and kafka broker is not good.
	// In the scenario that cannot get response from Kafka server, this default
	// setting can help to get response more quickly.
	config.Metadata.Retry.Max = 1
	config.Metadata.Retry.Backoff = 100 * time.Millisecond
	// This Timeout is useless if the `RefreshMetadata` time cost is less than it.
	config.Metadata.Timeout = 1 * time.Minute

	// Admin.Retry take effect on `ClusterAdmin` related operations,
	// only `CreateTopic` for cdc now. set the `Timeout` to `1m` to make CI stable.
	config.Admin.Retry.Max = 5
	config.Admin.Retry.Backoff = 100 * time.Millisecond
	config.Admin.Timeout = 1 * time.Minute

	// Producer.Retry take effect when the producer try to send message to kafka
	// brokers. If kafka cluster is healthy, just the default value should be enough.
	// For kafka cluster with a bad network condition, producer should not try to
	// waster too much time on sending a message, get response no matter success
	// or fail as soon as possible is preferred.
	config.Producer.Retry.Max = 3
	config.Producer.Retry.Backoff = 100 * time.Millisecond

	// make sure sarama producer flush messages as soon as possible.
	config.Producer.Flush.Bytes = 0
	config.Producer.Flush.Messages = 0
	config.Producer.Flush.Frequency = time.Duration(0)

	config.Net.DialTimeout = c.DialTimeout
	config.Net.WriteTimeout = c.WriteTimeout
	config.Net.ReadTimeout = c.ReadTimeout

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	switch strings.ToLower(strings.TrimSpace(c.Compression)) {
	case "none":
		config.Producer.Compression = sarama.CompressionNone
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
	case "zstd":
		config.Producer.Compression = sarama.CompressionZSTD
	default:
		log.Warn("Unsupported compression algorithm", zap.String("compression", c.Compression))
		config.Producer.Compression = sarama.CompressionNone
	}

	if c.Credential != nil && len(c.Credential.CAPath) != 0 {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config, err = c.Credential.ToTLSConfig()
		if err != nil {
			return nil, errors.Trace(err)
		}
	}
	if c.SaslScram != nil && len(c.SaslScram.SaslUser) != 0 {
		config.Net.SASL.Enable = true
		config.Net.SASL.User = c.SaslScram.SaslUser
		config.Net.SASL.Password = c.SaslScram.SaslPassword
		config.Net.SASL.Mechanism = sarama.SASLMechanism(c.SaslScram.SaslMechanism)
		if strings.EqualFold(c.SaslScram.SaslMechanism, "SCRAM-SHA-256") {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA256} }
		} else if strings.EqualFold(c.SaslScram.SaslMechanism, "SCRAM-SHA-512") {
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient { return &security.XDGSCRAMClient{HashGeneratorFcn: security.SHA512} }
		} else {
			return nil, errors.New("Unsupported sasl-mechanism, should be SCRAM-SHA-256 or SCRAM-SHA-512")
		}
	}

	return config, err
}
