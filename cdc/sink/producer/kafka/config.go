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
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"go.uber.org/zap"
)

func init() {
	sarama.MaxRequestSize = 1024 * 1024 * 1024 // 1GB
}

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
}

const (
	defaultVersion           = "2.4.0"
	defaultReplicationFactor = 1
	defaultCompression       = "none"
)

// NewConfig returns a default Kafka configuration
func NewConfig() *Config {
	return &Config{
		Version: defaultVersion,
		// MaxMessageBytes will be used to initialize producer
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		ReplicationFactor: defaultReplicationFactor,
		Compression:       defaultCompression,
		Credential:        &security.Credential{},
		SaslScram:         &security.SaslScram{},
		AutoCreate:        true,
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

func (c *Config) fillBySinkURI(sinkURI *url.URL) error {
	c.BrokerEndpoints = strings.Split(sinkURI.Host, ",")
	params := sinkURI.Query()
	s := params.Get("partition-num")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		c.PartitionNum = int32(a)
		if c.PartitionNum <= 0 {
			return cerror.ErrKafkaInvalidPartitionNum.GenWithStackByArgs(c.PartitionNum)
		}
	}

	s = params.Get("replication-factor")
	if s != "" {
		a, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		c.ReplicationFactor = int16(a)
	}

	s = params.Get("kafka-version")
	if s != "" {
		c.Version = s
	}

	s = params.Get("max-message-bytes")
	if s != "" {
		a, err := strconv.Atoi(s)
		if err != nil {
			return err
		}
		c.MaxMessageBytes = a
	}

	s = params.Get("compression")
	if s != "" {
		c.Compression = s
	}

	c.ClientID = params.Get("kafka-client-id")

	s = params.Get("ca")
	if s != "" {
		c.Credential.CAPath = s
	}

	s = params.Get("cert")
	if s != "" {
		c.Credential.CertPath = s
	}

	s = params.Get("key")
	if s != "" {
		c.Credential.KeyPath = s
	}

	s = params.Get("sasl-user")
	if s != "" {
		c.SaslScram.SaslUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		c.SaslScram.SaslPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		c.SaslScram.SaslMechanism = s
	}

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
	}

	return nil
}

func completeOpts(sinkURI *url.URL, opts map[string]string, saramaConfig *sarama.Config, replicaConfig *config.ReplicaConfig) error {
	params := sinkURI.Query()
	s := params.Get("enable-tidb-extension")
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

	s = params.Get("max-batch-size")
	if s != "" {
		opts["max-batch-size"] = s
	}

	opts["max-message-bytes"] = strconv.Itoa(saramaConfig.Producer.MaxMessageBytes)

	return nil
}

// InitializeConfigurations build the kafka configuration, sarama configuration,
// also fill replication configuration and opts.
func InitializeConfigurations(
	ctx context.Context,
	topic string,
	sinkURI *url.URL,
	replicaConfig *config.ReplicaConfig,
	opts map[string]string,
) (producerConfig *Config, saramaConfig *sarama.Config, err error) {
	producerConfig = NewConfig()
	if err := producerConfig.fillBySinkURI(sinkURI); err != nil {
		return nil, nil, errors.Trace(err)
	}

	saramaConfig, err = newSaramaConfigImpl(ctx, producerConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	admin, err := NewAdminClientImpl(producerConfig.BrokerEndpoints, saramaConfig)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	defer func() {
		if err := admin.Close(); err != nil {
			log.Warn("close kafka cluster admin failed", zap.Error(err))
		}
	}()

	if err := adjustConfig(admin, topic, producerConfig); err != nil {
		return nil, nil, errors.Trace(err)
	}

	adjustSaramaConfig(saramaConfig, producerConfig)
	replicaConfig.FillBySInkURI(sinkURI)

	if err := completeOpts(sinkURI, opts, saramaConfig, replicaConfig); err != nil {
		return nil, nil, cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
	}

	return producerConfig, saramaConfig, nil
}

// adjustConfig will adjust `Config` and `sarama.Config` by check whether the topic exist or not.
func adjustConfig(admin kafka.ClusterAdminClient, topic string, config *Config) error {
	topics, err := admin.ListTopics()
	if err != nil {
		return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
	}

	info, exists := topics[topic]
	// once we have found the topic, no matter `auto-create-topic`, make sure user input parameters are valid.
	if exists {
		// make sure that producer's `MaxMessageBytes` smaller than topic's `max.message.bytes`
		topicMaxMessageBytes, err := getTopicMaxMessageBytes(admin, info)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaNewSaramaProducer, err)
		}

		if topicMaxMessageBytes < config.MaxMessageBytes {
			log.Warn("topic's `max.message.bytes` less than the user set `max-message-bytes`,"+
				"use topic's `max.message.bytes` to initialize the Kafka producer",
				zap.Int("max.message.bytes", topicMaxMessageBytes),
				zap.Int("max-message-bytes", config.MaxMessageBytes))
			config.MaxMessageBytes = topicMaxMessageBytes
		}

		if err := config.setPartitionNum(info.NumPartitions); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	brokerMessageMaxBytes, err := getBrokerMessageMaxBytes(admin)
	if err != nil {
		log.Warn("TiCDC cannot find `message.max.bytes` from broker's configuration")
		return errors.Trace(err)
	}

	// when create the topic, `max.message.bytes` is decided by the broker,
	// it would use broker's `message.max.bytes` to set topic's `max.message.bytes`.
	// TiCDC need to make sure that the producer's `MaxMessageBytes` won't larger than
	// broker's `message.max.bytes`.
	if brokerMessageMaxBytes < config.MaxMessageBytes {
		log.Warn("broker's `message.max.bytes` less than the user set `max-message-bytes`,"+
			"use broker's `message.max.bytes` to initialize the Kafka producer",
			zap.Int("message.max.bytes", brokerMessageMaxBytes),
			zap.Int("max-message-bytes", config.MaxMessageBytes))
		config.MaxMessageBytes = brokerMessageMaxBytes
	}

	// topic not exists yet, and user does not specify the `partition-num` in the sink uri.
	if config.PartitionNum == 0 {
		config.PartitionNum = defaultPartitionNum
		log.Warn("partition-num is not set, use the default partition count",
			zap.String("topic", topic), zap.Int32("partitions", config.PartitionNum))
	}
	return nil
}

func adjustSaramaConfig(saramaConfig *sarama.Config, producerConfig *Config) {
	saramaConfig.Producer.MaxMessageBytes = producerConfig.MaxMessageBytes
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
	// See: https://kafka.apache.org/documentation/#replication
	// When one of the brokers in a Kafka cluster is down, the partition leaders
	// in this broker is broken, Kafka will election a new partition leader and
	// replication logs, this process will last from a few seconds to a few minutes.
	// Kafka cluster will not provide a writing service in this process.
	// Time out in one minute.
	config.Metadata.Retry.Max = 120
	config.Metadata.Retry.Backoff = 500 * time.Millisecond
	// If it is not set, this means a metadata request against an unreachable
	// cluster (all brokers are unreachable or unresponsive) can take up to
	// `Net.[Dial|Read]Timeout * BrokerCount * (Metadata.Retry.Max + 1) +
	// Metadata.Retry.Backoff * Metadata.Retry.Max`
	// to fail.
	// See: https://github.com/Shopify/sarama/issues/765
	// and https://github.com/pingcap/tiflow/issues/3352.
	config.Metadata.Timeout = 1 * time.Minute

	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Producer.MaxMessageBytes = c.MaxMessageBytes
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	// Time out in five minutes(600 * 500ms).
	config.Producer.Retry.Max = 600
	config.Producer.Retry.Backoff = 500 * time.Millisecond
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

	// Time out in one minute(120 * 500ms).
	config.Admin.Retry.Max = 120
	config.Admin.Retry.Backoff = 500 * time.Millisecond
	config.Admin.Timeout = 1 * time.Minute

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
