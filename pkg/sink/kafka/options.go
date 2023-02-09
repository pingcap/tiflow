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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

const (
	// defaultPartitionNum specifies the default number of partitions when we create the topic.
	defaultPartitionNum = 3
)

const (
	// SASLTypePlaintext represents the plain mechanism
	SASLTypePlaintext = "PLAIN"
	// SASLTypeSCRAMSHA256 represents the SCRAM-SHA-256 mechanism.
	SASLTypeSCRAMSHA256 = "SCRAM-SHA-256"
	// SASLTypeSCRAMSHA512 represents the SCRAM-SHA-512 mechanism.
	SASLTypeSCRAMSHA512 = "SCRAM-SHA-512"
	// SASLTypeGSSAPI represents the gssapi mechanism.
	SASLTypeGSSAPI = "GSSAPI"
)

// RequiredAcks is used in Produce Requests to tell the broker how many replica acknowledgements
// it must see before responding. Any of the constants defined here are valid. On broker versions
// prior to 0.8.2.0 any other positive int16 is also valid (the broker will wait for that many
// acknowledgements) but in 0.8.2.0 and later this will raise an exception (it has been replaced
// by setting the `min.isr` value in the brokers configuration).
type RequiredAcks int16

const (
	// NoResponse doesn't send any response, the TCP ACK is all you get.
	NoResponse RequiredAcks = 0
	// WaitForLocal waits for only the local commit to succeed before responding.
	WaitForLocal RequiredAcks = 1
	// WaitForAll waits for all in-sync replicas to commit before responding.
	// The minimum number of in-sync replicas is configured on the broker via
	// the `min.insync.replicas` configuration key.
	WaitForAll RequiredAcks = -1
	// Unknown should never have been use in real config.
	Unknown RequiredAcks = 2
)

func requireAcksFromString(acks string) (RequiredAcks, error) {
	i, err := strconv.Atoi(acks)
	if err != nil {
		return Unknown, err
	}

	switch i {
	case int(WaitForAll):
		return WaitForAll, nil
	case int(WaitForLocal):
		return WaitForLocal, nil
	case int(NoResponse):
		return NoResponse, nil
	default:
		return Unknown, cerror.ErrKafkaInvalidRequiredAcks.GenWithStackByArgs(i)
	}
}

// Options stores user specified configurations
type Options struct {
	BrokerEndpoints []string

	// control whether to create topic
	AutoCreate   bool
	PartitionNum int32
	// User should make sure that `replication-factor` not greater than the number of kafka brokers.
	ReplicationFactor int16
	Version           string
	MaxMessageBytes   int
	Compression       string
	ClientID          string
	RequiredAcks      RequiredAcks
	// Only for test. User can not set this value.
	// The current prod default value is 0.
	MaxMessages int

	// Credential is used to connect to kafka cluster.
	EnableTLS  bool
	Credential *security.Credential
	SASL       *security.SASL

	// Timeout for network configurations, default to `10s`
	DialTimeout  time.Duration
	WriteTimeout time.Duration
	ReadTimeout  time.Duration
}

// NewOptions returns a default Kafka configuration
func NewOptions() *Options {
	return &Options{
		Version: "2.4.0",
		// MaxMessageBytes will be used to initialize producer
		MaxMessageBytes:   config.DefaultMaxMessageBytes,
		ReplicationFactor: 1,
		Compression:       "none",
		RequiredAcks:      WaitForAll,
		Credential:        &security.Credential{},
		SASL:              &security.SASL{},
		AutoCreate:        true,
		DialTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		ReadTimeout:       10 * time.Second,
	}
}

// SetPartitionNum set the partition-num by the topic's partition count.
func (c *Options) SetPartitionNum(realPartitionCount int32) error {
	// user does not specify the `partition-num` in the sink-uri
	if c.PartitionNum == 0 {
		c.PartitionNum = realPartitionCount
		log.Info("partitionNum is not set, set by topic's partition-num",
			zap.Int32("partitionNum", realPartitionCount))
		return nil
	}

	if c.PartitionNum < realPartitionCount {
		log.Warn("number of partition specified in sink-uri is less than that of the actual topic. "+
			"Some partitions will not have messages dispatched to",
			zap.Int32("sinkUriPartitions", c.PartitionNum),
			zap.Int32("topicPartitions", realPartitionCount))
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

// Apply the sinkURI to update Options
func (c *Options) Apply(sinkURI *url.URL) error {
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

	s = params.Get("auto-create-topic")
	if s != "" {
		autoCreate, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.AutoCreate = autoCreate
	}

	s = params.Get("dial-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.DialTimeout = a
	}

	s = params.Get("write-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.WriteTimeout = a
	}

	s = params.Get("read-timeout")
	if s != "" {
		a, err := time.ParseDuration(s)
		if err != nil {
			return err
		}
		c.ReadTimeout = a
	}

	s = params.Get("required-acks")
	if s != "" {
		r, err := requireAcksFromString(s)
		if err != nil {
			return err
		}
		c.RequiredAcks = r
	}

	err := c.applySASL(params)
	if err != nil {
		return err
	}

	err = c.applyTLS(params)
	if err != nil {
		return err
	}

	return nil
}

func (c *Options) applyTLS(params url.Values) error {
	s := params.Get("ca")
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

	if c.Credential != nil && !c.Credential.IsEmpty() &&
		!c.Credential.IsTLSEnabled() {
		return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
			errors.New("ca, cert and key files should all be supplied"))
	}

	// if enable-tls is not set, but credential files are set,
	//    then tls should be enabled, and the self-signed CA certificate is used.
	// if enable-tls is set to true, and credential files are not set,
	//	  then tls should be enabled, and the trusted CA certificate on OS is used.
	// if enable-tls is set to false, and credential files are set,
	//	  then an error is returned.
	s = params.Get("enable-tls")
	if s != "" {
		enableTLS, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}

		if c.Credential != nil && c.Credential.IsTLSEnabled() && !enableTLS {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig,
				errors.New("credential files are supplied, but 'enable-tls' is set to false"))
		}
		c.EnableTLS = enableTLS
	} else {
		if c.Credential != nil && c.Credential.IsTLSEnabled() {
			c.EnableTLS = true
		}
	}

	return nil
}

func (c *Options) applySASL(params url.Values) error {
	s := params.Get("sasl-user")
	if s != "" {
		c.SASL.SASLUser = s
	}

	s = params.Get("sasl-password")
	if s != "" {
		c.SASL.SASLPassword = s
	}

	s = params.Get("sasl-mechanism")
	if s != "" {
		mechanism, err := security.SASLMechanismFromString(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		c.SASL.SASLMechanism = mechanism
	}

	s = params.Get("sasl-gssapi-auth-type")
	if s != "" {
		authType, err := security.AuthTypeFromString(s)
		if err != nil {
			return cerror.WrapError(cerror.ErrKafkaInvalidConfig, err)
		}
		c.SASL.GSSAPI.AuthType = authType
	}

	s = params.Get("sasl-gssapi-keytab-path")
	if s != "" {
		c.SASL.GSSAPI.KeyTabPath = s
	}

	s = params.Get("sasl-gssapi-kerberos-config-path")
	if s != "" {
		c.SASL.GSSAPI.KerberosConfigPath = s
	}

	s = params.Get("sasl-gssapi-service-name")
	if s != "" {
		c.SASL.GSSAPI.ServiceName = s
	}

	s = params.Get("sasl-gssapi-user")
	if s != "" {
		c.SASL.GSSAPI.Username = s
	}

	s = params.Get("sasl-gssapi-password")
	if s != "" {
		c.SASL.GSSAPI.Password = s
	}

	s = params.Get("sasl-gssapi-realm")
	if s != "" {
		c.SASL.GSSAPI.Realm = s
	}

	s = params.Get("sasl-gssapi-disable-pafxfast")
	if s != "" {
		disablePAFXFAST, err := strconv.ParseBool(s)
		if err != nil {
			return err
		}
		c.SASL.GSSAPI.DisablePAFXFAST = disablePAFXFAST
	}

	return nil
}

// AutoCreateTopicConfig is used to create topic configuration.
type AutoCreateTopicConfig struct {
	AutoCreate        bool
	PartitionNum      int32
	ReplicationFactor int16
}

// DeriveTopicConfig derive a `topicConfig` from the `Options`
func (c *Options) DeriveTopicConfig() *AutoCreateTopicConfig {
	return &AutoCreateTopicConfig{
		AutoCreate:        c.AutoCreate,
		PartitionNum:      c.PartitionNum,
		ReplicationFactor: c.ReplicationFactor,
	}
}

var (
	validClientID     = regexp.MustCompile(`\A[A-Za-z0-9._-]+\z`)
	commonInvalidChar = regexp.MustCompile(`[\?:,"]`)
)

// NewKafkaClientID generates kafka client id
func NewKafkaClientID(role, captureAddr string,
	changefeedID model.ChangeFeedID,
	configuredClientID string,
) (clientID string, err error) {
	if configuredClientID != "" {
		clientID = configuredClientID
	} else {
		clientID = fmt.Sprintf("TiCDC_producer_%s_%s_%s_%s",
			role, captureAddr, changefeedID.Namespace, changefeedID.ID)
		clientID = commonInvalidChar.ReplaceAllString(clientID, "_")
	}
	if !validClientID.MatchString(clientID) {
		return "", cerror.ErrKafkaInvalidClientID.GenWithStackByArgs(clientID)
	}
	return
}

// AdjustOptions adjust the `Options` and `sarama.Config` by condition.
func AdjustOptions(
	ctx context.Context,
	admin ClusterAdminClient,
	options *Options,
	topic string,
) error {
	topics, err := admin.GetAllTopicsMeta(ctx)
	if err != nil {
		return errors.Trace(err)
	}

	// Only check replicationFactor >= minInsyncReplicas when producer's required acks is -1.
	// If we don't check it, the producer probably can not send message to the topic.
	// Because it will wait for the ack from all replicas. But we do not have enough replicas.
	if options.RequiredAcks == WaitForAll {
		err = validateMinInsyncReplicas(ctx, admin, topics, topic, int(options.ReplicationFactor))
		if err != nil {
			return errors.Trace(err)
		}
	}

	info, exists := topics[topic]
	// once we have found the topic, no matter `auto-create-topic`, make sure user input parameters are valid.
	if exists {
		// make sure that producer's `MaxMessageBytes` smaller than topic's `max.message.bytes`
		topicMaxMessageBytesStr, err := getTopicConfig(
			ctx, admin, info,
			TopicMaxMessageBytesConfigName,
			BrokerMessageMaxBytesConfigName,
		)
		if err != nil {
			return errors.Trace(err)
		}
		topicMaxMessageBytes, err := strconv.Atoi(topicMaxMessageBytesStr)
		if err != nil {
			return errors.Trace(err)
		}

		if topicMaxMessageBytes < options.MaxMessageBytes {
			log.Warn("topic's `max.message.bytes` less than the `max-message-bytes`,"+
				"use topic's `max.message.bytes` to initialize the Kafka producer",
				zap.Int("max.message.bytes", topicMaxMessageBytes),
				zap.Int("max-message-bytes", options.MaxMessageBytes))
			options.MaxMessageBytes = topicMaxMessageBytes
		}

		// no need to create the topic, but we would have to log user if they found enter wrong topic name later
		if options.AutoCreate {
			log.Warn("topic already exist, TiCDC will not create the topic",
				zap.String("topic", topic), zap.Any("detail", info))
		}

		if err := options.SetPartitionNum(info.NumPartitions); err != nil {
			return errors.Trace(err)
		}

		return nil
	}

	brokerMessageMaxBytesStr, err := admin.GetBrokerConfig(
		ctx,
		BrokerMessageMaxBytesConfigName,
	)
	if err != nil {
		log.Warn("TiCDC cannot find `message.max.bytes` from broker's configuration")
		return errors.Trace(err)
	}
	brokerMessageMaxBytes, err := strconv.Atoi(brokerMessageMaxBytesStr)
	if err != nil {
		return errors.Trace(err)
	}

	// when create the topic, `max.message.bytes` is decided by the broker,
	// it would use broker's `message.max.bytes` to set topic's `max.message.bytes`.
	// TiCDC need to make sure that the producer's `MaxMessageBytes` won't larger than
	// broker's `message.max.bytes`.
	if brokerMessageMaxBytes < options.MaxMessageBytes {
		log.Warn("broker's `message.max.bytes` less than the `max-message-bytes`,"+
			"use broker's `message.max.bytes` to initialize the Kafka producer",
			zap.Int("message.max.bytes", brokerMessageMaxBytes),
			zap.Int("max-message-bytes", options.MaxMessageBytes))
		options.MaxMessageBytes = brokerMessageMaxBytes
	}

	// topic not exists yet, and user does not specify the `partition-num` in the sink uri.
	if options.PartitionNum == 0 {
		options.PartitionNum = defaultPartitionNum
		log.Warn("partition-num is not set, use the default partition count",
			zap.String("topic", topic), zap.Int32("partitions", options.PartitionNum))
	}
	return nil
}

func validateMinInsyncReplicas(
	ctx context.Context,
	admin ClusterAdminClient,
	topics map[string]TopicDetail,
	topic string,
	replicationFactor int,
) error {
	minInsyncReplicasConfigGetter := func() (string, bool, error) {
		info, exists := topics[topic]
		if exists {
			minInsyncReplicasStr, err := getTopicConfig(
				ctx, admin, info,
				MinInsyncReplicasConfigName,
				MinInsyncReplicasConfigName)
			if err != nil {
				return "", true, err
			}
			return minInsyncReplicasStr, true, nil
		}

		minInsyncReplicasStr, err := admin.GetBrokerConfig(ctx,
			MinInsyncReplicasConfigName)
		if err != nil {
			return "", false, err
		}

		return minInsyncReplicasStr, false, nil
	}

	minInsyncReplicasStr, exists, err := minInsyncReplicasConfigGetter()
	if err != nil {
		// 'min.insync.replica' is invisible to us in Confluent Cloud Kafka.
		if cerror.ErrKafkaBrokerConfigNotFound.Equal(err) {
			log.Warn("TiCDC cannot find `min.insync.replicas` from broker's configuration, " +
				"please make sure that the replication factor is greater than or equal " +
				"to the minimum number of in-sync replicas" +
				"if you want to use `required-acks` = -1." +
				"Otherwise, TiCDC will not be able to send messages to the topic.")
			return nil
		}
		return err
	}
	minInsyncReplicas, err := strconv.Atoi(minInsyncReplicasStr)
	if err != nil {
		return err
	}

	configFrom := "topic"
	if !exists {
		configFrom = "broker"
	}

	if replicationFactor < minInsyncReplicas {
		msg := fmt.Sprintf("`replication-factor` cannot be smaller than the `%s` of %s",
			MinInsyncReplicasConfigName, configFrom)
		log.Error(msg, zap.Int("replication-factor", replicationFactor),
			zap.Int("min.insync.replicas", minInsyncReplicas))
		return cerror.ErrKafkaInvalidConfig.GenWithStack(
			"TiCDC Kafka producer's `request.required.acks` defaults to -1, "+
				"TiCDC cannot deliver messages when the `replication-factor` %d "+
				"is smaller than the `min.insync.replicas` %d of %s",
			replicationFactor, minInsyncReplicas, configFrom,
		)
	}

	return nil
}

// getTopicConfig gets topic config by name.
// If the topic does not have this configuration, we will try to get it from the broker's configuration.
// NOTICE: The configuration names of topic and broker may be different for the same configuration.
func getTopicConfig(
	ctx context.Context,
	admin ClusterAdminClient,
	detail TopicDetail,
	topicConfigName string,
	brokerConfigName string,
) (string, error) {
	if a, ok := detail.ConfigEntries[topicConfigName]; ok {
		return a, nil
	}

	return admin.GetBrokerConfig(ctx, brokerConfigName)
}
