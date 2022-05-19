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
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/sink/mq/codec"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/kafka"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
)

func TestNewSaramaConfig(t *testing.T) {
	ctx := context.Background()
	config := NewConfig()
	config.Version = "invalid"
	_, err := NewSaramaConfig(ctx, config)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	ctx = contextutil.SetOwnerInCtx(ctx)
	config.Version = "2.6.0"
	config.ClientID = "^invalid$"
	_, err = NewSaramaConfig(ctx, config)
	require.True(t, cerror.ErrKafkaInvalidClientID.Equal(err))

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
		cfg, err := NewSaramaConfig(ctx, config)
		require.Nil(t, err)
		require.Equal(t, cc.expected, cfg.Producer.Compression)
	}

	config.Credential = &security.Credential{
		CAPath: "/invalid/ca/path",
	}
	_, err = NewSaramaConfig(ctx, config)
	require.Regexp(t, ".*no such file or directory", errors.Cause(err))

	saslConfig := NewConfig()
	saslConfig.Version = "2.6.0"
	saslConfig.ClientID = "test-sasl-scram"
	saslConfig.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: sarama.SASLTypeSCRAMSHA256,
	}

	cfg, err := NewSaramaConfig(ctx, saslConfig)
	require.Nil(t, err)
	require.NotNil(t, cfg)
	require.Equal(t, "user", cfg.Net.SASL.User)
	require.Equal(t, "password", cfg.Net.SASL.Password)
	require.Equal(t, sarama.SASLMechanism("SCRAM-SHA-256"), cfg.Net.SASL.Mechanism)
}

func TestConfigTimeouts(t *testing.T) {
	cfg := NewConfig()
	require.Equal(t, 10*time.Second, cfg.DialTimeout)
	require.Equal(t, 10*time.Second, cfg.ReadTimeout)
	require.Equal(t, 10*time.Second, cfg.WriteTimeout)

	saramaConfig, err := NewSaramaConfig(context.Background(), cfg)
	require.Nil(t, err)
	require.Equal(t, cfg.DialTimeout, saramaConfig.Net.DialTimeout)
	require.Equal(t, cfg.WriteTimeout, saramaConfig.Net.WriteTimeout)
	require.Equal(t, cfg.ReadTimeout, saramaConfig.Net.ReadTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	err = cfg.Apply(sinkURI)
	require.Nil(t, err)

	require.Equal(t, 5*time.Second, cfg.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, cfg.ReadTimeout)
	require.Equal(t, 2*time.Minute, cfg.WriteTimeout)

	saramaConfig, err = NewSaramaConfig(context.Background(), cfg)
	require.Nil(t, err)
	require.Equal(t, 5*time.Second, saramaConfig.Net.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, saramaConfig.Net.ReadTimeout)
	require.Equal(t, 2*time.Minute, saramaConfig.Net.WriteTimeout)
}

func TestCompleteConfigByOpts(t *testing.T) {
	cfg := NewConfig()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	require.Nil(t, err)

	err = cfg.Apply(sinkURI)
	require.Nil(t, err)
	require.Equal(t, int32(1), cfg.PartitionNum)
	require.Equal(t, int16(3), cfg.ReplicationFactor)
	require.Equal(t, "2.6.0", cfg.Version)
	require.Equal(t, 4096, cfg.MaxMessageBytes)

	// multiple kafka broker endpoints
	uri = "kafka://127.0.0.1:9092,127.0.0.1:9091,127.0.0.1:9090/kafka-test?"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	cfg = NewConfig()
	err = cfg.Apply(sinkURI)
	require.Nil(t, err)
	require.Len(t, cfg.BrokerEndpoints, 3)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	cfg = NewConfig()
	err = cfg.Apply(sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	cfg = NewConfig()
	err = cfg.Apply(sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	cfg = NewConfig()
	err = cfg.Apply(sinkURI)
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	require.Nil(t, err)
	cfg = NewConfig()
	err = cfg.Apply(sinkURI)
	require.Regexp(t, ".*invalid partition num.*", errors.Cause(err))
}

func TestSetPartitionNum(t *testing.T) {
	cfg := NewConfig()
	err := cfg.setPartitionNum(2)
	require.Nil(t, err)
	require.Equal(t, int32(2), cfg.PartitionNum)

	cfg.PartitionNum = 1
	err = cfg.setPartitionNum(2)
	require.Nil(t, err)
	require.Equal(t, int32(1), cfg.PartitionNum)

	cfg.PartitionNum = 3
	err = cfg.setPartitionNum(2)
	require.True(t, cerror.ErrKafkaInvalidPartitionNum.Equal(err))
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
		// `max-message-bytes` not set, topic created, topic's `max.message.bytes` = `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{kafka.DefaultMockTopicName},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// `max-message-bytes` not set, topic created, topic's `max.message.bytes` > `max-message-bytes`
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
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
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
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.BrokerMessageMaxBytes,
			kafka.TopicMaxMessageBytes,
			kafka.TopicMaxMessageBytes,
		},
		// topic created
		// default `max-message-bytes` < user set `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			kafka.BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic created
		// default `max-message-bytes` < `max.message.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{kafka.DefaultMockTopicName, strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
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

		baseConfig := NewConfig()
		err = baseConfig.Apply(sinkURI)
		require.Nil(t, err)

		saramaConfig, err := NewSaramaConfig(context.Background(), baseConfig)
		require.Nil(t, err)

		adminClient, err := NewAdminClientImpl([]string{sinkURI.Host}, saramaConfig)
		require.Nil(t, err)

		topic, ok := a.uriParams[0].(string)
		require.True(t, ok)
		require.NotEqual(t, "", topic)
		err = AdjustConfig(adminClient, baseConfig, saramaConfig, topic)
		require.Nil(t, err)

		encoderConfig := codec.NewConfig(config.ProtocolOpen)
		err = encoderConfig.Apply(sinkURI, &config.ReplicaConfig{})
		require.Nil(t, err)
		encoderConfig.WithMaxMessageBytes(saramaConfig.Producer.MaxMessageBytes)

		err = encoderConfig.Validate()
		require.Nil(t, err)

		// producer's `MaxMessageBytes` = encoder's `MaxMessageBytes`.
		require.Equal(t, encoderConfig.MaxMessageBytes(), saramaConfig.Producer.MaxMessageBytes)

		expected, err := strconv.Atoi(a.expectedMaxMessageBytes)
		require.Nil(t, err)
		require.Equal(t, expected, saramaConfig.Producer.MaxMessageBytes)

		_ = adminClient.Close()
	}
}

func TestApplySASL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		URI       string
		exceptErr string
	}{
		{
			name:      "no params",
			URI:       "kafka://127.0.0.1:9092/abc",
			exceptErr: "",
		},
		{
			name: "valid PLAIN SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=plain",
			exceptErr: "",
		},
		{
			name: "valid SCRAM SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-user=user&sasl-password=password&sasl-mechanism=SCRAM-SHA-512",
			exceptErr: "",
		},
		{
			name: "valid GSSAPI user auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=USER" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-password=pwd" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			exceptErr: "",
		},
		{
			name: "valid GSSAPI keytab auth SASL",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=GSSAPI&sasl-gssapi-auth-type=keytab" +
				"&sasl-gssapi-kerberos-config-path=/root/config" +
				"&sasl-gssapi-service-name=a&sasl-gssapi-user=user" +
				"&sasl-gssapi-keytab-path=/root/keytab" +
				"&sasl-gssapi-realm=realm&sasl-gssapi-disable-pafxfast=false",
			exceptErr: "",
		},
		{
			name: "invalid mechanism",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=a",
			exceptErr: "unknown a SASL mechanism",
		},
		{
			name: "invalid GSSAPI auth type",
			URI: "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0" +
				"&sasl-mechanism=gssapi&sasl-gssapi-auth-type=keyta1b",
			exceptErr: "unknown keyta1b auth type",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cfg := NewConfig()
			sinkURI, err := url.Parse(test.URI)
			require.Nil(t, err)
			if test.exceptErr == "" {
				require.Nil(t, cfg.applySASL(sinkURI.Query()))
			} else {
				require.Regexp(t, test.exceptErr, cfg.applySASL(sinkURI.Query()).Error())
			}
		})
	}
}

func TestCompleteSaramaSASLConfig(t *testing.T) {
	t.Parallel()

	// Test that SASL is turned on correctly.
	cfg := NewConfig()
	cfg.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "",
		GSSAPI:        security.GSSAPI{},
	}
	saramaConfig := sarama.NewConfig()
	completeSaramaSASLConfig(saramaConfig, cfg)
	require.False(t, saramaConfig.Net.SASL.Enable)
	cfg.SASL.SASLMechanism = "plain"
	completeSaramaSASLConfig(saramaConfig, cfg)
	require.True(t, saramaConfig.Net.SASL.Enable)
	// Test that the SCRAMClientGeneratorFunc is set up correctly.
	cfg = NewConfig()
	cfg.SASL = &security.SASL{
		SASLUser:      "user",
		SASLPassword:  "password",
		SASLMechanism: "plain",
		GSSAPI:        security.GSSAPI{},
	}
	saramaConfig = sarama.NewConfig()
	completeSaramaSASLConfig(saramaConfig, cfg)
	require.Nil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
	cfg.SASL.SASLMechanism = "SCRAM-SHA-512"
	completeSaramaSASLConfig(saramaConfig, cfg)
	require.NotNil(t, saramaConfig.Net.SASL.SCRAMClientGeneratorFunc)
}
