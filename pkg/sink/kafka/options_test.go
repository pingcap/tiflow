// Copyright 2023 PingCAP, Inc.
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
	"strings"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/sink/codec/common"
	"github.com/stretchr/testify/require"
)

func TestCompleteOptions(t *testing.T) {
	options := NewOptions()

	// Normal config.
	uriTemplate := "kafka://127.0.0.1:9092/kafka-test?kafka-version=2.6.0&max-batch-size=5" +
		"&max-message-bytes=%s&partition-num=1&replication-factor=3" +
		"&kafka-client-id=unit-test&auto-create-topic=false&compression=gzip&required-acks=1"
	maxMessageSize := "4096" // 4kb
	uri := fmt.Sprintf(uriTemplate, maxMessageSize)
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	ctx := context.Background()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)
	require.Equal(t, int16(3), options.ReplicationFactor)
	require.Equal(t, "2.6.0", options.Version)
	require.Equal(t, 4096, options.MaxMessageBytes)
	require.Equal(t, WaitForLocal, options.RequiredAcks)

	// multiple kafka broker endpoints
	uri = "kafka://127.0.0.1:9092,127.0.0.1:9091,127.0.0.1:9090/kafka-test?"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.NoError(t, err)
	require.Len(t, options.BrokerEndpoints, 3)

	// Illegal replication-factor.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&replication-factor=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal max-message-bytes.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&max-message-bytes=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Illegal partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=a"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.Regexp(t, ".*invalid syntax.*", errors.Cause(err))

	// Out of range partition-num.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&partition-num=0"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.Regexp(t, ".*invalid partition num.*", errors.Cause(err))

	// Unknown required-acks.
	uri = "kafka://127.0.0.1:9092/abc?kafka-version=2.6.0&required-acks=3"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.Regexp(t, ".*invalid required acks 3.*", errors.Cause(err))

	// invalid kafka client id
	uri = "kafka://127.0.0.1:9092/abc?kafka-client-id=^invalid$"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	options = NewOptions()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.True(t, cerror.ErrKafkaInvalidClientID.Equal(err))
}

func TestSetPartitionNum(t *testing.T) {
	options := NewOptions()
	err := options.SetPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(2), options.PartitionNum)

	options.PartitionNum = 1
	err = options.SetPartitionNum(2)
	require.NoError(t, err)
	require.Equal(t, int32(1), options.PartitionNum)

	options.PartitionNum = 3
	err = options.SetPartitionNum(2)
	require.True(t, cerror.ErrKafkaInvalidPartitionNum.Equal(err))
}

func TestClientID(t *testing.T) {
	testCases := []struct {
		addr         string
		changefeedID string
		configuredID string
		hasError     bool
		expected     string
	}{
		{
			"domain:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_domain_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_default_123-121-121-121",
		},
		{
			"127.0.0.1:1234?:,\"", "123-121-121-121",
			"", false,
			"TiCDC_producer_127.0.0.1_1234_____default_123-121-121-121",
		},
		{
			"中文", "123-121-121-121",
			"", true, "",
		},
		{
			"127.0.0.1:1234",
			"123-121-121-121", "cdc-changefeed-1", false,
			"cdc-changefeed-1",
		},
	}
	for _, tc := range testCases {
		id, err := NewKafkaClientID(tc.addr,
			model.DefaultChangeFeedID(tc.changefeedID), tc.configuredID)
		if tc.hasError {
			require.Error(t, err)
		} else {
			require.NoError(t, err)
			require.Equal(t, tc.expected, id)
		}
	}
}

func TestTimeout(t *testing.T) {
	options := NewOptions()
	require.Equal(t, 10*time.Second, options.DialTimeout)
	require.Equal(t, 10*time.Second, options.ReadTimeout)
	require.Equal(t, 10*time.Second, options.WriteTimeout)

	uri := "kafka://127.0.0.1:9092/kafka-test?dial-timeout=5s&read-timeout=1000ms" +
		"&write-timeout=2m"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)

	ctx := context.Background()
	err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
	require.NoError(t, err)

	require.Equal(t, 5*time.Second, options.DialTimeout)
	require.Equal(t, 1000*time.Millisecond, options.ReadTimeout)
	require.Equal(t, 2*time.Minute, options.WriteTimeout)
}

func TestAdjustConfigTopicNotExist(t *testing.T) {
	adminClient := NewClusterAdminClientMockImpl()
	defer adminClient.Close()

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// When the topic does not exist, use the broker's configuration to create the topic.
	// topic not exist, `max-message-bytes` = `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	ctx := context.Background()
	saramaConfig, err := NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, "create-random1")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes := options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic not exist, `max-message-bytes` > `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)
	err = AdjustOptions(ctx, adminClient, options, "create-random2")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)

	// topic not exist, `max-message-bytes` < `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)
	err = AdjustOptions(ctx, adminClient, options, "create-random3")
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)
}

func TestAdjustConfigTopicExist(t *testing.T) {
	adminClient := NewClusterAdminClientMockImpl()
	defer adminClient.Close()

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// topic exists, `max-message-bytes` = `max.message.bytes`.
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes()

	ctx := context.Background()
	saramaConfig, err := NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes := options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// topic exists, `max-message-bytes` > `max.message.bytes`
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() + 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = adminClient.GetTopicMaxMessageBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)

	// topic exists, `max-message-bytes` < `max.message.bytes`
	options.MaxMessageBytes = adminClient.GetTopicMaxMessageBytes() - 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Nil(t, err)

	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic does not have `max.message.bytes`
	// create a topic without `max.message.bytes`
	topicName := "test-topic"
	detail := &TopicDetail{
		Name:          topicName,
		NumPartitions: 3,
	}
	err = adminClient.CreateTopic(context.Background(), detail, false)
	require.Nil(t, err)

	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() - 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, topicName)
	require.Nil(t, err)

	// since `max.message.bytes` cannot be found, use broker's `message.max.bytes` instead.
	expectedSaramaMaxMessageBytes = options.MaxMessageBytes
	require.Equal(t, expectedSaramaMaxMessageBytes, saramaConfig.Producer.MaxMessageBytes)

	// When the topic exists, but the topic doesn't have `max.message.bytes`
	// `max-message-bytes` > `message.max.bytes`
	options.MaxMessageBytes = adminClient.GetBrokerMessageMaxBytes() + 1
	saramaConfig, err = NewSaramaConfig(ctx, options)
	require.Nil(t, err)

	err = AdjustOptions(ctx, adminClient, options, topicName)
	require.Nil(t, err)
	expectedSaramaMaxMessageBytes = adminClient.GetBrokerMessageMaxBytes()
	require.Equal(t, expectedSaramaMaxMessageBytes, options.MaxMessageBytes)
}

func TestAdjustConfigMinInsyncReplicas(t *testing.T) {
	adminClient := NewClusterAdminClientMockImpl()
	defer adminClient.Close()

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does not exist.
	adminClient.SetMinInsyncReplicas("2")

	ctx := context.Background()
	err := AdjustOptions(
		ctx,
		adminClient,
		options,
		"create-new-fail-invalid-min-insync-replicas",
	)
	require.Regexp(
		t,
		".*`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of broker.*",
		errors.Cause(err),
	)

	// topic not exist, and `min.insync.replicas` not found in broker's configuration
	adminClient.DropBrokerConfig(MinInsyncReplicasConfigName)
	topicName := "no-topic-no-min-insync-replicas"
	err = AdjustOptions(ctx, adminClient, options, "no-topic-no-min-insync-replicas")
	require.Nil(t, err)
	err = adminClient.CreateTopic(context.Background(), &TopicDetail{
		Name:              topicName,
		ReplicationFactor: 1,
	}, false)
	require.ErrorIs(t, err, sarama.ErrPolicyViolation)

	// Report an error if the replication-factor is less than min.insync.replicas
	// when the topic does exist.

	// topic exist, but `min.insync.replicas` not found in topic and broker configuration
	topicName = "topic-no-options-entry"
	err = adminClient.CreateTopic(context.Background(), &TopicDetail{
		Name:              topicName,
		ReplicationFactor: 3,
		NumPartitions:     3,
	}, false)
	require.Nil(t, err)
	err = AdjustOptions(ctx, adminClient, options, topicName)
	require.Nil(t, err)

	// topic found, and have `min.insync.replicas`, but set to 2, larger than `replication-factor`.
	adminClient.SetMinInsyncReplicas("2")
	err = AdjustOptions(ctx, adminClient, options, adminClient.GetDefaultMockTopicName())
	require.Regexp(t,
		".*`replication-factor` 1 is smaller than the `min.insync.replicas` 2 of topic.*",
		errors.Cause(err),
	)
}

func TestSkipAdjustConfigMinInsyncReplicasWhenRequiredAcksIsNotWailAll(t *testing.T) {
	adminClient := NewClusterAdminClientMockImpl()
	defer adminClient.Close()

	options := NewOptions()
	options.BrokerEndpoints = []string{"127.0.0.1:9092"}
	options.RequiredAcks = WaitForLocal

	// Do not report an error if the replication-factor is less than min.insync.replicas(1<2).
	adminClient.SetMinInsyncReplicas("2")
	err := AdjustOptions(
		context.Background(),
		adminClient,
		options,
		"skip-check-min-insync-replicas",
	)
	require.Nil(t, err, "Should not report an error when `required-acks` is not `all`")
}

func TestCreateProducerFailed(t *testing.T) {
	options := NewOptions()
	options.Version = "invalid"
	saramaConfig, err := NewSaramaConfig(context.Background(), options)
	require.Regexp(t, "invalid version.*", errors.Cause(err))
	require.Nil(t, saramaConfig)
}

func TestConfigurationCombinations(t *testing.T) {
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
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			BrokerMessageMaxBytes,
		},
		// topic not created,
		// `max-message-bytes` not set, `message.max.bytes` = `max-message-bytes`
		// expected = min(`max-message-bytes`, `message.max.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"not-exist-topic"},
			strconv.Itoa(config.DefaultMaxMessageBytes),
			TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// topic not created,
		// `max-message-bytes` not set, broker `message.max.bytes` > `max-message-bytes`
		// expected = min(`max-message-bytes`, `message.max.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{"no-params"},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},

		// topic not created
		// user set `max-message-bytes` < `message.max.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 - 1)},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic not created
		// user set `max-message-bytes` < default `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes - 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic not created
		// `message.max.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(1024*1024 + 1)},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			BrokerMessageMaxBytes,
		},
		// topic not created
		// `message.max.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			BrokerMessageMaxBytes,
		},
		// topic not created
		// default `max-message-bytes` < user set `max-message-bytes` < `message.max.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 1)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic not created
		// default `max-message-bytes` < `message.max.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{"not-created-topic", strconv.Itoa(config.DefaultMaxMessageBytes + 2)},
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			TopicMaxMessageBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},

		// topic created,
		// `max-message-bytes` not set, topic's `max.message.bytes` < `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{DefaultMockTopicName},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			TopicMaxMessageBytes,
		},
		// `max-message-bytes` not set, topic created,
		// topic's `max.message.bytes` = `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{DefaultMockTopicName},
			BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},
		// `max-message-bytes` not set, topic created,
		// topic's `max.message.bytes` > `max-message-bytes`
		// expected = min(`max-message-bytes`, `max.message.bytes`) = `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s",
			[]interface{}{DefaultMockTopicName},
			BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes),
		},

		// topic created
		// user set `max-message-bytes` < `max.message.bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{DefaultMockTopicName, strconv.Itoa(1024*1024 - 1)},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			strconv.Itoa(1024*1024 - 1),
		},
		// topic created
		// user set `max-message-bytes` < default `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes - 1),
			},
			BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes - 1),
		},
		// topic created
		// `max.message.bytes` < user set `max-message-bytes` < default `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{DefaultMockTopicName, strconv.Itoa(1024*1024 + 1)},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			TopicMaxMessageBytes,
		},
		// topic created
		// `max.message.bytes` < default `max-message-bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			BrokerMessageMaxBytes,
			TopicMaxMessageBytes,
			TopicMaxMessageBytes,
		},
		// topic created
		// default `max-message-bytes` < user set `max-message-bytes` < `max.message.bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			},
			BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
		// topic created
		// default `max-message-bytes` < `max.message.bytes` < user set `max-message-bytes`
		{
			"kafka://127.0.0.1:9092/%s?max-message-bytes=%s",
			[]interface{}{
				DefaultMockTopicName,
				strconv.Itoa(config.DefaultMaxMessageBytes + 2),
			},
			BrokerMessageMaxBytes,
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
			strconv.Itoa(config.DefaultMaxMessageBytes + 1),
		},
	}

	for _, a := range combinations {
		BrokerMessageMaxBytes = a.brokerMessageMaxBytes
		TopicMaxMessageBytes = a.topicMaxMessageBytes

		uri := fmt.Sprintf(a.uriTemplate, a.uriParams...)
		sinkURI, err := url.Parse(uri)
		require.Nil(t, err)

		ctx := context.Background()
		options := NewOptions()
		err = options.Apply(ctx, sinkURI, config.GetDefaultReplicaConfig())
		require.Nil(t, err)

		changefeed := model.DefaultChangeFeedID("changefeed-test")
		factory, err := NewMockFactory(options, changefeed)
		require.NoError(t, err)

		adminClient, err := factory.AdminClient(ctx)
		require.NoError(t, err)

		topic, ok := a.uriParams[0].(string)
		require.True(t, ok)
		require.NotEqual(t, "", topic)
		err = AdjustOptions(ctx, adminClient, options, topic)
		require.Nil(t, err)

		encoderConfig := common.NewConfig(config.ProtocolOpen)
		err = encoderConfig.Apply(sinkURI, &config.ReplicaConfig{})
		require.Nil(t, err)
		encoderConfig.WithMaxMessageBytes(options.MaxMessageBytes)

		err = encoderConfig.Validate()
		require.Nil(t, err)

		// producer's `MaxMessageBytes` = encoder's `MaxMessageBytes`.
		require.Equal(t, encoderConfig.MaxMessageBytes, options.MaxMessageBytes)

		expected, err := strconv.Atoi(a.expectedMaxMessageBytes)
		require.Nil(t, err)
		require.Equal(t, expected, options.MaxMessageBytes)

		adminClient.Close()
	}
}

func TestMerge(t *testing.T) {
	uri := "kafka://topic/prefix"
	sinkURI, err := url.Parse(uri)
	require.NoError(t, err)
	replicaConfig := config.GetDefaultReplicaConfig()
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(12),
		ReplicationFactor:         aws.Int16(5),
		KafkaVersion:              aws.String("3.1.2"),
		MaxMessageBytes:           aws.Int(1024 * 1024),
		Compression:               aws.String("gzip"),
		KafkaClientID:             aws.String("test-id"),
		AutoCreateTopic:           aws.Bool(true),
		DialTimeout:               aws.String("1m1s"),
		WriteTimeout:              aws.String("2m1s"),
		RequiredAcks:              aws.Int(1),
		SASLUser:                  aws.String("abc"),
		SASLPassword:              aws.String("123"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("keytab"),
		SASLGssAPIKeytabPath:      aws.String("SASLGssAPIKeytabPath"),
		SASLGssAPIServiceName:     aws.String("service"),
		SASLGssAPIUser:            aws.String("user"),
		SASLGssAPIPassword:        aws.String("pass"),
		SASLGssAPIRealm:           aws.String("realm"),
		SASLGssAPIDisablePafxfast: aws.Bool(true),
		EnableTLS:                 aws.Bool(true),
		CA:                        aws.String("ca.pem"),
		Cert:                      aws.String("cert.pem"),
		Key:                       aws.String("key.pem"),
	}
	c := NewOptions()
	err = c.Apply(context.TODO(), sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.SASL.SASLUser)
	require.Equal(t, "123", c.SASL.SASLPassword)
	require.Equal(t, "plain", strings.ToLower(string(c.SASL.SASLMechanism)))
	require.Equal(t, 2, int(c.SASL.GSSAPI.AuthType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.SASL.GSSAPI.KeyTabPath)
	require.Equal(t, "service", c.SASL.GSSAPI.ServiceName)
	require.Equal(t, "user", c.SASL.GSSAPI.Username)
	require.Equal(t, "pass", c.SASL.GSSAPI.Password)
	require.Equal(t, "realm", c.SASL.GSSAPI.Realm)
	require.Equal(t, true, c.SASL.GSSAPI.DisablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)

	// test override
	uri = "kafka://topic?partition-num=12" +
		"&replication-factor=5" +
		"&kafka-version=3.1.2" +
		"&max-message-bytes=1048576" +
		"&compression=gzip" +
		"&kafka-client-id=test-id" +
		"&auto-create-topic=true" +
		"&dial-timeout=1m1s" +
		"&write-timeout=2m1s" +
		"&required-acks=1" +
		"&sasl-user=abc" +
		"&sasl-password=123" +
		"&sasl-mechanism=plain" +
		"&sasl-gssapi-auth-type=keytab" +
		"&sasl-gssapi-keytab-path=SASLGssAPIKeytabPath" +
		"&sasl-gssapi-service-name=service" +
		"&sasl-gssapi-user=user" +
		"&sasl-gssapi-password=pass" +
		"&sasl-gssapi-realm=realm" +
		"&sasl-gssapi-disable-pafxfast=true" +
		"&enable-tls=true" +
		"&ca=ca.pem" +
		"&cert=cert.pem" +
		"&key=key.pem"
	sinkURI, err = url.Parse(uri)
	require.NoError(t, err)
	replicaConfig.Sink.KafkaConfig = &config.KafkaConfig{
		PartitionNum:              aws.Int32(11),
		ReplicationFactor:         aws.Int16(3),
		KafkaVersion:              aws.String("3.2.2"),
		MaxMessageBytes:           aws.Int(1023 * 1024),
		Compression:               aws.String("none"),
		KafkaClientID:             aws.String("test2-id"),
		AutoCreateTopic:           aws.Bool(false),
		DialTimeout:               aws.String("1m2s"),
		WriteTimeout:              aws.String("2m3s"),
		RequiredAcks:              aws.Int(-1),
		SASLUser:                  aws.String("abcd"),
		SASLPassword:              aws.String("1234"),
		SASLMechanism:             aws.String("plain"),
		SASLGssAPIAuthType:        aws.String("user"),
		SASLGssAPIKeytabPath:      aws.String("path"),
		SASLGssAPIServiceName:     aws.String("service2"),
		SASLGssAPIUser:            aws.String("usera"),
		SASLGssAPIPassword:        aws.String("pass2"),
		SASLGssAPIRealm:           aws.String("realm2"),
		SASLGssAPIDisablePafxfast: aws.Bool(false),
		EnableTLS:                 aws.Bool(false),
		CA:                        aws.String("ca2.pem"),
		Cert:                      aws.String("cert2.pem"),
		Key:                       aws.String("key2.pem"),
	}
	c = NewOptions()
	err = c.Apply(context.TODO(), sinkURI, replicaConfig)
	require.NoError(t, err)
	require.Equal(t, int32(12), c.PartitionNum)
	require.Equal(t, int16(5), c.ReplicationFactor)
	require.Equal(t, "3.1.2", c.Version)
	require.Equal(t, 1024*1024, c.MaxMessageBytes)
	require.Equal(t, "gzip", c.Compression)
	require.Equal(t, "test-id", c.ClientID)
	require.Equal(t, true, c.AutoCreate)
	require.Equal(t, time.Minute+time.Second, c.DialTimeout)
	require.Equal(t, 2*time.Minute+time.Second, c.WriteTimeout)
	require.Equal(t, 1, int(c.RequiredAcks))
	require.Equal(t, "abc", c.SASL.SASLUser)
	require.Equal(t, "123", c.SASL.SASLPassword)
	require.Equal(t, "plain", strings.ToLower(string(c.SASL.SASLMechanism)))
	require.Equal(t, 2, int(c.SASL.GSSAPI.AuthType))
	require.Equal(t, "SASLGssAPIKeytabPath", c.SASL.GSSAPI.KeyTabPath)
	require.Equal(t, "service", c.SASL.GSSAPI.ServiceName)
	require.Equal(t, "user", c.SASL.GSSAPI.Username)
	require.Equal(t, "pass", c.SASL.GSSAPI.Password)
	require.Equal(t, "realm", c.SASL.GSSAPI.Realm)
	require.Equal(t, true, c.SASL.GSSAPI.DisablePAFXFAST)
	require.Equal(t, true, c.EnableTLS)
	require.Equal(t, "ca.pem", c.Credential.CAPath)
	require.Equal(t, "cert.pem", c.Credential.CertPath)
	require.Equal(t, "key.pem", c.Credential.KeyPath)
}
