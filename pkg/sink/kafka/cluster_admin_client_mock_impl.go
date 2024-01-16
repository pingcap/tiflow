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
	"strconv"

	"github.com/IBM/sarama"
	"github.com/pingcap/tiflow/pkg/errors"
)

const (
	// DefaultMockTopicName specifies the default mock topic name.
	DefaultMockTopicName = "mock_topic"
	// DefaultMockPartitionNum is the default partition number of default mock topic.
	DefaultMockPartitionNum = 3
	// defaultMockControllerID specifies the default mock controller ID.
	defaultMockControllerID = 1
	// topic replication factor must be 3 for Confluent Cloud Kafka.
	defaultReplicationFactor = 3
)

const (
	// defaultMaxMessageBytes specifies the default max message bytes,
	// default to 1048576, identical to kafka broker's `message.max.bytes` and topic's `max.message.bytes`
	// see: https://kafka.apache.org/documentation/#brokerconfigs_message.max.bytes
	// see: https://kafka.apache.org/documentation/#topicconfigs_max.message.bytes
	defaultMaxMessageBytes = "1048588"

	// defaultMinInsyncReplicas specifies the default `min.insync.replicas` for broker and topic.
	defaultMinInsyncReplicas = "1"
)

var (
	// BrokerMessageMaxBytes is the broker's `message.max.bytes`
	BrokerMessageMaxBytes = defaultMaxMessageBytes
	// TopicMaxMessageBytes is the topic's `max.message.bytes`
	TopicMaxMessageBytes = defaultMaxMessageBytes
	// MinInSyncReplicas is the `min.insync.replicas`
	MinInSyncReplicas = defaultMinInsyncReplicas
)

type topicDetail struct {
	TopicDetail
	fetchesRemainingUntilVisible int
}

// ClusterAdminClientMockImpl mock implements the admin client interface.
type ClusterAdminClientMockImpl struct {
	topics map[string]*topicDetail
	// Cluster controller ID.
	controllerID  int
	brokerConfigs map[string]string
	topicConfigs  map[string]map[string]string
}

// NewClusterAdminClientMockImpl news a ClusterAdminClientMockImpl struct with default configurations.
func NewClusterAdminClientMockImpl() *ClusterAdminClientMockImpl {
	topics := make(map[string]*topicDetail)
	topics[DefaultMockTopicName] = &topicDetail{
		fetchesRemainingUntilVisible: 0,
		TopicDetail: TopicDetail{
			Name:          DefaultMockTopicName,
			NumPartitions: 3,
		},
	}

	brokerConfigs := make(map[string]string)
	brokerConfigs[BrokerMessageMaxBytesConfigName] = BrokerMessageMaxBytes
	brokerConfigs[MinInsyncReplicasConfigName] = MinInSyncReplicas

	topicConfigs := make(map[string]map[string]string)
	topicConfigs[DefaultMockTopicName] = make(map[string]string)
	topicConfigs[DefaultMockTopicName][TopicMaxMessageBytesConfigName] = TopicMaxMessageBytes
	topicConfigs[DefaultMockTopicName][MinInsyncReplicasConfigName] = MinInSyncReplicas

	return &ClusterAdminClientMockImpl{
		topics:        topics,
		controllerID:  defaultMockControllerID,
		brokerConfigs: brokerConfigs,
		topicConfigs:  topicConfigs,
	}
}

// GetAllBrokers implement the ClusterAdminClient interface
func (c *ClusterAdminClientMockImpl) GetAllBrokers(context.Context) ([]Broker, error) {
	return nil, nil
}

// GetBrokerConfig implement the ClusterAdminClient interface
func (c *ClusterAdminClientMockImpl) GetBrokerConfig(
	_ context.Context,
	configName string,
) (string, error) {
	value, ok := c.brokerConfigs[configName]
	if !ok {
		return "", errors.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the broker's configuration", configName)
	}
	return value, nil
}

// GetTopicConfig implement the ClusterAdminClient interface
func (c *ClusterAdminClientMockImpl) GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error) {
	if _, ok := c.topics[topicName]; !ok {
		return "", errors.ErrKafkaConfigNotFound.GenWithStack("cannot find the `%s` from the topic's configuration", topicName)
	}
	value, ok := c.topicConfigs[topicName][configName]
	if !ok {
		return "", errors.ErrKafkaConfigNotFound.GenWithStack(
			"cannot find the `%s` from the topic's configuration", configName)
	}
	return value, nil
}

// SetRemainingFetchesUntilTopicVisible is used to control the visibility of a specific topic.
// It is used to mock the topic creation delay.
func (c *ClusterAdminClientMockImpl) SetRemainingFetchesUntilTopicVisible(
	topicName string,
	fetchesRemainingUntilVisible int,
) error {
	topic, ok := c.topics[topicName]
	if !ok {
		return fmt.Errorf("No such topic as %s", topicName)
	}

	topic.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible
	return nil
}

// GetTopicsMeta implement the ClusterAdminClient interface
func (c *ClusterAdminClientMockImpl) GetTopicsMeta(
	_ context.Context,
	topics []string,
	_ bool,
) (map[string]TopicDetail, error) {
	result := make(map[string]TopicDetail, len(topics))
	for _, topic := range topics {
		details, ok := c.topics[topic]
		if ok {
			if details.fetchesRemainingUntilVisible > 0 {
				details.fetchesRemainingUntilVisible--
				continue
			}
			result[topic] = details.TopicDetail
		}
	}
	return result, nil
}

// GetTopicsPartitionsNum implement the ClusterAdminClient interface
func (c *ClusterAdminClientMockImpl) GetTopicsPartitionsNum(
	_ context.Context, topics []string,
) (map[string]int32, error) {
	result := make(map[string]int32, len(topics))
	for _, topic := range topics {
		result[topic] = c.topics[topic].NumPartitions
	}
	return result, nil
}

// CreateTopic adds topic into map.
func (c *ClusterAdminClientMockImpl) CreateTopic(
	_ context.Context,
	detail *TopicDetail,
	_ bool,
) error {
	if detail.ReplicationFactor > defaultReplicationFactor {
		return sarama.ErrInvalidReplicationFactor
	}

	_, minInsyncReplicaConfigFound := c.brokerConfigs[MinInsyncReplicasConfigName]
	// For Confluent Cloud, min.insync.replica is invisible and replication factor must be 3.
	// Otherwise, ErrPolicyViolation is expected to be returned.
	if !minInsyncReplicaConfigFound &&
		detail.ReplicationFactor != defaultReplicationFactor {
		return sarama.ErrPolicyViolation
	}

	c.topics[detail.Name] = &topicDetail{
		TopicDetail: *detail,
	}
	return nil
}

// DeleteTopic deletes a topic, only used for testing.
func (c *ClusterAdminClientMockImpl) DeleteTopic(topicName string) {
	delete(c.topics, topicName)
}

// Close do nothing.
func (c *ClusterAdminClientMockImpl) Close() {}

// SetMinInsyncReplicas sets the MinInsyncReplicas for broker and default topic.
func (c *ClusterAdminClientMockImpl) SetMinInsyncReplicas(minInsyncReplicas string) {
	c.topicConfigs[DefaultMockTopicName][MinInsyncReplicasConfigName] = minInsyncReplicas
	c.brokerConfigs[MinInsyncReplicasConfigName] = minInsyncReplicas
}

// GetDefaultMockTopicName returns the default topic name
func (c *ClusterAdminClientMockImpl) GetDefaultMockTopicName() string {
	return DefaultMockTopicName
}

// GetBrokerMessageMaxBytes returns broker's `message.max.bytes`
func (c *ClusterAdminClientMockImpl) GetBrokerMessageMaxBytes() int {
	messageMaxBytes, _ := strconv.Atoi(BrokerMessageMaxBytes)
	return messageMaxBytes
}

// GetTopicMaxMessageBytes returns topic's `max.message.bytes`
func (c *ClusterAdminClientMockImpl) GetTopicMaxMessageBytes() int {
	maxMessageBytes, _ := strconv.Atoi(TopicMaxMessageBytes)
	return maxMessageBytes
}

// DropBrokerConfig remove all broker level configuration for test purpose.
func (c *ClusterAdminClientMockImpl) DropBrokerConfig(configName string) {
	delete(c.brokerConfigs, configName)
}
