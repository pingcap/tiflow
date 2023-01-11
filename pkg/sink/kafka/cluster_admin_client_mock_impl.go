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
	"fmt"
	"strconv"

	"github.com/Shopify/sarama"
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
	defaultMaxMessageBytes = "1048576"

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
	controllerID  int32
	brokerConfigs map[string]string
}

// NewClusterAdminClientMockImpl news a ClusterAdminClientMockImpl struct with default configurations.
func NewClusterAdminClientMockImpl() *ClusterAdminClientMockImpl {
	topics := make(map[string]*topicDetail)
	configEntries := make(map[string]string)
	configEntries[TopicMaxMessageBytesConfigName] = TopicMaxMessageBytes
	configEntries[MinInsyncReplicasConfigName] = MinInSyncReplicas
	topics[DefaultMockTopicName] = &topicDetail{
		fetchesRemainingUntilVisible: 0,
		TopicDetail: TopicDetail{
			NumPartitions: 3,
			ConfigEntries: configEntries,
		},
	}

	brokerConfigs := make(map[string]string)
	brokerConfigs[BrokerMessageMaxBytesConfigName] = BrokerMessageMaxBytes
	brokerConfigs[MinInsyncReplicasConfigName] = MinInSyncReplicas

	return &ClusterAdminClientMockImpl{
		topics:        topics,
		controllerID:  defaultMockControllerID,
		brokerConfigs: brokerConfigs,
	}
}

// ListTopics returns all topics directly.
func (c *ClusterAdminClientMockImpl) ListTopics() (map[string]*TopicDetail, error) {
	topicsDetailsMap := make(map[string]*TopicDetail)
	for topic, detail := range c.topics {
		topicsDetailsMap[topic] = &detail.TopicDetail
	}
	return topicsDetailsMap, nil
}

// DescribeCluster returns the controller ID.
func (c *ClusterAdminClientMockImpl) DescribeCluster() (brokers []Broker, controllerID int32, err error) {
	return nil, c.controllerID, nil
}

// DescribeConfig return brokerConfigs directly.
func (c *ClusterAdminClientMockImpl) DescribeConfig(resource ConfigResource) (map[string]string, error) {
	var result map[string]string
	for _, name := range resource.ConfigNames {
		if value, ok := c.brokerConfigs[name]; ok {
			result[name] = value
		}
	}
	return result, nil
}

// SetRemainingFetchesUntilTopicVisible is used to control the visibility of a specific topic.
// It is used to mock the topic creation delay.
func (c *ClusterAdminClientMockImpl) SetRemainingFetchesUntilTopicVisible(topicName string,
	fetchesRemainingUntilVisible int,
) error {
	topic, ok := c.topics[topicName]
	if !ok {
		return fmt.Errorf("No such topic as %s", topicName)
	}

	topic.fetchesRemainingUntilVisible = fetchesRemainingUntilVisible
	return nil
}

// DescribeTopics fetches metadata from some topics.
func (c *ClusterAdminClientMockImpl) DescribeTopics(topics []string) (
	metadata []TopicMetadata, err error,
) {
	topicDescriptions := make(map[string]TopicMetadata)

	for _, requestedTopic := range topics {
		for topicName, topicDetail := range c.topics {
			if topicName == requestedTopic {
				if topicDetail.fetchesRemainingUntilVisible > 0 {
					topicDetail.fetchesRemainingUntilVisible--
					continue
				} else {
					topicDescriptions[topicName] = TopicMetadata{
						Name:       topicName,
						Partitions: make([]*PartitionMetadata, topicDetail.NumPartitions),
					}
					break
				}
			}
		}

		if _, ok := topicDescriptions[requestedTopic]; !ok {
			topicDescriptions[requestedTopic] = TopicMetadata{
				Name: requestedTopic,
				// todo: make this error not related to the sarama.
				Err: sarama.ErrUnknownTopicOrPartition,
			}
		}
	}

	metadataRes := make([]TopicMetadata, 0)
	for _, meta := range topicDescriptions {
		metadataRes = append(metadataRes, meta)
	}

	return metadataRes, nil
}

// CreateTopic adds topic into map.
func (c *ClusterAdminClientMockImpl) CreateTopic(topic string, detail *TopicDetail, _ bool) error {
	if detail.ReplicationFactor > defaultReplicationFactor {
		return sarama.ErrInvalidReplicationFactor
	}

	minInsyncReplicaConfigFound := false
	for k, _ := range c.brokerConfigs {
		if k == MinInsyncReplicasConfigName {
			minInsyncReplicaConfigFound = true
		}
	}

	// For Confluent Cloud, min.insync.replica is invisible and replication factor must be 3.
	// Otherwise, ErrPolicyViolation is expected to be returned.
	if !minInsyncReplicaConfigFound &&
		detail.ReplicationFactor != defaultReplicationFactor {
		return sarama.ErrPolicyViolation
	}

	c.topics[topic] = &topicDetail{
		TopicDetail: *detail,
	}
	return nil
}

// Close do nothing.
func (c *ClusterAdminClientMockImpl) Close() error {
	return nil
}

// SetMinInsyncReplicas sets the MinInsyncReplicas for broker and default topic.
func (c *ClusterAdminClientMockImpl) SetMinInsyncReplicas(minInsyncReplicas string) {
	c.topics[DefaultMockTopicName].ConfigEntries[MinInsyncReplicasConfigName] = minInsyncReplicas
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
