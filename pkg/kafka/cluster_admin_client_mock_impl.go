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
	"strconv"

	"github.com/Shopify/sarama"
)

const (
	// DefaultMockTopicName specifies the default mock topic name.
	DefaultMockTopicName = "mock_topic"
	// defaultMockControllerID specifies the default mock controller ID.
	defaultMockControllerID = 1
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

// ClusterAdminClientMockImpl mock implements the admin client interface.
type ClusterAdminClientMockImpl struct {
	topics map[string]sarama.TopicDetail
	// Cluster controller ID.
	controllerID  int32
	brokerConfigs []sarama.ConfigEntry
}

// NewClusterAdminClientMockImpl news a ClusterAdminClientMockImpl struct with default configurations.
func NewClusterAdminClientMockImpl() *ClusterAdminClientMockImpl {
	topics := make(map[string]sarama.TopicDetail)
	configEntries := make(map[string]*string)
	configEntries[TopicMaxMessageBytesConfigName] = &TopicMaxMessageBytes
	configEntries[MinInsyncReplicasConfigName] = &MinInSyncReplicas
	topics[DefaultMockTopicName] = sarama.TopicDetail{
		NumPartitions: 3,
		ConfigEntries: configEntries,
	}

	brokerConfigs := []sarama.ConfigEntry{
		{
			Name:  BrokerMessageMaxBytesConfigName,
			Value: BrokerMessageMaxBytes,
		},
		{
			Name:  MinInsyncReplicasConfigName,
			Value: MinInSyncReplicas,
		},
	}

	return &ClusterAdminClientMockImpl{
		topics:        topics,
		controllerID:  defaultMockControllerID,
		brokerConfigs: brokerConfigs,
	}
}

// ListTopics returns all topics directly.
func (c *ClusterAdminClientMockImpl) ListTopics() (map[string]sarama.TopicDetail, error) {
	return c.topics, nil
}

// DescribeCluster returns the controller ID.
func (c *ClusterAdminClientMockImpl) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	return nil, c.controllerID, nil
}

// DescribeConfig return brokerConfigs directly.
func (c *ClusterAdminClientMockImpl) DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	var result []sarama.ConfigEntry
	for _, name := range resource.ConfigNames {
		for _, config := range c.brokerConfigs {
			if name == config.Name {
				result = append(result, config)
			}
		}
	}
	return result, nil
}

// CreateTopic adds topic into map.
func (c *ClusterAdminClientMockImpl) CreateTopic(topic string, detail *sarama.TopicDetail, _ bool) error {
	c.topics[topic] = *detail
	return nil
}

// Close do nothing.
func (c *ClusterAdminClientMockImpl) Close() error {
	return nil
}

// SetMinInsyncReplicas sets the MinInsyncReplicas for broker and default topic.
func (c *ClusterAdminClientMockImpl) SetMinInsyncReplicas(minInsyncReplicas string) {
	c.topics[DefaultMockTopicName].ConfigEntries[MinInsyncReplicasConfigName] = &minInsyncReplicas

	for i, config := range c.brokerConfigs {
		if config.Name == MinInsyncReplicasConfigName {
			c.brokerConfigs[i].Value = minInsyncReplicas
		}
	}
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
func (c *ClusterAdminClientMockImpl) DropBrokerConfig() {
	c.brokerConfigs = c.brokerConfigs[:0]
}
