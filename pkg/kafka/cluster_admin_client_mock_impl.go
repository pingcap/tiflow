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

var (
	// defaultMaxMessageBytes specifies the default max message bytes.
	defaultMaxMessageBytes = "10485760"
	// defaultMaxMessageBytes specifies the default min insync replicas for broker and default topic.
	defaultMinInsyncReplicas = "1"
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
	configEntries[TopicMaxMessageBytesConfigName] = &defaultMaxMessageBytes
	configEntries[MinInsyncReplicasConfigName] = &defaultMinInsyncReplicas
	topics[DefaultMockTopicName] = sarama.TopicDetail{
		NumPartitions: 3,
		ConfigEntries: configEntries,
	}

	brokerConfigs := []sarama.ConfigEntry{
		{
			Name:  BrokerMessageMaxBytesConfigName,
			Value: defaultMaxMessageBytes,
		},
		{
			Name:  MinInsyncReplicasConfigName,
			Value: defaultMinInsyncReplicas,
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

// GetDefaultMaxMessageBytes returns defaultMaxMessageBytes as a number.
func (c *ClusterAdminClientMockImpl) GetDefaultMaxMessageBytes() int {
	topicMaxMessage, _ := strconv.Atoi(defaultMaxMessageBytes)
	return topicMaxMessage
}
