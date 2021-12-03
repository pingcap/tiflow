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
	DefaultMockTopicName    = "mock_topic"
	defaultMockControllerID = 1
)

var defaultTopicMaxMessage = "1048576"

type ClusterAdminClientMockImpl struct {
	topics        map[string]sarama.TopicDetail
	controllerID  int32
	brokerConfigs []sarama.ConfigEntry
}

func NewClusterAdminClientMockImpl() *ClusterAdminClientMockImpl {
	topics := make(map[string]sarama.TopicDetail)
	configEntries := make(map[string]*string)
	configEntries[TopicMaxMessageBytesConfigName] = &defaultTopicMaxMessage
	topics[DefaultMockTopicName] = sarama.TopicDetail{
		NumPartitions: 3,
		ConfigEntries: configEntries,
	}

	brokerConfigs := []sarama.ConfigEntry{{
		Name:  BrokerMessageMaxBytesConfigName,
		Value: defaultTopicMaxMessage,
	}}

	return &ClusterAdminClientMockImpl{
		topics:        topics,
		controllerID:  defaultMockControllerID,
		brokerConfigs: brokerConfigs,
	}
}

func (c *ClusterAdminClientMockImpl) ListTopics() (map[string]sarama.TopicDetail, error) {
	return c.topics, nil
}

func (c *ClusterAdminClientMockImpl) DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error) {
	return nil, c.controllerID, nil
}

func (c *ClusterAdminClientMockImpl) DescribeConfig(_ sarama.ConfigResource) ([]sarama.ConfigEntry, error) {
	return c.brokerConfigs, nil
}

func (c *ClusterAdminClientMockImpl) CreateTopic(_ string, _ *sarama.TopicDetail, _ bool) error {
	return nil
}

func (c *ClusterAdminClientMockImpl) Close() error {
	return nil
}

func (c *ClusterAdminClientMockImpl) AddTopic(topic string, detail sarama.TopicDetail) {
	c.topics[topic] = detail
}

func (c *ClusterAdminClientMockImpl) GetDefaultMockTopicName() string {
	return DefaultMockTopicName
}

func (c *ClusterAdminClientMockImpl) GetDefaultMaxMessageBytes() int {
	topicMaxMessage, _ := strconv.Atoi(defaultTopicMaxMessage)
	return topicMaxMessage
}
