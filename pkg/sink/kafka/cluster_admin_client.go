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
	"github.com/Shopify/sarama"
)

<<<<<<< HEAD
// ClusterAdminClient is the administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// ListTopics list the topics available in the cluster with the default options.
	ListTopics() (map[string]sarama.TopicDetail, error)
	// DescribeCluster gets information about the nodes in the cluster
	DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error)
	// DescribeConfig gets the configuration for the specified resources.
	DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error)
	// DescribeTopics fetches metadata from some topics.
	DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error)
=======
// TopicDetail represent a topic's detail information.
type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
}

// Broker represents a Kafka broker.
type Broker struct {
	ID int32
}

// ClusterAdminClient is the administrative client for Kafka,
// which supports managing and inspecting topics, brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// GetAllBrokers return all brokers among the cluster
	GetAllBrokers(ctx context.Context) ([]Broker, error)

	// GetCoordinator return the coordinator's broker id of the cluster
	GetCoordinator(ctx context.Context) (controllerID int, err error)

	// GetBrokerConfig return the broker level configuration with the `configName`
	GetBrokerConfig(ctx context.Context, configName string) (string, error)

	// GetTopicConfig return the topic level configuration with the `configName`
	GetTopicConfig(ctx context.Context, topicName string, configName string) (string, error)

	// GetTopicsMeta return all target topics' metadata
	// if `ignoreTopicError` is true, ignore the topic error and return the metadata of valid topics
	GetTopicsMeta(ctx context.Context,
		topics []string, ignoreTopicError bool) (map[string]TopicDetail, error)

>>>>>>> 493c0f6b61 (pkg/sink(ticdc): add GetTopicConfig support (#9107))
	// CreateTopic creates a new topic.
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	// Close shuts down the admin and closes underlying client.
	Close() error
}

// ClusterAdminClientCreator defines the type of cluster admin client crater.
type ClusterAdminClientCreator func([]string, *sarama.Config) (ClusterAdminClient, error)

// NewSaramaAdminClient constructs a ClusterAdminClient with sarama.
func NewSaramaAdminClient(addrs []string, conf *sarama.Config) (ClusterAdminClient, error) {
	return sarama.NewClusterAdmin(addrs, conf)
}

// NewMockAdminClient constructs a ClusterAdminClient with mock implementation.
func NewMockAdminClient(_ []string, _ *sarama.Config) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}
