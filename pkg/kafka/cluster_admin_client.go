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

// ClusterAdminClient is the administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// DescribeCluster gets information about the nodes in the cluster
	DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error)
	// DescribeConfig gets the configuration for the specified resources.
	DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error)
	// DescribeTopics fetches metadata from some topics.
	DescribeTopics(topics []string) (metadata []*sarama.TopicMetadata, err error)
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
