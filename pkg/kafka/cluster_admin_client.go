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
	// ListTopics list the topics available in the cluster with the default options.
	ListTopics() (map[string]sarama.TopicDetail, error)
	// DescribeCluster gets information about the nodes in the cluster
	DescribeCluster() (brokers []*sarama.Broker, controllerID int32, err error)
	// DescribeConfig gets the configuration for the specified resources.
	DescribeConfig(resource sarama.ConfigResource) ([]sarama.ConfigEntry, error)
	// CreateTopic creates a new topic.
	CreateTopic(topic string, detail *sarama.TopicDetail, validateOnly bool) error
	// Close shuts down the admin and closes underlying client.
	Close() error
}

type ClusterAdminClientCrater func([]string, *sarama.Config) (ClusterAdminClient, error)

func NewSaramaAdminClient(addrs []string, conf *sarama.Config) (ClusterAdminClient, error) {
	return sarama.NewClusterAdmin(addrs, conf)
}

func NewMockAdminClient(_ []string, _ *sarama.Config) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}
