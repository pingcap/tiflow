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
)

// TopicDetail represent a topic's detail information.
type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	ConfigEntries     map[string]string
}

// Broker represents a Kafka broker.
type Broker struct {
	ID int32
}

// ClusterAdminClient is the administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// GetAllBrokers return all brokers among the cluster
	GetAllBrokers() ([]Broker, error)

	// GetCoordinator return the coordinator's broker id for the cluster
	GetCoordinator() (controllerID int32, err error)

	GetBrokerConfig(configName string) (string, error)

	// GetAllTopicsMeta return all topics' metadata
	// which available in the cluster with the default options.
	GetAllTopicsMeta() (map[string]TopicDetail, error)

	// CreateTopic creates a new topic.
	CreateTopic(topic string, detail *TopicDetail, validateOnly bool) error

	// GetTopicsMeta return all target topics' metadata
	// if `ignoreTopicError` is true, ignore the topic error and return the metadata of valid topics
	GetTopicsMeta(topics []string, ignoreTopicError bool) (map[string]TopicDetail, error)

	// Close shuts down the admin and closes underlying client.
	Close() error
}

// ClusterAdminClientCreator defines the type of cluster admin client crater.
type ClusterAdminClientCreator func(context.Context, *Options) (ClusterAdminClient, error)

// NewMockAdminClient constructs a ClusterAdminClient with mock implementation.
func NewMockAdminClient(_ context.Context, _ *Options) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}
