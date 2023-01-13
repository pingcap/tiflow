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

type TopicDetail struct {
	Name              string
	NumPartitions     int32
	ReplicationFactor int16
	ConfigEntries     map[string]string
}

type Broker struct {
	ID int32
}

// ConfigResourceType is a type for resources that have configs.
type ConfigResourceType int8

// Taken from:
// https://github.com/apache/kafka/blob/ed7c071e07f1f90e4c2895582f61ca090ced3c42/clients/src/main/java/org/apache/kafka/common/config/ConfigResource.java#L32-L55

const (
	// UnknownResource constant type
	UnknownResource ConfigResourceType = 0
	// TopicResource constant type
	TopicResource ConfigResourceType = 2
	// BrokerResource constant type
	BrokerResource ConfigResourceType = 4
	// BrokerLoggerResource constant type
	BrokerLoggerResource ConfigResourceType = 8
)

type ConfigResource struct {
	Type        ConfigResourceType
	Name        string
	ConfigNames []string
}

// ClusterAdminClient is the administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// GetAllBrokers return all brokers among the cluster
	GetAllBrokers() ([]Broker, error)

	// GetCoordinator return the coordinator's broker id for the cluster
	GetCoordinator() (controllerID int32, err error)

	GetBrokerConfig(configName string) (string, error)

	// ListTopics list the topics available in the cluster with the default options.
	ListTopics() (map[string]TopicDetail, error)

	// CreateTopic creates a new topic.
	CreateTopic(topic string, detail *TopicDetail, validateOnly bool) error

	// GetTopicsMeta return all target topics' metadata
	// if `ignoreTopicError` is true, ignore the topic error and return the metadata of valid topics
	GetTopicsMeta(topics []string, ignoreTopicError bool) ([]TopicDetail, error)

	// Close shuts down the admin and closes underlying client.
	Close() error
}

// ClusterAdminClientCreator defines the type of cluster admin client crater.
type ClusterAdminClientCreator func(context.Context, *Options) (ClusterAdminClient, error)

// NewMockAdminClient constructs a ClusterAdminClient with mock implementation.
func NewMockAdminClient(_ context.Context, _ *Options) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}
