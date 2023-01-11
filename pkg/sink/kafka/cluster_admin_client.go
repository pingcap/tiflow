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
	"github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
)

type TopicDetail struct {
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

type TopicMetadata struct {
	Name string
	Err  error

	Partitions []*PartitionMetadata
}

type PartitionMetadata struct {
	Err             error
	ID              int32
	Leader          int32
	Replicas        []int32
	Isr             []int32
	OfflineReplicas []int32
}

// ClusterAdminClient is the administrative client for Kafka, which supports managing and inspecting topics,
// brokers, configurations and ACLs.
type ClusterAdminClient interface {
	// ListTopics list the topics available in the cluster with the default options.
	ListTopics() (map[string]*TopicDetail, error)
	// DescribeCluster gets information about the nodes in the cluster
	DescribeCluster() (brokers []Broker, controllerID int32, err error)
	// DescribeConfig gets the configuration for the specified resources.
	DescribeConfig(resource ConfigResource) (map[string]string, error)
	// DescribeTopics fetches metadata from some topics.
	DescribeTopics(topics []string) (metadata []*TopicMetadata, err error)
	// CreateTopic creates a new topic.
	CreateTopic(topic string, detail *TopicDetail, validateOnly bool) error
	// Close shuts down the admin and closes underlying client.
	Close() error
}

// ClusterAdminClientCreator defines the type of cluster admin client crater.
type ClusterAdminClientCreator func([]string, *kafka.Config) (ClusterAdminClient, error)

// NewMockAdminClient constructs a ClusterAdminClient with mock implementation.
func NewMockAdminClient(_ []string, _ *kafka.Config) (ClusterAdminClient, error) {
	return NewClusterAdminClientMockImpl(), nil
}
