// Copyright 2022 PingCAP, Inc.
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
	"testing"
	"time"

	kafkaconfig "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	kafkamock "github.com/pingcap/tiflow/pkg/kafka"
	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	t.Parallel()

	client := kafkamock.NewClientMockImpl()
	adminClient := kafkamock.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafkamock.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager := NewTopicManager(client, adminClient, cfg)
	partitionsNum, err := manager.GetPartitionNum(
		kafkamock.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionsNum)
}

func TestTryRefreshMeta(t *testing.T) {
	t.Parallel()

	client := kafkamock.NewClientMockImpl()
	adminClient := kafkamock.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafkamock.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager := NewTopicManager(client, adminClient, cfg)
	partitionsNum, err := manager.GetPartitionNum(
		kafkamock.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionsNum)

	// Mock create a topic.
	client.AddTopic("test", 4)
	manager.lastMetadataRefresh.Store(time.Now().Add(-2 * time.Minute).Unix())
	partitionsNum, err = manager.GetPartitionNum("test")
	require.Nil(t, err)
	require.Equal(t, int32(4), partitionsNum)

	// Mock delete a topic.
	// NOTICE: we do not refresh metadata for the deleted topic.
	client.DeleteTopic("test")
	partitionsNum, err = manager.GetPartitionNum("test")
	require.Nil(t, err)
	require.Equal(t, int32(4), partitionsNum)
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	client := kafkamock.NewClientMockImpl()
	adminClient := kafkamock.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafkamock.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager := NewTopicManager(client, adminClient, cfg)
	partitionNum, err := manager.CreateTopic(kafkamock.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionNum)

	partitionNum, err = manager.CreateTopic("new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum("new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
	manager = NewTopicManager(client, adminClient, cfg)
	_, err = manager.CreateTopic("new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)
}
