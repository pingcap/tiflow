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

package manager

import (
	"context"
	"testing"

	kafkaconfig "github.com/pingcap/tiflow/cdc/sink/mq/producer/kafka"
	kafkamock "github.com/pingcap/tiflow/pkg/kafka"
	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD
	client := kafkamock.NewClientMockImpl()
	adminClient := kafkamock.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafkamock.ClusterAdminClientMockImpl) {
=======
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
>>>>>>> 8a3bb9cc05 (sink(ticdc): Optimize the performance when getting the metadata of Kafka topics (#9060) (#9104))
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager, err := NewKafkaTopicManager(context.TODO(), adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	partitionsNum, err := manager.GetPartitionNum(
		kafkamock.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionsNum)
}

<<<<<<< HEAD
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

	manager, err := NewKafkaTopicManager(client, adminClient, cfg)
	require.Nil(t, err)
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
=======
func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
>>>>>>> 8a3bb9cc05 (sink(ticdc): Optimize the performance when getting the metadata of Kafka topics (#9060) (#9104))
		_ = adminClient.Close()
	}(adminClient)

	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager, err := NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
<<<<<<< HEAD
	partitionNum, err := manager.createTopic(kafkamock.DefaultMockTopicName)
=======
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(kafka.DefaultMockTopicName)
>>>>>>> 8a3bb9cc05 (sink(ticdc): Optimize the performance when getting the metadata of Kafka topics (#9060) (#9104))
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible("new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum("new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
	manager, err = NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible("new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)

	// Invalid replication factor.
	// It happens when replication-factor is greater than the number of brokers.
	cfg = &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 4,
	}
	manager, err = NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible("new-topic-failed")
	require.Regexp(
		t,
		"new sarama producer: kafka server: Replication-factor is invalid",
		err,
	)
}

func TestCreateTopicWithDelay(t *testing.T) {
	t.Parallel()

<<<<<<< HEAD
	client := kafkamock.NewClientMockImpl()
	adminClient := kafkamock.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafkamock.ClusterAdminClientMockImpl) {
=======
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
>>>>>>> 8a3bb9cc05 (sink(ticdc): Optimize the performance when getting the metadata of Kafka topics (#9060) (#9104))
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager, err := NewKafkaTopicManager(context.TODO(), adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	partitionNum, err := manager.createTopic("new_topic")
	require.Nil(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible("new_topic", 3)
	require.Nil(t, err)
	err = manager.waitUntilTopicVisible("new_topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
}
