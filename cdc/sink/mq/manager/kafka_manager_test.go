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
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager_test.go
func TestPartitions(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	manager := NewKafkaTopicManager(context.TODO(), adminClient, cfg)
	defer manager.Close()
	partitionsNum, err := manager.GetPartitionNum(kafka.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionsNum)
}

=======
>>>>>>> d30b4c3793 (kafka(ticdc): topic manager return the partition number specified in the sink-uri (#9955)):cdc/sink/dmlsink/mq/manager/kafka_manager_test.go
func TestCreateTopic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)

	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager_test.go
	manager := NewKafkaTopicManager(ctx, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(kafka.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible("new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum("new-topic")
	require.Nil(t, err)
=======
	changefeedID := model.DefaultChangeFeedID("test")
	ctx := context.Background()
	manager := NewKafkaTopicManager(ctx, kafka.DefaultMockTopicName, changefeedID, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafka.DefaultMockTopicName)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum(ctx, "new-topic")
	require.NoError(t, err)
>>>>>>> d30b4c3793 (kafka(ticdc): topic manager return the partition number specified in the sink-uri (#9955)):cdc/sink/dmlsink/mq/manager/kafka_manager_test.go
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager_test.go
	manager = NewKafkaTopicManager(ctx, adminClient, cfg)
=======
	manager = NewKafkaTopicManager(ctx, "new-topic2", changefeedID, adminClient, cfg)
>>>>>>> d30b4c3793 (kafka(ticdc): topic manager return the partition number specified in the sink-uri (#9955)):cdc/sink/dmlsink/mq/manager/kafka_manager_test.go
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible("new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)

	topic := "new-topic-failed"
	// Invalid replication factor.
	// It happens when replication-factor is greater than the number of brokers.
	cfg = &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 4,
	}
<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager_test.go
	manager = NewKafkaTopicManager(ctx, adminClient, cfg)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible("new-topic-failed")
=======
	manager = NewKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, topic)
>>>>>>> d30b4c3793 (kafka(ticdc): topic manager return the partition number specified in the sink-uri (#9955)):cdc/sink/dmlsink/mq/manager/kafka_manager_test.go
	require.Regexp(
		t,
		"new sarama producer: kafka server: Replication-factor is invalid",
		err,
	)
}

func TestCreateTopicWithDelay(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer func(adminClient *kafka.ClusterAdminClientMockImpl) {
		_ = adminClient.Close()
	}(adminClient)
	cfg := &kafkaconfig.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

<<<<<<< HEAD:cdc/sink/mq/manager/kafka_manager_test.go
	manager := NewKafkaTopicManager(context.TODO(), adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.createTopic("new_topic")
	require.Nil(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible("new_topic", 3)
	require.Nil(t, err)
	err = manager.waitUntilTopicVisible("new_topic")
	require.Nil(t, err)
=======
	topic := "new_topic"
	changefeedID := model.DefaultChangeFeedID("test")
	ctx := context.Background()
	manager := NewKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg)
	defer manager.Close()
	partitionNum, err := manager.createTopic(ctx, topic)
	require.NoError(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible(topic, 3)
	require.NoError(t, err)
	err = manager.waitUntilTopicVisible(ctx, topic)
	require.NoError(t, err)
>>>>>>> d30b4c3793 (kafka(ticdc): topic manager return the partition number specified in the sink-uri (#9955)):cdc/sink/dmlsink/mq/manager/kafka_manager_test.go
	require.Equal(t, int32(2), partitionNum)
}
