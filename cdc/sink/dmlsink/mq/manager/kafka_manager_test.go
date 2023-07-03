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

	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

func TestPartitions(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	ctx := context.Background()
	manager, err := NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()

	partitionsNum, err := manager.GetPartitionNum(
		ctx,
		kafka.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionsNum)
}

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	ctx := context.Background()
	manager, err := NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafka.DefaultMockTopicName)
	require.Nil(t, err)
	require.Equal(t, int32(3), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum(ctx, "new-topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
	manager, err = NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)

	// Invalid replication factor.
	// It happens when replication-factor is greater than the number of brokers.
	cfg = &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 4,
	}
	manager, err = NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic-failed")
	require.Regexp(
		t,
		"kafka create topic failed: kafka server: Replication-factor is invalid",
		err,
	)
}

func TestCreateTopicWithDelay(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	ctx := context.Background()
	manager, err := NewKafkaTopicManager(ctx, adminClient, cfg)
	require.Nil(t, err)
	defer manager.Close()
	partitionNum, err := manager.createTopic(ctx, "new_topic")
	require.Nil(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible("new_topic", 3)
	require.Nil(t, err)
	err = manager.waitUntilTopicVisible(ctx, "new_topic")
	require.Nil(t, err)
	require.Equal(t, int32(2), partitionNum)
}
