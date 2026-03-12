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
	"math"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/sink/kafka"
	"github.com/stretchr/testify/require"
)

// A fake interval to keep the connection alive in kafka topic manager.
const FOREVER = time.Duration(math.MaxInt64)

func TestCreateTopic(t *testing.T) {
	t.Parallel()

	adminClient := kafka.NewClusterAdminClientMockImpl()
	defer adminClient.Close()
	cfg := &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 1,
	}

	changefeedID := model.DefaultChangeFeedID("test")
	ctx := context.Background()
	manager := NewKafkaTopicManager(ctx, kafka.DefaultMockTopicName, changefeedID, adminClient, cfg, FOREVER)
	defer manager.Close()
	partitionNum, err := manager.CreateTopicAndWaitUntilVisible(ctx, kafka.DefaultMockTopicName)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)

	partitionNum, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
	partitionsNum, err := manager.GetPartitionNum(ctx, "new-topic")
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionsNum)

	// Try to create a topic without auto create.
	cfg.AutoCreate = false
	manager = NewKafkaTopicManager(ctx, "new-topic2", changefeedID, adminClient, cfg, FOREVER)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, "new-topic2")
	require.Regexp(
		t,
		"`auto-create-topic` is false, and new-topic2 not found",
		err,
	)

	topic := "new-topic-failed"
	// Invalid replication factor.
	// It happens when replication-factor is greater than the number of brokers.
	cfg = &kafka.AutoCreateTopicConfig{
		AutoCreate:        true,
		PartitionNum:      2,
		ReplicationFactor: 4,
	}
	manager = NewKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg, FOREVER)
	defer manager.Close()
	_, err = manager.CreateTopicAndWaitUntilVisible(ctx, topic)
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

	topic := "new_topic"
	changefeedID := model.DefaultChangeFeedID("test")
	ctx := context.Background()
	manager := NewKafkaTopicManager(ctx, topic, changefeedID, adminClient, cfg, FOREVER)
	defer manager.Close()
	partitionNum, err := manager.createTopic(ctx, topic)
	require.NoError(t, err)
	err = adminClient.SetRemainingFetchesUntilTopicVisible(topic, 3)
	require.NoError(t, err)
	err = manager.waitUntilTopicVisible(ctx, topic)
	require.NoError(t, err)
	require.Equal(t, int32(2), partitionNum)
}

// mockAdminClientForHeartbeat is used to count the calls to HeartbeatBrokers.
type mockAdminClientForHeartbeat struct {
	kafka.ClusterAdminClientMockImpl
	heartbeatCount int
	mu             sync.Mutex
}

func (m *mockAdminClientForHeartbeat) HeartbeatBrokers() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.heartbeatCount++
}

func (m *mockAdminClientForHeartbeat) GetHeartbeatCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.heartbeatCount
}

func TestKafkaManagerHeartbeat(t *testing.T) {
	t.Parallel()

	adminClient := &mockAdminClientForHeartbeat{}
	cfg := &kafka.AutoCreateTopicConfig{AutoCreate: false}
	changefeedID := model.DefaultChangeFeedID("test-heartbeat")
	ctx, cancel := context.WithCancel(context.Background())

	// Use a short interval for testing.
	keepAliveInterval := 50 * time.Millisecond
	manager := NewKafkaTopicManager(ctx, "topic", changefeedID, adminClient, cfg, keepAliveInterval)

	// Ensure the manager is closed and the context is canceled at the end of the test.
	defer manager.Close()
	defer cancel()

	// Wait for a sufficient amount of time to ensure the heartbeat ticker triggers several times.
	// Waiting for 175ms should be enough to trigger 3 times (at 50ms, 100ms, 150ms).
	// Use Eventually to avoid test flakiness.
	require.Eventually(t, func() bool {
		return adminClient.GetHeartbeatCount() >= 2
	}, 2*time.Second, 50*time.Millisecond, "HeartbeatBrokers should be called periodically")

	// Verify that closing the manager stops the heartbeat.
	countBeforeClose := adminClient.GetHeartbeatCount()
	manager.Close()
	// Wait for a short period to ensure no new heartbeats occur.
	time.Sleep(keepAliveInterval * 2)
	require.Equal(t, countBeforeClose, adminClient.GetHeartbeatCount(), "Heartbeat should stop after manager is closed")
}
