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

package base

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/base/protocol"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/stretchr/testify/require"
)

const (
	numNodes = 3
)

func TestSchedulerBasics(t *testing.T) {
	_ = failpoint.Enable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectSendMessageTryAgain", "50%return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectSendMessageTryAgain")
	}()

	_ = failpoint.Enable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectClosed", "5*return(true)")
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/ClientInjectClosed")
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockCluster := p2p.NewMockCluster(t, numNodes)
	mockCaptures := map[model.CaptureID]*model.CaptureInfo{}

	for _, node := range mockCluster.Nodes {
		mockCaptures[node.ID] = &model.CaptureInfo{
			ID:            node.ID,
			AdvertiseAddr: node.Addr,
		}
	}

	mockOwnerNode := mockCluster.Nodes["capture-0"]

	sched, err := NewSchedulerV2(
		ctx,
		model.DefaultChangeFeedID("cf-1"),
		1000,
		mockOwnerNode.Server,
		mockOwnerNode.Router,
		1)
	require.NoError(t, err)

	for atomic.LoadInt64(&sched.stats.AnnounceSentCount) < numNodes {
		checkpointTs, resolvedTs, err := sched.Tick(
			ctx, 1000, []model.TableID{1, 2, 3}, mockCaptures)
		require.NoError(t, err)
		require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
		require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	}

	announceCh := receiveToChannels(
		ctx,
		t,
		mockOwnerNode.ID,
		mockCluster,
		protocol.AnnounceTopic(model.DefaultChangeFeedID("cf-1")),
		&protocol.AnnounceMessage{})
	dispatchCh := receiveToChannels(
		ctx,
		t,
		mockOwnerNode.ID,
		mockCluster,
		protocol.DispatchTableTopic(model.DefaultChangeFeedID("cf-1")),
		&protocol.DispatchTableMessage{})

	for id, ch := range announceCh {
		var msg interface{}
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case msg = <-ch:
		}

		require.IsType(t, &protocol.AnnounceMessage{}, msg)
		require.Equal(t, &protocol.AnnounceMessage{
			OwnerRev:     1,
			OwnerVersion: version.ReleaseSemver(),
		}, msg)

		_, err := mockCluster.Nodes[id].Router.GetClient(mockOwnerNode.ID).SendMessage(
			ctx,
			protocol.SyncTopic(model.DefaultChangeFeedID("cf-1")),
			&protocol.SyncMessage{
				ProcessorVersion: version.ReleaseSemver(),
			})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&sched.stats.SyncReceiveCount) == numNodes
	}, 5*time.Second, 100*time.Millisecond)

	for atomic.LoadInt64(&sched.stats.DispatchSentCount) < numNodes {
		checkpointTs, resolvedTs, err := sched.Tick(
			ctx, 1000, []model.TableID{1, 2, 3}, mockCaptures)
		require.NoError(t, err)
		require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
		require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	}
	log.Info("Tables have been dispatched")

	for id, ch := range dispatchCh {
		var msg interface{}
		select {
		case <-ctx.Done():
			t.Fatal("timeout")
		case msg = <-ch:
		}

		require.IsType(t, &protocol.DispatchTableMessage{}, msg)
		dispatchTableMessage := msg.(*protocol.DispatchTableMessage)
		require.Equal(t, int64(1), dispatchTableMessage.OwnerRev)
		require.False(t, dispatchTableMessage.IsDelete)
		require.Contains(t, []model.TableID{1, 2, 3}, dispatchTableMessage.ID)

		_, err := mockCluster.Nodes[id].Router.GetClient(mockOwnerNode.ID).SendMessage(
			ctx,
			protocol.DispatchTableResponseTopic(model.DefaultChangeFeedID("cf-1")),
			&protocol.DispatchTableResponseMessage{
				ID: dispatchTableMessage.ID,
			})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&sched.stats.DispatchResponseReceiveCount) == 3
	}, 5*time.Second, 100*time.Millisecond)

	checkpointTs, resolvedTs, err := sched.Tick(
		ctx, 1000, []model.TableID{1, 2, 3}, mockCaptures)
	require.NoError(t, err)
	require.Equal(t, model.Ts(1000), checkpointTs)
	require.Equal(t, model.Ts(1000), resolvedTs)

	for _, node := range mockCluster.Nodes {
		_, err := node.Router.GetClient(mockOwnerNode.ID).
			SendMessage(ctx, protocol.CheckpointTopic(
				model.DefaultChangeFeedID("cf-1")),
				&protocol.CheckpointMessage{
					CheckpointTs: 2000,
					ResolvedTs:   2000,
				})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		return atomic.LoadInt64(&sched.stats.CheckpointReceiveCount) == 3
	}, 5*time.Second, 100*time.Millisecond)

	sched.Close(ctx)
	mockCluster.Close()
}

func TestSchedulerNoPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	mockCluster := p2p.NewMockCluster(t, numNodes)
	mockCaptures := map[model.CaptureID]*model.CaptureInfo{}

	for _, node := range mockCluster.Nodes {
		mockCaptures[node.ID] = &model.CaptureInfo{
			ID:            node.ID,
			AdvertiseAddr: node.Addr,
		}
	}
	mockCaptures["dead-capture"] = &model.CaptureInfo{
		ID:            "dead-capture",
		AdvertiseAddr: "fake-ip",
	}

	mockOwnerNode := mockCluster.Nodes["capture-0"]

	sched, err := NewSchedulerV2(
		ctx,
		model.DefaultChangeFeedID("cf-1"),
		1000,
		mockOwnerNode.Server,
		mockOwnerNode.Router,
		1)
	require.NoError(t, err)

	// Ticks the scheduler 10 times. It should not panic.
	for i := 0; i < 10; i++ {
		checkpointTs, resolvedTs, err := sched.Tick(
			ctx, 1000, []model.TableID{1, 2, 3}, mockCaptures)
		require.NoError(t, err)
		require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
		require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	}

	// Remove the node from the captureInfos.
	delete(mockCaptures, "dead-capture")

	for atomic.LoadInt64(&sched.stats.AnnounceSentCount) < numNodes {
		checkpointTs, resolvedTs, err := sched.Tick(
			ctx, 1000, []model.TableID{1, 2, 3}, mockCaptures)
		require.NoError(t, err)
		require.Equal(t, internal.CheckpointCannotProceed, checkpointTs)
		require.Equal(t, internal.CheckpointCannotProceed, resolvedTs)
	}

	sched.Close(ctx)
	mockCluster.Close()
}

func TestInfoProvider(t *testing.T) {
	var sched interface{}
	sched = new(SchedulerV2)
	_, ok := sched.(internal.InfoProvider)
	require.True(t, ok)
}

func receiveToChannels(
	ctx context.Context,
	t *testing.T,
	ownerID p2p.NodeID,
	cluster *p2p.MockCluster,
	topic p2p.Topic,
	tpi interface{},
) map[p2p.NodeID]chan interface{} {
	channels := map[p2p.NodeID]chan interface{}{}
	for _, node := range cluster.Nodes {
		ch := make(chan interface{}, 16)
		_, err := node.Server.SyncAddHandler(ctx, topic, tpi, func(s string, i interface{}) error {
			require.Equal(t, ownerID, s)
			require.IsType(t, tpi, i)
			select {
			case <-ctx.Done():
				require.Fail(t, "context is canceled")
			case ch <- i:
			}
			return nil
		})
		require.NoError(t, err)
		channels[node.ID] = ch
	}
	return channels
}
