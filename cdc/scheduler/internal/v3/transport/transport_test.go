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

package transport

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/schedulepb"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/stretchr/testify/require"
)

func TestTransSendRecv(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	var schedulerAddr string
	var schedulerTrans Transport
	agentTransMap := make(map[string]Transport)
	for addr, node := range cluster.Nodes {
		if schedulerAddr == "" {
			schedulerTrans, err = NewTransport(
				ctx, changefeedID, SchedulerRole, node.Server, node.Router)
		} else {
			agentTransMap[addr], err = NewTransport(
				ctx, changefeedID, AgentRole, node.Server, node.Router)
		}
		require.Nil(t, err)
	}

	// Send messages, scheduler -> agent.
	for addr := range agentTransMap {
		err := schedulerTrans.Send(ctx, []*schedulepb.Message{{To: addr}})
		require.Nil(t, err)
	}

	// Recv messages from scheduler.
	total := len(agentTransMap)
	recvMegs := make([]*schedulepb.Message, 0, total)
	for _, trans := range agentTransMap {
		require.Eventually(t, func() bool {
			msgs, err := trans.Recv(ctx)
			require.Nil(t, err)
			recvMegs = append(recvMegs, msgs...)
			return len(recvMegs) == total
		}, 200*time.Millisecond, 25)
		recvMegs = recvMegs[:0]
	}

	// Send messages, agent -> scheduler.
	for _, trans := range agentTransMap {
		err := trans.Send(ctx, []*schedulepb.Message{{To: schedulerAddr}})
		require.Nil(t, err)
	}
	// Recv messages from agent.
	total = len(agentTransMap)
	recvMegs = make([]*schedulepb.Message, 0, total)
	for _, trans := range agentTransMap {
		require.Eventually(t, func() bool {
			msgs, err := trans.Recv(ctx)
			require.Nil(t, err)
			recvMegs = append(recvMegs, msgs...)
			return len(recvMegs) == total
		}, 200*time.Millisecond, 25)
		recvMegs = recvMegs[:0]
	}
}

func TestTransUnknownAddr(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	transMap := make(map[string]Transport)
	for addr, node := range cluster.Nodes {
		transMap[addr], err = NewTransport(ctx, changefeedID, AgentRole, node.Server, node.Router)
		require.Nil(t, err)
	}

	unknownAddr := "unknown"
	require.NotContains(t, transMap, unknownAddr)
	for _, trans := range transMap {
		err := trans.Send(ctx, []*schedulepb.Message{{To: unknownAddr}})
		require.Nil(t, err)
	}
}

func TestTransEmptyRecv(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	transMap := make(map[string]Transport)
	for addr, node := range cluster.Nodes {
		transMap[addr], err = NewTransport(ctx, changefeedID, AgentRole, node.Server, node.Router)
		require.Nil(t, err)
	}

	for _, trans := range transMap {
		msgs, err := trans.Recv(ctx)
		require.Nil(t, err)
		require.Empty(t, msgs)
	}
}

// P2P does not allow registering the same topic more than once.
func TestTransTwoRolesInOneNode(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 1)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	for _, node := range cluster.Nodes {
		_, err := NewTransport(ctx, changefeedID, AgentRole, node.Server, node.Router)
		require.Nil(t, err)
		_, err = NewTransport(ctx, changefeedID, SchedulerRole, node.Server, node.Router)
		require.Nil(t, err)
	}
}
