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

package tp

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/scheduler/internal/tp/schedulepb"
	"github.com/pingcap/tiflow/pkg/p2p"
	"github.com/stretchr/testify/require"
)

func TestSendRecv(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	transMap := make(map[string]transport)
	for addr, node := range cluster.Nodes {
		transMap[addr], err = newTranport(ctx, changefeedID, node.Server, node.Router)
		require.Nil(t, err)
	}

	// Send messages
	for _, trans := range transMap {
		for addr := range transMap {
			done, err := trans.Send(ctx, addr, []*schedulepb.Message{{To: addr}})
			require.Nil(t, err)
			require.True(t, done)
		}
	}

	// Recv messages
	total := len(transMap)
	recvMegs := make([]*schedulepb.Message, 0, total)
	for _, trans := range transMap {
		require.Eventually(t, func() bool {
			msgs, err := trans.Recv(ctx)
			require.Nil(t, err)
			recvMegs = append(recvMegs, msgs...)
			return len(recvMegs) == total
		}, 200*time.Millisecond, 25)
		recvMegs = recvMegs[:0]
	}
}

func TestUnknownAddr(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	transMap := make(map[string]transport)
	for addr, node := range cluster.Nodes {
		transMap[addr], err = newTranport(ctx, changefeedID, node.Server, node.Router)
		require.Nil(t, err)
	}

	unknownAddr := "unknown"
	require.NotContains(t, transMap, unknownAddr)
	for _, trans := range transMap {
		done, err := trans.Send(ctx, unknownAddr, []*schedulepb.Message{})
		require.Nil(t, err)
		require.False(t, done)
	}
}

func TestEmptyRecv(t *testing.T) {
	t.Parallel()

	cluster := p2p.NewMockCluster(t, 3)
	defer cluster.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	changefeedID := model.ChangeFeedID{Namespace: "test", ID: "test"}

	var err error
	transMap := make(map[string]transport)
	for addr, node := range cluster.Nodes {
		transMap[addr], err = newTranport(ctx, changefeedID, node.Server, node.Router)
		require.Nil(t, err)
	}

	for _, trans := range transMap {
		msgs, err := trans.Recv(ctx)
		require.Nil(t, err)
		require.Empty(t, msgs)
	}
}
