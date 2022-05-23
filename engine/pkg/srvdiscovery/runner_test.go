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

package srvdiscovery

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/adapter"
	"github.com/pingcap/tiflow/engine/test"
	"github.com/stretchr/testify/require"
)

func TestDiscoveryRunner(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, _, client, cleanFn := test.PrepareEtcd(t, "discovery-runner-test1")
	defer cleanFn()

	keyGen := func(i int) string { return fmt.Sprintf("server-%d", i+1) }
	newNodeOnline := func(i int) DiscoveryRunner {
		res := &ServiceResource{
			ID:   model.ExecutorID(keyGen(i)),
			Addr: fmt.Sprintf("127.0.0.1:%d", i+10001),
		}
		resStr, err := res.ToJSON()
		require.Nil(t, err)
		return NewDiscoveryRunnerImpl(
			client,
			5,
			time.Millisecond*50,
			adapter.NodeInfoKeyAdapter.Encode(string(res.ID)),
			resStr,
		)
	}

	runnerN := 5
	runners := make([]DiscoveryRunner, 0, runnerN)
	for i := 0; i < runnerN; i++ {
		runners = append(runners, newNodeOnline(i))
	}

	// simulate N nodes become online one by one
	for i, runner := range runners {
		_, err := runner.ResetDiscovery(ctx, true /*resetSession*/)
		require.Nil(t, err)
		snapshot := runner.GetSnapshot()
		require.Equal(t, i+1, len(snapshot))
		for j := 0; j < i; j++ {
			require.Contains(t, snapshot, keyGen(j))
		}
	}

	for i, runner := range runners {
		if i == runnerN-1 {
			continue
		}
		watcher := runner.GetWatcher()
		nodesOn := 0
	check:
		for {
			select {
			case resp := <-watcher:
				runner.ApplyWatchResult(resp)
				nodesOn += len(resp.AddSet)
				if nodesOn == runnerN-i-1 {
					break check
				}
			case <-time.After(time.Second):
				require.Failf(t, "not enough peer update received",
					"received: %d, expected: %d", nodesOn, runnerN-i-1)
			}
		}
	}

	// Test another runnerN nodes online, but existing N runners meet failure
	// or session is done and restart.
	for i := 0; i < runnerN; i++ {
		runner := newNodeOnline(i + runnerN)
		_, err := runner.ResetDiscovery(ctx, true /*resetSession*/)
		require.Nil(t, err)
	}

	for _, runner := range runners {
		_, err := runner.ResetDiscovery(ctx, true /*resetSession*/)
		require.Nil(t, err)
		snapshot := runner.GetSnapshot()
		require.Equal(t, runnerN, len(snapshot))
		watcher := runner.GetWatcher()
		nodesOn := 0
	check2:
		for {
			select {
			case resp := <-watcher:
				nodesOn += len(resp.AddSet)
				if nodesOn == runnerN {
					break check2
				}
			case <-time.After(time.Second):
				require.Failf(t, "not enough peer update received",
					"received: %d, expected: %d", nodesOn, runnerN)
			}
		}
	}
}
