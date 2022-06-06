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

package cluster

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"

	"github.com/pingcap/tiflow/engine/pkg/etcdutils"
	"github.com/stretchr/testify/require"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/atomic"
)

func setUpTest(t *testing.T) (newClient func() *clientv3.Client, close func()) {
	dir := t.TempDir()

	// Change the privilege to prevent a warning.
	err := os.Chmod(dir, 0o700)
	require.NoError(t, err)

	urls, server, err := etcdutils.SetupEmbedEtcd(dir)
	require.NoError(t, err)

	var endpoints []string
	for _, uri := range urls {
		endpoints = append(endpoints, uri.String())
	}

	return func() *clientv3.Client {
			cli, err := clientv3.NewFromURLs(endpoints)
			require.NoError(t, err)
			return cli
		}, func() {
			server.Close()
		}
}

const (
	numMockNodesForCampaignTest = 8
)

func TestEtcdElectionCampaign(t *testing.T) {
	// Sets up an embedded Etcd cluster
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		wg                     sync.WaitGroup
		leaderCount            atomic.Int32
		accumulatedLeaderCount atomic.Int32
	)

	for i := 0; i < numMockNodesForCampaignTest; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := newClient()
			election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
				TTL:    5,
				Prefix: "/test-election",
			})
			require.NoError(t, err)

			nodeID := fmt.Sprintf("node-%d", i)
			sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*10)
			require.NoError(t, err)

			// We have been elected
			newLeaderCount := leaderCount.Add(1)
			require.Equal(t, int32(1), newLeaderCount)

			accumulatedLeaderCount.Add(1)

			// We stay the leader for a while
			time.Sleep(10 * time.Millisecond)

			select {
			case <-sessCtx.Done():
				require.FailNow(t, "session done unexpectedly")
			default:
			}

			newLeaderCount = leaderCount.Sub(1)
			require.Equal(t, int32(0), newLeaderCount)
			resignFn()
		}()
	}

	wg.Wait()
	require.Equal(t, int32(numMockNodesForCampaignTest), accumulatedLeaderCount.Load())
}

func TestSessionDone(t *testing.T) {
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx := context.Background()
	client := newClient()
	election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
		TTL:    5,
		Prefix: "/test-election",
	})
	require.NoError(t, err)

	nodeID := "node-session-done"
	sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*5)
	require.NoError(t, err)

	select {
	case <-sessCtx.Done():
		require.FailNow(t, "sessCtx is not expected to be done by now.")
	default:
	}

	require.NoError(t, election.session.Close())
	<-sessCtx.Done()
	require.Regexp(t, ".*ErrMasterSessionDone.*", sessCtx.Err())

	// It should be okay to call resignFn even after the session is done.
	resignFn()
}

type failLease struct {
	clientv3.Lease
}

func (l *failLease) Grant(_ context.Context, _ int64) (*clientv3.LeaseGrantResponse, error) {
	return nil, errors.New("injected Grant fail")
}

func TestCreateSessionFail(t *testing.T) {
	ctx := context.Background()
	clientStub := clientv3.NewCtxClient(ctx)
	clientStub.Lease = &failLease{}
	_, err := NewEtcdElection(ctx, clientStub, nil, EtcdElectionConfig{
		TTL:    5,
		Prefix: "/test-election",
	})
	require.Error(t, err)
	require.Regexp(t, ".*ErrMasterEtcdCreateSessionFail.*", err)
}

// TODO (zixiong) add tests for failure cases
// We need a mock Etcd client.

func TestLeaderCtxCancelPropagate(t *testing.T) {
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx := context.Background()
	client := newClient()
	election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
		TTL:    5,
		Prefix: "/test-election",
	})
	require.NoError(t, err)

	nodeID := "node-cancel-propagate"
	sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*5)
	require.NoError(t, err)
	_, cancel := context.WithCancel(sessCtx)
	defer cancel()
	resignFn()

	<-sessCtx.Done()
	require.EqualError(t, sessCtx.Err(), "[DFLOW:ErrLeaderCtxCanceled]leader context is canceled")
}

func TestLeaderCtxDoneChCreatedOnce(t *testing.T) {
	newClient, closeFn := setUpTest(t)
	defer closeFn()

	ctx := context.Background()
	client := newClient()
	election, err := NewEtcdElection(ctx, client, nil, EtcdElectionConfig{
		TTL:    5,
		Prefix: "/test-election",
	})
	require.NoError(t, err)

	nodeID := "node-1"
	sessCtx, resignFn, err := election.Campaign(ctx, nodeID, time.Second*10)
	require.NoError(t, err)

	select {
	case <-sessCtx.Done():
		require.FailNow(t, "unreachable")
	default:
	}

	for i := 0; i < 10000; i++ {
		select {
		case <-sessCtx.Done():
		default:
		}
	}

	require.Eventually(t, func() bool {
		return sessCtx.(*leaderCtx).goroutineCount.Load() == 1
	}, 1*time.Second, 10*time.Millisecond)
	resignFn()

	require.Eventually(t, func() bool {
		return sessCtx.(*leaderCtx).goroutineCount.Load() == 0
	}, 1*time.Second, 10*time.Millisecond)
}
