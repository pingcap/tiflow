// Copyright 2020 PingCAP, Inc.
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

package kv

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tidb/pkg/store/mockstore/mockcopr"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/kv/sharedconn"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
)

func TestRequestedStreamRequestedRegions(t *testing.T) {
	stream := newRequestedStream(100)

	require.Nil(t, stream.getState(1, 2))
	require.Nil(t, stream.takeState(1, 2))

	stream.setState(1, 2, &regionFeedState{})
	require.NotNil(t, stream.getState(1, 2))
	require.NotNil(t, stream.takeState(1, 2))
	require.Nil(t, stream.getState(1, 2))
	require.Equal(t, 0, len(stream.requestedRegions.m))

	stream.setState(1, 2, &regionFeedState{})
	require.NotNil(t, stream.getState(1, 2))
	require.NotNil(t, stream.takeState(1, 2))
	require.Nil(t, stream.getState(1, 2))
	require.Equal(t, 0, len(stream.requestedRegions.m))
}

func TestRequestedTable(t *testing.T) {
	s := &SharedClient{resolveLockCh: chann.NewAutoDrainChann[resolveLockTask]()}
	span := tablepb.Span{TableID: 1, StartKey: []byte{'a'}, EndKey: []byte{'z'}}
	table := s.newRequestedTable(SubscriptionID(1), span, 100, nil)
	s.totalSpans.v = make(map[SubscriptionID]*requestedTable)
	s.totalSpans.v[SubscriptionID(1)] = table
	s.pdClock = pdutil.NewClock4Test()

	// Lock a range, and then ResolveLock will trigger a task for it.
	res := table.rangeLock.LockRange(context.Background(), []byte{'b'}, []byte{'c'}, 1, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	res.LockedRange.Initialzied.Store(true)

	s.ResolveLock(SubscriptionID(1), 200)
	select {
	case <-s.resolveLockCh.Out():
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}

	// Lock another range, no task will be triggered before initialized.
	res = table.rangeLock.LockRange(context.Background(), []byte{'c'}, []byte{'d'}, 2, 100)
	require.Equal(t, regionlock.LockRangeStatusSuccess, res.Status)
	state := newRegionFeedState(singleRegionInfo{lockedRange: res.LockedRange, requestedTable: table}, 1)
	select {
	case <-s.resolveLockCh.Out():
		require.True(t, false, "shouldn't get a resolve lock task")
	case <-time.After(100 * time.Millisecond):
	}

	// Task will be triggered after initialized.
	state.setInitialized()
	state.updateResolvedTs(101)
	select {
	case <-s.resolveLockCh.Out():
	case <-time.After(100 * time.Millisecond):
		require.True(t, false, "must get a resolve lock task")
	}

	s.resolveLockCh.CloseAndDrain()
}

func TestConnectToOfflineOrFailedTiKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	events1 := make(chan *cdcpb.ChangeDataEvent, 10)
	events2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataServer(events1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv2 := newMockChangeDataServer(events2)
	server2, addr2 := newMockService(ctx, t, srv2, wg)

	rpcClient, cluster, pdClient, _ := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}

	grpcPool := sharedconn.NewConnAndClientPool(&security.Credential{}, nil)

	regionCache := tikv.NewRegionCache(pdClient)

	pdClock := pdutil.NewClock4Test()

	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	lockResolver := txnutil.NewLockerResolver(kvStorage, model.ChangeFeedID{})

	invalidStore := "localhost:1"
	cluster.AddStore(1, addr1)
	cluster.AddStore(2, addr2)
	cluster.AddStore(3, invalidStore)
	cluster.Bootstrap(11, []uint64{1, 2, 3}, []uint64{4, 5, 6}, 6)

	client := NewSharedClient(
		model.ChangeFeedID{ID: "test"},
		&config.ServerConfig{
			KVClient: &config.KVClientConfig{WorkerConcurrent: 1, GrpcStreamConcurrent: 1},
			Debug:    &config.DebugConfig{Puller: &config.PullerConfig{LogRegionDetails: false}},
		},
		false, pdClient, grpcPool, regionCache, pdClock, lockResolver,
	)

	defer func() {
		cancel()
		client.Close()
		_ = kvStorage.Close()
		regionCache.Close()
		pdClient.Close()
		srv1.wg.Wait()
		srv2.wg.Wait()
		server1.Stop()
		server2.Stop()
		wg.Wait()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	subID := client.AllocSubscriptionID()
	span := tablepb.Span{TableID: 1, StartKey: []byte("a"), EndKey: []byte("b")}
	eventCh := make(chan MultiplexingEvent, 50)
	client.Subscribe(subID, span, 1, eventCh)

	makeTsEvent := func(regionID, ts, requestID uint64) *cdcpb.ChangeDataEvent {
		return &cdcpb.ChangeDataEvent{
			Events: []*cdcpb.Event{
				{
					RegionId:  regionID,
					RequestId: requestID,
					Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: ts},
				},
			},
		}
	}

	checkTsEvent := func(event model.RegionFeedEvent, ts uint64) {
		require.Equal(t, ts, event.Resolved.ResolvedTs)
	}

	events1 <- mockInitializedEvent(11, uint64(subID))
	ts := oracle.GoTimeToTS(pdClock.CurrentTime())
	events1 <- makeTsEvent(11, ts, uint64(subID))
	// After trying to receive something from the invalid store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case event := <-eventCh:
		checkTsEvent(event.RegionFeedEvent, ts)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}

	// Stop server1 and the client needs to handle it.
	server1.Stop()

	events2 <- mockInitializedEvent(11, uint64(subID))
	ts = oracle.GoTimeToTS(pdClock.CurrentTime())
	events2 <- makeTsEvent(11, ts, uint64(subID))
	// After trying to receive something from a failed store,
	// it should auto switch to other stores and fetch events finally.
	select {
	case event := <-eventCh:
		checkTsEvent(event.RegionFeedEvent, ts)
	case <-time.After(5 * time.Second):
		require.True(t, false, "reconnection not succeed in 5 second")
	}
}

type mockChangeDataServer struct {
	ch chan *cdcpb.ChangeDataEvent
	wg sync.WaitGroup
}

func newMockChangeDataServer(ch chan *cdcpb.ChangeDataEvent) *mockChangeDataServer {
	return &mockChangeDataServer{ch: ch}
}

func (m *mockChangeDataServer) EventFeed(s cdcpb.ChangeData_EventFeedServer) error {
	closed := make(chan struct{})
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer close(closed)
		for {
			if _, err := s.Recv(); err != nil {
				return
			}
		}
	}()
	m.wg.Add(1)
	defer m.wg.Done()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-closed:
			return nil
		case <-ticker.C:
		}
		select {
		case event := <-m.ch:
			if err := s.Send(event); err != nil {
				return err
			}
		default:
		}
	}
}

func (m *mockChangeDataServer) EventFeedV2(s cdcpb.ChangeData_EventFeedV2Server) error {
	return m.EventFeed(s)
}
