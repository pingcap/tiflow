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
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto" // nolint:staticcheck
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/stretchr/testify/require"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"
)

func Test(t *testing.T) {
	conf := config.GetDefaultServerConfig()
	config.StoreGlobalServerConfig(conf)
	InitWorkerPool()
	go func() {
		RunWorkerPool(context.Background()) //nolint:errcheck
	}()
}

func TestNewClient(t *testing.T) {
	rpcClient, _, pdClient, err := testutils.NewMockTiKV("", nil)
	require.Nil(t, err)
	defer pdClient.Close()
	defer rpcClient.Close()

	grpcPool := NewGrpcPoolImpl(context.Background(), &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cli := NewCDCClient(
		context.Background(), pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), model.DefaultChangeFeedID(""), 0, "", false)
	require.NotNil(t, cli)
}

func TestAssembleRowEvent(t *testing.T) {
	testCases := []struct {
		regionID uint64
		entry    *cdcpb.Event_Row
		expected model.RegionFeedEvent
		err      string
	}{{
		regionID: 1,
		entry: &cdcpb.Event_Row{
			StartTs:  1,
			CommitTs: 2,
			Key:      []byte("k1"),
			Value:    []byte("v1"),
			OpType:   cdcpb.Event_Row_PUT,
		},
		expected: model.RegionFeedEvent{
			RegionID: 1,
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				StartTs:  1,
				CRTs:     2,
				Key:      []byte("k1"),
				Value:    []byte("v1"),
				RegionID: 1,
			},
		},
	}, {
		regionID: 2,
		entry: &cdcpb.Event_Row{
			StartTs:  1,
			CommitTs: 2,
			Key:      []byte("k2"),
			Value:    []byte("v2"),
			OpType:   cdcpb.Event_Row_DELETE,
		},
		expected: model.RegionFeedEvent{
			RegionID: 2,
			Val: &model.RawKVEntry{
				OpType:   model.OpTypeDelete,
				StartTs:  1,
				CRTs:     2,
				Key:      []byte("k2"),
				Value:    []byte("v2"),
				RegionID: 2,
			},
		},
	}, {
		regionID: 4,
		entry: &cdcpb.Event_Row{
			StartTs:  1,
			CommitTs: 2,
			Key:      []byte("k3"),
			Value:    []byte("v3"),
			OldValue: []byte("ov3"),
			OpType:   cdcpb.Event_Row_PUT,
		},
		expected: model.RegionFeedEvent{
			RegionID: 4,
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				StartTs:  1,
				CRTs:     2,
				Key:      []byte("k3"),
				Value:    []byte("v3"),
				OldValue: []byte("ov3"),
				RegionID: 4,
			},
		},
	}, {
		regionID: 2,
		entry: &cdcpb.Event_Row{
			StartTs:  1,
			CommitTs: 2,
			Key:      []byte("k2"),
			Value:    []byte("v2"),
			OpType:   cdcpb.Event_Row_UNKNOWN,
		},
		err: "[CDC:ErrUnknownKVEventType]unknown kv optype: UNKNOWN, entry: start_ts:1 " +
			"commit_ts:2 key:\"k2\" value:\"v2\" ",
	}}

	for _, tc := range testCases {
		event, err := assembleRowEvent(tc.regionID, tc.entry)
		require.Equal(t, tc.expected, event)
		if err != nil {
			require.Equal(t, tc.err, err.Error())
		}
	}
}

type mockChangeDataService struct {
	t           *testing.T
	ch          chan *cdcpb.ChangeDataEvent
	recvLoop    func(server cdcpb.ChangeData_EventFeedServer)
	exitNotify  sync.Map
	eventFeedID uint64
}

func newMockChangeDataService(t *testing.T, ch chan *cdcpb.ChangeDataEvent) *mockChangeDataService {
	s := &mockChangeDataService{
		t:  t,
		ch: ch,
	}
	return s
}

type notifyCh struct {
	notify   chan struct{}
	callback chan struct{}
}

func (s *mockChangeDataService) registerExitNotify(id uint64, ch *notifyCh) {
	s.exitNotify.Store(id, ch)
}

func (s *mockChangeDataService) notifyExit(id uint64) chan struct{} {
	if ch, ok := s.exitNotify.Load(id); ok {
		nch := ch.(*notifyCh)
		nch.notify <- struct{}{}
		return nch.callback
	}
	return nil
}

func (s *mockChangeDataService) EventFeed(server cdcpb.ChangeData_EventFeedServer) error {
	if s.recvLoop != nil {
		go func() {
			s.recvLoop(server)
		}()
	}
	notify := &notifyCh{
		notify:   make(chan struct{}),
		callback: make(chan struct{}, 1), // callback is not always retrieved
	}
	s.registerExitNotify(atomic.LoadUint64(&s.eventFeedID), notify)
	atomic.AddUint64(&s.eventFeedID, 1)
loop:
	for {
		select {
		case e := <-s.ch:
			if e == nil {
				break loop
			}
			_ = server.Send(e)
		case <-notify.notify:
			break loop
		}
	}
	notify.callback <- struct{}{}
	return nil
}

func newMockService(
	ctx context.Context,
	t *testing.T,
	srv cdcpb.ChangeDataServer,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	return newMockServiceSpecificAddr(ctx, t, srv, "127.0.0.1:0", wg)
}

func newMockServiceSpecificAddr(
	ctx context.Context,
	t *testing.T,
	srv cdcpb.ChangeDataServer,
	listenAddr string,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	lc := &net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", listenAddr)
	require.Nil(t, err)
	addr = lis.Addr().String()
	kaep := keepalive.EnforcementPolicy{
		// force minimum ping interval
		MinTime:             3 * time.Second,
		PermitWithoutStream: true,
	}
	// Some tests rely on connect timeout and ping test, so we use a smaller num
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     10 * time.Second, // If a client is idle for 20 seconds, send a GOAWAY
		MaxConnectionAge:      10 * time.Second, // If any connection is alive for more than 20 seconds, send a GOAWAY
		MaxConnectionAgeGrace: 5 * time.Second,  // Allow 5 seconds for pending RPCs to complete before forcibly closing connections
		Time:                  3 * time.Second,  // Ping the client if it is idle for 5 seconds to ensure the connection is still active
		Timeout:               1 * time.Second,  // Wait 1 second for the ping ack before assuming the connection is dead
	}
	grpcServer = grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	// grpcServer is the server, srv is the service
	cdcpb.RegisterChangeDataServer(grpcServer, srv)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := grpcServer.Serve(lis)
		require.Nil(t, err)
	}()
	return
}

// waitRequestID waits request ID larger than the given allocated ID
func waitRequestID(t *testing.T, allocatedID uint64) {
	err := retry.Do(context.Background(), func() error {
		if currentRequestID() > allocatedID {
			return nil
		}
		return errors.Errorf("request id %d is not larger than %d", currentRequestID(), allocatedID)
	}, retry.WithBackoffBaseDelay(10), retry.WithMaxTries(20))

	require.Nil(t, err)
}

func TestConnectOfflineTiKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv := newMockChangeDataService(t, ch2)
	server2, addr := newMockService(ctx, t, srv, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	invalidStore := "localhost:1"
	cluster.AddStore(1, invalidStore)
	cluster.AddStore(2, addr)
	// {1,2} is the storeID, {4,5} is the peerID, means peer4 is in the store1
	cluster.Bootstrap(3, []uint64{1, 2}, []uint64{4, 5}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		context.Background(), pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	// Take care of the eventCh, it's used to output resolvedTs event or kv event
	// It will stuck the normal routine
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			1, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// new session, request to store 1, request to store 2
	waitRequestID(t, baseAllocatedID+2)

	makeEvent := func(ts uint64) *cdcpb.ChangeDataEvent {
		return &cdcpb.ChangeDataEvent{
			Events: []*cdcpb.Event{
				{
					RegionId:  3,
					RequestId: currentRequestID(),
					Event: &cdcpb.Event_ResolvedTs{
						ResolvedTs: ts,
					},
				},
			},
		}
	}

	checkEvent := func(event model.RegionFeedEvent, ts uint64) {
		require.Equal(t, ts, event.Resolved.ResolvedTs)
	}

	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized

	cluster.ChangeLeader(3, 5)

	ts, err := kvStorage.CurrentTimestamp(oracle.GlobalTxnScope)
	ver := kv.NewVersion(ts)
	require.Nil(t, err)
	ch2 <- makeEvent(ver.Ver)
	var event model.RegionFeedEvent
	// consume the first resolved ts event, which is sent before region starts
	<-eventCh
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		require.FailNow(t, "reconnection not succeed in 1 second")
	}
	checkEvent(event, 1)

	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		require.FailNow(t, "reconnection not succeed in 1 second")
	}
	checkEvent(event, ver.Ver)

	// check gRPC connection active counter is updated correctly
	bucket, ok := grpcPool.bucketConns[invalidStore]
	require.True(t, ok)
	empty := bucket.recycle()
	require.True(t, empty)

	cancel()
}

// [NOTICE]: I concern this ut may cost too much time when resource limit
func TestRecvLargeMessageSize(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv := newMockChangeDataService(t, ch2)
	server2, addr := newMockService(ctx, t, srv, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	defer pdClient.Close() //nolint:errcheck
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(2, addr)
	cluster.Bootstrap(3, []uint64{2}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			1, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// new session, new request
	waitRequestID(t, baseAllocatedID+1)

	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized

	var event model.RegionFeedEvent
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		require.FailNow(t, "recving message takes too long")
	}
	require.NotNil(t, event)

	largeValSize := 128*1024*1024 + 1 // 128MB + 1
	largeMsg := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("a"),
						Value:    make([]byte, largeValSize),
						CommitTs: 2, // ResolvedTs = 1
					}},
				},
			},
		},
	}}
	ch2 <- largeMsg
	select {
	case event = <-eventCh:
	case <-time.After(30 * time.Second): // Send 128MB object may costs lots of time.
		require.FailNow(t, "receiving message takes too long")
	}
	require.Equal(t, largeValSize, len(event.Val.Value))
	cancel()
}

func TestHandleError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(t, ch2)
	server2, addr2 := newMockService(ctx, t, srv2, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	region3 := uint64(3)
	region4 := uint64(4)
	region5 := uint64(5)
	cluster.AddStore(1, addr1)
	cluster.AddStore(2, addr2)
	cluster.Bootstrap(region3, []uint64{1, 2}, []uint64{4, 5}, 4)
	// split two regions with leader on different TiKV nodes to avoid region
	// worker exits because of empty maintained region
	cluster.SplitRaw(region3, region4, []byte("b"), []uint64{6, 7}, 6)
	cluster.SplitRaw(region4, region5, []byte("c"), []uint64{8, 9}, 9)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("d")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)

	var event model.RegionFeedEvent
	notLeader := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					NotLeader: &errorpb.NotLeader{
						RegionId: 3,
						Leader: &metapb.Peer{
							StoreId: 2,
						},
					},
				},
			},
		},
	}}
	ch1 <- notLeader
	cluster.ChangeLeader(3, 5)

	// wait request id allocated with:
	// new session, no leader request, epoch not match request
	waitRequestID(t, baseAllocatedID+2)
	epochNotMatch := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					EpochNotMatch: &errorpb.EpochNotMatch{},
				},
			},
		},
	}}
	ch2 <- epochNotMatch

	waitRequestID(t, baseAllocatedID+3)
	regionNotFound := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					RegionNotFound: &errorpb.RegionNotFound{},
				},
			},
		},
	}}
	ch2 <- regionNotFound

	waitRequestID(t, baseAllocatedID+4)
	unknownErr := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{},
			},
		},
	}}
	ch2 <- unknownErr

	// `singleEventFeed` always emits a resolved event with ResolvedTs == StartTs
	// when it starts.
consumePreResolvedTs:
	for {
		select {
		case event = <-eventCh:
			require.NotNil(t, event.Resolved)
			require.Equal(t, uint64(100), event.Resolved.ResolvedTs)
		case <-time.After(time.Second):
			break consumePreResolvedTs
		}
	}

	// wait request id allocated with:
	// new session, no leader request, epoch not match request,
	// region not found request, unknown error request, normal request
	waitRequestID(t, baseAllocatedID+5)
	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized

	makeEvent := func(ts uint64) *cdcpb.ChangeDataEvent {
		return &cdcpb.ChangeDataEvent{
			Events: []*cdcpb.Event{
				{
					RegionId:  3,
					RequestId: currentRequestID(),
					Event: &cdcpb.Event_ResolvedTs{
						ResolvedTs: ts,
					},
				},
			},
		}
	}
	// fallback resolved ts event from TiKV
	ch2 <- makeEvent(90)
	// normal resolved ts event
	ch2 <- makeEvent(120)
	select {
	case event = <-eventCh:
	case <-time.After(3 * time.Second):
		require.FailNow(t, "reconnection not succeed in 3 seconds")
	}
	require.NotNil(t, event.Resolved)
	require.Equal(t, uint64(120), event.Resolved.ResolvedTs)

	cancel()
}

// TestCompatibilityWithSameConn tests kv client returns an error when TiKV returns
// the Compatibility error. This error only happens when the same connection to
// TiKV have different versions.
func TestCompatibilityWithSameConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.True(t, cerror.ErrVersionIncompatible.Equal(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	incompatibility := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					Compatibility: &cdcpb.Compatibility{
						RequiredVersion: "v4.0.7",
					},
				},
			},
		},
	}}
	ch1 <- incompatibility
	wg2.Wait()
	cancel()
}

// TestClusterIDMismatch tests kv client returns an error when TiKV returns
// the cluster ID mismatch error.
func TestClusterIDMismatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	changeDataCh := make(chan *cdcpb.ChangeDataEvent, 10)
	changeDataService := newMockChangeDataService(t, changeDataCh)
	mockService, addr := newMockService(ctx, t, changeDataService, wg)
	defer func() {
		close(changeDataCh)
		mockService.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)

	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.True(t, cerror.ErrClusterIDMismatch.Equal(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	clusterIDMismatchEvent := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					ClusterIdMismatch: &cdcpb.ClusterIDMismatch{
						Current: 0,
						Request: 1,
					},
				},
			},
		},
	}}

	changeDataCh <- clusterIDMismatchEvent

	wg2.Wait()
	cancel()
}

func testHandleFeedEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)

	eventsBeforeInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		// before initialized, prewrite and commit could be in any sequence,
		// simulate commit comes before prewrite
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aaa"),
						StartTs:  112,
						CommitTs: 122,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("aaa"),
						Value:   []byte("commit-prewrite-sequence-before-init"),
						StartTs: 112,
					}},
				},
			},
		},

		// prewrite and commit in the normal sequence
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("aaa"),
						Value:   []byte("prewrite-commit-sequence-before-init"),
						StartTs: 110, // ResolvedTs = 100
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aaa"),
						StartTs:  110, // ResolvedTs = 100
						CommitTs: 120,
					}},
				},
			},
		},

		// commit event before initializtion without prewrite matched will be ignored
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aa"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aaaa"),
						Value:    []byte("committed put event before init"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_DELETE,
						Key:      []byte("aaaa"),
						Value:    []byte("committed delete event before init"),
						StartTs:  108,
						CommitTs: 118,
					}},
				},
			},
		},
	}}
	initialized := mockInitializedEvent(3 /*regionID */, currentRequestID())
	eventsAfterInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("a-rollback-event"),
						StartTs: 128,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_ROLLBACK,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("a-rollback-event"),
						StartTs:  128,
						CommitTs: 129,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_DELETE,
						Key:     []byte("a-delete-event"),
						StartTs: 130,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_DELETE,
						Key:      []byte("a-delete-event"),
						StartTs:  130,
						CommitTs: 140,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("a-normal-put"),
						Value:   []byte("normal put event"),
						StartTs: 135,
					}},
				},
			},
		},
		// simulate TiKV sends txn heartbeat, which is a prewrite event with empty value
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("a-normal-put"),
						StartTs: 135,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("a-normal-put"),
						StartTs:  135,
						CommitTs: 145,
					}},
				},
			},
		},
	}}
	eventResolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 135},
		},
	}}
	// batch resolved ts
	eventResolvedBatch := &cdcpb.ChangeDataEvent{
		ResolvedTs: &cdcpb.ResolvedTs{
			Regions: []uint64{3},
			Ts:      145,
		},
	}
	multiSize := 100
	regions := make([]uint64, multiSize)
	for i := range regions {
		regions[i] = 3
	}
	multipleResolved := &cdcpb.ChangeDataEvent{
		ResolvedTs: &cdcpb.ResolvedTs{
			Regions: regions,
			Ts:      160,
		},
	}

	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 100,
			},
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaa"),
				Value:    []byte("prewrite-commit-sequence-before-init"),
				StartTs:  110,
				CRTs:     120,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaaa"),
				Value:    []byte("committed put event before init"),
				StartTs:  105,
				CRTs:     115,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypeDelete,
				Key:      []byte("aaaa"),
				Value:    []byte("committed delete event before init"),
				StartTs:  108,
				CRTs:     118,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaa"),
				Value:    []byte("commit-prewrite-sequence-before-init"),
				StartTs:  112,
				CRTs:     122,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypeDelete,
				Key:      []byte("a-delete-event"),
				StartTs:  130,
				CRTs:     140,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("a-normal-put"),
				Value:    []byte("normal put event"),
				StartTs:  135,
				CRTs:     145,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 135,
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 145,
			},
		},
	}
	multipleExpected := model.RegionFeedEvent{
		Resolved: &model.ResolvedSpans{
			Spans:      make([]model.RegionComparableSpan, multiSize),
			ResolvedTs: 160,
		},
	}
	for i := range multipleExpected.Resolved.Spans {
		multipleExpected.Resolved.Spans[i] = model.RegionComparableSpan{
			Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			Region: 3,
		}
	}

	ch1 <- eventsBeforeInit
	ch1 <- initialized
	ch1 <- eventsAfterInit
	ch1 <- eventResolved
	ch1 <- eventResolvedBatch

	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			require.Equal(t, expectedEv, event)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expectedEv))
		}
	}

	ch1 <- multipleResolved
	select {
	case event := <-eventCh:
		require.Equal(t, multipleExpected, event)
	case <-time.After(time.Second):
		require.Fail(t, fmt.Sprintf("expected event %v not received", multipleExpected))
	}
	cancel()
}

func TestHandleFeedEvent(t *testing.T) {
	testHandleFeedEvent(t)
}

func TestHandleFeedEventWithWorkerPool(t *testing.T) {
	hwm := regionWorkerHighWatermark
	lwm := regionWorkerLowWatermark
	regionWorkerHighWatermark = 8
	regionWorkerLowWatermark = 2
	defer func() {
		regionWorkerHighWatermark = hwm
		regionWorkerLowWatermark = lwm
	}()
	testHandleFeedEvent(t)
}

// TestStreamSendWithError mainly tests the scenario that the `Send` call of a gPRC
// stream of kv client meets error, and kv client can clean up the broken stream,
// establish a new one and recover the normal event feed processing.
func TestStreamSendWithError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	defer wg.Wait()
	defer cancel()

	var server1StopFlag int32
	server1Stopped := make(chan struct{})
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		defer func() {
			// TiCDC may reestalish stream again, so we need to add failpoint-inject
			// and atomic int here
			if atomic.LoadInt32(&server1StopFlag) == int32(1) {
				return
			}
			atomic.StoreInt32(&server1StopFlag, 1)
			close(ch1)
			server1.Stop()
			server1Stopped <- struct{}{}
		}()
		// Only receives the first request to simulate a following kv client
		// stream.Send error.
		_, err := server.Recv()
		if err != nil {
			log.Error("mock server error", zap.Error(err))
		}
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID3 := uint64(3)
	regionID4 := uint64(4)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID3, []uint64{1}, []uint64{4}, 4)
	cluster.SplitRaw(regionID3, regionID4, []byte("b"), []uint64{5}, 5)

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockerResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
			100, lockerResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	var requestIds sync.Map
	<-server1Stopped
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(t, ch2)
	srv2.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			requestIds.Store(req.RegionId, req.RequestId)
		}
	}
	// Reuse the same listen address as server 1
	server2, _ := newMockServiceSpecificAddr(ctx, t, srv2, addr1, wg)
	defer func() {
		close(ch2)
		server2.Stop()
	}()

	// The expected request ids are agnostic because the kv client could retry
	// for more than one time, so we wait until the newly started server receives
	// requests for both two regions.
	err = retry.Do(context.Background(), func() error {
		_, ok1 := requestIds.Load(regionID3)
		_, ok2 := requestIds.Load(regionID4)
		if ok1 && ok2 {
			return nil
		}
		return errors.New("waiting for kv client requests received by server")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)
	reqID1, _ := requestIds.Load(regionID3)
	reqID2, _ := requestIds.Load(regionID4)
	initialized1 := mockInitializedEvent(regionID3, reqID1.(uint64))
	initialized2 := mockInitializedEvent(regionID4, reqID2.(uint64))
	ch2 <- initialized1
	ch2 <- initialized2

	// the event sequence is undeterministic
	initRegions := make(map[uint64]struct{})
	for i := 0; i < 2; i++ {
		select {
		case event := <-eventCh:
			require.NotNil(t, event.Resolved)
			require.Equal(t, 1, len(event.Resolved.Spans))
			require.NotNil(t, 0, event.RegionID)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected events are not receive, received: %v", initRegions))
		}
	}

	// a hack way to check the goroutine count of region worker is 1
	buf := make([]byte, 1<<20)
	stackLen := runtime.Stack(buf, true)
	stack := string(buf[:stackLen])
	require.Equal(t, 1, strings.Count(stack, "resolveLock"))
	require.Equal(t, 1, strings.Count(stack, "collectWorkpoolError"))
}

func testStreamRecvWithError(t *testing.T, failpointStr string) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError", failpointStr)
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError")
	}()
	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	initialized1 := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized1
	err = retry.Do(context.Background(), func() error {
		if len(ch1) == 0 {
			return nil
		}
		return errors.New("message is not sent")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)

	// another stream will be established, so we notify and wait the first
	// EventFeed loop exits.
	callback := srv1.notifyExit(0)
	select {
	case <-callback:
	case <-time.After(time.Second * 3):
		require.Fail(t, "event feed loop can't exit")
	}

	// wait request id allocated with: new session, new request*2
	waitRequestID(t, baseAllocatedID+2)
	initialized2 := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized2

	resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 120},
		},
	}}
	ch1 <- resolved
	ch1 <- resolved

	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 120,
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 120,
			},
		},
	}

	events := make([]model.RegionFeedEvent, 0, 2)
eventLoop:
	for {
		select {
		case ev := <-eventCh:
			if ev.Resolved.ResolvedTs != uint64(100) {
				events = append(events, ev)
			}
		case <-time.After(time.Second):
			break eventLoop
		}
	}
	require.Equal(t, expected, events)
	cancel()
}

// TestStreamRecvWithErrorAndResolvedGoBack mainly tests the scenario that the `Recv` call of a gPRC
// stream in kv client meets error, and kv client reconnection with tikv with the current tso
func TestStreamRecvWithErrorAndResolvedGoBack(t *testing.T) {
	if !util.FailpointBuild {
		t.Skip("skip when this is not a failpoint build")
	}
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	var requestID uint64
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			atomic.StoreUint64(&requestID, req.RequestId)
		}
	}
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(eventCh)
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	err = retry.Do(context.Background(), func() error {
		if atomic.LoadUint64(&requestID) == currentRequestID() {
			return nil
		}
		return errors.Errorf("request is not received, requestID: %d, expected: %d",
			atomic.LoadUint64(&requestID), currentRequestID())
	}, retry.WithBackoffBaseDelay(50), retry.WithMaxTries(10))

	require.Nil(t, err)
	initialized1 := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized1
	err = retry.Do(context.Background(), func() error {
		if len(ch1) == 0 {
			return nil
		}
		return errors.New("message is not sent")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)

	resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 120},
		},
	}}
	ch1 <- resolved
	err = retry.Do(context.Background(), func() error {
		if len(ch1) == 0 {
			return nil
		}
		return errors.New("message is not sent")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError", "1*return(\"\")")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError")
	}()
	ch1 <- resolved

	// another stream will be established, so we notify and wait the first
	// EventFeed loop exits.
	callback := srv1.notifyExit(0)
	select {
	case <-callback:
	case <-time.After(time.Second * 3):
		require.Fail(t, "event feed loop can't exit")
	}

	// wait request id allocated with: new session, new request*2
	waitRequestID(t, baseAllocatedID+2)
	err = retry.Do(context.Background(), func() error {
		if atomic.LoadUint64(&requestID) == currentRequestID() {
			return nil
		}
		return errors.Errorf("request is not received, requestID: %d, expected: %d",
			atomic.LoadUint64(&requestID), currentRequestID())
	}, retry.WithBackoffBaseDelay(50), retry.WithMaxTries(10))

	require.Nil(t, err)
	initialized2 := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized2
	err = retry.Do(context.Background(), func() error {
		if len(ch1) == 0 {
			return nil
		}
		return errors.New("message is not sent")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)

	resolved = &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 130},
		},
	}}
	ch1 <- resolved

	received := make([]model.RegionFeedEvent, 0, 4)
	defer cancel()
ReceiveLoop:
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				break ReceiveLoop
			}
			received = append(received, event)
			if event.Resolved.ResolvedTs == 130 {
				break ReceiveLoop
			}
		case <-time.After(time.Second):
			require.Fail(t, "event received timeout")
		}
	}
	var lastResolvedTs uint64
	for _, e := range received {
		if lastResolvedTs > e.Resolved.ResolvedTs {
			require.Fail(t, fmt.Sprintf("the resolvedTs is back off %#v", resolved))
		}
	}
}

// TestStreamRecvWithErrorNormal mainly tests the scenario that the `Recv` call
// of a gPRC stream in kv client meets a **logical related** error, and kv client
// logs the error and re-establish new request.
func TestStreamRecvWithErrorNormal(t *testing.T) {
	testStreamRecvWithError(t, "1*return(\"injected stream recv error\")")
}

// TestStreamRecvWithErrorIOEOF mainly tests the scenario that the `Recv` call
// of a gPRC stream in kv client meets error io.EOF, and kv client logs the error
// and re-establish new request
func TestStreamRecvWithErrorIOEOF(t *testing.T) {
	testStreamRecvWithError(t, "1*return(\"EOF\")")
	testStreamRecvWithError(t, "1*return(\"EOF\")")
}

// TestIncompatibleTiKV tests TiCDC new request to TiKV meets `ErrVersionIncompatible`
// error (in fact this error is raised before EventFeed API is really called),
// TiCDC will wait 20s and then retry. This is a common scenario when rolling
// upgrade a cluster and the new version is not compatible with the old version
// (upgrade TiCDC before TiKV, since upgrade TiKV often takes much longer).
func TestIncompatibleTiKV(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// the minimum valid TiKV version is "4.0.0-rc.1"
	incompatibilityVers := []string{"v2.1.10", "v3.0.10", "v3.1.0", "v4.0.0-rc"}
	var genLock sync.Mutex
	nextVer := -1
	call := int32(0)
	versionGenCallBoundary := int32(8)
	gen := func() string {
		genLock.Lock()
		defer genLock.Unlock()
		atomic.AddInt32(&call, 1)
		if atomic.LoadInt32(&call) < versionGenCallBoundary {
			nextVer = (nextVer + 1) % len(incompatibilityVers)
			return incompatibilityVers[nextVer]
		}
		return defaultVersionGen()
	}

	var requestIds sync.Map
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			requestIds.Store(req.RegionId, req.RequestId)
		}
	}
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: gen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientDelayWhenIncompatible", "return(true)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientDelayWhenIncompatible")
	}()

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	// NOTICE: eventCh may block the main logic of EventFeed
	eventCh := make(chan model.RegionFeedEvent, 128)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	err = retry.Do(context.Background(), func() error {
		if atomic.LoadInt32(&call) >= versionGenCallBoundary {
			return nil
		}
		return errors.Errorf("version generator is not updated in time, call time %d", atomic.LoadInt32(&call))
	}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))

	require.Nil(t, err)
	err = retry.Do(context.Background(), func() error {
		_, ok := requestIds.Load(regionID)
		if ok {
			return nil
		}
		return errors.New("waiting for kv client requests received by server")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)
	reqID, _ := requestIds.Load(regionID)
	initialized := mockInitializedEvent(regionID, reqID.(uint64))
	ch1 <- initialized
	select {
	case event := <-eventCh:
		require.Equal(t, 1, len(event.Resolved.Spans))
	case <-time.After(time.Second):
		require.Fail(t, "expected events are not receive")
	}

	cancel()
}

// TestPendingRegionError tests kv client should return an error when receiving
// a new subscription (the first event of specific region) but the corresponding
// region is not found in pending regions.
func TestNoPendingRegionError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	noPendingRegionEvent := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID() + 1, // an invalid request id
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 100},
		},
	}}
	ch1 <- noPendingRegionEvent

	initialized := mockInitializedEvent(3, currentRequestID())
	ch1 <- initialized
	ev := <-eventCh
	require.NotNil(t, ev.Resolved)
	require.Equal(t, uint64(100), ev.Resolved.ResolvedTs)

	resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 200},
		},
	}}
	ch1 <- resolved
	ev = <-eventCh
	require.NotNil(t, ev.Resolved)
	require.Equal(t, uint64(200), ev.Resolved.ResolvedTs)

	cancel()
}

// TestDropStaleRequest tests kv client should drop an event if its request id is outdated.
func TestDropStaleRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)

	initialized := mockInitializedEvent(regionID, currentRequestID())
	eventsAfterInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 120},
		},
		// This event will be dropped
		{
			RegionId:  regionID,
			RequestId: currentRequestID() - 1,
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 125},
		},
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 130},
		},
	}}
	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 100,
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 120,
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 130,
			},
		},
	}

	ch1 <- initialized
	ch1 <- eventsAfterInit

	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			require.Equal(t, expectedEv, event)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expectedEv))
		}
	}
	cancel()
}

// TestResolveLock tests the resolve lock logic in kv client
func TestResolveLock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientResolveLockInterval", "return(3)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientResolveLockInterval")
	}()
	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	initialized := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized
	physical, logical, err := pdClient.GetTS(ctx)
	require.Nil(t, err)
	tso := oracle.ComposeTS(physical, logical)
	resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID,
			RequestId: currentRequestID(),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: tso},
		},
	}}
	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 100,
			},
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: tso,
			},
		},
	}
	ch1 <- resolved
	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			require.Equal(t, expectedEv, event)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expectedEv))
		}
	}

	// sleep 10s to simulate no resolved event longer than ResolveLockInterval
	// resolve lock check ticker is 5s.
	time.Sleep(10 * time.Second)

	cancel()
}

func testEventCommitTsFallback(t *testing.T, events []*cdcpb.ChangeDataEvent) {
	InitWorkerPool()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	logPanic = log.Error
	defer func() {
		logPanic = log.Panic
	}()

	// This inject will make regionWorker exit directly and trigger execution line cancel when meet error
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientErrUnreachable", "return(true)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientErrUnreachable")
	}()
	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	var clientWg sync.WaitGroup
	clientWg.Add(1)
	go func() {
		defer clientWg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, errUnreachable, err)
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	for _, event := range events {
		for _, ev := range event.Events {
			ev.RequestId = currentRequestID()
		}
		ch1 <- event
	}
	clientWg.Wait()
	cancel()
}

// TestCommittedFallback tests kv client should panic when receiving a fallback committed event
func TestCommittedFallback(t *testing.T) {
	events := []*cdcpb.ChangeDataEvent{
		{Events: []*cdcpb.Event{
			{
				RegionId:  3,
				RequestId: currentRequestID(),
				Event: &cdcpb.Event_Entries_{
					Entries: &cdcpb.Event_Entries{
						Entries: []*cdcpb.Event_Row{{
							Type:     cdcpb.Event_COMMITTED,
							OpType:   cdcpb.Event_Row_PUT,
							Key:      []byte("a"),
							Value:    []byte("committed with commit ts before resolved ts"),
							StartTs:  92,
							CommitTs: 98,
						}},
					},
				},
			},
		}},
	}
	testEventCommitTsFallback(t, events)
}

// TestDeuplicateRequest tests kv client should panic when meeting a duplicate error
func TestDuplicateRequest(t *testing.T) {
	events := []*cdcpb.ChangeDataEvent{
		{Events: []*cdcpb.Event{
			{
				RegionId:  3,
				RequestId: currentRequestID(),
				Event: &cdcpb.Event_Error{
					Error: &cdcpb.Error{
						DuplicateRequest: &cdcpb.DuplicateRequest{RegionId: 3},
					},
				},
			},
		}},
	}
	testEventCommitTsFallback(t, events)
}

// testEventAfterFeedStop tests kv client can drop events sent after region feed is stopped
// TODO: testEventAfterFeedStop is not stable, re-enable it after it is stable
// nolint
func testEventAfterFeedStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	var server1StopFlag int32
	server1Stopped := make(chan struct{})
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		defer func() {
			// TiCDC may reestalish stream again, so we need to add failpoint-inject
			// and atomic int here
			if atomic.LoadInt32(&server1StopFlag) == int32(1) {
				return
			}
			atomic.StoreInt32(&server1StopFlag, 1)
			close(ch1)
			server1.Stop()
			server1Stopped <- struct{}{}
		}()
		for {
			_, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				break
			}
		}
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	// add 2s delay to simulate event feed processor has been marked stopped, but
	// before event feed processor is reconstruct, some duplicated events are
	// sent to event feed processor.
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientSingleFeedProcessDelay", "1*sleep(2000)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientSingleFeedProcessDelay")
	}()
	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	// an error event will mark the corresponding region feed as stopped
	epochNotMatch := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					EpochNotMatch: &errorpb.EpochNotMatch{},
				},
			},
		},
	}}
	ch1 <- epochNotMatch

	// sleep to ensure event feed processor has been marked as stopped
	time.Sleep(1 * time.Second)
	committed := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("a"),
						Value:    []byte("committed put event before init"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
	}}
	initialized := mockInitializedEvent(regionID, currentRequestID())
	resolved := &cdcpb.ChangeDataEvent{
		ResolvedTs: &cdcpb.ResolvedTs{
			Regions: []uint64{3},
			Ts:      120,
		},
	}
	// clone to avoid data race, these are exactly the same events
	committedClone := proto.Clone(committed).(*cdcpb.ChangeDataEvent)
	initializedClone := proto.Clone(initialized).(*cdcpb.ChangeDataEvent)
	resolvedClone := proto.Clone(resolved).(*cdcpb.ChangeDataEvent)
	ch1 <- committed
	ch1 <- initialized
	ch1 <- resolved

	<-server1Stopped

	var requestID uint64
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(t, ch2)
	// Reuse the same listen addresss as server 1 to simulate TiKV handles the
	// gRPC stream terminate and reconnect.
	server2, _ := newMockServiceSpecificAddr(ctx, t, srv2, addr1, wg)
	srv2.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			atomic.StoreUint64(&requestID, req.RequestId)
		}
	}
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	err = retry.Do(context.Background(), func() error {
		if atomic.LoadUint64(&requestID) > 0 {
			return nil
		}
		return errors.New("waiting for kv client requests received by server")
	}, retry.WithMaxTries(10), retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000))

	log.Info("retry check request id", zap.Error(err))
	require.Nil(t, err)

	// wait request id allocated with: new session, 2 * new request
	committedClone.Events[0].RequestId = currentRequestID()
	initializedClone.Events[0].RequestId = currentRequestID()
	ch2 <- committedClone
	ch2 <- initializedClone
	ch2 <- resolvedClone

	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 100,
			},
			RegionID: regionID,
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 100,
			},
			RegionID: regionID,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("a"),
				Value:    []byte("committed put event before init"),
				StartTs:  105,
				CRTs:     115,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: regionID,
				}}, ResolvedTs: 120,
			},
			RegionID: regionID,
		},
	}
	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			require.Equal(t, expectedEv, event)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expectedEv))
		}
	}
	cancel()
}

func TestOutOfRegionRangeEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)

	eventsBeforeInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		// will be filtered out
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("ccc"),
						Value:    []byte("key out of region range"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMITTED,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aaaa"),
						Value:    []byte("committed put event before init"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
	}}
	initialized := mockInitializedEvent(3 /*regionID */, currentRequestID())
	eventsAfterInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		// will be filtered out
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("cccd"),
						Value:   []byte("key out of region range"),
						StartTs: 135,
					}},
				},
			},
		},
		// will be filtered out
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("cccd"),
						StartTs:  135,
						CommitTs: 145,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("a-normal-put"),
						Value:   []byte("normal put event"),
						StartTs: 135,
					}},
				},
			},
		},
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("a-normal-put"),
						StartTs:  135,
						CommitTs: 145,
					}},
				},
			},
		},
	}}
	// batch resolved ts
	eventResolvedBatch := &cdcpb.ChangeDataEvent{
		ResolvedTs: &cdcpb.ResolvedTs{
			Regions: []uint64{3},
			Ts:      145,
		},
	}

	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 100,
			},
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaaa"),
				Value:    []byte("committed put event before init"),
				StartTs:  105,
				CRTs:     115,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("a-normal-put"),
				Value:    []byte("normal put event"),
				StartTs:  135,
				CRTs:     145,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 145,
			},
		},
	}

	ch1 <- eventsBeforeInit
	ch1 <- initialized
	ch1 <- eventsAfterInit
	ch1 <- eventResolvedBatch

	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			require.Equal(t, expectedEv, event)
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expectedEv))
		}
	}

	cancel()
}

// TestResolveLockNoCandidate tests the resolved ts manager can work normally
// when no region exceeds resolve lock interval, that is what candidate means.
func TestResolveLockNoCandidate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	storeID := uint64(1)
	peerID := uint64(4)
	cluster.AddStore(storeID, addr1)
	cluster.Bootstrap(regionID, []uint64{storeID}, []uint64{peerID}, peerID)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	initialized := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		for i := 0; i < 6; i++ {
			physical, logical, err := pdClient.GetTS(ctx)
			require.Nil(t, err)
			tso := oracle.ComposeTS(physical, logical)
			resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
				{
					RegionId:  regionID,
					RequestId: currentRequestID(),
					Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: tso},
				},
			}}
			ch1 <- resolved
			select {
			case event := <-eventCh:
				require.NotNil(t, event.Resolved)
			case <-time.After(time.Second):
				require.Fail(t, "resolved event not received")
			}
			// will sleep 6s totally, to ensure resolve lock fired once
			time.Sleep(time.Second)
		}
	}()

	wg2.Wait()
	cancel()
}

// TestFailRegionReentrant tests one region could be failover multiple times,
// kv client must avoid duplicated `onRegionFail` call for the same region.
// In this test
//  1. An `unknownErr` is sent to kv client first to trigger `handleSingleRegionError` in region worker.
//  2. We delay the kv client to re-create a new region request by 500ms via failpoint.
//  3. Before new region request is fired, simulate kv client `stream.Recv` returns an error, the stream
//     handler will signal region worker to exit, which will evict all active region states then.
func TestFailRegionReentrant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientRegionReentrantError", "1*return(\"ok\")->1*return(\"error\")")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientRegionReentrantErrorDelay", "sleep(500)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientRegionReentrantError")
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientRegionReentrantErrorDelay")
	}()
	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	unknownErr := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{},
			},
		},
	}}
	ch1 <- unknownErr
	// use a fake event to trigger one more stream.Recv
	initialized := mockInitializedEvent(regionID, currentRequestID())
	ch1 <- initialized
	// since re-establish new region request is delayed by `kvClientRegionReentrantErrorDelay`
	// there will be reentrant region failover, the kv client should not panic.
	time.Sleep(time.Second)
	cancel()
}

// TestClientV1UnlockRangeReentrant tests clientV1 can handle region reconnection
// with unstable TiKV store correctly. The test workflow is as follows:
//  1. kv client establishes two regions request, naming region-1, region-2, they
//     belong to the same TiKV store.
//  2. The region-1 is firstly established, yet region-2 has some delay after its
//     region state is inserted into `pendingRegions`
//  3. At this time the TiKV store crashes and `stream.Recv` returns error. In the
//     defer function of `receiveFromStream`, all pending regions will be cleaned
//     up, which means the region lock will be unlocked once for these regions.
//  4. In step-2, the region-2 continues to run, it can't get store stream which
//     has been deleted in step-3, so it will create new stream but fails because
//     of unstable TiKV store, at this point, the kv client should handle with the
//     pending region correctly.
func TestClientV1UnlockRangeReentrant(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID3 := uint64(3)
	regionID4 := uint64(4)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID3, []uint64{1}, []uint64{4}, 4)
	cluster.SplitRaw(regionID3, regionID4, []byte("b"), []uint64{5}, 5)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError", "1*return(\"injected stream recv error\")")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientPendingRegionDelay", "1*sleep(0)->1*sleep(2000)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError")
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientPendingRegionDelay")
	}()

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait the second region is scheduled
	time.Sleep(time.Millisecond * 500)
	close(ch1)
	server1.Stop()
	// wait the kvClientPendingRegionDelay ends, and the second region is processed
	time.Sleep(time.Second * 2)
	cancel()
	wg.Wait()
}

// TestClientErrNoPendingRegion has the similar procedure with TestClientV1UnlockRangeReentrant
// The difference is the delay injected point for region 2
func TestClientErrNoPendingRegion(t *testing.T) {
	testClientErrNoPendingRegion(t)
}

func testClientErrNoPendingRegion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID3 := uint64(3)
	regionID4 := uint64(4)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID3, []uint64{1}, []uint64{4}, 4)
	cluster.SplitRaw(regionID3, regionID4, []byte("b"), []uint64{5}, 5)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError", "1*return(\"injected error\")")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientPendingRegionDelay", "1*sleep(0)->2*sleep(1000)")
	require.Nil(t, err)
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamCloseDelay", "sleep(2000)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamRecvError")
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientPendingRegionDelay")
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientStreamCloseDelay")
	}()

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	baseAllocatedID := currentRequestID()
	// wait the second region is scheduled
	time.Sleep(time.Millisecond * 500)
	waitRequestID(t, baseAllocatedID+1)
	initialized := mockInitializedEvent(regionID3, currentRequestID())
	ch1 <- initialized
	waitRequestID(t, baseAllocatedID+2)
	initialized = mockInitializedEvent(regionID4, currentRequestID())
	ch1 <- initialized
	// wait the kvClientPendingRegionDelay ends, and the second region is processed
	time.Sleep(time.Second * 2)
	cancel()
	close(ch1)
	server1.Stop()
	wg.Wait()
}

// TestKVClientForceReconnect force reconnect gRPC stream can work
func testKVClientForceReconnect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	var server1StopFlag int32
	server1Stopped := make(chan struct{})
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		defer func() {
			// There may be a gap between server.Recv error and ticdc stream reconnect, so we need to add failpoint-inject
			// and atomic int here
			if atomic.LoadInt32(&server1StopFlag) == int32(1) {
				return
			}
			atomic.StoreInt32(&server1StopFlag, 1)
			close(ch1)
			server1.Stop()
			server1Stopped <- struct{}{}
		}()
		for {
			_, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				break
			}
		}
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID3 := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID3, []uint64{1}, []uint64{4}, 4)

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	baseAllocatedID := currentRequestID()
	waitRequestID(t, baseAllocatedID+1)
	initialized := mockInitializedEvent(regionID3, currentRequestID())
	ch1 <- initialized

	// Connection close for timeout
	<-server1Stopped

	var requestIds sync.Map
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(t, ch2)
	srv2.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			requestIds.Store(req.RegionId, req.RequestId)
		}
	}
	// Reuse the same listen addresss as server 1 to simulate TiKV handles the
	// gRPC stream terminate and reconnect.
	server2, _ := newMockServiceSpecificAddr(ctx, t, srv2, addr1, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	// The second TiKV could start up slowly, which causes the kv client retries
	// to TiKV for more than one time, so we can't determine the correct requestID
	// here, we must use the real request ID received by TiKV server
	err = retry.Do(context.Background(), func() error {
		_, ok := requestIds.Load(regionID3)
		if ok {
			return nil
		}
		return errors.New("waiting for kv client requests received by server")
	}, retry.WithBackoffBaseDelay(300), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)
	requestID, _ := requestIds.Load(regionID3)

	initialized = mockInitializedEvent(regionID3, requestID.(uint64))
	ch2 <- initialized

	resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID3,
			RequestId: requestID.(uint64),
			Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 135},
		},
	}}
	ch2 <- resolved

	expected := model.RegionFeedEvent{
		Resolved: &model.ResolvedSpans{
			Spans: []model.RegionComparableSpan{{
				Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
				Region: regionID3,
			}}, ResolvedTs: 135,
		},
	}

eventLoop:
	for {
		select {
		case ev := <-eventCh:
			if ev.Resolved != nil && ev.Resolved.ResolvedTs == uint64(100) {
				continue
			}
			require.Equal(t, expected, ev)
			break eventLoop
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event %v not received", expected))
		}
	}

	cancel()
}

func TestKVClientForceReconnect(t *testing.T) {
	testKVClientForceReconnect(t)
}

// TestConcurrentProcessRangeRequest when region range request channel is full,
// the kv client can process it correctly without deadlock. This is more likely
// to happen when region split and merge frequently and large stale requests exist.
func TestConcurrentProcessRangeRequest(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	requestIDs := new(sync.Map)
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				return
			}
			requestIDs.Store(req.RegionId, req.RequestId)
		}
	}
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	storeID := uint64(1)
	regionID := uint64(1000)
	peerID := regionID + 1
	cluster.AddStore(storeID, addr1)
	cluster.Bootstrap(regionID, []uint64{storeID}, []uint64{peerID}, peerID)

	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientMockRangeLock", "1*return(20)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientMockRangeLock")
	}()

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 100)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("z")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// the kv client is blocked by failpoint injection, and after region has split
	// into more sub regions, the kv client will continue to handle and will find
	// stale region requests (which is also caused by failpoint injection).
	regionNum := 20
	for i := 1; i < regionNum; i++ {
		regionID := uint64(i + 1000)
		peerID := regionID + 1
		// split regions to [min, b1001), [b1001, b1002), ... [bN, max)
		cluster.SplitRaw(regionID-1, regionID, []byte(fmt.Sprintf("b%d", regionID)), []uint64{peerID}, peerID)
	}

	// wait for all regions requested from cdc kv client
	err = retry.Do(context.Background(), func() error {
		count := 0
		requestIDs.Range(func(_, _ interface{}) bool {
			count++
			return true
		})
		if count == regionNum {
			return nil
		}
		return errors.Errorf("region number %d is not as expected %d", count, regionNum)
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))

	require.Nil(t, err)

	// send initialized event and a resolved ts event to each region
	requestIDs.Range(func(key, value interface{}) bool {
		regionID := key.(uint64)
		requestID := value.(uint64)
		initialized := mockInitializedEvent(regionID, requestID)
		ch1 <- initialized
		resolved := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
			{
				RegionId:  regionID,
				RequestId: requestID,
				Event:     &cdcpb.Event_ResolvedTs{ResolvedTs: 120},
			},
		}}
		ch1 <- resolved
		return true
	})

	resolvedCount := 0
checkEvent:
	for {
		select {
		case <-eventCh:
			resolvedCount++
			log.Info("receive resolved count", zap.Int("count", resolvedCount))
			if resolvedCount == regionNum*2 {
				break checkEvent
			}
		case <-time.After(time.Second):
			require.Fail(t, "no more events received")
		}
	}

	cancel()
}

// TestEvTimeUpdate creates a new event feed, send N committed events every 100ms,
// use failpoint to set reconnect interval to 1s, the last event time of region
// should be updated correctly and no reconnect is triggered
func TestEvTimeUpdate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	originalReconnectInterval := reconnectInterval
	reconnectInterval = 1500 * time.Millisecond
	err = failpoint.Enable("github.com/pingcap/tiflow/cdc/kv/kvClientCheckUnInitRegionInterval", "return(2)")
	require.Nil(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/cdc/kv/kvClientCheckUnInitRegionInterval")
		reconnectInterval = originalReconnectInterval
	}()

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)

	eventCount := 20
	for i := 0; i < eventCount; i++ {
		events := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
			{
				RegionId:  3,
				RequestId: currentRequestID(),
				Event: &cdcpb.Event_Entries_{
					Entries: &cdcpb.Event_Entries{
						Entries: []*cdcpb.Event_Row{{
							Type:     cdcpb.Event_COMMITTED,
							OpType:   cdcpb.Event_Row_PUT,
							Key:      []byte("aaaa"),
							Value:    []byte("committed put event before init"),
							StartTs:  105,
							CommitTs: 115,
						}},
					},
				},
			},
		}}
		ch1 <- events
		time.Sleep(time.Millisecond * 100)
	}

	expected := []model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{{
					Span:   regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
					Region: 3,
				}}, ResolvedTs: 100,
			},
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaaa"),
				Value:    []byte("committed put event before init"),
				StartTs:  105,
				CRTs:     115,
				RegionID: 3,
			},
			RegionID: 3,
		},
	}

	for i := 0; i < eventCount+1; i++ {
		select {
		case event := <-eventCh:
			if i == 0 {
				require.Equal(t, expected[0], event)
			} else {
				require.Equal(t, expected[1], event)
			}
		case <-time.After(time.Second):
			require.Fail(t, fmt.Sprintf("expected event not received, %d received", i))
		}
	}

	cancel()
}

// TestRegionWorkerExitWhenIsIdle tests region worker can exit, and cancel gRPC
// stream automatically when it is idle.
// Idle means having no any effective region state
func TestRegionWorkerExitWhenIsIdle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	server1Stopped := make(chan struct{})
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	defer close(ch1)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	defer server1.Stop()
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		defer func() {
			// When meet regionWorker some error, new stream may be created successfully before the old one close.
			select {
			case server1Stopped <- struct{}{}:
			default:
			}
		}()
		for {
			_, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				break
			}
		}
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	regionID := uint64(3)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(t, baseAllocatedID+1)
	// an error event will mark the corresponding region feed as stopped
	epochNotMatch := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Error{
				Error: &cdcpb.Error{
					EpochNotMatch: &errorpb.EpochNotMatch{},
				},
			},
		},
	}}
	ch1 <- epochNotMatch

	select {
	case <-server1Stopped:
	case <-time.After(time.Second):
		require.Fail(t, "stream is not terminated by cdc kv client")
	}
	cancel()
}

// TestPrewriteNotMatchError tests TiKV sends a commit event without a matching
// prewrite(which is a bug, ref: https://github.com/tikv/tikv/issues/11055,
// TiCDC catches this error and resets the gRPC stream. TiCDC must not send a
// new request before closing gRPC stream since currently there is no mechanism
// to release an existing region connection.
func TestPrewriteNotMatchError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	var requestIds sync.Map
	var server1Stopped int32 = 0
	server1StoppedCh := make(chan struct{})
	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(t, ch1)
	server1, addr1 := newMockService(ctx, t, srv1, wg)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		if atomic.LoadInt32(&server1Stopped) == int32(1) {
			return
		}
		defer func() {
			atomic.StoreInt32(&server1Stopped, 1)
			close(ch1)
			server1.Stop()
			server1StoppedCh <- struct{}{}
		}()
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			requestIds.Store(req.RegionId, req.RequestId)
		}
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	require.Nil(t, err)
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	require.Nil(t, err)
	defer kvStorage.Close() //nolint:errcheck

	// create two regions to avoid the stream is canceled by no region remained
	regionID3 := uint64(3)
	regionID4 := uint64(4)
	cluster.AddStore(1, addr1)
	cluster.Bootstrap(regionID3, []uint64{1}, []uint64{4}, 4)
	cluster.SplitRaw(regionID3, regionID4, []byte("b"), []uint64{5}, 5)

	changefeed := model.DefaultChangeFeedID("changefeed-test")
	lockResolver := txnutil.NewLockerResolver(kvStorage, changefeed, util.RoleTester)
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, grpcPool, regionCache, pdutil.NewClock4Test(),
		config.GetDefaultServerConfig(), changefeed, 0, "", false)
	eventCh := make(chan model.RegionFeedEvent, 50)
	baseAllocatedID := currentRequestID()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("c")},
			100, lockResolver, eventCh)
		require.Equal(t, context.Canceled, errors.Cause(err))
	}()

	// The expected request ids are agnostic because the kv client could retry
	// for more than one time, so we wait until the newly started server receives
	// requests for both two regions.
	err = retry.Do(context.Background(), func() error {
		_, ok1 := requestIds.Load(regionID3)
		_, ok2 := requestIds.Load(regionID4)
		if ok1 && ok2 {
			return nil
		}
		return errors.New("waiting for kv client requests received by server")
	}, retry.WithBackoffBaseDelay(200), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(10))

	require.Nil(t, err)
	reqID1, _ := requestIds.Load(regionID3)
	reqID2, _ := requestIds.Load(regionID4)
	initialized1 := mockInitializedEvent(regionID3, reqID1.(uint64))
	initialized2 := mockInitializedEvent(regionID4, reqID2.(uint64))
	ch1 <- initialized1
	ch1 <- initialized2

	prewriteNotMatchEvent := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  regionID3,
			RequestId: reqID1.(uint64),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:     cdcpb.Event_COMMIT,
						OpType:   cdcpb.Event_Row_PUT,
						Key:      []byte("aaaa"),
						Value:    []byte("commit event before prewrite"),
						StartTs:  105,
						CommitTs: 115,
					}},
				},
			},
		},
	}}
	ch1 <- prewriteNotMatchEvent

	<-server1StoppedCh
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(t, ch2)
	srv2.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				log.Error("mock server error", zap.Error(err))
				return
			}
			requestIds.Store(req.RegionId, req.RequestId)
		}
	}
	// Reuse the same listen address as server 1
	server2, _ := newMockServiceSpecificAddr(ctx, t, srv2, addr1, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	// After the gRPC stream is canceled, two more reqeusts will be sent, so the
	// allocated id is increased by 2 from baseAllocatedID+2.
	waitRequestID(t, baseAllocatedID+4)
	cancel()
}

func createFakeEventFeedSession() *eventFeedSession {
	return newEventFeedSession(
		&CDCClient{config: config.GetDefaultServerConfig()},
		regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
		nil, /*lockResolver*/
		100, /*startTs*/
		nil, /*eventCh*/
	)
}
