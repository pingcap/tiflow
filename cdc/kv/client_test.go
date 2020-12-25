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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
	"github.com/pingcap/ticdc/pkg/util/testleak"
	"github.com/pingcap/ticdc/pkg/version"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	pd "github.com/tikv/pd/client"
	"google.golang.org/grpc"
)

func Test(t *testing.T) { check.TestingT(t) }

type clientSuite struct {
}

var _ = check.Suite(&clientSuite{})

func (s *clientSuite) TestNewClose(c *check.C) {
	defer testleak.AfterTest(c)()
	store := mocktikv.MustNewMVCCStore()
	defer store.Close() //nolint:errcheck
	cluster := mocktikv.NewCluster(store)
	pdCli := mocktikv.NewPDClient(cluster)
	defer pdCli.Close() //nolint:errcheck

	cli := NewCDCClient(context.Background(), pdCli, nil, &security.Credential{})
	err := cli.Close()
	c.Assert(err, check.IsNil)
}

func mockInitializedEvent(regionID, requestID uint64) *cdcpb.ChangeDataEvent {
	initialized := &cdcpb.ChangeDataEvent{
		Events: []*cdcpb.Event{
			{
				RegionId:  regionID,
				RequestId: requestID,
				Event: &cdcpb.Event_Entries_{
					Entries: &cdcpb.Event_Entries{
						Entries: []*cdcpb.Event_Row{
							{
								Type: cdcpb.Event_INITIALIZED,
							},
						},
					},
				},
			},
		},
	}
	return initialized
}

type mockChangeDataService struct {
	c  *check.C
	ch chan *cdcpb.ChangeDataEvent
}

func newMockChangeDataService(c *check.C, ch chan *cdcpb.ChangeDataEvent) *mockChangeDataService {
	s := &mockChangeDataService{
		c:  c,
		ch: ch,
	}
	return s
}

func (s *mockChangeDataService) EventFeed(server cdcpb.ChangeData_EventFeedServer) error {
	for e := range s.ch {
		err := server.Send(e)
		s.c.Assert(err, check.IsNil)
	}
	return nil
}

func newMockService(
	ctx context.Context,
	c *check.C,
	srv cdcpb.ChangeDataServer,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	lc := &net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	c.Assert(err, check.IsNil)
	addr = lis.Addr().String()
	grpcServer = grpc.NewServer()
	cdcpb.RegisterChangeDataServer(grpcServer, srv)
	wg.Add(1)
	go func() {
		err := grpcServer.Serve(lis)
		c.Assert(err, check.IsNil)
		wg.Done()
	}()
	return
}

type mockPDClient struct {
	pd.Client
	version string
}

var _ pd.Client = &mockPDClient{}

func (m *mockPDClient) GetStore(ctx context.Context, storeID uint64) (*metapb.Store, error) {
	s, err := m.Client.GetStore(ctx, storeID)
	if err != nil {
		return nil, err
	}
	s.Version = m.version
	return s, nil
}

// waitRequestID waits request ID larger than the given allocated ID
func waitRequestID(c *check.C, allocatedID uint64) {
	err := retry.Run(time.Millisecond*20, 10, func() error {
		if currentRequestID() > allocatedID {
			return nil
		}
		return errors.Errorf("request id %d is not larger than %d", currentRequestID(), allocatedID)
	})
	c.Assert(err, check.IsNil)
}

// Use etcdSuite to workaround the race. See comments of `TestConnArray`.
func (s *etcdSuite) TestConnectOfflineTiKV(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv := newMockChangeDataService(c, ch2)
	server2, addr := newMockService(ctx, c, srv, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, "localhost:1")
	cluster.AddStore(2, addr)
	cluster.Bootstrap(3, []uint64{1, 2}, []uint64{4, 5}, 4)

	baseAllocatedID := currentRequestID()
	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient := NewCDCClient(context.Background(), pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	defer cdcClient.Close() //nolint:errcheck
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 1, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		wg.Done()
	}()

	// new session, request to store 1, request to store 2
	waitRequestID(c, baseAllocatedID+2)

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

	checkEvent := func(event *model.RegionFeedEvent, ts uint64) {
		c.Assert(event.Resolved.ResolvedTs, check.Equals, ts)
	}

	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized

	cluster.ChangeLeader(3, 5)

	ts, err := kvStorage.CurrentVersion()
	c.Assert(err, check.IsNil)
	ch2 <- makeEvent(ts.Ver)
	var event *model.RegionFeedEvent
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		c.Fatalf("reconnection not succeed in 1 second")
	}
	checkEvent(event, 1)

	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		c.Fatalf("reconnection not succeed in 1 second")
	}
	checkEvent(event, ts.Ver)
	cancel()
}

func (s *etcdSuite) TestRecvLargeMessageSize(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv := newMockChangeDataService(c, ch2)
	server2, addr := newMockService(ctx, c, srv, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()
	// Cancel first, and then close the server.
	defer cancel()

	rpcClient, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	defer pdClient.Close() //nolint:errcheck
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(2, addr)
	cluster.Bootstrap(3, []uint64{2}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient := NewCDCClient(ctx, pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 1, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		cdcClient.Close() //nolint:errcheck
		wg.Done()
	}()

	// new session, new request
	waitRequestID(c, baseAllocatedID+1)

	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized

	var event *model.RegionFeedEvent
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		c.Fatalf("recving message takes too long")
	}
	c.Assert(event, check.NotNil)

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
		c.Fatalf("recving message takes too long")
	}
	c.Assert(len(event.Val.Value), check.Equals, largeValSize)
	cancel()
}

func (s *etcdSuite) TestHandleError(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(c, ch1)
	server1, addr1 := newMockService(ctx, c, srv1, wg)

	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv2 := newMockChangeDataService(c, ch2)
	server2, addr2 := newMockService(ctx, c, srv2, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.AddStore(2, addr2)
	cluster.Bootstrap(3, []uint64{1, 2}, []uint64{4, 5}, 4)

	baseAllocatedID := currentRequestID()
	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient := NewCDCClient(ctx, pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 100, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		cdcClient.Close() //nolint:errcheck
		wg.Done()
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(c, baseAllocatedID+1)

	var event *model.RegionFeedEvent
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
	waitRequestID(c, baseAllocatedID+2)
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

	waitRequestID(c, baseAllocatedID+3)
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

	waitRequestID(c, baseAllocatedID+4)
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
			c.Assert(event.Resolved, check.NotNil)
			c.Assert(event.Resolved.ResolvedTs, check.Equals, uint64(100))
		case <-time.After(time.Second):
			break consumePreResolvedTs
		}
	}

	// wait request id allocated with:
	// new session, no leader request, epoch not match request,
	// region not found request, unknown error request, normal request
	waitRequestID(c, baseAllocatedID+5)
	initialized := mockInitializedEvent(3 /* regionID */, currentRequestID())
	ch2 <- initialized
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		c.Fatalf("recving message takes too long")
	}
	c.Assert(event, check.NotNil)

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
	// normal resolved ts evnet
	ch2 <- makeEvent(120)
	select {
	case event = <-eventCh:
	case <-time.After(time.Second):
		c.Fatalf("reconnection not succeed in 1 second")
	}
	c.Assert(event.Resolved, check.NotNil)
	c.Assert(event.Resolved.ResolvedTs, check.Equals, uint64(120))

	cancel()
}

func (s *etcdSuite) TestHandleFeedEvent(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	ch1 := make(chan *cdcpb.ChangeDataEvent, 10)
	srv1 := newMockChangeDataService(c, ch1)
	server1, addr1 := newMockService(ctx, c, srv1, wg)

	defer func() {
		close(ch1)
		server1.Stop()
		wg.Wait()
	}()

	rpcClient, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)
	defer kvStorage.Close() //nolint:errcheck

	cluster.AddStore(1, addr1)
	cluster.Bootstrap(3, []uint64{1}, []uint64{4}, 4)

	baseAllocatedID := currentRequestID()
	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient := NewCDCClient(ctx, pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 100, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		cdcClient.Close() //nolint:errcheck
		wg.Done()
	}()

	// wait request id allocated with: new session, new request
	waitRequestID(c, baseAllocatedID+1)

	eventsBeforeInit := &cdcpb.ChangeDataEvent{Events: []*cdcpb.Event{
		{
			RegionId:  3,
			RequestId: currentRequestID(),
			Event: &cdcpb.Event_Entries_{
				Entries: &cdcpb.Event_Entries{
					Entries: []*cdcpb.Event_Row{{
						Type:    cdcpb.Event_PREWRITE,
						OpType:  cdcpb.Event_Row_PUT,
						Key:     []byte("aaa"),
						Value:   []byte("sss"),
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
						StartTs:  105, // ResolvedTs = 100
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
						Value:    []byte("ss"),
						StartTs:  105, // ResolvedTs = 100
						CommitTs: 115,
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
						OpType:  cdcpb.Event_Row_DELETE,
						Key:     []byte("atsl"),
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
						Key:      []byte("atsl"),
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
						Key:     []byte("astonmatin"),
						Value:   []byte("db11"),
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
						Key:      []byte("astonmatin"),
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

	expected := []*model.RegionFeedEvent{
		{
			Resolved: &model.ResolvedSpan{
				Span:       regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
				ResolvedTs: 100,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("aaa"),
				Value:    []byte("sss"),
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
				Value:    []byte("ss"),
				StartTs:  105,
				CRTs:     115,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypeDelete,
				Key:      []byte("atsl"),
				StartTs:  130,
				CRTs:     140,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Val: &model.RawKVEntry{
				OpType:   model.OpTypePut,
				Key:      []byte("astonmatin"),
				Value:    []byte("db11"),
				StartTs:  135,
				CRTs:     145,
				RegionID: 3,
			},
			RegionID: 3,
		},
		{
			Resolved: &model.ResolvedSpan{
				Span:       regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
				ResolvedTs: 135,
			},
			RegionID: 3,
		},
		{
			Resolved: &model.ResolvedSpan{
				Span:       regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
				ResolvedTs: 145,
			},
			RegionID: 3,
		},
	}

	ch1 <- eventsBeforeInit
	ch1 <- initialized
	ch1 <- eventsAfterInit
	ch1 <- eventResolved
	ch1 <- eventResolvedBatch

	for _, expectedEv := range expected {
		select {
		case event := <-eventCh:
			c.Assert(event, check.DeepEquals, expectedEv)
		case <-time.After(time.Second):
			c.Errorf("expected event %v not received", expectedEv)
		}
	}

	cancel()
}

// TODO enable the test
func (s *etcdSuite) TodoTestIncompatibleTiKV(c *check.C) {
	rpcClient, cluster, pdClient, err := mocktikv.NewTiKVAndPDClient("")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: "v2.1.0" /* CDC is not compatible with 2.1.0 */}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)

	cluster.AddStore(1, "localhost:23375")
	cluster.Bootstrap(2, []uint64{1}, []uint64{3}, 3)

	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient := NewCDCClient(context.Background(), pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	eventCh := make(chan *model.RegionFeedEvent, 10)
	err = cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 1, false, lockresolver, isPullInit, eventCh)
	_ = err
	// TODO find a way to verify the error
}

// Use etcdSuite for some special reasons, the embed etcd uses zap as the only candidate
// logger and in the logger initializtion it also initializes the grpclog/loggerv2, which
// is not a thread-safe operation and it must be called before any gRPC functions
// ref: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L67-L72
func (s *etcdSuite) TestConnArray(c *check.C) {
	defer testleak.AfterTest(c)()
	defer s.TearDownTest(c)
	addr := "127.0.0.1:2379"
	ca, err := newConnArray(context.TODO(), 2, addr, &security.Credential{})
	c.Assert(err, check.IsNil)

	conn1 := ca.Get()
	conn2 := ca.Get()
	c.Assert(conn1, check.Not(check.Equals), conn2)

	conn3 := ca.Get()
	c.Assert(conn1, check.Equals, conn3)

	ca.Close()
}
