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
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
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
	cluster := mocktikv.NewCluster()
	pdCli := mocktikv.NewPDClient(cluster)

	cli, err := NewCDCClient(context.Background(), pdCli, nil, &security.Credential{})
	c.Assert(err, check.IsNil)

	err = cli.Close()
	c.Assert(err, check.IsNil)
}

type mockChangeDataService struct {
	c  *check.C
	ch chan *cdcpb.ChangeDataEvent
}

func (s *mockChangeDataService) EventFeed(server cdcpb.ChangeData_EventFeedServer) error {
	req, err := server.Recv()
	s.c.Assert(err, check.IsNil)
	initialized := &cdcpb.ChangeDataEvent{
		Events: []*cdcpb.Event{
			{
				RegionId:  req.RegionId,
				RequestId: req.RequestId,
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
	err = server.Send(initialized)
	s.c.Assert(err, check.IsNil)
	for e := range s.ch {
		for _, event := range e.Events {
			event.RequestId = req.RequestId
		}
		err := server.Send(e)
		s.c.Assert(err, check.IsNil)
	}
	return nil
}

func newMockService(ctx context.Context, c *check.C, ch chan *cdcpb.ChangeDataEvent, wg *sync.WaitGroup) (grpcServer *grpc.Server, addr string) {
	lc := &net.ListenConfig{}
	lis, err := lc.Listen(ctx, "tcp", "127.0.0.1:0")
	c.Assert(err, check.IsNil)
	addr = lis.Addr().String()
	grpcServer = grpc.NewServer()
	mockService := &mockChangeDataService{c: c, ch: ch}
	cdcpb.RegisterChangeDataServer(grpcServer, mockService)
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

// Use etcdSuite to workaround the race. See comments of `TestConnArray`.
func (s *etcdSuite) TestConnectOfflineTiKV(c *check.C) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	server2, addr := newMockService(ctx, c, ch2, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	cluster := mocktikv.NewCluster()
	mvccStore := mocktikv.MustNewMVCCStore()
	rpcClient, pdClient, err := mocktikv.NewTiKVAndPDClient(cluster, mvccStore, "")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)

	cluster.AddStore(1, "localhost:1")
	cluster.AddStore(2, addr)
	cluster.Bootstrap(3, []uint64{1, 2}, []uint64{4, 5}, 4)

	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient, err := NewCDCClient(context.Background(), pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	c.Assert(err, check.IsNil)
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 1, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		wg.Done()
	}()

	makeEvent := func(ts uint64) *cdcpb.ChangeDataEvent {
		return &cdcpb.ChangeDataEvent{
			Events: []*cdcpb.Event{
				{
					RegionId: 3,
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

	time.Sleep(time.Millisecond * 10)
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
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	server2, addr := newMockService(ctx, c, ch2, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()
	// Cancel first, and then close the server.
	defer cancel()

	cluster := mocktikv.NewCluster()
	mvccStore := mocktikv.MustNewMVCCStore()
	rpcClient, pdClient, err := mocktikv.NewTiKVAndPDClient(cluster, mvccStore, "")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: version.MinTiKVVersion.String()}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)

	cluster.AddStore(2, addr)
	cluster.Bootstrap(3, []uint64{2}, []uint64{4}, 4)

	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient, err := NewCDCClient(ctx, pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	c.Assert(err, check.IsNil)
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")}, 1, false, lockresolver, isPullInit, eventCh)
		c.Assert(errors.Cause(err), check.Equals, context.Canceled)
		wg.Done()
	}()

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
			RegionId: 3,
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

// TODO enable the test
func (s *etcdSuite) TodoTestIncompatibleTiKV(c *check.C) {
	cluster := mocktikv.NewCluster()
	mvccStore := mocktikv.MustNewMVCCStore()
	rpcClient, pdClient, err := mocktikv.NewTiKVAndPDClient(cluster, mvccStore, "")
	c.Assert(err, check.IsNil)
	pdClient = &mockPDClient{Client: pdClient, version: "v2.1.0" /* CDC is not compatible with 2.1.0 */}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)

	cluster.AddStore(1, "localhost:23375")
	cluster.Bootstrap(2, []uint64{1}, []uint64{3}, 3)

	lockresolver := txnutil.NewLockerResolver(kvStorage.(tikv.Storage))
	isPullInit := &mockPullerInit{}
	cdcClient, err := NewCDCClient(context.Background(), pdClient, kvStorage.(tikv.Storage), &security.Credential{})
	c.Assert(err, check.IsNil)
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
