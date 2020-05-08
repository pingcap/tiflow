package kv

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/tidb/store/mockstore/mocktikv"
	"github.com/pingcap/tidb/store/tikv"
	"google.golang.org/grpc"
)

func Test(t *testing.T) { check.TestingT(t) }

type clientSuite struct {
}

var _ = check.Suite(&clientSuite{})

func (s *clientSuite) TestNewClose(c *check.C) {
	cluster := mocktikv.NewCluster()
	pdCli := mocktikv.NewPDClient(cluster)

	cli, err := NewCDCClient(pdCli, nil)
	c.Assert(err, check.IsNil)

	err = cli.Close()
	c.Assert(err, check.IsNil)
}

func (s *clientSuite) TestUpdateCheckpointTS(c *check.C) {
	var checkpointTS uint64
	var g sync.WaitGroup
	maxValueCh := make(chan uint64, 64)
	update := func() uint64 {
		var maxValue uint64
		for i := 0; i < 1024; i++ {
			value := rand.Uint64()
			if value > maxValue {
				maxValue = value
			}
			updateCheckpointTS(&checkpointTS, value)
		}
		return maxValue
	}
	for i := 0; i < 64; i++ {
		g.Add(1)
		go func() {
			maxValueCh <- update()
			g.Done()
		}()
	}
	g.Wait()
	close(maxValueCh)
	var maxValue uint64
	for v := range maxValueCh {
		if maxValue < v {
			maxValue = v
		}
	}
	c.Assert(checkpointTS, check.Equals, maxValue)
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

func newMockService(c *check.C, port int, ch chan *cdcpb.ChangeDataEvent, wg *sync.WaitGroup) *grpc.Server {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	c.Assert(err, check.IsNil)
	grpcServer := grpc.NewServer()
	mockService := &mockChangeDataService{c: c, ch: ch}
	cdcpb.RegisterChangeDataServer(grpcServer, mockService)
	wg.Add(1)
	go func() {
		err := grpcServer.Serve(lis)
		c.Assert(err, check.IsNil)
		wg.Done()
	}()
	return grpcServer
}

// Use etcdSuite to workaround the race. See comments of `TestConnArray`.
func (s *etcdSuite) TestConnectOfflineTiKV(c *check.C) {
	wg := &sync.WaitGroup{}
	ch2 := make(chan *cdcpb.ChangeDataEvent, 10)
	server2 := newMockService(c, 23376, ch2, wg)
	defer func() {
		close(ch2)
		server2.Stop()
		wg.Wait()
	}()

	cluster := mocktikv.NewCluster()
	rpcClient, pdClient, err := mocktikv.NewTiKVAndPDClient(cluster, nil, "")
	c.Assert(err, check.IsNil)
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	c.Assert(err, check.IsNil)

	cluster.AddStore(1, "localhost:23375")
	cluster.AddStore(2, "localhost:23376")
	cluster.Bootstrap(3, []uint64{1, 2}, []uint64{4, 5}, 4)

	cdcClient, err := NewCDCClient(pdClient, kvStorage.(tikv.Storage))
	c.Assert(err, check.IsNil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	eventCh := make(chan *model.RegionFeedEvent, 10)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx, regionspan.Span{Start: []byte("a"), End: []byte("b")}, 1, eventCh)
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

	checkEvent(event, ts.Ver)
	cancel()
}

// Use etcdSuite for some special reasons, the embed etcd uses zap as the only candidate
// logger and in the logger initializtion it also initializes the grpclog/loggerv2, which
// is not a thread-safe operation and it must be called before any gRPC functions
// ref: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L67-L72
func (s *etcdSuite) TestConnArray(c *check.C) {
	addr := "127.0.0.1:2379"
	ca, err := newConnArray(context.TODO(), 2, addr)
	c.Assert(err, check.IsNil)

	conn1 := ca.Get()
	conn2 := ca.Get()
	c.Assert(conn1, check.Not(check.Equals), conn2)

	conn3 := ca.Get()
	c.Assert(conn1, check.Equals, conn3)

	ca.Close()
}
