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

package kv

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/store/mockstore/mockcopr"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/testutils"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc"
)

const batchResolvedSize = 100

type mockPullerInit struct{}

func (*mockPullerInit) IsInitialized() bool {
	return true
}

type mockChangeDataService2 struct {
	b        *testing.B
	ch       chan *cdcpb.ChangeDataEvent
	recvLoop func(server cdcpb.ChangeData_EventFeedServer)
}

func newMockChangeDataService2(b *testing.B, ch chan *cdcpb.ChangeDataEvent) *mockChangeDataService2 {
	s := &mockChangeDataService2{
		b:  b,
		ch: ch,
	}
	return s
}

func (s *mockChangeDataService2) EventFeed(server cdcpb.ChangeData_EventFeedServer) error {
	if s.recvLoop != nil {
		go func() {
			s.recvLoop(server)
		}()
	}
	for e := range s.ch {
		if e == nil {
			break
		}
		err := server.Send(e)
		if err != nil {
			s.b.Error(err)
		}
	}
	return nil
}

func newMockService2(
	ctx context.Context,
	b *testing.B,
	srv cdcpb.ChangeDataServer,
	wg *sync.WaitGroup,
) (grpcServer *grpc.Server, addr string) {
	lc := &net.ListenConfig{}
	listenAddr := "127.0.0.1:0"
	lis, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		b.Error(err)
	}
	addr = lis.Addr().String()
	grpcServer = grpc.NewServer()
	cdcpb.RegisterChangeDataServer(grpcServer, srv)
	wg.Add(1)
	go func() {
		err := grpcServer.Serve(lis)
		if err != nil {
			b.Error(err)
		}
		wg.Done()
	}()
	return
}

func prepareBenchMultiStore(b *testing.B, storeNum, regionNum int) (
	*sync.Map, /* regionID -> requestID/storeID */
	*sync.WaitGroup, /* ensure eventfeed routine exit */
	context.CancelFunc, /* cancle both mock server and cdc kv client */
	chan model.RegionFeedEvent, /* kv client output channel */
	[]chan *cdcpb.ChangeDataEvent, /* mock server data channels */
) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	requestIDs := new(sync.Map)

	servers := make([]*grpc.Server, storeNum)
	inputs := make([]chan *cdcpb.ChangeDataEvent, storeNum)
	addrs := make([]string, storeNum)
	for i := 0; i < storeNum; i++ {
		mockSrvCh := make(chan *cdcpb.ChangeDataEvent, 100000)
		srv := newMockChangeDataService2(b, mockSrvCh)
		srv.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
			for {
				req, err := server.Recv()
				if err != nil {
					return
				}
				requestIDs.Store(req.RegionId, req.RequestId)
			}
		}
		server, addr := newMockService2(ctx, b, srv, wg)
		servers[i] = server
		inputs[i] = mockSrvCh
		addrs[i] = addr
	}

	for i := 0; i < storeNum; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			<-ctx.Done()
			close(inputs[i])
			servers[i].Stop()
		}()
	}

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	if err != nil {
		b.Error(err)
	}
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	if err != nil {
		b.Error(err)
	}
	defer kvStorage.Close() //nolint:errcheck

	// we set each region has `storeNum` peers
	regionID := uint64(1_000_000)
	peers := make([]uint64, storeNum)
	stores := make([]uint64, storeNum)
	for i := 0; i < storeNum; i++ {
		peers[i] = uint64(100_000_000*i) + regionID
		stores[i] = uint64(i + 1)
		cluster.AddStore(uint64(i+1), addrs[i])
	}
	// bootstrap cluster with the first region
	cluster.Bootstrap(regionID, stores, peers, peers[0])
	for i := 0; i < storeNum; i++ {
		// first region of stores except for the first store
		if i > 0 {
			regionID += 1
			peers := make([]uint64, storeNum)
			for j := 0; j < storeNum; j++ {
				peers[j] = uint64(100_000_000*j) + regionID
			}
			// peers[i] is the leader peer, locates in store with index i(storeID=i+1)
			cluster.SplitRaw(regionID-1, regionID, []byte(fmt.Sprintf("a%d", regionID)), peers, peers[i])
		}
		// regions following, split from its previous region
		for j := 1; j < regionNum; j++ {
			regionID += 1
			peers := make([]uint64, storeNum)
			for k := 0; k < storeNum; k++ {
				peers[k] = uint64(100_000_000*k) + regionID
			}
			// peers[i] is the leader peer, locates in store with index i(storeID=i+1)
			cluster.SplitRaw(regionID-1, regionID, []byte(fmt.Sprintf("a%d", regionID)), peers, peers[i])
		}
	}

	lockResolver := txnutil.NewLockerResolver(kvStorage,
		model.DefaultChangeFeedID("changefeed-test"),
		util.RoleTester)
	isPullInit := &mockPullerInit{}
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, kvStorage, grpcPool, regionCache, pdutil.NewClock4Test(),
		model.DefaultChangeFeedID(""),
		config.GetDefaultServerConfig().KVClient)
	eventCh := make(chan model.RegionFeedEvent, 1000000)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("b")},
			100, lockResolver, isPullInit, eventCh)
		if errors.Cause(err) != context.Canceled {
			b.Error(err)
		}
		wg.Done()
	}()

	// wait all regions requested from cdc kv client
	err = retry.Do(context.Background(), func() error {
		count := 0
		requestIDs.Range(func(_, _ interface{}) bool {
			count++
			return true
		})
		if count == regionNum*storeNum {
			return nil
		}
		return errors.Errorf("region number %d is not as expected %d", count, regionNum)
	}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))
	if err != nil {
		b.Error(err)
	}

	return requestIDs, wg, cancel, eventCh, inputs
}

func prepareBench(b *testing.B, regionNum int) (
	*sync.Map, /* regionID -> requestID */
	*sync.WaitGroup, /* ensure eventfeed routine exit */
	context.CancelFunc, /* cancle both mock server and cdc kv client */
	chan model.RegionFeedEvent, /* kv client output channel */
	chan *cdcpb.ChangeDataEvent, /* mock server data channel */
) {
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	requestIDs := new(sync.Map)
	mockSrvCh := make(chan *cdcpb.ChangeDataEvent, 100000)
	srv1 := newMockChangeDataService2(b, mockSrvCh)
	srv1.recvLoop = func(server cdcpb.ChangeData_EventFeedServer) {
		for {
			req, err := server.Recv()
			if err != nil {
				return
			}
			requestIDs.Store(req.RegionId, req.RequestId)
		}
	}
	server1, addr1 := newMockService2(ctx, b, srv1, wg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		close(mockSrvCh)
		server1.Stop()
	}()

	rpcClient, cluster, pdClient, err := testutils.NewMockTiKV("", mockcopr.NewCoprRPCHandler())
	if err != nil {
		b.Error(err)
	}
	pdClient = &mockPDClient{Client: pdClient, versionGen: defaultVersionGen}
	kvStorage, err := tikv.NewTestTiKVStore(rpcClient, pdClient, nil, nil, 0)
	if err != nil {
		b.Error(err)
	}
	defer kvStorage.Close() //nolint:errcheck

	storeID := uint64(1)
	cluster.AddStore(storeID, addr1)
	// bootstrap with region 100_000(100k)
	cluster.Bootstrap(uint64(100_000), []uint64{storeID}, []uint64{100_001}, 100_001)
	for i := 1; i < regionNum; i++ {
		regionID := uint64(i + 100_000)
		peerID := regionID + 1
		// split regions to [min, b100_001), [b100_001, b100_002), ... [bN, max)
		cluster.SplitRaw(regionID-1, regionID, []byte(fmt.Sprintf("b%d", regionID)), []uint64{peerID}, peerID)
	}

	lockResolver := txnutil.NewLockerResolver(kvStorage,
		model.DefaultChangeFeedID("changefeed-test"),
		util.RoleTester)
	isPullInit := &mockPullerInit{}
	grpcPool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer grpcPool.Close()
	regionCache := tikv.NewRegionCache(pdClient)
	defer regionCache.Close()
	cdcClient := NewCDCClient(
		ctx, pdClient, kvStorage, grpcPool, regionCache, pdutil.NewClock4Test(),
		model.DefaultChangeFeedID(""),
		config.GetDefaultServerConfig().KVClient)
	eventCh := make(chan model.RegionFeedEvent, 1000000)
	wg.Add(1)
	go func() {
		err := cdcClient.EventFeed(ctx,
			regionspan.ComparableSpan{Start: []byte("a"), End: []byte("z")},
			100, lockResolver, isPullInit, eventCh)
		if errors.Cause(err) != context.Canceled {
			b.Error(err)
		}
		wg.Done()
	}()

	// wait all regions requested from cdc kv client
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
	}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))
	if err != nil {
		b.Error(err)
	}

	return requestIDs, wg, cancel, eventCh, mockSrvCh
}

func benchmarkSingleWorkerResolvedTs(b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	tests := []struct {
		name      string
		regionNum int
	}{
		{name: "10", regionNum: 10},
		{name: "100", regionNum: 100},
		{name: "1k", regionNum: 1000},
		{name: "10k", regionNum: 10_000},
		{name: "20k", regionNum: 20_000},
	}

	for _, test := range tests {
		requestIDs, wg, cancel, eventCh, mockSrvCh := prepareBench(b, test.regionNum)

		// copy to a normal map to reduce access latency
		copyReqIDs := make(map[uint64]uint64, test.regionNum)
		requestIDs.Range(func(key, value interface{}) bool {
			regionID := key.(uint64)
			requestID := value.(uint64)
			initialized := mockInitializedEvent(regionID, requestID)
			mockSrvCh <- initialized
			copyReqIDs[regionID] = requestID
			return true
		})

		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				batch := 0
				regions := make([]uint64, 0, batchResolvedSize)
				rts := oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0)
				for regionID := range copyReqIDs {
					batch++
					regions = append(regions, regionID)
					if batch == batchResolvedSize {
						eventResolvedBatch := &cdcpb.ChangeDataEvent{
							ResolvedTs: &cdcpb.ResolvedTs{
								Regions: regions,
								Ts:      rts,
							},
						}
						mockSrvCh <- eventResolvedBatch
						batch = 0
						regions = regions[:0]
					}
				}
				if len(regions) > 0 {
					eventResolvedBatch := &cdcpb.ChangeDataEvent{
						ResolvedTs: &cdcpb.ResolvedTs{
							Regions: regions,
							Ts:      rts,
						},
					}
					mockSrvCh <- eventResolvedBatch
				}
				count := 0
				for range eventCh {
					count++
					if count == test.regionNum {
						break
					}
				}
			}
		})
		err := retry.Do(context.Background(), func() error {
			if len(mockSrvCh) == 0 {
				return nil
			}
			return errors.New("not all events are sent yet")
		}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(20))
		if err != nil {
			b.Error(err)
		}

		cancel()
		wg.Wait()
	}
}

func benchmarkResolvedTsClient(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	InitWorkerPool()
	go func() {
		RunWorkerPool(ctx) //nolint:errcheck
	}()
	benchmarkSingleWorkerResolvedTs(b)
}

func BenchmarkResolvedTsClient(b *testing.B) {
	benchmarkResolvedTsClient(b)
}

func BenchmarkResolvedTsClientWorkerPool(b *testing.B) {
	hwm := regionWorkerHighWatermark
	lwm := regionWorkerLowWatermark
	regionWorkerHighWatermark = 10000
	regionWorkerLowWatermark = 2000
	defer func() {
		regionWorkerHighWatermark = hwm
		regionWorkerLowWatermark = lwm
	}()
	benchmarkResolvedTsClient(b)
}

func benchmarkMultipleStoreResolvedTs(b *testing.B) {
	log.SetLevel(zapcore.ErrorLevel)
	tests := []struct {
		name      string
		storeNum  int
		regionNum int
	}{
		{name: "10", storeNum: 10, regionNum: 1},
		{name: "100", storeNum: 10, regionNum: 10},
		{name: "1k", storeNum: 10, regionNum: 100},
		{name: "10k", storeNum: 10, regionNum: 1_000},
		{name: "20k", storeNum: 10, regionNum: 2_000},
	}

	for _, test := range tests {
		requestIDs, wg, cancel, eventCh, inputs := prepareBenchMultiStore(b, test.storeNum, test.regionNum)

		// copy to a normal map to reduce access latency, mapping from store index to region id list
		copyReqIDs := make(map[int][]uint64, test.regionNum*test.storeNum)
		requestIDs.Range(func(key, value interface{}) bool {
			regionID := key.(uint64)
			requestID := value.(uint64)
			initialized := mockInitializedEvent(regionID, requestID)
			index := int(regionID-1_000_000) / test.regionNum
			inputs[index] <- initialized
			if _, ok := copyReqIDs[index]; !ok {
				copyReqIDs[index] = make([]uint64, 0, test.regionNum)
			}
			copyReqIDs[index] = append(copyReqIDs[index], regionID)
			return true
		})

		b.Run(test.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rts := oracle.ComposeTS(oracle.GetPhysical(time.Now()), 0)
				for storeID, regionIDs := range copyReqIDs {
					batch := 0
					regions := make([]uint64, 0, batchResolvedSize)
					for _, regionID := range regionIDs {
						batch++
						regions = append(regions, regionID)
						if batch == batchResolvedSize {
							eventResolvedBatch := &cdcpb.ChangeDataEvent{
								ResolvedTs: &cdcpb.ResolvedTs{
									Regions: regions,
									Ts:      rts,
								},
							}
							inputs[storeID] <- eventResolvedBatch
							batch = 0
							regions = regions[:0]
						}
					}
					if len(regions) > 0 {
						eventResolvedBatch := &cdcpb.ChangeDataEvent{
							ResolvedTs: &cdcpb.ResolvedTs{
								Regions: regions,
								Ts:      rts,
							},
						}
						inputs[storeID] <- eventResolvedBatch
					}
				}
				count := 0
				for range eventCh {
					count++
					if count == test.regionNum*test.storeNum {
						break
					}
				}
			}
		})

		err := retry.Do(context.Background(), func() error {
			for _, input := range inputs {
				if len(input) != 0 {
					return errors.New("not all events are sent yet")
				}
			}
			return nil
		}, retry.WithBackoffBaseDelay(500), retry.WithBackoffMaxDelay(60*1000), retry.WithMaxTries(1000))
		if err != nil {
			b.Error(err)
		}

		cancel()
		wg.Wait()
	}
}

func benchmarkMultiStoreResolvedTsClient(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	InitWorkerPool()
	go func() {
		RunWorkerPool(ctx) //nolint:errcheck
	}()
	benchmarkMultipleStoreResolvedTs(b)
}

func BenchmarkMultiStoreResolvedTsClient(b *testing.B) {
	benchmarkMultiStoreResolvedTsClient(b)
}

func BenchmarkMultiStoreResolvedTsClientWorkerPool(b *testing.B) {
	hwm := regionWorkerHighWatermark
	lwm := regionWorkerLowWatermark
	regionWorkerHighWatermark = 1000
	regionWorkerLowWatermark = 200
	defer func() {
		regionWorkerHighWatermark = hwm
		regionWorkerLowWatermark = lwm
	}()
	benchmarkMultiStoreResolvedTsClient(b)
}
