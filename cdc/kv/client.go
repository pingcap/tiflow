// Copyright 2019 PingCAP, Inc.
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
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/v4/client"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
	"github.com/pingcap/tidb/store/tikv/tikvrpc"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	gbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout               = 10 * time.Second
	maxRetry                  = 100
	tikvRequestMaxBackoff     = 20000 // Maximum total sleep time(in ms)
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
	grpcConnCount             = 10
)

type singleRegionInfo struct {
	verID        tikv.RegionVerID
	span         util.Span
	ts           uint64
	failStoreIDs map[uint64]struct{}
	rpcCtx       *tikv.RPCContext
}

var (
	metricFeedNotLeaderCounter        = eventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = eventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = eventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = eventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = eventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = eventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
)

func newSingleRegionInfo(verID tikv.RegionVerID, span util.Span, ts uint64, rpcCtx *tikv.RPCContext) singleRegionInfo {
	return singleRegionInfo{
		verID:        verID,
		span:         span,
		ts:           ts,
		failStoreIDs: make(map[uint64]struct{}),
		rpcCtx:       rpcCtx,
	}
}

type regionErrorInfo struct {
	singleRegionInfo
	err error
}

type regionFeedState struct {
	sri       singleRegionInfo
	requestID uint64
	eventCh   chan *cdcpb.Event
	stopped   int32
}

func newRegionFeedState(sri singleRegionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		sri:       sri,
		requestID: requestID,
		eventCh:   make(chan *cdcpb.Event, 16),
		stopped:   0,
	}
}

func (s *regionFeedState) markStopped() {
	atomic.StoreInt32(&s.stopped, 1)
}

func (s *regionFeedState) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) > 0
}

type syncRegionFeedStateMap struct {
	mu            *sync.Mutex
	regionInfoMap map[uint64]*regionFeedState
}

func newSyncRegionFeedStateMap() *syncRegionFeedStateMap {
	return &syncRegionFeedStateMap{
		mu:            &sync.Mutex{},
		regionInfoMap: make(map[uint64]*regionFeedState),
	}
}

func (m *syncRegionFeedStateMap) insert(requestID uint64, state *regionFeedState) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	_, ok := m.regionInfoMap[requestID]
	m.regionInfoMap[requestID] = state
	return ok
}

func (m *syncRegionFeedStateMap) take(requestID uint64) (*regionFeedState, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, ok := m.regionInfoMap[requestID]
	if ok {
		delete(m.regionInfoMap, requestID)
	}
	return state, ok
}

func (m *syncRegionFeedStateMap) takeAll() map[uint64]*regionFeedState {
	m.mu.Lock()
	defer m.mu.Unlock()

	state := m.regionInfoMap
	m.regionInfoMap = make(map[uint64]*regionFeedState)
	return state
}

type connArray struct {
	target string
	index  uint32
	v      []*grpc.ClientConn
}

func newConnArray(ctx context.Context, maxSize uint, addr string) (*connArray, error) {
	a := &connArray{
		target: addr,
		index:  0,
		v:      make([]*grpc.ClientConn, maxSize),
	}
	err := a.Init(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return a, nil
}

func (a *connArray) Init(ctx context.Context) error {
	for i := range a.v {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)

		conn, err := grpc.DialContext(
			ctx,
			a.target,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithInsecure(),
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff: gbackoff.Config{
					BaseDelay:  time.Second,
					Multiplier: 1.1,
					Jitter:     0.1,
					MaxDelay:   3 * time.Second,
				},
				MinConnectTimeout: 3 * time.Second,
			}),
			grpc.WithKeepaliveParams(keepalive.ClientParameters{
				Time:                10 * time.Second,
				Timeout:             3 * time.Second,
				PermitWithoutStream: true,
			}),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(128*1024*1024)),
		)
		cancel()

		if err != nil {
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn
	}
	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			if err != nil {
				log.Warn("close grpc conn", zap.Error(err))
			}
		}
		a.v[i] = nil
	}
}

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	clusterID uint64

	mu struct {
		sync.Mutex
		conns map[string]*connArray
	}

	regionCache *tikv.RegionCache
	kvStorage   tikv.Storage
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(pd pd.Client, kvStorage tikv.Storage) (c *CDCClient, err error) {
	clusterID := pd.GetClusterID(context.Background())
	log.Info("get clusterID", zap.Uint64("id", clusterID))

	c = &CDCClient{
		clusterID:   clusterID,
		pd:          pd,
		kvStorage:   kvStorage,
		regionCache: tikv.NewRegionCache(pd),
		mu: struct {
			sync.Mutex
			conns map[string]*connArray
		}{
			conns: make(map[string]*connArray),
		},
	}

	return
}

// Close CDCClient
func (c *CDCClient) Close() error {
	c.mu.Lock()
	for _, conn := range c.mu.conns {
		conn.Close()
	}
	c.mu.Unlock()
	c.regionCache.Close()

	return nil
}

func (c *CDCClient) getConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conns, ok := c.mu.conns[addr]; ok {
		return conns.Get(), nil
	}
	ca, err := newConnArray(ctx, grpcConnCount, addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.mu.conns[addr] = ca
	return ca.Get(), nil
}

func (c *CDCClient) newStream(ctx context.Context, addr string) (stream cdcpb.ChangeData_EventFeedClient, err error) {
	err = retry.Run(50*time.Millisecond, 20, func() error {
		conn, err := c.getConn(ctx, addr)
		if err != nil {
			return errors.Trace(err)
		}
		client := cdcpb.NewChangeDataClient(conn)
		stream, err = client.EventFeed(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("created stream to store", zap.String("addr", addr))
		return nil
	})
	return
}

// EventFeed divides a EventFeed request on range boundaries and establishes
// a EventFeed to each of the individual region. It streams back result on the
// provided channel.
// The `Start` and `End` field in input span must be memcomparable encoded.
func (c *CDCClient) EventFeed(
	ctx context.Context, span util.Span, ts uint64, eventCh chan<- *model.RegionFeedEvent,
) error {
	s := newEventFeedSession(c, c.regionCache, c.kvStorage, span, eventCh)
	return s.eventFeed(ctx, ts)
}

var currentID uint64 = 0

func allocID() uint64 {
	return atomic.AddUint64(&currentID, 1)
}

type eventFeedSession struct {
	client      *CDCClient
	regionCache *tikv.RegionCache
	kvStorage   tikv.Storage

	// The whole range that is being subscribed.
	totalSpan util.Span

	// The channel to send the processed events.
	eventCh chan<- *model.RegionFeedEvent
	// The channel to put the region that will be sent requests.
	regionCh chan singleRegionInfo
	// The channel to notify that an error is happening, so that the error will be handled and the affected region
	// will be re-requested.
	errCh chan regionErrorInfo
	// The channel to schedule scanning and requesting regions in a specified range.
	requestRangeCh chan rangeRequestTask

	rangeLock *util.RegionRangeLock

	// To identify metrics of different eventFeedSession
	id                string
	regionChSizeGauge prometheus.Gauge
	errChSizeGauge    prometheus.Gauge
	rangeChSizeGauge  prometheus.Gauge
}

type rangeRequestTask struct {
	span util.Span
	ts   uint64
}

func newEventFeedSession(
	client *CDCClient,
	regionCache *tikv.RegionCache,
	kvStorage tikv.Storage,
	totalSpan util.Span,
	eventCh chan<- *model.RegionFeedEvent,
) *eventFeedSession {
	id := strconv.FormatUint(allocID(), 10)
	return &eventFeedSession{
		client:            client,
		regionCache:       regionCache,
		kvStorage:         kvStorage,
		totalSpan:         totalSpan,
		eventCh:           eventCh,
		regionCh:          make(chan singleRegionInfo, 16),
		errCh:             make(chan regionErrorInfo, 16),
		requestRangeCh:    make(chan rangeRequestTask, 16),
		rangeLock:         util.NewRegionRangeLock(),
		id:                strconv.FormatUint(allocID(), 10),
		regionChSizeGauge: clientChannelSize.WithLabelValues(id, "region"),
		errChSizeGauge:    clientChannelSize.WithLabelValues(id, "err"),
		rangeChSizeGauge:  clientChannelSize.WithLabelValues(id, "range"),
	}
}

func (s *eventFeedSession) eventFeed(ctx context.Context, ts uint64) error {
	eventFeedGauge.Inc()
	defer eventFeedGauge.Dec()

	log.Debug("event feed started", zap.Reflect("span", s.totalSpan), zap.Uint64("ts", ts))

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.dispatchRequest(ctx, g)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case task := <-s.requestRangeCh:
				s.rangeChSizeGauge.Dec()
				err := s.divideAndSendEventFeedToRegions(ctx, task.span, task.ts)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case errInfo := <-s.errCh:
				s.errChSizeGauge.Dec()
				s.handleError(ctx, errInfo, true)
			}
		}
	})

	s.requestRangeCh <- rangeRequestTask{span: s.totalSpan, ts: ts}
	s.rangeChSizeGauge.Inc()

	return g.Wait()
}

// scheduleDivideRegionAndRequest schedules a range to be divided by regions, and these regions will be then scheduled
// to send ChangeData requests.
func (s *eventFeedSession) scheduleDivideRegionAndRequest(ctx context.Context, span util.Span, ts uint64, blocking bool) {
	task := rangeRequestTask{span: span, ts: ts}
	if blocking {
		select {
		case s.requestRangeCh <- task:
			s.rangeChSizeGauge.Inc()
		case <-ctx.Done():
		}
	} else {
		// Try to send without blocking. If channel is full, spawn a goroutine to do the blocking sending.
		select {
		case s.requestRangeCh <- task:
			s.rangeChSizeGauge.Inc()
		default:
			go func() {
				select {
				case s.requestRangeCh <- task:
					s.rangeChSizeGauge.Inc()
				case <-ctx.Done():
				}
			}()
		}
	}
}

// scheduleRegionRequest locks the region's range and schedules sending ChangeData request to the region.
func (s *eventFeedSession) scheduleRegionRequest(ctx context.Context, sri singleRegionInfo, blocking bool) {
	handleResult := func(res util.LockRangeResult) {
		switch res.Status {
		case util.LockRangeStatusSuccess:
			if sri.ts > res.CheckpointTs {
				sri.ts = res.CheckpointTs
			}
			select {
			case s.regionCh <- sri:
				s.regionChSizeGauge.Inc()
			case <-ctx.Done():
			}

		case util.LockRangeStatusStale:
			log.Info("request expired",
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Reflect("span", sri.span),
				zap.Reflect("retrySpans", res.RetryRanges))
			for _, r := range res.RetryRanges {
				// This call can be always blocking because if `blocking` is set to false, this will in a new goroutine,
				// so it won't block the caller of `schedulerRegionRequest`.
				s.scheduleDivideRegionAndRequest(ctx, r, sri.ts, true)
			}
		default:
			panic("unreachable")
		}
	}

	res := s.rangeLock.LockRange(sri.span.Start, sri.span.End, sri.verID.GetID(), sri.verID.GetVer())

	if res.Status == util.LockRangeStatusWait {
		if blocking {
			res = res.WaitFn()
		} else {
			go func() {
				res := res.WaitFn()
				handleResult(res)
			}()
			return
		}
	}

	handleResult(res)
}

// onRegionFail handles a region's failure, which means, unlock the region's range and send the error to the errCh for
// error handling.
// CAUTION: Note that this should only be called in a context that the region has locked it's range.
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo, blocking bool) error {
	log.Debug("region failed", zap.Uint64("regionID", errorInfo.verID.GetID()), zap.Error(errorInfo.err))
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End, errorInfo.verID.GetVer(), errorInfo.ts)
	if blocking {
		select {
		case s.errCh <- errorInfo:
			s.errChSizeGauge.Inc()
		case <-ctx.Done():
			return ctx.Err()
		}
	} else {
		select {
		case s.errCh <- errorInfo:
			s.errChSizeGauge.Inc()
		default:
			go func() {
				select {
				case s.errCh <- errorInfo:
					s.errChSizeGauge.Inc()
				case <-ctx.Done():
				}
			}()
		}
	}
	return nil
}

// dispatchRequest manages a set of streams and dispatch event feed requests
// to these streams. Streams to each store will be created on need. After
// establishing new stream, a goroutine will be spawned to handle events from
// the stream.
// Regions from `regionCh` will be connected. If any error happens to a
// region, the error will be send to `errCh` and the receiver of `errCh` is
// responsible for handling the error.
func (s *eventFeedSession) dispatchRequest(
	ctx context.Context,
	g *errgroup.Group,
) error {
	streams := make(map[string]cdcpb.ChangeData_EventFeedClient)
	// Stores pending regions info for each stream. After sending a new request, the region info wil be put to the map,
	// and it will be loaded by the receiver thread when it receives the first response from that region. We need this
	// to pass the region info to the receiver since the region info cannot be inferred from the response from TiKV.
	storePendingRegions := make(map[string]*syncRegionFeedStateMap)

MainLoop:
	for {
		// Note that when a region is received from the channel, it's range has been already locked.
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sri = <-s.regionCh:
			s.regionChSizeGauge.Dec()
		}

		log.Debug("dispatching region", zap.Uint64("regionID", sri.verID.GetID()))

		// Loop for retrying in case the stream has disconnected.
		// TODO: Should we break if retries and fails too many times?
		for {
			rpcCtx, err := s.getRPCContextForRegion(ctx, sri.verID)
			if err != nil {
				return errors.Trace(err)
			}
			if rpcCtx == nil {
				// The region info is invalid. Retry the span.
				log.Info("cannot get rpcCtx, retry span",
					zap.Uint64("regionID", sri.verID.GetID()),
					zap.Reflect("span", sri.span))
				err = s.onRegionFail(ctx, regionErrorInfo{
					singleRegionInfo: sri,
					err: &rpcCtxUnavailableErr{
						verID: sri.verID,
					},
				}, false)
				if err != nil {
					return errors.Trace(err)
				}
				continue MainLoop
			}
			sri.rpcCtx = rpcCtx

			requestID := allocID()

			req := &cdcpb.ChangeDataRequest{
				Header: &cdcpb.Header{
					ClusterId: s.client.clusterID,
				},
				RegionId:     rpcCtx.Meta.GetId(),
				RequestId:    requestID,
				RegionEpoch:  rpcCtx.Meta.RegionEpoch,
				CheckpointTs: sri.ts,
				StartKey:     sri.span.Start,
				EndKey:       sri.span.End,
			}

			// The receiver thread need to know the span, which is only known in the sender thread. So create the
			// receiver thread for region here so that it can know the span.
			// TODO: Find a better way to handle this.
			// TODO: Make sure there will not be goroutine leak.
			// TODO: Here we use region id to index the regionInfo. However, in case that region merge is enabled, there
			// may be multiple streams to the same regions. Maybe we need to add a requestID field to the protocol for it.

			// Get region info collection of the addr
			pendingRegions, ok := storePendingRegions[rpcCtx.Addr]
			if !ok {
				pendingRegions = newSyncRegionFeedStateMap()
				storePendingRegions[rpcCtx.Addr] = pendingRegions
			}

			state := newRegionFeedState(sri, requestID)
			pendingRegions.insert(requestID, state)

			stream, ok := streams[rpcCtx.Addr]
			// Establish the stream if it has not been connected yet.
			if !ok {
				stream, err = s.client.newStream(ctx, rpcCtx.Addr)
				if err != nil {
					// if get stream failed, maybe the store is down permanently, we should try to relocate the active store
					log.Warn("get grpc stream client failed",
						zap.Uint64("regionID", sri.verID.GetID()), zap.Uint64("requestID", requestID), zap.Error(err))
					bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
					s.client.regionCache.OnSendFail(bo, rpcCtx, needReloadRegion(sri.failStoreIDs, rpcCtx), err)
					// Delete the pendingRegion info from `pendingRegions` and retry connecting and sending the request.
					pendingRegions.take(requestID)
					continue
				}
				streams[rpcCtx.Addr] = stream

				g.Go(func() error {
					return s.receiveFromStream(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream, pendingRegions)
				})
			}

			log.Info("start new request", zap.Reflect("request", req), zap.String("addr", rpcCtx.Addr))
			err = stream.Send(req)

			// If Send error, the receiver should have received error too or will receive error soon. So we doesn't need
			// to do extra work here.
			if err != nil {

				log.Error("send request to stream failed",
					zap.String("addr", rpcCtx.Addr),
					zap.Uint64("storeID", getStoreID(rpcCtx)),
					zap.Uint64("regionID", sri.verID.GetID()),
					zap.Uint64("requestID", requestID),
					zap.Error(err))
				err1 := stream.CloseSend()
				if err1 != nil {
					log.Error("failed to close stream", zap.Error(err1))
				}
				// Delete the stream from the map so that the next time the store is accessed, the stream will be
				// re-established.
				delete(streams, rpcCtx.Addr)
				// Delete `pendingRegions` from `storePendingRegions` so that the next time a region of this store is
				// requested, it will create a new one. So if the `receiveFromStream` goroutine tries to stop all
				// pending regions, the new pending regions that are requested after reconnecting won't be stopped
				// incorrectly.
				delete(storePendingRegions, rpcCtx.Addr)

				// Remove the region from pendingRegions. If it's already removed, it should be already retried by
				// `receiveFromStream`, so no need to retry here.
				_, ok := pendingRegions.take(requestID)
				if !ok {
					break
				}

				// Wait for a while and retry sending the request
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				// Break if ctx has been canceled.
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				continue
			}

			break
		}
	}
}

func needReloadRegion(failStoreIDs map[uint64]struct{}, rpcCtx *tikv.RPCContext) (need bool) {
	failStoreIDs[getStoreID(rpcCtx)] = struct{}{}
	need = len(failStoreIDs) == len(rpcCtx.Meta.GetPeers())
	if need {
		for k := range failStoreIDs {
			delete(failStoreIDs, k)
		}
	}
	return
}

// partialRegionFeed establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection. If any error happens (region split, leader change, etc), the region
// and error info will be sent to `errCh`, and the receiver of `errCh` is
// responsible for handling the error and re-establish the connection to the region.
func (s *eventFeedSession) partialRegionFeed(
	ctx context.Context,
	state *regionFeedState,
) error {
	receiver := state.eventCh
	defer func() {
		state.markStopped()
		// Workaround to avoid remaining messages in the channel blocks the receiver thread.
		// TODO: Find a better solution.
		timer := time.After(time.Second * 2)
		for {
			select {
			case <-receiver:
			case <-timer:
				return
			}
		}
	}()

	ts := state.sri.ts
	rl := rate.NewLimiter(0.1, 5)

	if !rl.Allow() {
		return errors.New("partialRegionFeed exceeds rate limit")
	}

	maxTs, err := s.singleEventFeed(ctx, state.sri.verID.GetID(), state.sri.span, state.sri.ts, receiver)
	log.Debug("singleEventFeed quit")

	if err == nil || errors.Cause(err) == context.Canceled {
		return nil
	}

	if maxTs > ts {
		ts = maxTs
	}

	log.Info("EventFeed disconnected",
		zap.Reflect("regionID", state.sri.verID.GetID()),
		zap.Reflect("span", state.sri.span),
		zap.Uint64("checkpoint", ts),
		zap.Error(err))

	state.sri.ts = ts

	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
	state.markStopped()
	return s.onRegionFail(ctx, regionErrorInfo{
		singleRegionInfo: state.sri,
		err:              err,
	}, false)
}

// divideAndSendEventFeedToRegions split up the input span into spans aligned
// to region boundaries. When region merging happens, it's possible that it
// will produce some overlapping spans.
func (s *eventFeedSession) divideAndSendEventFeedToRegions(
	ctx context.Context, span util.Span, ts uint64,
) error {
	limit := 20

	nextSpan := span
	captureID := util.CaptureIDFromCtx(ctx)

	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Run(50*time.Millisecond, maxRetry,
			func() error {
				scanT0 := time.Now()
				bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
				regions, err = s.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.Start, nextSpan.End, limit)
				scanRegionsDuration.WithLabelValues(captureID).Observe(time.Since(scanT0).Seconds())
				if err != nil {
					return errors.Trace(err)
				}
				metas := make([]*metapb.Region, 0, len(regions))
				for _, region := range regions {
					if region.GetMeta() == nil {
						err = errors.New("meta not exists in region")
						log.Warn("batch load region", zap.Reflect("span", nextSpan), zap.Error(err))
						return err
					}
					metas = append(metas, region.GetMeta())
				}
				if !util.CheckRegionsLeftCover(metas, nextSpan) {
					err = errors.Errorf("regions not completely left cover span, span %v regions: %v", nextSpan, metas)
					log.Warn("ScanRegions", zap.Reflect("span", nextSpan), zap.Reflect("regions", metas), zap.Error(err))
					return err
				}
				log.Debug("ScanRegions", zap.Reflect("span", nextSpan), zap.Reflect("regions", metas))
				return nil
			})

		if retryErr != nil {
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := util.Intersect(s.totalSpan, util.Span{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("get partialSpan", zap.Reflect("span", partialSpan), zap.Uint64("regionID", region.Id))

			nextSpan.Start = region.EndKey

			sri := newSingleRegionInfo(tiRegion.VerID(), partialSpan, ts, nil)
			s.scheduleRegionRequest(ctx, sri, true)
			log.Debug("partialSpan scheduled", zap.Reflect("span", partialSpan), zap.Uint64("regionID", region.Id))

			// return if no more regions
			if util.EndCompare(nextSpan.Start, span.End) >= 0 {
				return nil
			}
		}
	}
}

// handleError handles error returned by a region. If some new EventFeed connection should be established, the region
// info will be sent to `regionCh`.
// CAUTION: Note that this should only be invoked in a context that the region is not locked, otherwise use onRegionFail
// instead.
func (s *eventFeedSession) handleError(ctx context.Context, errInfo regionErrorInfo, blocking bool) {
	err := errInfo.err
	switch eerr := errors.Cause(err).(type) {
	case *eventError:
		innerErr := eerr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			// TODO: Handle the case that notleader.GetLeader() is nil.
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader().GetStoreId(), errInfo.rpcCtx.PeerIdx)
		} else if innerErr.GetEpochNotMatch() != nil {
			// TODO: If only confver is updated, we don't need to reload the region from region cache.
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts, blocking)
			return
		} else if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts, blocking)
			return
		} else if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			metricFeedDuplicateRequestCounter.Inc()
			log.Error("tikv reported duplicated request to the same region, which is not expected",
				zap.Uint64("regionID", duplicatedRequest.RegionId))
			return
		} else {
			metricFeedUnknownErrorCounter.Inc()
			log.Warn("receive empty or unknown error msg", zap.Stringer("error", innerErr))
		}
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts, blocking)
		return
	default:
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		if errInfo.rpcCtx.Meta != nil {
			s.regionCache.OnSendFail(bo, errInfo.rpcCtx, needReloadRegion(errInfo.failStoreIDs, errInfo.rpcCtx), err)
		}
	}

	s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo, blocking)
}

func (s *eventFeedSession) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, id, tidbkv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rpcCtx, nil
}

func (s *eventFeedSession) receiveFromStream(
	ctx context.Context,
	g *errgroup.Group,
	addr string,
	storeID uint64,
	stream cdcpb.ChangeData_EventFeedClient,
	pendingRegions *syncRegionFeedStateMap,
) error {
	// Cancel the pending regions if the stream failed. Otherwise it will remain unhandled in the pendingRegions list
	// however not registered in the new reconnected stream.
	defer func() {
		log.Info("stream to store closed", zap.String("addr", addr), zap.Uint64("storeID", storeID))

		remainingRegions := pendingRegions.takeAll()

		for _, state := range remainingRegions {
			err := s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: state.sri,
				err:              errors.New("pending region cancelled due to stream disconnecting"),
			}, false)
			if err != nil {
				// The only possible is that the ctx is cancelled. Simply return.
				return
			}
		}
	}()

	// Each region has it's own goroutine to handle its messages. `regionStates` stores states of these regions.
	regionStates := make(map[uint64]*regionFeedState)

	for {
		cevent, err := stream.Recv()

		// TODO: Should we have better way to handle the errors?
		if err == io.EOF {
			for _, state := range regionStates {
				close(state.eventCh)
			}
			return nil
		}
		if err != nil {
			log.Error(
				"failed to receive from stream",
				zap.String("addr", addr),
				zap.Uint64("storeID", storeID),
				zap.Error(err))

			for _, state := range regionStates {
				select {
				case state.eventCh <- nil:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// Do no return error but gracefully stop the goroutine here. Then the whole job will not be canceled and
			// connection will be retried.
			return nil
		}

		for _, event := range cevent.Events {
			state, ok := regionStates[event.RegionId]
			// Every region's range is locked before sending requests and unlocked after exiting, and the requestID
			// is allocated while holding the range lock. Therefore the requestID is always incrementing. If a region
			// is receiving messages with different requestID, only the messages with the larges requestID is valid.
			isNewSubscription := !ok
			if ok {
				if state.requestID < event.RequestId {
					log.Debug("region state entry will be replaced because received message of newer requestID",
						zap.Uint64("regionID", event.RegionId),
						zap.Uint64("oldRequestID", state.requestID),
						zap.Uint64("requestID", event.RequestId),
						zap.String("addr", addr))
					isNewSubscription = true
				} else if state.requestID > event.RequestId {
					log.Warn("drop event due to event belongs to a stale request",
						zap.Uint64("regionID", event.RegionId),
						zap.Uint64("requestID", event.RequestId),
						zap.Uint64("currRequestID", state.requestID),
						zap.String("addr", addr))
					continue
				}
			}

			if isNewSubscription {
				// It's the first response for this region. If the region is newly connected, the region info should
				// have been put in `pendingRegions`. So here we load the region info from `pendingRegions` and start
				// a new goroutine to handle messages from this region.
				// Firstly load the region info.
				state, ok = pendingRegions.take(event.RequestId)
				if !ok {
					log.Error("received an event but neither pending region nor running region was found",
						zap.Uint64("regionID", event.RegionId),
						zap.Uint64("requestID", event.RequestId),
						zap.String("addr", addr))
					return errors.Errorf("received event regionID %v, requestID %v from %v but neither pending "+
						"region nor running region was found", event.RegionId, event.RequestId, addr)
				}

				// Then spawn the goroutine to process messages of this region.
				regionStates[event.RegionId] = state

				g.Go(func() error {
					return s.partialRegionFeed(ctx, state)
				})
			} else if state.isStopped() {
				log.Warn("drop event due to region feed stopped",
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", addr))
				continue
			}

			select {
			case state.eventCh <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// singleEventFeed handles events of a single EventFeed stream.
// Results will be send to eventCh
// EventFeed RPC will not return checkpoint event directly
// Resolved event is generate while there's not non-match pre-write
// Return the maximum checkpoint
func (s *eventFeedSession) singleEventFeed(
	ctx context.Context,
	regionID uint64,
	span util.Span,
	checkpointTs uint64,
	receiverCh <-chan *cdcpb.Event,
) (uint64, error) {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricEventSize := eventSize.WithLabelValues(captureID)
	metricPullEventInitializedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_INITIALIZED.String(), captureID, changefeedID)
	metricPullEventCommittedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMITTED.String(), captureID, changefeedID)
	metricPullEventCommitCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMIT.String(), captureID, changefeedID)
	metricPullEventPrewriteCounter := pullEventCounter.WithLabelValues(cdcpb.Event_PREWRITE.String(), captureID, changefeedID)
	metricPullEventRollbackCounter := pullEventCounter.WithLabelValues(cdcpb.Event_ROLLBACK.String(), captureID, changefeedID)
	metricSendEventResolvedCounter := sendEventCounter.WithLabelValues("native resolved", captureID, changefeedID)
	metricSendEventCommitCounter := sendEventCounter.WithLabelValues("commit", captureID, changefeedID)
	metricSendEventCommittedCounter := sendEventCounter.WithLabelValues("committed", captureID, changefeedID)

	var initialized uint32

	matcher := newMatcher()
	advanceCheckTicker := time.NewTicker(time.Second * 5)
	defer advanceCheckTicker.Stop()
	lastReceivedEventTime := time.Now()
	startFeedTime := time.Now()
	var lastResolvedTs uint64

	for {
		var event *cdcpb.Event
		var ok bool
		select {
		case <-ctx.Done():
			return atomic.LoadUint64(&checkpointTs), ctx.Err()
		case <-advanceCheckTicker.C:
			if time.Since(startFeedTime) < 20*time.Second {
				continue
			}
			sinceLastEvent := time.Since(lastReceivedEventTime)
			if sinceLastEvent > time.Second*20 {
				log.Warn("region not receiving event from tikv for too long time",
					zap.Uint64("regionID", regionID), zap.Reflect("span", span), zap.Duration("duration", sinceLastEvent))
			}
			version, err := s.kvStorage.CurrentVersion()
			if err != nil {
				log.Warn("failed to get current version from PD", zap.Error(err))
				continue
			}
			currentTimeFromPD := oracle.GetTimeFromTS(version.Ver)
			sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(lastResolvedTs))
			if sinceLastResolvedTs > time.Second*20 {
				log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
					zap.Uint64("regionID", regionID), zap.Reflect("span", span), zap.Duration("duration", sinceLastResolvedTs), zap.Uint64("lastResolvedTs", lastResolvedTs))
				maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTimeFromPD.Add(-10*time.Second)), 0)
				err = s.resolveLock(ctx, regionID, maxVersion)
				if err != nil {
					log.Warn("failed to resolve lock", zap.Uint64("regionID", regionID), zap.Error(err))
					continue
				}
			}
			continue
		case event, ok = <-receiverCh:
		}
		if !ok {
			log.Debug("singleEventFeed receiver closed")
			return atomic.LoadUint64(&checkpointTs), nil
		}

		if event == nil {
			log.Debug("singleEventFeed closed by error")
			return atomic.LoadUint64(&checkpointTs), errors.New("single event feed aborted")
		}
		lastReceivedEventTime = time.Now()

		metricEventSize.Observe(float64(event.Event.Size()))
		switch x := event.Event.(type) {
		case *cdcpb.Event_Entries_:
			for _, entry := range x.Entries.GetEntries() {
				switch entry.Type {
				case cdcpb.Event_INITIALIZED:
					metricPullEventInitializedCounter.Inc()
					atomic.StoreUint32(&initialized, 1)
					for _, cacheEntry := range matcher.cachedCommit {
						value, ok := matcher.matchRow(cacheEntry)
						if !ok {
							// when cdc receives a commit log without a corresponding
							// prewrite log before initialized, a committed log  with
							// the same key and start-ts must have been received.
							log.Info("ignore commit event without prewrite",
								zap.Binary("key", cacheEntry.GetKey()),
								zap.Uint64("ts", cacheEntry.GetStartTs()))
							continue
						}
						revent, err := assembleCommitEvent(cacheEntry, value)
						if err != nil {
							return atomic.LoadUint64(&checkpointTs), errors.Trace(err)
						}
						select {
						case s.eventCh <- revent:
							metricSendEventCommitCounter.Inc()
						case <-ctx.Done():
							return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
						}
					}
					matcher.clearCacheCommit()
				case cdcpb.Event_COMMITTED:
					metricPullEventCommittedCounter.Inc()
					var opType model.OpType
					switch entry.GetOpType() {
					case cdcpb.Event_Row_DELETE:
						opType = model.OpTypeDelete
					case cdcpb.Event_Row_PUT:
						opType = model.OpTypePut
					default:
						return atomic.LoadUint64(&checkpointTs), errors.Errorf("unknown tp: %v", entry.GetOpType())
					}

					revent := &model.RegionFeedEvent{
						Val: &model.RawKVEntry{
							OpType: opType,
							Key:    entry.Key,
							Value:  entry.GetValue(),
							Ts:     entry.CommitTs,
						},
					}
					select {
					case s.eventCh <- revent:
						metricSendEventCommittedCounter.Inc()
					case <-ctx.Done():
						return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
					}
				case cdcpb.Event_PREWRITE:
					metricPullEventPrewriteCounter.Inc()
					matcher.putPrewriteRow(entry)
				case cdcpb.Event_COMMIT:
					metricPullEventCommitCounter.Inc()
					// emit a value
					value, ok := matcher.matchRow(entry)
					if !ok {
						if atomic.LoadUint32(&initialized) == 0 {
							matcher.cacheCommitRow(entry)
							continue
						}
						return atomic.LoadUint64(&checkpointTs),
							errors.Errorf("prewrite not match, key: %b, start-ts: %d",
								entry.GetKey(), entry.GetStartTs())
					}

					revent, err := assembleCommitEvent(entry, value)
					if err != nil {
						return atomic.LoadUint64(&checkpointTs), errors.Trace(err)
					}

					select {
					case s.eventCh <- revent:
						metricSendEventCommitCounter.Inc()
					case <-ctx.Done():
						return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
					}
				case cdcpb.Event_ROLLBACK:
					metricPullEventRollbackCounter.Inc()
					matcher.rollbackRow(entry)
				}
			}
		case *cdcpb.Event_Admin_:
			log.Info("receive admin event", zap.Stringer("event", event))
		case *cdcpb.Event_Error:
			return atomic.LoadUint64(&checkpointTs), errors.Trace(&eventError{err: x.Error})
		case *cdcpb.Event_ResolvedTs:
			lastResolvedTs = x.ResolvedTs
			if atomic.LoadUint32(&initialized) == 0 {
				continue
			}
			// emit a checkpointTs
			revent := &model.RegionFeedEvent{
				Resolved: &model.ResolvedSpan{
					Span:       span,
					ResolvedTs: x.ResolvedTs,
				},
			}

			updateCheckpointTS(&checkpointTs, x.ResolvedTs)

			select {
			case s.eventCh <- revent:
				metricSendEventResolvedCounter.Inc()
			case <-ctx.Done():
				return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
			}
		}

	}
}

const scanLockLimit = 1024

func (s *eventFeedSession) resolveLock(ctx context.Context, regionID uint64, maxVersion uint64) error {
	// TODO test whether this function will kill active transaction
	req := tikvrpc.NewRequest(tikvrpc.CmdScanLock, &kvrpcpb.ScanLockRequest{
		MaxVersion: maxVersion,
		Limit:      scanLockLimit,
	})

	bo := tikv.NewBackoffer(ctx, tikv.GcResolveLockMaxBackoff)
	var loc *tikv.KeyLocation
	var key []byte
	flushRegion := func() error {
		var err error
		loc, err = s.kvStorage.GetRegionCache().LocateRegionByID(bo, regionID)
		if err != nil {
			return err
		}
		key = loc.StartKey
		return nil
	}
	if err := flushRegion(); err != nil {
		return errors.Trace(err)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		req.ScanLock().StartKey = key
		resp, err := s.kvStorage.SendReq(bo, req, loc.Region, tikv.ReadTimeoutMedium)
		if err != nil {
			return errors.Trace(err)
		}
		regionErr, err := resp.GetRegionError()
		if err != nil {
			return errors.Trace(err)
		}
		if regionErr != nil {
			err = bo.Backoff(tikv.BoRegionMiss, errors.New(regionErr.String()))
			if err != nil {
				return errors.Trace(err)
			}
			if err := flushRegion(); err != nil {
				return errors.Trace(err)
			}
			continue
		}
		if resp.Resp == nil {
			return errors.Trace(tikv.ErrBodyMissing)
		}
		locksResp := resp.Resp.(*kvrpcpb.ScanLockResponse)
		if locksResp.GetError() != nil {
			return errors.Errorf("unexpected scanlock error: %s", locksResp)
		}
		locksInfo := locksResp.GetLocks()
		locks := make([]*tikv.Lock, len(locksInfo))
		for i := range locksInfo {
			locks[i] = tikv.NewLock(locksInfo[i])
		}

		_, _, err1 := s.kvStorage.GetLockResolver().ResolveLocks(bo, 0, locks)
		if err1 != nil {
			return errors.Trace(err1)
		}
		if len(locks) < scanLockLimit {
			key = loc.EndKey
		} else {
			key = locks[len(locks)-1].Key
		}

		if len(key) == 0 || (len(loc.EndKey) != 0 && bytes.Compare(key, loc.EndKey) >= 0) {
			break
		}
		bo = tikv.NewBackoffer(ctx, tikv.GcResolveLockMaxBackoff)
	}
	log.Info("resolve lock successfully", zap.Uint64("regionID", regionID), zap.Uint64("maxVersion", maxVersion))
	return nil
}

func assembleCommitEvent(entry *cdcpb.Event_Row, value []byte) (*model.RegionFeedEvent, error) {
	var opType model.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = model.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = model.OpTypePut
	default:
		return nil, errors.Errorf("unknow tp: %v", entry.GetOpType())
	}

	revent := &model.RegionFeedEvent{
		Val: &model.RawKVEntry{
			OpType: opType,
			Key:    entry.Key,
			Value:  value,
			Ts:     entry.CommitTs,
		},
	}
	return revent, nil
}

func updateCheckpointTS(checkpointTs *uint64, newValue uint64) {
	for {
		oldValue := atomic.LoadUint64(checkpointTs)
		if oldValue >= newValue || atomic.CompareAndSwapUint64(checkpointTs, oldValue, newValue) {
			return
		}
	}
}

// eventError wrap cdcpb.Event_Error to implements error interface.
type eventError struct {
	err *cdcpb.Error
}

// Error implement error interface.
func (e *eventError) Error() string {
	return e.err.String()
}

type rpcCtxUnavailableErr struct {
	verID tikv.RegionVerID
}

func (e *rpcCtxUnavailableErr) Error() string {
	return fmt.Sprintf("cannot get rpcCtx for region %v. ver:%v, confver:%v",
		e.verID.GetID(), e.verID.GetVer(), e.verID.GetConfVer())
}

func getStoreID(rpcCtx *tikv.RPCContext) uint64 {
	if rpcCtx != nil && rpcCtx.Peer != nil {
		return rpcCtx.Peer.GetStoreId()
	}
	return 0
}
