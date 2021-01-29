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
	"io"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/prometheus/client_golang/prometheus"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	gbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout               = 10 * time.Second
	maxRetry                  = 100
	tikvRequestMaxBackoff     = 20000   // Maximum total sleep time(in ms)
	grpcInitialWindowSize     = 1 << 30 // The value for initial window size on a stream
	grpcInitialConnWindowSize = 1 << 30 // The value for initial window size on a connection
	grpcMaxCallRecvMsgSize    = 1 << 30 // The maximum message size the client can receive
	grpcConnCount             = 10

	// The threshold of warning a message is too large. TiKV split events into 6MB per-message.
	warnRecvMsgSizeThreshold = 12 * 1024 * 1024
)

type singleRegionInfo struct {
	verID        tikv.RegionVerID
	span         regionspan.ComparableSpan
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

var (
	// unreachable error, only used in unit test
	errUnreachable = errors.New("kv client unreachable error")
	logPanic       = log.Panic
)

func newSingleRegionInfo(verID tikv.RegionVerID, span regionspan.ComparableSpan, ts uint64, rpcCtx *tikv.RPCContext) singleRegionInfo {
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

type regionEvent struct {
	changeEvent *cdcpb.Event
	resolvedTs  *cdcpb.ResolvedTs
}

type regionStatefulEvent struct {
	changeEvent *cdcpb.Event
	resolvedTs  *cdcpb.ResolvedTs
	state       *regionFeedState
}

type regionFeedState struct {
	sri                   singleRegionInfo
	requestID             uint64
	regionEventCh         chan *regionEvent
	stopped               int32
	initialized           bool
	matcher               *matcher
	startFeedTime         time.Time
	lastReceivedEventTime time.Time
	lastResolvedTs        uint64
}

func newRegionFeedState(sri singleRegionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		sri:           sri,
		requestID:     requestID,
		regionEventCh: make(chan *regionEvent, 16),
		stopped:       0,
	}
}

func (s *regionFeedState) start() {
	s.startFeedTime = time.Now()
	s.lastResolvedTs = s.sri.ts
	s.lastReceivedEventTime = time.Now()
	s.matcher = newMatcher()
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
	credential *security.Credential
	target     string
	index      uint32
	v          []*grpc.ClientConn
}

func newConnArray(ctx context.Context, maxSize uint, addr string, credential *security.Credential) (*connArray, error) {
	a := &connArray{
		target:     addr,
		credential: credential,
		index:      0,
		v:          make([]*grpc.ClientConn, maxSize),
	}
	err := a.Init(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return a, nil
}

func (a *connArray) Init(ctx context.Context) error {
	grpcTLSOption, err := a.credential.ToGRPCDialOption()
	if err != nil {
		return errors.Trace(err)
	}
	for i := range a.v {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)

		conn, err := grpc.DialContext(
			ctx,
			a.target,
			grpcTLSOption,
			grpc.WithInitialWindowSize(grpcInitialWindowSize),
			grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
			grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
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
		)
		cancel()

		if err != nil {
			a.Close()
			return cerror.WrapError(cerror.ErrGRPCDialFailed, err)
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

type regionEventFeedLimiters struct {
	sync.Mutex
	// TODO replace with a LRU cache.
	limiters map[uint64]*rate.Limiter
}

var defaultRegionEventFeedLimiters *regionEventFeedLimiters = &regionEventFeedLimiters{
	limiters: make(map[uint64]*rate.Limiter),
}

func (rl *regionEventFeedLimiters) getLimiter(regionID uint64) *rate.Limiter {
	var limiter *rate.Limiter
	var ok bool

	rl.Lock()
	limiter, ok = rl.limiters[regionID]
	if !ok {
		// In most cases, region replica count is 3.
		replicaCount := 3
		limiter = rate.NewLimiter(rate.Every(100*time.Millisecond), replicaCount)
		rl.limiters[regionID] = limiter
	}
	rl.Unlock()
	return limiter
}

// CDCKVClient is an interface to receives kv changed logs from TiKV
type CDCKVClient interface {
	EventFeed(
		ctx context.Context,
		span regionspan.ComparableSpan,
		ts uint64,
		enableOldValue bool,
		enableRegionWorker bool,
		lockResolver txnutil.LockResolver,
		isPullerInit PullerInitialization,
		eventCh chan<- *model.RegionFeedEvent,
	) error
	Close() error
}

// NewCDCKVClient is the constructor of CDC KV client
var NewCDCKVClient func(
	ctx context.Context,
	pd pd.Client,
	kvStorage tikv.Storage,
	credential *security.Credential,
) CDCKVClient = NewCDCClient

// CDCClient to get events from TiKV
type CDCClient struct {
	pd         pd.Client
	credential *security.Credential

	clusterID uint64

	mu struct {
		sync.Mutex
		conns map[string]*connArray
	}

	regionCache *tikv.RegionCache
	kvStorage   tikv.Storage

	regionLimiters *regionEventFeedLimiters
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(ctx context.Context, pd pd.Client, kvStorage tikv.Storage, credential *security.Credential) (c CDCKVClient) {
	clusterID := pd.GetClusterID(ctx)
	log.Info("get clusterID", zap.Uint64("id", clusterID))

	c = &CDCClient{
		clusterID:   clusterID,
		pd:          pd,
		credential:  credential,
		kvStorage:   kvStorage,
		regionCache: tikv.NewRegionCache(pd),
		mu: struct {
			sync.Mutex
			conns map[string]*connArray
		}{
			conns: make(map[string]*connArray),
		},
		regionLimiters: defaultRegionEventFeedLimiters,
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
	ca, err := newConnArray(ctx, grpcConnCount, addr, c.credential)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.mu.conns[addr] = ca
	return ca.Get(), nil
}

func (c *CDCClient) getRegionLimiter(regionID uint64) *rate.Limiter {
	return c.regionLimiters.getLimiter(regionID)
}

func (c *CDCClient) newStream(ctx context.Context, addr string, storeID uint64) (stream cdcpb.ChangeData_EventFeedClient, err error) {
	err = retry.Run(50*time.Millisecond, 3, func() error {
		conn, err := c.getConn(ctx, addr)
		if err != nil {
			log.Info("get connection to store failed, retry later", zap.String("addr", addr), zap.Error(err))
			return errors.Trace(err)
		}
		err = version.CheckStoreVersion(ctx, c.pd, storeID)
		if err != nil {
			// TODO: we don't close gPRC conn here, let it goes into TransientFailure
			// state. If the store recovers, the gPRC conn can be reused. But if
			// store goes away forever, the conn will be leaked, we need a better
			// connection pool.
			log.Error("check tikv version failed", zap.Error(err), zap.Uint64("storeID", storeID))
			return errors.Trace(err)
		}
		client := cdcpb.NewChangeDataClient(conn)
		stream, err = client.EventFeed(ctx)
		if err != nil {
			// TODO: we don't close gPRC conn here, let it goes into TransientFailure
			// state. If the store recovers, the gPRC conn can be reused. But if
			// store goes away forever, the conn will be leaked, we need a better
			// connection pool.
			err = cerror.WrapError(cerror.ErrTiKVEventFeed, err)
			log.Info("establish stream to store failed, retry later", zap.String("addr", addr), zap.Error(err))
			return err
		}
		log.Debug("created stream to store", zap.String("addr", addr))
		return nil
	})
	return
}

// PullerInitialization is a workaround to solved cyclic import.
type PullerInitialization interface {
	IsInitialized() bool
}

// EventFeed divides a EventFeed request on range boundaries and establishes
// a EventFeed to each of the individual region. It streams back result on the
// provided channel.
// The `Start` and `End` field in input span must be memcomparable encoded.
func (c *CDCClient) EventFeed(
	ctx context.Context, span regionspan.ComparableSpan, ts uint64,
	enableOldValue bool,
	enableRegionWorker bool,
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	s := newEventFeedSession(c, c.regionCache, c.kvStorage, span,
		lockResolver, isPullerInit,
		enableOldValue, enableRegionWorker, ts, eventCh)
	return s.eventFeed(ctx, ts)
}

var currentID uint64 = 0

func allocID() uint64 {
	return atomic.AddUint64(&currentID, 1)
}

// used in test only
func currentRequestID() uint64 {
	return atomic.LoadUint64(&currentID)
}

type eventFeedSession struct {
	client      *CDCClient
	regionCache *tikv.RegionCache
	kvStorage   tikv.Storage

	lockResolver txnutil.LockResolver
	isPullerInit PullerInitialization

	// The whole range that is being subscribed.
	totalSpan regionspan.ComparableSpan

	// The channel to send the processed events.
	eventCh chan<- *model.RegionFeedEvent
	// The channel to put the region that will be sent requests.
	regionCh chan singleRegionInfo
	// The channel to notify that an error is happening, so that the error will be handled and the affected region
	// will be re-requested.
	errCh chan regionErrorInfo
	// The channel to schedule scanning and requesting regions in a specified range.
	requestRangeCh chan rangeRequestTask

	rangeLock          *regionspan.RegionRangeLock
	enableOldValue     bool
	enableRegionWorker bool

	// To identify metrics of different eventFeedSession
	id                string
	regionChSizeGauge prometheus.Gauge
	errChSizeGauge    prometheus.Gauge
	rangeChSizeGauge  prometheus.Gauge

	streams     map[string]cdcpb.ChangeData_EventFeedClient
	streamsLock sync.RWMutex

	workers     map[string]*regionWorker
	workersLock sync.RWMutex
}

type rangeRequestTask struct {
	span regionspan.ComparableSpan
	ts   uint64
}

func newEventFeedSession(
	client *CDCClient,
	regionCache *tikv.RegionCache,
	kvStorage tikv.Storage,
	totalSpan regionspan.ComparableSpan,
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	enableOldValue bool,
	enableRegionWorker bool,
	startTs uint64,
	eventCh chan<- *model.RegionFeedEvent,
) *eventFeedSession {
	id := strconv.FormatUint(allocID(), 10)
	return &eventFeedSession{
		client:             client,
		regionCache:        regionCache,
		kvStorage:          kvStorage,
		totalSpan:          totalSpan,
		eventCh:            eventCh,
		regionCh:           make(chan singleRegionInfo, 16),
		errCh:              make(chan regionErrorInfo, 16),
		requestRangeCh:     make(chan rangeRequestTask, 16),
		rangeLock:          regionspan.NewRegionRangeLock(totalSpan.Start, totalSpan.End, startTs),
		enableOldValue:     enableOldValue,
		enableRegionWorker: enableRegionWorker,
		lockResolver:       lockResolver,
		isPullerInit:       isPullerInit,
		id:                 id,
		regionChSizeGauge:  clientChannelSize.WithLabelValues(id, "region"),
		errChSizeGauge:     clientChannelSize.WithLabelValues(id, "err"),
		rangeChSizeGauge:   clientChannelSize.WithLabelValues(id, "range"),
		streams:            make(map[string]cdcpb.ChangeData_EventFeedClient),
		workers:            make(map[string]*regionWorker),
	}
}

func (s *eventFeedSession) eventFeed(ctx context.Context, ts uint64) error {
	eventFeedGauge.Inc()
	defer eventFeedGauge.Dec()

	log.Debug("event feed started", zap.Stringer("span", s.totalSpan), zap.Uint64("ts", ts))

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
				err := s.handleError(ctx, errInfo)
				if err != nil {
					return err
				}
			}
		}
	})

	s.requestRangeCh <- rangeRequestTask{span: s.totalSpan, ts: ts}
	s.rangeChSizeGauge.Inc()

	return g.Wait()
}

// scheduleDivideRegionAndRequest schedules a range to be divided by regions, and these regions will be then scheduled
// to send ChangeData requests.
func (s *eventFeedSession) scheduleDivideRegionAndRequest(ctx context.Context, span regionspan.ComparableSpan, ts uint64) {
	task := rangeRequestTask{span: span, ts: ts}
	select {
	case s.requestRangeCh <- task:
		s.rangeChSizeGauge.Inc()
	case <-ctx.Done():
	}
}

// scheduleRegionRequest locks the region's range and schedules sending ChangeData request to the region.
// This function is blocking until the region range is locked successfully
func (s *eventFeedSession) scheduleRegionRequest(ctx context.Context, sri singleRegionInfo) {
	handleResult := func(res regionspan.LockRangeResult) {
		switch res.Status {
		case regionspan.LockRangeStatusSuccess:
			sri.ts = res.CheckpointTs
			select {
			case s.regionCh <- sri:
				s.regionChSizeGauge.Inc()
			case <-ctx.Done():
			}

		case regionspan.LockRangeStatusStale:
			log.Info("request expired",
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Reflect("retrySpans", res.RetryRanges))
			for _, r := range res.RetryRanges {
				// This call is always blocking, otherwise if scheduling in a new
				// goroutine, it won't block the caller of `schedulerRegionRequest`.
				s.scheduleDivideRegionAndRequest(ctx, r, sri.ts)
			}
		default:
			panic("unreachable")
		}
	}

	res := s.rangeLock.LockRange(sri.span.Start, sri.span.End, sri.verID.GetID(), sri.verID.GetVer())

	if res.Status == regionspan.LockRangeStatusWait {
		res = res.WaitFn()
	}

	handleResult(res)
}

// onRegionFail handles a region's failure, which means, unlock the region's range and send the error to the errCh for
// error handling. This function is non blocking even if error channel is full.
// CAUTION: Note that this should only be called in a context that the region has locked it's range.
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo) error {
	log.Debug("region failed", zap.Uint64("regionID", errorInfo.verID.GetID()), zap.Error(errorInfo.err))
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End, errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.ts)
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
					zap.Stringer("span", sri.span))
				err = s.onRegionFail(ctx, regionErrorInfo{
					singleRegionInfo: sri,
					err: &rpcCtxUnavailableErr{
						verID: sri.verID,
					},
				})
				if err != nil {
					return errors.Trace(err)
				}
				continue MainLoop
			}
			sri.rpcCtx = rpcCtx

			requestID := allocID()

			extraOp := kvrpcpb.ExtraOp_Noop
			if s.enableOldValue {
				extraOp = kvrpcpb.ExtraOp_ReadOldValue
			}

			regionID := rpcCtx.Meta.GetId()
			req := &cdcpb.ChangeDataRequest{
				Header: &cdcpb.Header{
					ClusterId:    s.client.clusterID,
					TicdcVersion: version.ReleaseSemver(),
				},
				RegionId:     regionID,
				RequestId:    requestID,
				RegionEpoch:  rpcCtx.Meta.RegionEpoch,
				CheckpointTs: sri.ts,
				StartKey:     sri.span.Start,
				EndKey:       sri.span.End,
				ExtraOp:      extraOp,
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

			stream, ok := s.getStream(rpcCtx.Addr)
			// Establish the stream if it has not been connected yet.
			if !ok {
				storeID := rpcCtx.Peer.GetStoreId()
				log.Info("creating new stream to store to send request",
					zap.Uint64("regionID", sri.verID.GetID()),
					zap.Uint64("requestID", requestID),
					zap.Uint64("storeID", storeID),
					zap.String("addr", rpcCtx.Addr))
				stream, err = s.client.newStream(ctx, rpcCtx.Addr, storeID)
				if err != nil {
					// if get stream failed, maybe the store is down permanently, we should try to relocate the active store
					log.Warn("get grpc stream client failed",
						zap.Uint64("regionID", sri.verID.GetID()),
						zap.Uint64("requestID", requestID),
						zap.Uint64("storeID", storeID),
						zap.String("error", err.Error()))
					if cerror.ErrVersionIncompatible.Equal(err) {
						// It often occurs on rolling update. Sleep 20s to reduce logs.
						delay := 20 * time.Second
						failpoint.Inject("kvClientDelayWhenIncompatible", func() {
							delay = 100 * time.Millisecond
						})
						time.Sleep(delay)
					}
					bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
					s.client.regionCache.OnSendFail(bo, rpcCtx, needReloadRegion(sri.failStoreIDs, rpcCtx), err)
					// Delete the pendingRegion info from `pendingRegions` and retry connecting and sending the request.
					pendingRegions.take(requestID)
					continue
				}
				s.addStream(rpcCtx.Addr, stream)

				limiter := s.client.getRegionLimiter(regionID)
				g.Go(func() error {
					if !s.enableRegionWorker {
						return s.singleReceiveFromStream(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream, pendingRegions, limiter)
					}
					return s.receiveFromStream(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream, pendingRegions, limiter)
				})
			}

			logReq := log.Debug
			if s.isPullerInit.IsInitialized() {
				logReq = log.Info
			}
			logReq("start new request", zap.Reflect("request", req), zap.String("addr", rpcCtx.Addr))

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
				s.deleteStream(rpcCtx.Addr)
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

// divideAndSendEventFeedToRegions split up the input span into spans aligned
// to region boundaries. When region merging happens, it's possible that it
// will produce some overlapping spans.
func (s *eventFeedSession) divideAndSendEventFeedToRegions(
	ctx context.Context, span regionspan.ComparableSpan, ts uint64,
) error {
	limit := 20

	nextSpan := span
	captureAddr := util.CaptureAddrFromCtx(ctx)

	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Run(50*time.Millisecond, maxRetry,
			func() error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
				scanT0 := time.Now()
				bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
				regions, err = s.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.Start, nextSpan.End, limit)
				scanRegionsDuration.WithLabelValues(captureAddr).Observe(time.Since(scanT0).Seconds())
				if err != nil {
					return cerror.WrapError(cerror.ErrPDBatchLoadRegions, err)
				}
				metas := make([]*metapb.Region, 0, len(regions))
				for _, region := range regions {
					if region.GetMeta() == nil {
						err = cerror.ErrMetaNotInRegion.GenWithStackByArgs()
						log.Warn("batch load region", zap.Stringer("span", nextSpan), zap.Error(err))
						return err
					}
					metas = append(metas, region.GetMeta())
				}
				if !regionspan.CheckRegionsLeftCover(metas, nextSpan) {
					err = cerror.ErrRegionsNotCoverSpan.GenWithStackByArgs(nextSpan, metas)
					log.Warn("ScanRegions", zap.Stringer("span", nextSpan), zap.Reflect("regions", metas), zap.Error(err))
					return err
				}
				log.Debug("ScanRegions", zap.Stringer("span", nextSpan), zap.Reflect("regions", metas))
				return nil
			})

		if retryErr != nil {
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := regionspan.Intersect(s.totalSpan, regionspan.ComparableSpan{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("get partialSpan", zap.Stringer("span", partialSpan), zap.Uint64("regionID", region.Id))

			nextSpan.Start = region.EndKey

			sri := newSingleRegionInfo(tiRegion.VerID(), partialSpan, ts, nil)
			s.scheduleRegionRequest(ctx, sri)
			log.Debug("partialSpan scheduled", zap.Stringer("span", partialSpan), zap.Uint64("regionID", region.Id))

			// return if no more regions
			if regionspan.EndCompare(nextSpan.Start, span.End) >= 0 {
				return nil
			}
		}
	}
}

// handleError handles error returned by a region. If some new EventFeed connection should be established, the region
// info will be sent to `regionCh`. Note if region channel is full, this function will be blocked.
// CAUTION: Note that this should only be invoked in a context that the region is not locked, otherwise use onRegionFail
// instead.
func (s *eventFeedSession) handleError(ctx context.Context, errInfo regionErrorInfo) error {
	err := errInfo.err
	switch eerr := errors.Cause(err).(type) {
	case *eventError:
		innerErr := eerr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			// TODO: Handle the case that notleader.GetLeader() is nil.
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader().GetStoreId(), errInfo.rpcCtx.AccessIdx)
		} else if innerErr.GetEpochNotMatch() != nil {
			// TODO: If only confver is updated, we don't need to reload the region from region cache.
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts)
			return nil
		} else if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts)
			return nil
		} else if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			metricFeedDuplicateRequestCounter.Inc()
			logPanic("tikv reported duplicated request to the same region, which is not expected",
				zap.Uint64("regionID", duplicatedRequest.RegionId))
			return errUnreachable
		} else if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			log.Error("tikv reported compatibility error, which is not expected",
				zap.String("rpcCtx", errInfo.rpcCtx.String()),
				zap.Stringer("error", compatibility))
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		} else {
			metricFeedUnknownErrorCounter.Inc()
			log.Warn("receive empty or unknown error msg", zap.Stringer("error", innerErr))
		}
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.ts)
		return nil
	default:
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		if errInfo.rpcCtx.Meta != nil {
			s.regionCache.OnSendFail(bo, errInfo.rpcCtx, needReloadRegion(errInfo.failStoreIDs, errInfo.rpcCtx), err)
		}
	}

	s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
	return nil
}

func (s *eventFeedSession) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, id, tidbkv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGetTiKVRPCContext, err)
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
	limiter *rate.Limiter,
) error {
	// Cancel the pending regions if the stream failed. Otherwise it will remain unhandled in the pendingRegions list
	// however not registered in the new reconnected stream.
	var wg sync.WaitGroup
	defer func() {
		log.Info("stream to store closed", zap.String("addr", addr), zap.Uint64("storeID", storeID))

		remainingRegions := pendingRegions.takeAll()
		for _, state := range remainingRegions {
			err := s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: state.sri,
				err:              cerror.ErrPendingRegionCancel.GenWithStackByArgs(),
			})
			if err != nil {
				// The only possible is that the ctx is cancelled. Simply return.
				return
			}
		}

		wg.Wait()

		s.workersLock.Lock()
		delete(s.workers, addr)
		s.workersLock.Unlock()
	}()

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.WithLabelValues(captureAddr, changefeedID)

	s.workersLock.Lock()
	worker, ok := s.workers[addr]
	if !ok {
		worker = &regionWorker{
			session:        s,
			limiter:        limiter,
			inputCh:        make(chan *regionStatefulEvent, 1024),
			outputCh:       s.eventCh,
			regionStates:   make(map[uint64]*regionFeedState),
			enableOldValue: s.enableOldValue,
		}
		s.workers[addr] = worker
	}
	s.workersLock.Unlock()

	wg.Add(1)
	go func() {
		err := worker.run(ctx)
		if err != nil && errors.Cause(err) != context.Canceled && !cerror.ErrEventFeedAborted.Equal(err) {
			// TODO: should this error be propagated?
			log.Error("run region worker failed", zap.Error(err))
		}
		wg.Done()
	}()

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientStreamRecvError", func() {
			err = errors.New("injected stream recv error")
		})
		if err == io.EOF {
			close(worker.inputCh)
			return nil
		}
		if err != nil {
			if status.Code(errors.Cause(err)) == codes.Canceled {
				log.Debug(
					"receive from stream canceled",
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
				)
			} else {
				log.Error(
					"failed to receive from stream",
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
					zap.Error(err),
				)
			}

			// Use the same delay mechanism as `stream.Send` error handling, since
			// these two errors often mean upstream store suffers an accident, which
			// needs time to recover, kv client doesn't need to retry frequently.
			// TODO: add a better retry backoff or rate limitter
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))

			// TODO: better to closes the send direction of the stream to notify
			// the other side, but it is not safe to call CloseSend concurrently
			// with SendMsg, in future refactor we should refine the recv loop
			s.deleteStream(addr)

			// send nil regionStatefulEvent to signal worker exit
			select {
			case worker.inputCh <- nil:
			case <-ctx.Done():
				return ctx.Err()
			}

			// Do no return error but gracefully stop the goroutine here. Then the whole job will not be canceled and
			// connection will be retried.
			return nil
		}

		size := cevent.Size()
		if size > warnRecvMsgSizeThreshold {
			regionCount := 0
			if cevent.ResolvedTs != nil {
				regionCount = len(cevent.ResolvedTs.Regions)
			}
			log.Warn("change data event size too large",
				zap.Int("size", size), zap.Int("event length", len(cevent.Events)),
				zap.Int("resolved region count", regionCount))
		}

		for _, event := range cevent.Events {
			err = s.sendRegionChangeEvent(ctx, g, event, worker, pendingRegions, addr, limiter)
			if err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTs(ctx, g, cevent.ResolvedTs, worker, addr)
			if err != nil {
				return err
			}
		}
	}
}

func (s *eventFeedSession) sendRegionChangeEvent(
	ctx context.Context,
	g *errgroup.Group,
	event *cdcpb.Event,
	worker *regionWorker,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
	limiter *rate.Limiter,
) error {
	state, ok := worker.regionStates[event.RegionId]
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
			return nil
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
			return cerror.ErrNoPendingRegion.GenWithStackByArgs(event.RegionId, event.RequestId, addr)
		}

		state.start()
		// Then spawn the goroutine to process messages of this region.
		worker.regionStates[event.RegionId] = state

		// send resolved event when starting a single event feed
		select {
		case <-ctx.Done():
			return ctx.Err()
		case s.eventCh <- &model.RegionFeedEvent{
			RegionID: state.sri.verID.GetID(),
			Resolved: &model.ResolvedSpan{
				Span:       state.sri.span,
				ResolvedTs: state.sri.ts,
			},
		}:
		}
		// g.Go(func() error {
		// 	return s.partialRegionFeed(ctx, state, limiter)
		// })
	} else if state.isStopped() {
		log.Warn("drop event due to region feed stopped",
			zap.Uint64("regionID", event.RegionId),
			zap.Uint64("requestID", event.RequestId),
			zap.String("addr", addr))
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case worker.inputCh <- &regionStatefulEvent{
		changeEvent: event,
		state:       state,
	}:
	}
	return nil
}

func (s *eventFeedSession) sendResolvedTs(
	ctx context.Context,
	g *errgroup.Group,
	resolvedTs *cdcpb.ResolvedTs,
	worker *regionWorker,
	addr string,
) error {
	for _, regionID := range resolvedTs.Regions {
		state, ok := worker.regionStates[regionID]
		if ok {
			if state.isStopped() {
				log.Warn("drop resolved ts due to region feed stopped",
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("addr", addr))
				continue
			}
			select {
			case worker.inputCh <- &regionStatefulEvent{
				resolvedTs: resolvedTs,
				state:      state,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (s *eventFeedSession) addStream(storeAddr string, stream cdcpb.ChangeData_EventFeedClient) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	s.streams[storeAddr] = stream
}

func (s *eventFeedSession) deleteStream(storeAddr string) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	delete(s.streams, storeAddr)
}

func (s *eventFeedSession) getStream(storeAddr string) (stream cdcpb.ChangeData_EventFeedClient, ok bool) {
	s.streamsLock.RLock()
	defer s.streamsLock.RUnlock()
	stream, ok = s.streams[storeAddr]
	return
}

func assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row, enableOldValue bool) (*model.RegionFeedEvent, error) {
	var opType model.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = model.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = model.OpTypePut
	default:
		return nil, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}

	revent := &model.RegionFeedEvent{
		RegionID: regionID,
		Val: &model.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
		},
	}

	// when old-value is disabled, it is still possible for the tikv to send a event containing the old value
	// we need avoid a old-value sent to downstream when old-value is disabled
	if enableOldValue {
		revent.Val.OldValue = entry.GetOldValue()
	}
	return revent, nil
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
