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
	"github.com/pingcap/ticdc/pkg/config"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/regionspan"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/pkg/txnutil"
	"github.com/pingcap/ticdc/pkg/util"
	"github.com/pingcap/ticdc/pkg/version"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/store/tikv/oracle"
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
	dialTimeout           = 10 * time.Second
	maxRetry              = 100
	tikvRequestMaxBackoff = 20000 // Maximum total sleep time(in ms)
	// TODO find optimal values and test extensively before releasing
	// The old values cause the gRPC stream to stall for some unknown reason.
	grpcInitialWindowSize     = 1 << 26 // 64 MB The value for initial window size on a stream
	grpcInitialConnWindowSize = 1 << 27 // 128 MB The value for initial window size on a connection
	grpcMaxCallRecvMsgSize    = 1 << 28 // 256 MB The maximum message size the client can receive
	grpcConnCount             = 2

	// The threshold of warning a message is too large. TiKV split events into 6MB per-message.
	warnRecvMsgSizeThreshold = 12 * 1024 * 1024

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region any more.
	regionScheduleReload = false

	// defaultRegionChanSize is the default channel size for region channel, including
	// range request, region request and region error.
	// Note the producer of region error channel, and the consumer of range request
	// channel work in an asynchronous way, the larger channel can decrease the
	// frequency of creating new goroutine.
	defaultRegionChanSize = 128
)

// time interval to force kv client to terminate gRPC stream and reconnect
var reconnectInterval = 60 * time.Minute

// hard code switch
// true: use kv client v2, which has a region worker for each stream
// false: use kv client v1, which runs a goroutine for every single region
var enableKVClientV2 = false

type singleRegionInfo struct {
	verID  tikv.RegionVerID
	span   regionspan.ComparableSpan
	ts     uint64
	rpcCtx *tikv.RPCContext
}

var (
	metricFeedNotLeaderCounter        = eventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = eventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = eventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = eventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = eventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = eventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricStoreSendRequestErr         = eventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricConnectToStoreErr           = eventFeedErrorCounter.WithLabelValues("ConnectToStore")
)

var (
	// unreachable error, only used in unit test
	errUnreachable = errors.New("kv client unreachable error")
	// internal error, force the gPRC stream terminate and reconnect
	errReconnect = errors.New("internal error, reconnect all regions")
	logPanic     = log.Panic
)

func newSingleRegionInfo(verID tikv.RegionVerID, span regionspan.ComparableSpan, ts uint64, rpcCtx *tikv.RPCContext) singleRegionInfo {
	return singleRegionInfo{
		verID:  verID,
		span:   span,
		ts:     ts,
		rpcCtx: rpcCtx,
	}
}

// partialClone clones part fields of singleRegionInfo, this is used when error
// happens, kv client needs to recover region request from singleRegionInfo
func (s *singleRegionInfo) partialClone() singleRegionInfo {
	sri := singleRegionInfo{
		verID:  s.verID,
		span:   s.span.Clone(),
		ts:     s.ts,
		rpcCtx: &tikv.RPCContext{},
	}
	if s.rpcCtx != nil {
		sri.rpcCtx.Addr = s.rpcCtx.Addr
	}
	return sri
}

type regionErrorInfo struct {
	singleRegionInfo
	err error
}

type regionEvent struct {
	changeEvent *cdcpb.Event
	resolvedTs  *cdcpb.ResolvedTs
}

type regionFeedState struct {
	sri           singleRegionInfo
	requestID     uint64
	regionEventCh chan *regionEvent
	stopped       int32

	lock           sync.RWMutex
	initialized    bool
	matcher        *matcher
	startFeedTime  time.Time
	lastResolvedTs uint64
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
	s.matcher = newMatcher()
}

func (s *regionFeedState) markStopped() {
	atomic.StoreInt32(&s.stopped, 1)
}

func (s *regionFeedState) isStopped() bool {
	return atomic.LoadInt32(&s.stopped) > 0
}

func (s *regionFeedState) getLastResolvedTs() uint64 {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.lastResolvedTs
}

func (s *regionFeedState) getRegionSpan() regionspan.ComparableSpan {
	s.lock.RLock()
	defer s.lock.RUnlock()
	return s.sri.span
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
	kvStorage   TiKVStorage

	regionLimiters *regionEventFeedLimiters
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(ctx context.Context, pd pd.Client, kvStorage tikv.Storage, credential *security.Credential) (c CDCKVClient) {
	clusterID := pd.GetClusterID(ctx)

	var store TiKVStorage
	if kvStorage != nil {
		// wrap to TiKVStorage if need.
		if s, ok := kvStorage.(TiKVStorage); ok {
			store = s
		} else {
			store = newStorageWithCurVersionCache(kvStorage, kvStorage.UUID())
		}
	}

	c = &CDCClient{
		clusterID:   clusterID,
		pd:          pd,
		kvStorage:   store,
		credential:  credential,
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
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	s := newEventFeedSession(ctx, c, c.regionCache, c.kvStorage, span,
		lockResolver, isPullerInit,
		enableOldValue, ts, eventCh)
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
	kvStorage   TiKVStorage

	lockResolver txnutil.LockResolver
	isPullerInit PullerInitialization

	// The whole range that is being subscribed.
	totalSpan regionspan.ComparableSpan

	// The channel to send the processed events.
	eventCh chan<- *model.RegionFeedEvent
	// The token based region router, it controls the uninitialzied regions with
	// a given size limit.
	regionRouter LimitRegionRouter
	// The channel to put the region that will be sent requests.
	regionCh chan singleRegionInfo
	// The channel to notify that an error is happening, so that the error will be handled and the affected region
	// will be re-requested.
	errCh chan regionErrorInfo
	// The channel to schedule scanning and requesting regions in a specified range.
	requestRangeCh chan rangeRequestTask

	rangeLock        *regionspan.RegionRangeLock
	enableOldValue   bool
	enableKVClientV2 bool

	// To identify metrics of different eventFeedSession
	id                string
	regionChSizeGauge prometheus.Gauge
	errChSizeGauge    prometheus.Gauge
	rangeChSizeGauge  prometheus.Gauge

	streams          map[string]cdcpb.ChangeData_EventFeedClient
	streamsLock      sync.RWMutex
	streamsCanceller map[string]context.CancelFunc
}

type rangeRequestTask struct {
	span regionspan.ComparableSpan
	ts   uint64
}

func newEventFeedSession(
	ctx context.Context,
	client *CDCClient,
	regionCache *tikv.RegionCache,
	kvStorage TiKVStorage,
	totalSpan regionspan.ComparableSpan,
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	enableOldValue bool,
	startTs uint64,
	eventCh chan<- *model.RegionFeedEvent,
) *eventFeedSession {
	id := strconv.FormatUint(allocID(), 10)
	kvClientCfg := config.GetGlobalServerConfig().KVClient
	return &eventFeedSession{
		client:            client,
		regionCache:       regionCache,
		kvStorage:         kvStorage,
		totalSpan:         totalSpan,
		eventCh:           eventCh,
		regionRouter:      NewSizedRegionRouter(ctx, kvClientCfg.RegionScanLimit),
		regionCh:          make(chan singleRegionInfo, defaultRegionChanSize),
		errCh:             make(chan regionErrorInfo, defaultRegionChanSize),
		requestRangeCh:    make(chan rangeRequestTask, defaultRegionChanSize),
		rangeLock:         regionspan.NewRegionRangeLock(totalSpan.Start, totalSpan.End, startTs),
		enableOldValue:    enableOldValue,
		enableKVClientV2:  enableKVClientV2,
		lockResolver:      lockResolver,
		isPullerInit:      isPullerInit,
		id:                id,
		regionChSizeGauge: clientChannelSize.WithLabelValues(id, "region"),
		errChSizeGauge:    clientChannelSize.WithLabelValues(id, "err"),
		rangeChSizeGauge:  clientChannelSize.WithLabelValues(id, "range"),
		streams:           make(map[string]cdcpb.ChangeData_EventFeedClient),
		streamsCanceller:  make(map[string]context.CancelFunc),
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
		return s.requestRegionToStore(ctx, g)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case task := <-s.requestRangeCh:
				s.rangeChSizeGauge.Dec()
				// divideAndSendEventFeedToRegions could be block for some time,
				// since it must wait for the region lock available. In order to
				// consume region range request from `requestRangeCh` as soon as
				// possible, we create a new goroutine to handle it.
				// The sequence of region range we process is not matter, the
				// region lock keeps the region access sequence.
				// Besides the count or frequency of range request is limitted,
				// we use ephemeral goroutine instead of permanent gourotine.
				g.Go(func() error {
					return s.divideAndSendEventFeedToRegions(ctx, task.span, task.ts)
				})
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

	g.Go(func() error {
		return s.regionRouter.Run(ctx)
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
		case regionspan.LockRangeStatusCancel:
			return
		default:
			panic("unreachable")
		}
	}

	res := s.rangeLock.LockRange(ctx, sri.span.Start, sri.span.End, sri.verID.GetID(), sri.verID.GetVer())
	failpoint.Inject("kvClientMockRangeLock", func(val failpoint.Value) {
		// short sleep to wait region has split
		time.Sleep(time.Second)
		s.rangeLock.UnlockRange(sri.span.Start, sri.span.End, sri.verID.GetID(), sri.verID.GetVer(), sri.ts)
		regionNum := val.(int)
		retryRanges := make([]regionspan.ComparableSpan, 0, regionNum)
		start := []byte("a")
		end := []byte("b1001")
		for i := 0; i < regionNum; i++ {
			span := regionspan.Span{Start: start, End: end}
			retryRanges = append(retryRanges, regionspan.ToComparableSpan(span))
			start = end
			end = []byte(fmt.Sprintf("b%d", 1002+i))
		}
		res = regionspan.LockRangeResult{
			Status:      regionspan.LockRangeStatusStale,
			RetryRanges: retryRanges,
		}
	})

	if res.Status == regionspan.LockRangeStatusWait {
		res = res.WaitFn()
	}

	handleResult(res)
}

// onRegionFail handles a region's failure, which means, unlock the region's range and send the error to the errCh for
// error handling. This function is non blocking even if error channel is full.
// CAUTION: Note that this should only be called in a context that the region has locked it's range.
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo, revokeToken bool) error {
	log.Debug("region failed", zap.Uint64("regionID", errorInfo.verID.GetID()), zap.Error(errorInfo.err))
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End, errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.ts)
	if revokeToken {
		s.regionRouter.Release(errorInfo.rpcCtx.Addr)
	}
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

// requestRegionToStore gets singleRegionInfo from regionRouter, which is a token
// based limitter, sends request to TiKV.
// If the send request to TiKV returns error, fail the region with sendRequestToStoreErr
// and kv client will redispatch the region.
// If initialize gPRC stream with an error, fail the region with connectToStoreErr
// and kv client will also redispatch the region.
func (s *eventFeedSession) requestRegionToStore(
	ctx context.Context,
	g *errgroup.Group,
) error {
	// Stores pending regions info for each stream. After sending a new request, the region info wil be put to the map,
	// and it will be loaded by the receiver thread when it receives the first response from that region. We need this
	// to pass the region info to the receiver since the region info cannot be inferred from the response from TiKV.
	storePendingRegions := make(map[string]*syncRegionFeedStateMap)

	var sri singleRegionInfo
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Chan():
		}
		requestID := allocID()

		extraOp := kvrpcpb.ExtraOp_Noop
		if s.enableOldValue {
			extraOp = kvrpcpb.ExtraOp_ReadOldValue
		}

		rpcCtx := sri.rpcCtx
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

		failpoint.Inject("kvClientPendingRegionDelay", nil)

		// each TiKV store has an independent pendingRegions.
		var pendingRegions *syncRegionFeedStateMap

		var err error
		stream, ok := s.getStream(rpcCtx.Addr)
		if ok {
			var ok bool
			pendingRegions, ok = storePendingRegions[rpcCtx.Addr]
			if !ok {
				// Should never happen
				log.Panic("pending regions is not found for store", zap.String("store", rpcCtx.Addr))
			}
		} else {
			// when a new stream is established, always create a new pending
			// regions map, the old map will be used in old `receiveFromStream`
			// and won't be deleted until that goroutine exits.
			pendingRegions = newSyncRegionFeedStateMap()
			storePendingRegions[rpcCtx.Addr] = pendingRegions
			storeID := rpcCtx.Peer.GetStoreId()
			log.Info("creating new stream to store to send request",
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Uint64("requestID", requestID),
				zap.Uint64("storeID", storeID),
				zap.String("addr", rpcCtx.Addr))
			streamCtx, streamCancel := context.WithCancel(ctx)
			_ = streamCancel // to avoid possible context leak warning from govet
			stream, err = s.client.newStream(streamCtx, rpcCtx.Addr, storeID)
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
				s.client.regionCache.OnSendFail(bo, rpcCtx, regionScheduleReload, err)
				err = s.onRegionFail(ctx, regionErrorInfo{
					singleRegionInfo: sri,
					err:              &connectToStoreErr{},
				}, false /* revokeToken */)
				if err != nil {
					return errors.Trace(err)
				}
				continue
			}
			s.addStream(rpcCtx.Addr, stream, streamCancel)

			limiter := s.client.getRegionLimiter(regionID)
			g.Go(func() error {
				if !s.enableKVClientV2 {
					return s.receiveFromStream(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream, pendingRegions, limiter)
				}
				return s.receiveFromStreamV2(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream, pendingRegions, limiter)
			})
		}

		state := newRegionFeedState(sri, requestID)
		pendingRegions.insert(requestID, state)

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
				// since this pending region has been removed, the token has been
				// released in advance, re-add one token here.
				s.regionRouter.Acquire(rpcCtx.Addr)
				continue
			}

			// Wait for a while and retry sending the request
			time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
			err = s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: sri,
				err:              &sendRequestToStoreErr{},
			}, false /* revokeToken */)
			if err != nil {
				return errors.Trace(err)
			}
		} else {
			s.regionRouter.Acquire(rpcCtx.Addr)
		}
	}
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

		// Send a resolved ts to event channel first, for two reasons:
		// 1. Since we have locked the region range, and have maintained correct
		//    checkpoint ts for the range, it is safe to report the resolved ts
		//    to puller at this moment.
		// 2. Before the kv client gets region rpcCtx, sends request to TiKV and
		//    receives the first kv event from TiKV, the region could split or
		//    merge in advance, which should cause the change of resolved ts
		//    distribution in puller, so this resolved ts event is needed.
		// After this resolved ts event is sent, we don't need to send one more
		// resolved ts event when the region starts to work.
		resolvedEv := &model.RegionFeedEvent{
			RegionID: sri.verID.GetID(),
			Resolved: &model.ResolvedSpan{
				Span:       sri.span,
				ResolvedTs: sri.ts,
			},
		}
		select {
		case s.eventCh <- resolvedEv:
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}

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
			}, false /* revokeToken */)
			if err != nil {
				return errors.Trace(err)
			}
			continue
		}
		sri.rpcCtx = rpcCtx
		s.regionRouter.AddRegion(sri)
	}
}

// partialRegionFeed establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection. If any error happens (region split, leader change, etc), the region
// and error info will be sent to `errCh`, and the receiver of `errCh` is
// responsible for handling the error and re-establish the connection to the region.
func (s *eventFeedSession) partialRegionFeed(
	ctx context.Context,
	state *regionFeedState,
	limiter *rate.Limiter,
) error {
	receiver := state.regionEventCh
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
	maxTs, initialized, err := s.singleEventFeed(ctx, state.sri.verID.GetID(), state.sri.span,
		state.sri.ts, state.sri.rpcCtx.Addr, receiver)
	log.Debug("singleEventFeed quit")

	if err == nil || errors.Cause(err) == context.Canceled {
		return nil
	}

	if errors.Cause(err) == errReconnect {
		cancel, ok := s.getStreamCancel(state.sri.rpcCtx.Addr)
		if ok {
			// cancel the stream to trigger strem.Recv with context cancel error
			// Note use context cancel is the only way to terminate a gRPC stream
			cancel()
			// Failover in stream.Recv has 0-100ms delay, the onRegionFail
			// should be called after stream has been deleted. Add a delay here
			// to avoid too frequent region rebuilt.
			time.Sleep(time.Second)
		}
		// if stream is already deleted, just ignore errReconnect
	}

	failpoint.Inject("kvClientErrUnreachable", func() {
		if err == errUnreachable {
			failpoint.Return(err)
		}
	})

	if maxTs > ts {
		ts = maxTs
	}

	regionID := state.sri.verID.GetID()
	log.Info("EventFeed disconnected",
		zap.Uint64("regionID", regionID),
		zap.Uint64("requestID", state.requestID),
		zap.Stringer("span", state.sri.span),
		zap.Uint64("checkpoint", ts),
		zap.String("error", err.Error()))

	state.sri.ts = ts

	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
	state.markStopped()

	failpoint.Inject("kvClientSingleFeedProcessDelay", nil)

	now := time.Now()
	delay := limiter.ReserveN(now, 1).Delay()
	if delay != 0 {
		log.Info("EventFeed retry rate limited",
			zap.Duration("delay", delay), zap.Reflect("regionID", regionID))
		t := time.NewTimer(delay)
		defer t.Stop()
		select {
		case <-t.C:
			// We can proceed.
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	revokeToken := !initialized
	return s.onRegionFail(ctx, regionErrorInfo{
		singleRegionInfo: state.sri,
		err:              err,
	}, revokeToken)
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
	case *connectToStoreErr:
		metricConnectToStoreErr.Inc()
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
	default:
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		if errInfo.rpcCtx.Meta != nil {
			s.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		}
	}

	failpoint.Inject("kvClientRegionReentrantErrorDelay", nil)
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
	defer func() {
		log.Info("stream to store closed", zap.String("addr", addr), zap.Uint64("storeID", storeID))

		failpoint.Inject("kvClientStreamCloseDelay", nil)

		remainingRegions := pendingRegions.takeAll()

		for _, state := range remainingRegions {
			err := s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: state.sri,
				err:              cerror.ErrPendingRegionCancel.GenWithStackByArgs(),
			}, true /* revokeToken */)
			if err != nil {
				// The only possible is that the ctx is cancelled. Simply return.
				return
			}
		}
	}()

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.WithLabelValues(captureAddr, changefeedID)

	// Each region has it's own goroutine to handle its messages. `regionStates` stores states of these regions.
	regionStates := make(map[uint64]*regionFeedState)

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientStreamRecvError", func(msg failpoint.Value) {
			errStr := msg.(string)
			if errStr == io.EOF.Error() {
				err = io.EOF
			} else {
				err = errors.New(errStr)
			}
		})
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

			for _, state := range regionStates {
				select {
				case state.regionEventCh <- nil:
				case <-ctx.Done():
					return ctx.Err()
				}
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
			err = s.sendRegionChangeEvent(ctx, g, event, regionStates, pendingRegions, addr, limiter)
			if err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTs(ctx, g, cevent.ResolvedTs, regionStates, pendingRegions, addr)
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
	regionStates map[uint64]*regionFeedState,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
	limiter *rate.Limiter,
) error {
	state, ok := regionStates[event.RegionId]
	// Every region's range is locked before sending requests and unlocked after exiting, and the requestID
	// is allocated while holding the range lock. Therefore the requestID is always incrementing. If a region
	// is receiving messages with different requestID, only the messages with the larges requestID is valid.
	isNewSubscription := !ok
	if ok {
		if state.requestID < event.RequestId {
			log.Info("region state entry will be replaced because received message of newer requestID",
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

		// Then spawn the goroutine to process messages of this region.
		regionStates[event.RegionId] = state

		g.Go(func() error {
			return s.partialRegionFeed(ctx, state, limiter)
		})
	} else if state.isStopped() {
		log.Warn("drop event due to region feed stopped",
			zap.Uint64("regionID", event.RegionId),
			zap.Uint64("requestID", event.RequestId),
			zap.String("addr", addr))
		return nil
	}

	select {
	case state.regionEventCh <- &regionEvent{
		changeEvent: event,
	}:
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func (s *eventFeedSession) sendResolvedTs(
	ctx context.Context,
	g *errgroup.Group,
	resolvedTs *cdcpb.ResolvedTs,
	regionStates map[uint64]*regionFeedState,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
) error {
	for _, regionID := range resolvedTs.Regions {
		state, ok := regionStates[regionID]
		if ok {
			if state.isStopped() {
				log.Debug("drop resolved ts due to region feed stopped",
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("addr", addr))
				continue
			}
			select {
			case state.regionEventCh <- &regionEvent{
				resolvedTs: resolvedTs,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

// singleEventFeed handles events of a single EventFeed stream.
// Results will be send to eventCh
// EventFeed RPC will not return resolved event directly
// Resolved event is generate while there's not non-match pre-write
// Return the maximum resolved
func (s *eventFeedSession) singleEventFeed(
	ctx context.Context,
	regionID uint64,
	span regionspan.ComparableSpan,
	startTs uint64,
	storeAddr string,
	receiverCh <-chan *regionEvent,
) (lastResolvedTs uint64, initialized bool, err error) {
	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricEventSize := eventSize.WithLabelValues(captureAddr)
	metricPullEventInitializedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_INITIALIZED.String(), captureAddr, changefeedID)
	metricPullEventCommittedCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMITTED.String(), captureAddr, changefeedID)
	metricPullEventCommitCounter := pullEventCounter.WithLabelValues(cdcpb.Event_COMMIT.String(), captureAddr, changefeedID)
	metricPullEventPrewriteCounter := pullEventCounter.WithLabelValues(cdcpb.Event_PREWRITE.String(), captureAddr, changefeedID)
	metricPullEventRollbackCounter := pullEventCounter.WithLabelValues(cdcpb.Event_ROLLBACK.String(), captureAddr, changefeedID)
	metricSendEventResolvedCounter := sendEventCounter.WithLabelValues("native-resolved", captureAddr, changefeedID)
	metricSendEventCommitCounter := sendEventCounter.WithLabelValues("commit", captureAddr, changefeedID)
	metricSendEventCommittedCounter := sendEventCounter.WithLabelValues("committed", captureAddr, changefeedID)

	matcher := newMatcher()
	advanceCheckTicker := time.NewTicker(time.Second * 5)
	defer advanceCheckTicker.Stop()
	lastReceivedEventTime := time.Now()
	startFeedTime := time.Now()
	lastResolvedTs = startTs
	handleResolvedTs := func(resolvedTs uint64) error {
		if !initialized {
			return nil
		}
		if resolvedTs < lastResolvedTs {
			log.Warn("The resolvedTs is fallen back in kvclient",
				zap.String("Event Type", "RESOLVED"),
				zap.Uint64("resolvedTs", resolvedTs),
				zap.Uint64("lastResolvedTs", lastResolvedTs),
				zap.Uint64("regionID", regionID))
			return nil
		}
		// emit a checkpointTs
		revent := &model.RegionFeedEvent{
			RegionID: regionID,
			Resolved: &model.ResolvedSpan{
				Span:       span,
				ResolvedTs: resolvedTs,
			},
		}
		lastResolvedTs = resolvedTs

		select {
		case s.eventCh <- revent:
			metricSendEventResolvedCounter.Inc()
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		}
		return nil
	}

	resolveLockInterval := 20 * time.Second
	failpoint.Inject("kvClientResolveLockInterval", func(val failpoint.Value) {
		resolveLockInterval = time.Duration(val.(int)) * time.Second
	})

	for {
		var event *regionEvent
		var ok bool
		select {
		case <-ctx.Done():
			err = errors.Trace(ctx.Err())
			return
		case <-advanceCheckTicker.C:
			failpoint.Inject("kvClientForceReconnect", func() {
				log.Warn("kv client reconnect triggered by failpoint")
				failpoint.Return(lastResolvedTs, initialized, errReconnect)
			})
			if time.Since(startFeedTime) < resolveLockInterval {
				continue
			}
			if !s.isPullerInit.IsInitialized() {
				// Initializing a puller may take a long time, skip resolved lock to save unnecessary overhead.
				continue
			}
			sinceLastEvent := time.Since(lastReceivedEventTime)
			if sinceLastEvent > resolveLockInterval {
				log.Warn("region not receiving event from tikv for too long time",
					zap.Uint64("regionID", regionID), zap.Stringer("span", span), zap.Duration("duration", sinceLastEvent))
			}
			if sinceLastEvent > reconnectInterval && initialized {
				log.Warn("kv client reconnect triggered", zap.Duration("duration", sinceLastEvent))
				err = errReconnect
				return
			}
			version, err := s.kvStorage.GetCachedCurrentVersion()
			if err != nil {
				log.Warn("failed to get current version from PD", zap.Error(err))
				continue
			}
			currentTimeFromPD := oracle.GetTimeFromTS(version.Ver)
			sinceLastResolvedTs := currentTimeFromPD.Sub(oracle.GetTimeFromTS(lastResolvedTs))
			if sinceLastResolvedTs > resolveLockInterval && initialized {
				log.Warn("region not receiving resolved event from tikv or resolved ts is not pushing for too long time, try to resolve lock",
					zap.Uint64("regionID", regionID), zap.Stringer("span", span),
					zap.Duration("duration", sinceLastResolvedTs),
					zap.Duration("lastEvent", sinceLastEvent),
					zap.Uint64("resolvedTs", lastResolvedTs))
				maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTimeFromPD.Add(-10*time.Second)), 0)
				err = s.lockResolver.Resolve(ctx, regionID, maxVersion)
				if err != nil {
					log.Warn("failed to resolve lock", zap.Uint64("regionID", regionID), zap.Error(err))
					continue
				}
			}
			continue
		case event, ok = <-receiverCh:
		}

		if !ok || event == nil {
			log.Debug("singleEventFeed closed by error")
			err = cerror.ErrEventFeedAborted.GenWithStackByArgs()
			return
		}
		var revent *model.RegionFeedEvent
		lastReceivedEventTime = time.Now()
		if event.changeEvent != nil {
			metricEventSize.Observe(float64(event.changeEvent.Event.Size()))
			switch x := event.changeEvent.Event.(type) {
			case *cdcpb.Event_Entries_:
				for _, entry := range x.Entries.GetEntries() {
					// if a region with kv range [a, z)
					// and we only want the get [b, c) from this region,
					// tikv will return all key events in the region although we specified [b, c) int the request.
					// we can make tikv only return the events about the keys in the specified range.
					comparableKey := regionspan.ToComparableKey(entry.GetKey())
					// key for initialized event is nil
					if !regionspan.KeyInSpan(comparableKey, span) && entry.Type != cdcpb.Event_INITIALIZED {
						continue
					}
					switch entry.Type {
					case cdcpb.Event_INITIALIZED:
						if time.Since(startFeedTime) > 20*time.Second {
							log.Warn("The time cost of initializing is too much",
								zap.Duration("timeCost", time.Since(startFeedTime)),
								zap.Uint64("regionID", regionID))
						}
						metricPullEventInitializedCounter.Inc()
						initialized = true
						s.regionRouter.Release(storeAddr)
						cachedEvents := matcher.matchCachedRow()
						for _, cachedEvent := range cachedEvents {
							revent, err = assembleRowEvent(regionID, cachedEvent, s.enableOldValue)
							if err != nil {
								return
							}
							select {
							case s.eventCh <- revent:
								metricSendEventCommitCounter.Inc()
							case <-ctx.Done():
								err = errors.Trace(ctx.Err())
								return
							}
						}
					case cdcpb.Event_COMMITTED:
						metricPullEventCommittedCounter.Inc()
						revent, err = assembleRowEvent(regionID, entry, s.enableOldValue)
						if err != nil {
							return
						}

						if entry.CommitTs <= lastResolvedTs {
							logPanic("The CommitTs must be greater than the resolvedTs",
								zap.String("Event Type", "COMMITTED"),
								zap.Uint64("CommitTs", entry.CommitTs),
								zap.Uint64("resolvedTs", lastResolvedTs),
								zap.Uint64("regionID", regionID))
							err = errUnreachable
							return
						}
						select {
						case s.eventCh <- revent:
							metricSendEventCommittedCounter.Inc()
						case <-ctx.Done():
							err = errors.Trace(ctx.Err())
							return
						}
					case cdcpb.Event_PREWRITE:
						metricPullEventPrewriteCounter.Inc()
						matcher.putPrewriteRow(entry)
					case cdcpb.Event_COMMIT:
						metricPullEventCommitCounter.Inc()
						if entry.CommitTs <= lastResolvedTs {
							logPanic("The CommitTs must be greater than the resolvedTs",
								zap.String("Event Type", "COMMIT"),
								zap.Uint64("CommitTs", entry.CommitTs),
								zap.Uint64("resolvedTs", lastResolvedTs),
								zap.Uint64("regionID", regionID))
							err = errUnreachable
							return
						}
						ok := matcher.matchRow(entry)
						if !ok {
							if !initialized {
								matcher.cacheCommitRow(entry)
								continue
							}
							err = cerror.ErrPrewriteNotMatch.GenWithStackByArgs(entry.GetKey(), entry.GetStartTs())
							return
						}

						revent, err = assembleRowEvent(regionID, entry, s.enableOldValue)
						if err != nil {
							return
						}

						select {
						case s.eventCh <- revent:
							metricSendEventCommitCounter.Inc()
						case <-ctx.Done():
							err = errors.Trace(ctx.Err())
							return
						}
					case cdcpb.Event_ROLLBACK:
						metricPullEventRollbackCounter.Inc()
						matcher.rollbackRow(entry)
					}
				}
			case *cdcpb.Event_Admin_:
				log.Info("receive admin event", zap.Stringer("event", event.changeEvent))
			case *cdcpb.Event_Error:
				err = cerror.WrapError(cerror.ErrEventFeedEventError, &eventError{err: x.Error})
				return
			case *cdcpb.Event_ResolvedTs:
				if err = handleResolvedTs(x.ResolvedTs); err != nil {
					return
				}
			}
		}

		if event.resolvedTs != nil {
			if err = handleResolvedTs(event.resolvedTs.Ts); err != nil {
				return
			}
		}
	}
}

func (s *eventFeedSession) addStream(storeAddr string, stream cdcpb.ChangeData_EventFeedClient, cancel context.CancelFunc) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	s.streams[storeAddr] = stream
	s.streamsCanceller[storeAddr] = cancel
}

func (s *eventFeedSession) deleteStream(storeAddr string) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	delete(s.streams, storeAddr)
	delete(s.streamsCanceller, storeAddr)
}

func (s *eventFeedSession) getStream(storeAddr string) (stream cdcpb.ChangeData_EventFeedClient, ok bool) {
	s.streamsLock.RLock()
	defer s.streamsLock.RUnlock()
	stream, ok = s.streams[storeAddr]
	return
}

func (s *eventFeedSession) getStreamCancel(storeAddr string) (cancel context.CancelFunc, ok bool) {
	s.streamsLock.RLock()
	defer s.streamsLock.RUnlock()
	cancel, ok = s.streamsCanceller[storeAddr]
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

type connectToStoreErr struct{}

func (e *connectToStoreErr) Error() string { return "connect to store error" }

type sendRequestToStoreErr struct{}

func (e *sendRequestToStoreErr) Error() string { return "send request to store error" }

func getStoreID(rpcCtx *tikv.RPCContext) uint64 {
	if rpcCtx != nil && rpcCtx.Peer != nil {
		return rpcCtx.Peer.GetStoreId()
	}
	return 0
}
