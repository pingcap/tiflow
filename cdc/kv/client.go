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
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	tidbkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout           = 10 * time.Second
	tikvRequestMaxBackoff = 20000 // Maximum total sleep time(in ms)

	// TiCDC may open numerous gRPC streams,
	// with 65535 bytes window size, 10K streams takes about 27GB memory.
	//
	// 65535 bytes, the initial window size in http2 spec.
	grpcInitialWindowSize = (1 << 16) - 1
	// 8 MB The value for initial window size on a connection
	grpcInitialConnWindowSize = 1 << 23
	// 256 MB The maximum message size the client can receive
	grpcMaxCallRecvMsgSize = 1 << 28

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

	// initial size for region rate limit queue.
	defaultRegionRateLimitQueueSize = 128
	// Interval of check region retry rate limit queue.
	defaultCheckRegionRateLimitInterval = 50 * time.Millisecond
	// Duration of warning region retry rate limited too long.
	defaultLogRegionRateLimitDuration = 10 * time.Second
)

// time interval to force kv client to terminate gRPC stream and reconnect
var reconnectInterval = 60 * time.Minute

type singleRegionInfo struct {
	verID  tikv.RegionVerID
	span   regionspan.ComparableSpan
	ts     uint64
	rpcCtx *tikv.RPCContext
}

type regionStatefulEvent struct {
	changeEvent     *cdcpb.Event
	resolvedTsEvent *resolvedTsEvent
	state           *regionFeedState

	// regionID is used for load balancer, we don't use fields in state to reduce lock usage
	regionID uint64

	// finishedCallbackCh is used to mark events that are sent from a give region
	// worker to this worker(one of the workers in worker pool) are all processed.
	finishedCallbackCh chan struct{}
}

type resolvedTsEvent struct {
	resolvedTs uint64
	regions    []*regionFeedState
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

type regionErrorInfo struct {
	singleRegionInfo
	err error

	retryLimitTime       *time.Time
	logRateLimitDuration time.Duration
}

func newRegionErrorInfo(info singleRegionInfo, err error) regionErrorInfo {
	return regionErrorInfo{
		singleRegionInfo: info,
		err:              err,

		logRateLimitDuration: defaultLogRegionRateLimitDuration,
	}
}

func (r *regionErrorInfo) logRateLimitedHint() bool {
	now := time.Now()
	if r.retryLimitTime == nil {
		// Caller should log on the first rate limited.
		r.retryLimitTime = &now
		return true
	}
	if now.Sub(*r.retryLimitTime) > r.logRateLimitDuration {
		// Caller should log if it lasts too long.
		r.retryLimitTime = &now
		return true
	}
	return false
}

type regionFeedState struct {
	sri       singleRegionInfo
	requestID uint64
	stopped   int32

	lock           sync.RWMutex
	initialized    bool
	matcher        *matcher
	startFeedTime  time.Time
	lastResolvedTs uint64
}

func newRegionFeedState(sri singleRegionInfo, requestID uint64) *regionFeedState {
	return &regionFeedState{
		sri:       sri,
		requestID: requestID,
		stopped:   0,
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

// eventFeedStream stores an EventFeed stream and pointer to the underlying gRPC connection
type eventFeedStream struct {
	client cdcpb.ChangeData_EventFeedClient
	conn   *sharedConn
}

// CDCKVClient is an interface to receives kv changed logs from TiKV
type CDCKVClient interface {
	EventFeed(
		ctx context.Context,
		span regionspan.ComparableSpan,
		ts uint64,
		lockResolver txnutil.LockResolver,
		isPullerInit PullerInitialization,
		eventCh chan<- model.RegionFeedEvent,
	) error
}

// NewCDCKVClient is the constructor of CDC KV client
var NewCDCKVClient = NewCDCClient

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	config    *config.KVClientConfig
	clusterID uint64

	grpcPool GrpcPool

	regionCache *tikv.RegionCache
	kvStorage   tikv.Storage
	pdClock     pdutil.Clock
	changefeed  model.ChangeFeedID

	regionLimiters *regionEventFeedLimiters
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(
	ctx context.Context,
	pd pd.Client,
	kvStorage tikv.Storage,
	grpcPool GrpcPool,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	changefeed model.ChangeFeedID,
	cfg *config.KVClientConfig,
) (c CDCKVClient) {
	clusterID := pd.GetClusterID(ctx)

	c = &CDCClient{
		clusterID:      clusterID,
		config:         cfg,
		pd:             pd,
		kvStorage:      kvStorage,
		grpcPool:       grpcPool,
		regionCache:    regionCache,
		pdClock:        pdClock,
		changefeed:     changefeed,
		regionLimiters: defaultRegionEventFeedLimiters,
	}
	return
}

func (c *CDCClient) getRegionLimiter(regionID uint64) *rate.Limiter {
	return c.regionLimiters.getLimiter(regionID)
}

func (c *CDCClient) newStream(ctx context.Context, addr string, storeID uint64) (stream *eventFeedStream, newStreamErr error) {
	newStreamErr = retry.Do(ctx, func() (err error) {
		var conn *sharedConn
		defer func() {
			if err != nil && conn != nil {
				c.grpcPool.ReleaseConn(conn, addr)
			}
		}()
		conn, err = c.grpcPool.GetConn(addr)
		if err != nil {
			log.Info("get connection to store failed, retry later",
				zap.String("addr", addr), zap.Error(err),
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID))
			return
		}
		err = version.CheckStoreVersion(ctx, c.pd, storeID)
		if err != nil {
			log.Error("check tikv version failed",
				zap.Error(err), zap.Uint64("storeID", storeID),
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID))
			return
		}
		client := cdcpb.NewChangeDataClient(conn.ClientConn)
		var streamClient cdcpb.ChangeData_EventFeedClient
		streamClient, err = client.EventFeed(ctx)
		if err != nil {
			err = cerror.WrapError(cerror.ErrTiKVEventFeed, err)
			log.Info("establish stream to store failed, retry later",
				zap.String("addr", addr), zap.Error(err),
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID))
			return
		}
		stream = &eventFeedStream{
			client: streamClient,
			conn:   conn,
		}
		log.Debug("created stream to store",
			zap.String("addr", addr),
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID))
		return nil
	}, retry.WithBackoffBaseDelay(500), retry.WithMaxTries(2), retry.WithIsRetryableErr(cerror.IsRetryableError))
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
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	eventCh chan<- model.RegionFeedEvent,
) error {
	s := newEventFeedSession(
		ctx, c, span, lockResolver, isPullerInit, ts, eventCh)
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
	client *CDCClient

	lockResolver txnutil.LockResolver
	isPullerInit PullerInitialization

	// The whole range that is being subscribed.
	totalSpan regionspan.ComparableSpan

	// The channel to send the processed events.
	eventCh chan<- model.RegionFeedEvent
	// The token based region router, it controls the uninitialized regions with
	// a given size limit.
	regionRouter LimitRegionRouter
	// The channel to put the region that will be sent requests.
	regionCh chan singleRegionInfo
	// The channel to notify that an error is happening, so that the error will be handled and the affected region
	// will be re-requested.
	errCh chan regionErrorInfo
	// The channel to schedule scanning and requesting regions in a specified range.
	requestRangeCh chan rangeRequestTask
	// The queue is used to store region that reaches limit
	rateLimitQueue []regionErrorInfo

	rangeLock *regionspan.RegionRangeLock

	// To identify metrics of different eventFeedSession
	id                string
	regionChSizeGauge prometheus.Gauge
	errChSizeGauge    prometheus.Gauge
	rangeChSizeGauge  prometheus.Gauge

	streams          map[string]*eventFeedStream
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
	totalSpan regionspan.ComparableSpan,
	lockResolver txnutil.LockResolver,
	isPullerInit PullerInitialization,
	startTs uint64,
	eventCh chan<- model.RegionFeedEvent,
) *eventFeedSession {
	id := strconv.FormatUint(allocID(), 10)
	rangeLock := regionspan.NewRegionRangeLock(
		totalSpan.Start, totalSpan.End, startTs,
		client.changefeed.Namespace+"-"+client.changefeed.ID)
	return &eventFeedSession{
		client:            client,
		totalSpan:         totalSpan,
		eventCh:           eventCh,
		regionRouter:      NewSizedRegionRouter(ctx, client.config.RegionScanLimit),
		regionCh:          make(chan singleRegionInfo, defaultRegionChanSize),
		errCh:             make(chan regionErrorInfo, defaultRegionChanSize),
		requestRangeCh:    make(chan rangeRequestTask, defaultRegionChanSize),
		rateLimitQueue:    make([]regionErrorInfo, 0, defaultRegionRateLimitQueueSize),
		rangeLock:         rangeLock,
		lockResolver:      lockResolver,
		isPullerInit:      isPullerInit,
		id:                id,
		regionChSizeGauge: clientChannelSize.WithLabelValues("region"),
		errChSizeGauge:    clientChannelSize.WithLabelValues("err"),
		rangeChSizeGauge:  clientChannelSize.WithLabelValues("range"),
		streams:           make(map[string]*eventFeedStream),
		streamsCanceller:  make(map[string]context.CancelFunc),
	}
}

func (s *eventFeedSession) eventFeed(ctx context.Context, ts uint64) error {
	eventFeedGauge.Inc()
	defer eventFeedGauge.Dec()

	log.Info("event feed started",
		zap.Stringer("span", s.totalSpan), zap.Uint64("startTs", ts),
		zap.String("namespace", s.client.changefeed.Namespace),
		zap.String("changefeed", s.client.changefeed.ID))

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.dispatchRequest(ctx)
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
				// Besides the count or frequency of range request is limited,
				// we use ephemeral goroutine instead of permanent goroutine.
				g.Go(func() error {
					return s.divideAndSendEventFeedToRegions(ctx, task.span, task.ts)
				})
			}
		}
	})

	tableID, tableName := contextutil.TableIDFromCtx(ctx)
	g.Go(func() error {
		timer := time.NewTimer(defaultCheckRegionRateLimitInterval)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				s.handleRateLimit(ctx)
				timer.Reset(defaultCheckRegionRateLimitInterval)
			case errInfo := <-s.errCh:
				s.errChSizeGauge.Dec()
				allowed := s.checkRateLimit(errInfo.singleRegionInfo.verID.GetID())
				if !allowed {
					if errInfo.logRateLimitedHint() {
						zapFieldAddr := zap.Skip()
						if errInfo.singleRegionInfo.rpcCtx != nil {
							// rpcCtx may be nil if we fails to get region info
							// from pd. It could cause by pd down or the region
							// has been merged.
							zapFieldAddr = zap.String("addr", errInfo.singleRegionInfo.rpcCtx.Addr)
						}
						log.Info("EventFeed retry rate limited",
							zap.String("namespace", s.client.changefeed.Namespace),
							zap.String("changefeed", s.client.changefeed.ID),
							zap.Uint64("regionID", errInfo.singleRegionInfo.verID.GetID()),
							zap.Uint64("ts", errInfo.singleRegionInfo.ts),
							zap.Int64("tableID", tableID), zap.String("tableName", tableName),
							zap.Any("errInfo", errInfo),
							zapFieldAddr)
					}
					// rate limit triggers, add the error info to the rate limit queue.
					s.rateLimitQueue = append(s.rateLimitQueue, errInfo)
				} else {
					err := s.handleError(ctx, errInfo)
					if err != nil {
						return err
					}
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
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Reflect("retrySpans", res.RetryRanges),
				zap.Any("sri", sri))
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
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo, revokeToken bool) {
	log.Debug("region failed",
		zap.String("namespace", s.client.changefeed.Namespace),
		zap.String("changefeed", s.client.changefeed.ID),
		zap.Uint64("regionID", errorInfo.verID.GetID()),
		zap.Error(errorInfo.err),
		zap.Any("errorInfo", errorInfo))
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End, errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.ts)
	if revokeToken {
		s.regionRouter.Release(errorInfo.rpcCtx.Addr)
	}
	s.enqueueError(ctx, errorInfo)
}

// requestRegionToStore gets singleRegionInfo from regionRouter, which is a token
// based limiter, sends request to TiKV.
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

		// Always read old value.
		extraOp := kvrpcpb.ExtraOp_ReadOldValue
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
				log.Panic("pending regions is not found for store",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.String("store", rpcCtx.Addr))
			}
		} else {
			// when a new stream is established, always create a new pending
			// regions map, the old map will be used in old `receiveFromStream`
			// and won't be deleted until that goroutine exits.
			pendingRegions = newSyncRegionFeedStateMap()
			storePendingRegions[rpcCtx.Addr] = pendingRegions
			storeID := rpcCtx.Peer.GetStoreId()
			log.Info("creating new stream to store to send request",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
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
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
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
				errInfo := newRegionErrorInfo(sri, &connectToStoreErr{})
				s.onRegionFail(ctx, errInfo, false /* revokeToken */)
				continue
			}
			s.addStream(rpcCtx.Addr, stream, streamCancel)

			g.Go(func() error {
				defer s.deleteStream(rpcCtx.Addr)
				return s.receiveFromStream(ctx, g, rpcCtx.Addr, getStoreID(rpcCtx), stream.client, pendingRegions)
			})
		}

		state := newRegionFeedState(sri, requestID)
		pendingRegions.insert(requestID, state)

		logReq := log.Debug
		if s.isPullerInit.IsInitialized() {
			logReq = log.Info
		}
		logReq("start new request",
			zap.String("namespace", s.client.changefeed.Namespace),
			zap.String("changefeed", s.client.changefeed.ID),
			zap.Reflect("request", req), zap.String("addr", rpcCtx.Addr))

		err = stream.client.Send(req)

		// If Send error, the receiver should have received error too or will receive error soon. So we doesn't need
		// to do extra work here.
		if err != nil {
			log.Warn("send request to stream failed",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.String("addr", rpcCtx.Addr),
				zap.Uint64("storeID", getStoreID(rpcCtx)),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Uint64("requestID", requestID),
				zap.Error(err))
			err1 := stream.client.CloseSend()
			if err1 != nil {
				log.Warn("failed to close stream",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID))
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

			errInfo := newRegionErrorInfo(sri, &sendRequestToStoreErr{})
			s.onRegionFail(ctx, errInfo, false /* revokeToken */)
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

		log.Debug("dispatching region",
			zap.String("namespace", s.client.changefeed.Namespace),
			zap.String("changefeed", s.client.changefeed.ID),
			zap.Uint64("regionID", sri.verID.GetID()))

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
		resolvedEv := model.RegionFeedEvent{
			Resolved: &model.ResolvedSpans{
				Spans: []model.RegionComparableSpan{
					{
						Span:   sri.span,
						Region: sri.verID.GetID(),
					},
				},
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
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Any("sri", sri))
			errInfo := newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID})
			s.onRegionFail(ctx, errInfo, false /* revokeToken */)
			continue
		}
		sri.rpcCtx = rpcCtx
		s.regionRouter.AddRegion(sri)
	}
}

// divideAndSendEventFeedToRegions split up the input span into spans aligned
// to region boundaries. When region merging happens, it's possible that it
// will produce some overlapping spans.
func (s *eventFeedSession) divideAndSendEventFeedToRegions(
	ctx context.Context, span regionspan.ComparableSpan, ts uint64,
) error {
	limit := 20
	nextSpan := span

	// Max backoff 500ms.
	scanRegionMaxBackoff := int64(500)
	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Do(ctx, func() error {
			scanT0 := time.Now()
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			regions, err = s.client.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.Start, nextSpan.End, limit)
			scanRegionsDuration.Observe(time.Since(scanT0).Seconds())
			if err != nil {
				return cerror.WrapError(cerror.ErrPDBatchLoadRegions, err)
			}
			metas := make([]*metapb.Region, 0, len(regions))
			for _, region := range regions {
				if region.GetMeta() == nil {
					err = cerror.ErrMetaNotInRegion.GenWithStackByArgs()
					log.Warn("batch load region",
						zap.Stringer("span", nextSpan), zap.Error(err),
						zap.String("namespace", s.client.changefeed.Namespace),
						zap.String("changefeed", s.client.changefeed.ID),
					)
					return err
				}
				metas = append(metas, region.GetMeta())
			}
			if !regionspan.CheckRegionsLeftCover(metas, nextSpan) {
				err = cerror.ErrRegionsNotCoverSpan.GenWithStackByArgs(nextSpan, metas)
				log.Warn("ScanRegions",
					zap.Stringer("span", nextSpan),
					zap.Reflect("regions", metas), zap.Error(err),
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
				)
				return err
			}
			log.Debug("ScanRegions",
				zap.Stringer("span", nextSpan),
				zap.Reflect("regions", metas),
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID))
			return nil
		}, retry.WithBackoffMaxDelay(scanRegionMaxBackoff),
			retry.WithTotalRetryDuratoin(time.Duration(s.client.config.RegionRetryDuration)))
		if retryErr != nil {
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := regionspan.Intersect(s.totalSpan, regionspan.ComparableSpan{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("get partialSpan",
				zap.Stringer("span", partialSpan),
				zap.Uint64("regionID", region.Id),
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID))
			nextSpan.Start = region.EndKey

			sri := newSingleRegionInfo(tiRegion.VerID(), partialSpan, ts, nil)
			s.scheduleRegionRequest(ctx, sri)
			log.Debug("partialSpan scheduled",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Stringer("span", partialSpan),
				zap.Uint64("regionID", region.Id),
				zap.Any("sri", sri))

			// return if no more regions
			if regionspan.EndCompare(nextSpan.Start, span.End) >= 0 {
				return nil
			}
		}
	}
}

// enqueueError sends error to the eventFeedSession's error channel in a none blocking way
// TODO: refactor enqueueError to avoid too many goroutines spawned when a lot of regions meet error.
func (s *eventFeedSession) enqueueError(ctx context.Context, errorInfo regionErrorInfo) {
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

func (s *eventFeedSession) handleRateLimit(ctx context.Context) {
	var (
		i       int
		errInfo regionErrorInfo
	)
	if len(s.rateLimitQueue) == 0 {
		return
	}
	for i, errInfo = range s.rateLimitQueue {
		s.enqueueError(ctx, errInfo)
		// to avoid too many goroutines spawn, since if the error region count
		// exceeds the size of errCh, new goroutine will be spawned
		if i == defaultRegionChanSize-1 {
			break
		}
	}
	if i == len(s.rateLimitQueue)-1 {
		s.rateLimitQueue = make([]regionErrorInfo, 0, defaultRegionRateLimitQueueSize)
	} else {
		s.rateLimitQueue = append(make([]regionErrorInfo, 0, len(s.rateLimitQueue)-i-1), s.rateLimitQueue[i+1:]...)
	}
}

// checkRateLimit checks whether a region can be reconnected based on its rate limiter
func (s *eventFeedSession) checkRateLimit(regionID uint64) bool {
	limiter := s.client.getRegionLimiter(regionID)
	// use Limiter.Allow here since if exceed the rate limit, we skip this region
	// and try it later.
	return limiter.Allow()
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
			s.client.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
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
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.String("rpcCtx", errInfo.rpcCtx.String()),
				zap.Stringer("error", compatibility))
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		} else if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			log.Error("tikv reported the request cluster ID mismatch error, which is not expected",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Uint64("tikvCurrentClusterID", mismatch.Current),
				zap.Uint64("requestClusterID", mismatch.Request))
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		} else {
			metricFeedUnknownErrorCounter.Inc()
			log.Warn("receive empty or unknown error msg",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Stringer("error", innerErr))
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
		//[TODO] Move all OnSendFail logic here
		// We expect some unknown error to trigger RegionCache recheck its store state and change leader to peer to
		// make some detection(peer may tell us where new leader is)
		// RegionCache.OnSendFail is thread_safe inner.
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.client.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
	}

	failpoint.Inject("kvClientRegionReentrantErrorDelay", nil)
	s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
	return nil
}

func (s *eventFeedSession) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.client.regionCache.GetTiKVRPCContext(bo, id, tidbkv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGetTiKVRPCContext, err)
	}
	return rpcCtx, nil
}

// receiveFromStream receives gRPC messages from a stream continuously and sends
// messages to region worker, if `stream.Recv` meets error, this routine will exit
// silently. As for regions managed by this routine, there are two situations:
//  1. established regions: a `nil` event will be sent to region worker, and region
//     worker call `s.onRegionFail` to re-establish these regions.
//  2. pending regions: call `s.onRegionFail` for each pending region before this
//     routine exits to establish these regions.
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
		log.Info("stream to store closed",
			zap.String("namespace", s.client.changefeed.Namespace),
			zap.String("changefeed", s.client.changefeed.ID),
			zap.String("addr", addr), zap.Uint64("storeID", storeID))

		failpoint.Inject("kvClientStreamCloseDelay", nil)

		remainingRegions := pendingRegions.takeAll()
		for _, state := range remainingRegions {
			errInfo := newRegionErrorInfo(state.sri, cerror.ErrPendingRegionCancel.FastGenByArgs())
			s.onRegionFail(ctx, errInfo, true /* revokeToken */)
		}
	}()

	changefeedID := contextutil.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.
		WithLabelValues(changefeedID.Namespace, changefeedID.ID)

	// always create a new region worker, because `receiveFromStream` is ensured
	// to call exactly once from outer code logic
	worker := newRegionWorker(changefeedID, s, addr)

	defer worker.evictAllRegions()

	g.Go(func() error {
		return worker.run(ctx)
	})

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientRegionReentrantError", func(op failpoint.Value) {
			if op.(string) == "error" {
				_ = worker.sendEvents(ctx, []*regionStatefulEvent{nil})
			}
		})
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
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
				)
			} else {
				log.Warn(
					"failed to receive from stream",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
					zap.Error(err),
				)
				// Note that pd need at lease 10s+ to tag a kv node as disconnect if kv node down
				// tikv raft need wait (raft-base-tick-interval * raft-election-timeout-ticks) 10s to start a new
				// election
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
			err = worker.sendEvents(ctx, []*regionStatefulEvent{nil})
			if err != nil {
				return err
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
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Int("size", size), zap.Int("eventLen", len(cevent.Events)),
				zap.Int("resolvedRegionCount", regionCount))
		}

		err = s.sendRegionChangeEvents(ctx, cevent.Events, worker, pendingRegions, addr)
		if err != nil {
			return err
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTs(ctx, cevent.ResolvedTs, worker, addr)
			if err != nil {
				return err
			}
		}
	}
}

func (s *eventFeedSession) sendRegionChangeEvents(
	ctx context.Context,
	events []*cdcpb.Event,
	worker *regionWorker,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
) error {
	statefulEvents := make([][]*regionStatefulEvent, worker.inputSlots)
	for i := 0; i < worker.inputSlots; i++ {
		// Allocate a buffer with 1.5x length than average to reduce reallocate.
		buffLen := len(events) / worker.inputSlots * 3 / 2
		statefulEvents[i] = make([]*regionStatefulEvent, 0, buffLen)
	}

	for _, event := range events {
		state, valid := worker.getRegionState(event.RegionId)
		// Every region's range is locked before sending requests and unlocked after exiting, and the requestID
		// is allocated while holding the range lock. Therefore the requestID is always incrementing. If a region
		// is receiving messages with different requestID, only the messages with the larges requestID is valid.
		if valid {
			if state.requestID < event.RequestId {
				log.Debug("region state entry will be replaced because received message of newer requestID",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("oldRequestID", state.requestID),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", addr))
				valid = false
			} else if state.requestID > event.RequestId {
				log.Warn("drop event due to event belongs to a stale request",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.Uint64("currRequestID", state.requestID),
					zap.String("addr", addr))
				continue
			}
		}

		if !valid {
			// It's the first response for this region. If the region is newly connected, the region info should
			// have been put in `pendingRegions`. So here we load the region info from `pendingRegions` and start
			// a new goroutine to handle messages from this region.
			// Firstly load the region info.
			state, valid = pendingRegions.take(event.RequestId)
			if !valid {
				log.Warn("drop event due to region feed is removed",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", addr))
				continue
			}
			state.start()
			worker.setRegionState(event.RegionId, state)
		} else if state.isStopped() {
			log.Warn("drop event due to region feed stopped",
				zap.String("namespace", s.client.changefeed.Namespace),
				zap.String("changefeed", s.client.changefeed.ID),
				zap.Uint64("regionID", event.RegionId),
				zap.Uint64("requestID", event.RequestId),
				zap.String("addr", addr))
			continue
		}

		slot := worker.inputCalcSlot(event.RegionId)
		statefulEvents[slot] = append(statefulEvents[slot], &regionStatefulEvent{
			changeEvent: event,
			regionID:    event.RegionId,
			state:       state,
		})
	}
	for _, events := range statefulEvents {
		if len(events) > 0 {
			err := worker.sendEvents(ctx, events)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *eventFeedSession) sendResolvedTs(
	ctx context.Context,
	resolvedTs *cdcpb.ResolvedTs,
	worker *regionWorker,
	addr string,
) error {
	statefulEvents := make([]*regionStatefulEvent, worker.inputSlots)
	// split resolved ts
	for i := 0; i < worker.inputSlots; i++ {
		// Allocate a buffer with 1.5x length than average to reduce reallocate.
		buffLen := len(resolvedTs.Regions) / worker.inputSlots * 3 / 2
		statefulEvents[i] = &regionStatefulEvent{
			resolvedTsEvent: &resolvedTsEvent{
				resolvedTs: resolvedTs.Ts,
				regions:    make([]*regionFeedState, 0, buffLen),
			},
		}
	}

	for _, regionID := range resolvedTs.Regions {
		state, ok := worker.getRegionState(regionID)
		if ok {
			if state.isStopped() {
				log.Debug("drop resolved ts due to region feed stopped",
					zap.String("namespace", s.client.changefeed.Namespace),
					zap.String("changefeed", s.client.changefeed.ID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", state.requestID),
					zap.String("addr", addr))
				continue
			}
			slot := worker.inputCalcSlot(regionID)
			statefulEvents[slot].resolvedTsEvent.regions = append(
				statefulEvents[slot].resolvedTsEvent.regions, state,
			)
			// regionID is just an slot index
			statefulEvents[slot].regionID = regionID
		}
	}
	for _, event := range statefulEvents {
		if len(event.resolvedTsEvent.regions) > 0 {
			err := worker.sendEvents(ctx, []*regionStatefulEvent{event})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *eventFeedSession) addStream(storeAddr string, stream *eventFeedStream, cancel context.CancelFunc) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	s.streams[storeAddr] = stream
	s.streamsCanceller[storeAddr] = cancel
}

func (s *eventFeedSession) deleteStream(storeAddr string) {
	s.streamsLock.Lock()
	defer s.streamsLock.Unlock()
	if stream, ok := s.streams[storeAddr]; ok {
		s.client.grpcPool.ReleaseConn(stream.conn, storeAddr)
		delete(s.streams, storeAddr)
	}
	if cancel, ok := s.streamsCanceller[storeAddr]; ok {
		cancel()
		delete(s.streamsCanceller, storeAddr)
	}
}

func (s *eventFeedSession) getStream(storeAddr string) (stream *eventFeedStream, ok bool) {
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

func assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row) (model.RegionFeedEvent, error) {
	var opType model.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = model.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = model.OpTypePut
	default:
		return model.RegionFeedEvent{}, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}

	revent := model.RegionFeedEvent{
		RegionID: regionID,
		Val: &model.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		},
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
