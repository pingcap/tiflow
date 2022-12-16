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
	"container/list"
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
	// don't need to force reload region anymore.
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

type regionEventFeedLimiters struct {
	sync.Mutex
	// TODO replace with a LRU cache.
	limiters map[uint64]*rate.Limiter
}

var defaultRegionEventFeedLimiters = &regionEventFeedLimiters{
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
		eventCh chan<- model.RegionFeedEvent,
	) error

	// RegionCount returns the number of captured regions.
	RegionCount() uint64
	// ResolvedTs returns the current ingress resolved ts.
	ResolvedTs() model.Ts
	// CommitTs returns the current ingress commit ts.
	CommitTs() model.Ts
}

// NewCDCKVClient is the constructor of CDC KV client
var NewCDCKVClient = NewCDCClient

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	config    *config.KVClientConfig
	clusterID uint64

	grpcPool GrpcPool

	regionCache    *tikv.RegionCache
	pdClock        pdutil.Clock
	regionLimiters *regionEventFeedLimiters

	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string

	regionCounts struct {
		sync.Mutex
		counts *list.List
	}
	ingressCommitTs   model.Ts
	ingressResolvedTs model.Ts
	// filterLoop is used in BDR mode, when it is true, tikv cdc component
	// will filter data that are written by another TiCDC.
	filterLoop bool
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(
	ctx context.Context,
	pd pd.Client,
	grpcPool GrpcPool,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	cfg *config.KVClientConfig,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
	filterLoop bool,
) (c CDCKVClient) {
	clusterID := pd.GetClusterID(ctx)

	c = &CDCClient{
		clusterID:      clusterID,
		config:         cfg,
		pd:             pd,
		grpcPool:       grpcPool,
		regionCache:    regionCache,
		pdClock:        pdClock,
		regionLimiters: defaultRegionEventFeedLimiters,

		changefeed: changefeed,
		tableID:    tableID,
		tableName:  tableName,
		regionCounts: struct {
			sync.Mutex
			counts *list.List
		}{
			counts: list.New(),
		},
		filterLoop: filterLoop,
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
			return errors.Trace(err)
		}
		err = version.CheckStoreVersion(ctx, c.pd, storeID)
		if err != nil {
			return errors.Trace(err)
		}
		client := cdcpb.NewChangeDataClient(conn.ClientConn)
		var streamClient cdcpb.ChangeData_EventFeedClient
		streamClient, err = client.EventFeed(ctx)
		if err != nil {
			return cerror.WrapError(cerror.ErrTiKVEventFeed, err)
		}
		stream = &eventFeedStream{
			client: streamClient,
			conn:   conn,
		}
		log.Debug("created stream to store",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.String("addr", addr))
		return nil
	}, retry.WithBackoffBaseDelay(500),
		retry.WithMaxTries(2),
		retry.WithIsRetryableErr(cerror.IsRetryableError),
	)
	return
}

// EventFeed divides a EventFeed request on range boundaries and establishes
// a EventFeed to each of the individual region. It streams back result on the
// provided channel.
// The `Start` and `End` field in input span must be memcomparable encoded.
func (c *CDCClient) EventFeed(
	ctx context.Context, span regionspan.ComparableSpan, ts uint64,
	lockResolver txnutil.LockResolver,
	eventCh chan<- model.RegionFeedEvent,
) error {
	c.regionCounts.Lock()
	regionCount := int64(0)
	c.regionCounts.counts.PushBack(&regionCount)
	c.regionCounts.Unlock()
	s := newEventFeedSession(
		ctx, c, span, lockResolver, ts, eventCh, c.changefeed, c.tableID, c.tableName)
	return s.eventFeed(ctx, ts, &regionCount)
}

// RegionCount returns the number of captured regions.
func (c *CDCClient) RegionCount() uint64 {
	c.regionCounts.Lock()
	defer c.regionCounts.Unlock()

	totalCount := uint64(0)
	for e := c.regionCounts.counts.Front(); e != nil; e = e.Next() {
		totalCount += uint64(atomic.LoadInt64(e.Value.(*int64)))
	}
	return totalCount
}

// ResolvedTs returns the current ingress resolved ts.
func (c *CDCClient) ResolvedTs() model.Ts {
	return atomic.LoadUint64(&c.ingressResolvedTs)
}

// CommitTs returns the current ingress commit ts.
func (c *CDCClient) CommitTs() model.Ts {
	return atomic.LoadUint64(&c.ingressCommitTs)
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

	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string

	// use sync.Pool to store resolved ts event only, because resolved ts event
	// has the same size and generate cycle.
	resolvedTsPool sync.Pool
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
	startTs uint64,
	eventCh chan<- model.RegionFeedEvent,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
) *eventFeedSession {
	id := strconv.FormatUint(allocID(), 10)
	rangeLock := regionspan.NewRegionRangeLock(
		totalSpan.Start, totalSpan.End, startTs,
		changefeed.Namespace+"."+changefeed.ID)
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
		id:                id,
		regionChSizeGauge: clientChannelSize.WithLabelValues("region"),
		errChSizeGauge:    clientChannelSize.WithLabelValues("err"),
		rangeChSizeGauge:  clientChannelSize.WithLabelValues("range"),
		streams:           make(map[string]*eventFeedStream),
		streamsCanceller:  make(map[string]context.CancelFunc),
		resolvedTsPool: sync.Pool{
			New: func() any {
				return &regionStatefulEvent{
					resolvedTsEvent: &resolvedTsEvent{},
				}
			},
		},

		changefeed: changefeed,
		tableID:    tableID,
		tableName:  tableName,
	}
}

func (s *eventFeedSession) eventFeed(ctx context.Context, ts uint64, regionCount *int64) error {
	eventFeedGauge.Inc()
	defer eventFeedGauge.Dec()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		return s.dispatchRequest(ctx)
	})

	g.Go(func() error {
		return s.requestRegionToStore(ctx, g, regionCount)
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case task := <-s.requestRangeCh:
				s.rangeChSizeGauge.Dec()
				// divideAndSendEventFeedToRegions could be blocked for some time,
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
				if allowed {
					if err := s.handleError(ctx, errInfo); err != nil {
						return err
					}
					continue
				}
				if errInfo.logRateLimitedHint() {
					zapFieldAddr := zap.Skip()
					if errInfo.singleRegionInfo.rpcCtx != nil {
						// rpcCtx may be nil if we failed to get region info
						// from pd. It could cause by pd down or the region
						// has been merged.
						zapFieldAddr = zap.String("addr", errInfo.singleRegionInfo.rpcCtx.Addr)
					}
					log.Info("EventFeed retry rate limited",
						zap.String("namespace", s.changefeed.Namespace),
						zap.String("changefeed", s.changefeed.ID),
						zap.Int64("tableID", s.tableID),
						zap.String("tableName", s.tableName),
						zap.Uint64("regionID", errInfo.singleRegionInfo.verID.GetID()),
						zap.Uint64("resolvedTs", errInfo.singleRegionInfo.resolvedTs),
						zap.Error(errInfo.err),
						zapFieldAddr)
				}
				// rate limit triggers, add the error info to the rate limit queue.
				s.rateLimitQueue = append(s.rateLimitQueue, errInfo)
			}
		}
	})

	g.Go(func() error {
		return s.regionRouter.Run(ctx)
	})

	s.requestRangeCh <- rangeRequestTask{span: s.totalSpan, ts: ts}
	s.rangeChSizeGauge.Inc()

	log.Info("event feed started",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Int64("tableID", s.tableID),
		zap.String("tableName", s.tableName),
		zap.Uint64("startTs", ts),
		zap.Stringer("span", s.totalSpan))

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
			sri.resolvedTs = res.CheckpointTs
			select {
			case s.regionCh <- sri:
				s.regionChSizeGauge.Inc()
			case <-ctx.Done():
			}
		case regionspan.LockRangeStatusStale:
			log.Info("request expired",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs),
				zap.Any("retrySpans", res.RetryRanges))
			for _, r := range res.RetryRanges {
				// This call is always blocking, otherwise if scheduling in a new
				// goroutine, it won't block the caller of `schedulerRegionRequest`.
				s.scheduleDivideRegionAndRequest(ctx, r, sri.resolvedTs)
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
		s.rangeLock.UnlockRange(sri.span.Start, sri.span.End,
			sri.verID.GetID(), sri.verID.GetVer(), sri.resolvedTs)
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
// error handling. This function is non-blocking even if error channel is full.
// CAUTION: Note that this should only be called in a context that the region has locked its range.
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo, revokeToken bool) {
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End,
		errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.resolvedTs)
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
	regionCount *int64,
) error {
	// Stores pending regions info for each stream. After sending a new request, the region info wil be put to the map,
	// and it will be loaded by the receiver thread when it receives the first response from that region. We need this
	// to pass the region info to the receiver since the region info cannot be inferred from the response from TiKV.
	storePendingRegions := make(map[string]*syncRegionFeedStateMap)

	header := &cdcpb.Header{
		ClusterId:    s.client.clusterID,
		TicdcVersion: version.ReleaseSemver(),
	}
	// Always read old value.
	extraOp := kvrpcpb.ExtraOp_ReadOldValue

	var sri singleRegionInfo
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Chan():
		}
		requestID := allocID()

		rpcCtx := sri.rpcCtx
		regionID := rpcCtx.Meta.GetId()
		regionEpoch := rpcCtx.Meta.RegionEpoch
		req := &cdcpb.ChangeDataRequest{
			Header:       header,
			RegionId:     regionID,
			RequestId:    requestID,
			RegionEpoch:  regionEpoch,
			CheckpointTs: sri.resolvedTs,
			StartKey:     sri.span.Start,
			EndKey:       sri.span.End,
			ExtraOp:      extraOp,
			FilterLoop:   s.client.filterLoop,
		}

		failpoint.Inject("kvClientPendingRegionDelay", nil)

		// each TiKV store has an independent pendingRegions.
		storeAddr := rpcCtx.Addr
		storeID := rpcCtx.Peer.GetStoreId()
		var (
			stream *eventFeedStream
			err    error
		)
		stream, ok := s.getStream(storeAddr)
		if !ok {
			// when a new stream is established, always create a new pending
			// regions map, the old map will be used in old `receiveFromStream`
			// and won't be deleted until that goroutine exits.
			pendingRegions := newSyncRegionFeedStateMap()
			storePendingRegions[storeAddr] = pendingRegions
			streamCtx, streamCancel := context.WithCancel(ctx)
			_ = streamCancel // to avoid possible context leak warning from govet
			stream, err = s.client.newStream(streamCtx, storeAddr, storeID)
			if err != nil {
				// get stream failed, maybe the store is down permanently, we should try to relocate the active store
				log.Warn("get grpc stream client failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", requestID),
					zap.Uint64("storeID", storeID),
					zap.Error(err))
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
			s.addStream(storeAddr, stream, streamCancel)
			log.Info("creating new stream to store to send request",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", requestID),
				zap.Uint64("storeID", storeID),
				zap.String("addr", storeAddr))

			g.Go(func() error {
				defer s.deleteStream(storeAddr)
				return s.receiveFromStream(
					ctx, g, storeAddr, storeID, stream.client, pendingRegions, regionCount)
			})
		}

		pendingRegions, ok := storePendingRegions[storeAddr]
		if !ok {
			// Should never happen
			log.Panic("pending regions is not found for store",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.String("store", storeAddr))
		}

		state := newRegionFeedState(sri, requestID)
		pendingRegions.setByRequestID(requestID, state)

		log.Debug("start new request",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.String("addr", storeAddr),
			zap.Any("request", req))

		err = stream.client.Send(req)

		// If Send error, the receiver should have received error too or will receive error soon. So we don't need
		// to do extra work here.
		if err != nil {
			log.Warn("send request to stream failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.String("addr", storeAddr),
				zap.Uint64("storeID", storeID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", requestID),
				zap.Error(err))
			if err := stream.client.CloseSend(); err != nil {
				log.Warn("failed to close stream",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Int64("tableID", s.tableID),
					zap.String("tableName", s.tableName),
					zap.String("addr", storeAddr),
					zap.Uint64("storeID", storeID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", requestID),
					zap.Error(err))
			}
			// Delete the stream from the map so that the next time the store is accessed, the stream will be
			// re-established.
			s.deleteStream(storeAddr)
			// Delete `pendingRegions` from `storePendingRegions` so that the next time a region of this store is
			// requested, it will create a new one. So if the `receiveFromStream` goroutine tries to stop all
			// pending regions, the new pending regions that are requested after reconnecting won't be stopped
			// incorrectly.
			delete(storePendingRegions, storeAddr)

			// Remove the region from pendingRegions. If it's already removed, it should be already retried by
			// `receiveFromStream`, so no need to retry here.
			_, ok := pendingRegions.takeByRequestID(requestID)
			if !ok {
				// since this pending region has been removed, the token has been
				// released in advance, re-add one token here.
				s.regionRouter.Acquire(storeAddr)
				continue
			}

			errInfo := newRegionErrorInfo(sri, &sendRequestToStoreErr{})
			s.onRegionFail(ctx, errInfo, false /* revokeToken */)
		} else {
			s.regionRouter.Acquire(storeAddr)
		}
	}
}

// dispatchRequest manages a set of streams and dispatch event feed requests
// to these streams. Streams to each store will be created on need. After
// establishing new stream, a goroutine will be spawned to handle events from
// the stream.
// Regions from `regionCh` will be connected. If any error happens to a
// region, the error will be sent to `errCh` and the receiver of `errCh` is
// responsible for handling the error.
func (s *eventFeedSession) dispatchRequest(ctx context.Context) error {
	for {
		// Note that when a region is received from the channel, it's range has been already locked.
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionCh:
			s.regionChSizeGauge.Dec()
		}

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
				ResolvedTs: sri.resolvedTs,
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
			log.Info("get rpc context for region is nil, retry it",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs))
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

	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Do(ctx, func() error {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			start := time.Now()
			regions, err = s.client.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.Start, nextSpan.End, limit)
			scanRegionsDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				return cerror.WrapError(cerror.ErrPDBatchLoadRegions, err)
			}
			metas := make([]*metapb.Region, 0, len(regions))
			for _, region := range regions {
				if region.GetMeta() == nil {
					return cerror.ErrMetaNotInRegion.FastGenByArgs()
				}
				metas = append(metas, region.GetMeta())
			}
			if !regionspan.CheckRegionsLeftCover(metas, nextSpan) {
				return cerror.ErrRegionsNotCoverSpan.FastGenByArgs(nextSpan, metas)
			}
			return nil
		}, retry.WithBackoffMaxDelay(500),
			retry.WithTotalRetryDuratoin(time.Duration(s.client.config.RegionRetryDuration)))
		if retryErr != nil {
			log.Warn("load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("span", nextSpan),
				zap.Error(retryErr))
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := regionspan.Intersect(s.totalSpan, regionspan.ComparableSpan{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			nextSpan.Start = region.EndKey
			// the End key return by the PD API will be nil to represent the biggest key,
			partialSpan = partialSpan.Hack()

			sri := newSingleRegionInfo(tiRegion.VerID(), partialSpan, ts, nil)
			s.scheduleRegionRequest(ctx, sri)
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
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs)
			return nil
		} else if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs)
			return nil
		} else if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			metricFeedDuplicateRequestCounter.Inc()
			logPanic("tikv reported duplicated request to the same region, which is not expected",
				zap.Uint64("regionID", duplicatedRequest.RegionId))
			return errUnreachable
		} else if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			log.Error("tikv reported compatibility error, which is not expected",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.String("rpcCtx", errInfo.rpcCtx.String()),
				zap.Stringer("error", compatibility))
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		} else if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			log.Error("tikv reported the request cluster ID mismatch error, which is not expected",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("tikvCurrentClusterID", mismatch.Current),
				zap.Uint64("requestClusterID", mismatch.Request))
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		} else {
			metricFeedUnknownErrorCounter.Inc()
			log.Warn("receive empty or unknown error msg",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Stringer("error", innerErr))
		}
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs)
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
	// todo: add metrics to track rpc cost
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
	regionCount *int64,
) error {
	// Cancel the pending regions if the stream failed.
	// Otherwise, it will remain unhandled in the pendingRegions list
	// however not registered in the new reconnected stream.
	defer func() {
		log.Info("stream to store closed",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.String("addr", addr), zap.Uint64("storeID", storeID))

		failpoint.Inject("kvClientStreamCloseDelay", nil)

		remainingRegions := pendingRegions.takeAll()
		for _, state := range remainingRegions {
			errInfo := newRegionErrorInfo(state.sri, cerror.ErrPendingRegionCancel.FastGenByArgs())
			s.onRegionFail(ctx, errInfo, true /* revokeToken */)
		}
	}()

	metricSendEventBatchResolvedSize := batchResolvedEventSize.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)

	// always create a new region worker, because `receiveFromStream` is ensured
	// to call exactly once from outer code logic
	worker := newRegionWorker(s.changefeed, s, addr)

	defer worker.evictAllRegions()

	g.Go(func() error {
		return worker.run(ctx)
	})

	maxCommitTs := model.Ts(0)
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
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.String("addr", addr),
					zap.Uint64("storeID", storeID),
				)
			} else {
				log.Warn(
					"failed to receive from stream",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
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
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int("size", size), zap.Int("eventLen", len(cevent.Events)),
				zap.Int("resolvedRegionCount", regionCount))
		}

		if len(cevent.Events) != 0 {
			if entries, ok := cevent.Events[0].Event.(*cdcpb.Event_Entries_); ok {
				commitTs := entries.Entries.Entries[0].CommitTs
				if maxCommitTs < commitTs {
					maxCommitTs = commitTs
				}
			}
		}
		err = s.sendRegionChangeEvents(ctx, cevent.Events, worker, pendingRegions, addr)
		if err != nil {
			return err
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTs(ctx, cevent.ResolvedTs, worker)
			if err != nil {
				return err
			}
			// TiKV send resolved ts events every second by default.
			// We check and update region count here to save CPU.
			atomic.StoreInt64(regionCount, worker.statesManager.regionCount())
			atomic.StoreUint64(&s.client.ingressResolvedTs, cevent.ResolvedTs.Ts)
			if maxCommitTs == 0 {
				// In case, there is no write for the table,
				// we use resolved ts as maxCommitTs to make the stats meaningful.
				atomic.StoreUint64(&s.client.ingressCommitTs, cevent.ResolvedTs.Ts)
			} else {
				atomic.StoreUint64(&s.client.ingressCommitTs, maxCommitTs)
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
	statefulEvents := make([][]*regionStatefulEvent, worker.concurrency)
	for i := 0; i < worker.concurrency; i++ {
		// Allocate a buffer with 2x length than average to reduce reallocate.
		buffLen := len(events) / worker.concurrency * 3 / 2
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
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("oldRequestID", state.requestID),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", addr))
				valid = false
			} else if state.requestID > event.RequestId {
				log.Warn("drop event due to event belongs to a stale request",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
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
			state, valid = pendingRegions.takeByRequestID(event.RequestId)
			if !valid {
				log.Warn("drop event due to region feed is removed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", addr))
				continue
			}
			state.start()
			worker.setRegionState(event.RegionId, state)
		} else if state.isStopped() {
			log.Warn("drop event due to region feed stopped",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
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
) error {
	statefulEvents := make([]*regionStatefulEvent, worker.concurrency)
	// split resolved ts
	for i := 0; i < worker.concurrency; i++ {
		// Allocate a buffer with 1.5x length than average to reduce reallocate.
		buffLen := len(resolvedTs.Regions) / worker.concurrency * 2
		ev := s.resolvedTsPool.Get().(*regionStatefulEvent)
		// must reset fields to prevent dirty data
		ev.resolvedTsEvent.resolvedTs = resolvedTs.Ts
		ev.resolvedTsEvent.regions = make([]*regionFeedState, 0, buffLen)
		ev.finishedCallbackCh = nil
		statefulEvents[i] = ev
	}

	for _, regionID := range resolvedTs.Regions {
		state, ok := worker.getRegionState(regionID)
		if ok {
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
