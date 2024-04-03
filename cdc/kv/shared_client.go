// Copyright 2023 PingCAP, Inc.
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
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"blainsmith.com/go/seahash"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/kv/sharedconn"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/cdc/processor/tablepb"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/spanz"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/prometheus/client_golang/prometheus"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

const (
	dialTimeout = 10 * time.Second
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

	// TiCDC may open numerous gRPC streams,
	// with 65535 bytes window size, 10K streams takes about 27GB memory.
	//
	// 65535 bytes, the initial window size in http2 spec.
	grpcInitialWindowSize = (1 << 16) - 1
	// 8 MB The value for initial window size on a connection
	grpcInitialConnWindowSize = 1 << 23
	// 256 MB The maximum message size the client can receive
	grpcMaxCallRecvMsgSize = 1 << 28

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	scanRegionsConcurrency = 1024
)

var (
	metricFeedNotLeaderCounter        = eventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = eventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = eventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = eventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = eventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = eventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricStoreSendRequestErr         = eventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricKvIsBusyCounter             = eventFeedErrorCounter.WithLabelValues("KvIsBusy")
)

var (
	// unreachable error, only used in unit test
	errUnreachable = errors.New("kv client unreachable error")
	logPanic       = log.Panic
)

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

type sendRequestToStoreErr struct{}

func (e *sendRequestToStoreErr) Error() string { return "send request to store error" }

// SubscriptionID comes from `SharedClient.AllocSubscriptionID`.
type SubscriptionID uint64

// MultiplexingEvent is like model.RegionFeedEvent.
type MultiplexingEvent struct {
	model.RegionFeedEvent
	SubscriptionID SubscriptionID
	Start          time.Time
}

// SharedClient is shared by many tables to pull events from TiKV.
// All exported Methods are thread-safe.
type SharedClient struct {
	changefeed model.ChangeFeedID
	config     *config.ServerConfig
	metrics    sharedClientMetrics

	clusterID  uint64
	filterLoop bool

	pd           pd.Client
	grpcPool     *sharedconn.ConnAndClientPool
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	lockResolver txnutil.LockResolver

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh *chann.DrainableChann[rangeTask]
	// regionCh is used to cache region tasks after ranges locked.
	// The region will be handled in `dispatchRegionRequest` goroutine.
	regionCh *chann.DrainableChann[regionInfo]
	// regionRouter is used to cache region tasks with rpcCtx attached.
	regionRouter *chann.DrainableChann[regionInfo]
	// resolveLockCh is used to retrieve resolve lock tasks.
	resolveLockCh *chann.DrainableChann[resolveLockTask]
	errCh         *chann.DrainableChann[regionErrorInfo]

	workers []*sharedRegionWorker

	// only modified in requestRegionToStore so lock is unnecessary.
	requestedStores map[string]*requestedStore

	totalSpans struct {
		sync.RWMutex
		v map[SubscriptionID]*requestedTable
	}

	logRegionDetails func(msg string, fields ...zap.Field)
}

type resolveLockTask struct {
	regionID   uint64
	maxVersion uint64
	state      *regionlock.LockedRange
	enter      time.Time
}

// rangeTask represents a task to subscribe a range span of a table.
// It can be a part of a table or a whole table, it also can be a part of a region.
type rangeTask struct {
	span           tablepb.Span
	requestedTable *requestedTable
}

type requestedStore struct {
	storeID    uint64
	storeAddr  string
	nextStream atomic.Uint32
	streams    []*requestedStream
}

type requestedTable struct {
	subscriptionID SubscriptionID

	span      tablepb.Span
	startTs   model.Ts
	rangeLock *regionlock.RegionRangeLock
	eventCh   chan<- MultiplexingEvent

	lastAdvanceTime atomic.Int64

	// To handle table removing.
	stopped atomic.Bool

	// To handle lock resolvings.
	postUpdateRegionResolvedTs func(regionID, version uint64, state *regionlock.LockedRange, span tablepb.Span)
	staleLocksVersion          atomic.Uint64
}

// NewSharedClient creates a client.
func NewSharedClient(
	changefeed model.ChangeFeedID,
	cfg *config.ServerConfig,
	filterLoop bool,
	pd pd.Client,
	grpcPool *sharedconn.ConnAndClientPool,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	lockResolver txnutil.LockResolver,
) *SharedClient {
	s := &SharedClient{
		changefeed: changefeed,
		config:     cfg,
		clusterID:  0,
		filterLoop: filterLoop,

		pd:           pd,
		grpcPool:     grpcPool,
		regionCache:  regionCache,
		pdClock:      pdClock,
		lockResolver: lockResolver,

		rangeTaskCh:   chann.NewAutoDrainChann[rangeTask](),
		regionCh:      chann.NewAutoDrainChann[regionInfo](),
		regionRouter:  chann.NewAutoDrainChann[regionInfo](),
		resolveLockCh: chann.NewAutoDrainChann[resolveLockTask](),
		errCh:         chann.NewAutoDrainChann[regionErrorInfo](),

		requestedStores: make(map[string]*requestedStore),
	}
	s.totalSpans.v = make(map[SubscriptionID]*requestedTable)
	if cfg.Debug.Puller.LogRegionDetails {
		s.logRegionDetails = log.Info
	} else {
		s.logRegionDetails = log.Debug
	}

	s.initMetrics()
	return s
}

// AllocSubscriptionID gets an ID can be used in `Subscribe`.
func (s *SharedClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(subscriptionIDGen.Add(1))
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
// It new a requestedTable and store it in `s.totalSpans`,
// and send a rangeTask to `s.requestRangeCh`.
// The rangeTask will be handled in `handleRequestRanges` goroutine.
func (s *SharedClient) Subscribe(subID SubscriptionID, span tablepb.Span, startTs uint64, eventCh chan<- MultiplexingEvent) {
	if span.TableID == 0 {
		log.Panic("event feed subscribe with zero tablepb.Span.TableID",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))
	}

	rt := s.newRequestedTable(subID, span, startTs, eventCh)
	s.totalSpans.Lock()
	s.totalSpans.v[subID] = rt
	s.totalSpans.Unlock()

	s.rangeTaskCh.In() <- rangeTask{span: span, requestedTable: rt}
	log.Info("event feed subscribes table success",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", rt.subscriptionID),
		zap.String("span", rt.span.String()))
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *SharedClient) Unsubscribe(subID SubscriptionID) {
	// NOTE: `subID` is cleared from `s.totalSpans` in `onTableDrained`.
	s.totalSpans.Lock()
	rt := s.totalSpans.v[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		s.setTableStopped(rt)
	}

	log.Info("event feed unsubscribes table",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", rt.subscriptionID),
		zap.Bool("exists", rt != nil))
}

// ResolveLock is a function. If outsider subscribers find a span resolved timestamp is
// advanced slowly or stopped, they can try to resolve locks in the given span.
func (s *SharedClient) ResolveLock(subID SubscriptionID, maxVersion uint64) {
	s.totalSpans.Lock()
	rt := s.totalSpans.v[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		rt.updateStaleLocks(s, maxVersion)
	}
}

// RegionCount returns subscribed region count for the span.
func (s *SharedClient) RegionCount(subID SubscriptionID) uint64 {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	if rt := s.totalSpans.v[subID]; rt != nil {
		return rt.rangeLock.RefCount()
	}
	return 0
}

// Run the client.
func (s *SharedClient) Run(ctx context.Context) error {
	s.clusterID = s.pd.GetClusterID(ctx)

	g, ctx := errgroup.WithContext(ctx)
	s.workers = make([]*sharedRegionWorker, 0, s.config.KVClient.WorkerConcurrent)
	for i := uint(0); i < s.config.KVClient.WorkerConcurrent; i++ {
		worker := newSharedRegionWorker(s)
		g.Go(func() error { return worker.run(ctx) })
		s.workers = append(s.workers, worker)
	}

	g.Go(func() error { return s.handleRangeTask(ctx) })
	g.Go(func() error { return s.handleRegionRequest(ctx) })
	g.Go(func() error { return s.requestRegionToStore(ctx, g) })
	g.Go(func() error { return s.handleErrors(ctx) })
	g.Go(func() error { return s.resolveLock(ctx) })
	g.Go(func() error { return s.logSlowRegions(ctx) })

	log.Info("event feed started",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID))
	defer log.Info("event feed exits",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID))
	return g.Wait()
}

// Close closes the client. Must be called after `Run` returns.
func (s *SharedClient) Close() {
	s.rangeTaskCh.CloseAndDrain()
	s.regionCh.CloseAndDrain()
	s.regionRouter.CloseAndDrain()
	s.resolveLockCh.CloseAndDrain()
	s.errCh.CloseAndDrain()
	s.clearMetrics()

	for _, rs := range s.requestedStores {
		for _, stream := range rs.streams {
			stream.requests.CloseAndDrain()
		}
	}
}

// GetPDClock returns a pdutil.Clock.
func (s *SharedClient) GetPDClock() pdutil.Clock {
	return s.pdClock
}

func (s *SharedClient) setTableStopped(rt *requestedTable) {
	log.Info("event feed starts to stop table",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", rt.subscriptionID))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionRouter.In() <- regionInfo{requestedTable: rt}
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *SharedClient) onTableDrained(rt *requestedTable) {
	log.Info("event feed stop table is finished",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", rt.subscriptionID))

	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	delete(s.totalSpans.v, rt.subscriptionID)
}

func (s *SharedClient) onRegionFail(errInfo regionErrorInfo) {
	s.errCh.In() <- errInfo
}

// handleRegionRequest receives regionInfo from regionCh and attch rpcCtx to them,
// then send them to regionRouter.
func (s *SharedClient) handleRegionRequest(ctx context.Context) error {
	attachCtx := func(ctx context.Context, sri regionInfo) {
		rpcCtx, err := s.getRPCContextForRegion(ctx, sri.verID)
		if rpcCtx != nil {
			sri.rpcCtx = rpcCtx
			locateTime := time.Since(sri.lockedRange.Created).Milliseconds()
			s.metrics.regionLocateDuration.Observe(float64(locateTime))
			s.regionRouter.In() <- sri
			return
		}
		if err != nil {
			log.Debug("event feed get RPC context fail",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("subscriptionID", sri.requestedTable.subscriptionID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Error(err))
		}
		s.onRegionFail(newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID}))
	}

	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri := <-s.regionCh.Out():
			attachCtx(ctx, sri)
		}
	}
}

func (s *SharedClient) requestRegionToStore(ctx context.Context, g *errgroup.Group) error {
	for {
		var region regionInfo
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case region = <-s.regionRouter.Out():
		}
		// If lockedRange is nil it means it's a special task from stopping the table.
		if region.lockedRange == nil {
			for _, rs := range s.requestedStores {
				s.broadcastRequest(rs, region)
			}
			continue
		}

		storeID := region.rpcCtx.Peer.StoreId
		storeAddr := region.rpcCtx.Addr
		s.appendRequest(s.requestStore(ctx, g, storeID, storeAddr), region)
	}
}

func (s *SharedClient) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, id, kvclientv2.ReplicaReadLeader, 0)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return rpcCtx, nil
}

func (s *SharedClient) requestStore(
	ctx context.Context, g *errgroup.Group,
	storeID uint64, storeAddr string,
) *requestedStore {
	var rs *requestedStore
	if rs = s.requestedStores[storeAddr]; rs != nil {
		return rs
	}

	rs = &requestedStore{storeID: storeID, storeAddr: storeAddr}
	s.requestedStores[storeAddr] = rs
	for i := uint(0); i < s.config.KVClient.GrpcStreamConcurrent; i++ {
		stream := newStream(ctx, s, g, rs)
		rs.streams = append(rs.streams, stream)
	}

	return rs
}

func (s *SharedClient) createRegionRequest(sri regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     sri.verID.GetID(),
		RequestId:    uint64(sri.requestedTable.subscriptionID),
		RegionEpoch:  sri.rpcCtx.Meta.RegionEpoch,
		CheckpointTs: sri.resolvedTs(),
		StartKey:     sri.span.StartKey,
		EndKey:       sri.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   s.filterLoop,
	}
}

func (s *SharedClient) appendRequest(r *requestedStore, sri regionInfo) {
	offset := r.nextStream.Add(1) % uint32(len(r.streams))
	s.logRegionDetails("event feed will request a region",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Uint64("streamID", r.streams[offset].streamID),
		zap.Any("subscriptionID", sri.requestedTable.subscriptionID),
		zap.Uint64("regionID", sri.verID.GetID()),
		zap.Uint64("storeID", r.storeID),
		zap.String("addr", r.storeAddr))
	r.streams[offset].requests.In() <- sri
}

func (s *SharedClient) broadcastRequest(r *requestedStore, sri regionInfo) {
	for _, stream := range r.streams {
		stream.requests.In() <- sri
	}
}

func (s *SharedClient) handleRangeTask(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(scanRegionsConcurrency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh.Out():
			g.Go(func() error { return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.requestedTable) })
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the requestedTable.span.
// 3. Schedule a region request to subscribe the region.
func (s *SharedClient) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span tablepb.Span,
	requestedTable *requestedTable,
) error {
	// Limit the number of regions loaded at a time to make the load more stable.
	limit := 1024
	nextSpan := span
	backoffBeforeLoad := false
	for {
		if backoffBeforeLoad {
			if err := util.Hang(ctx, loadRegionRetryInterval); err != nil {
				return err
			}
			backoffBeforeLoad = false
		}
		log.Debug("event feed is going to load regions",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", requestedTable.subscriptionID),
			zap.Any("span", nextSpan))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("event feed load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("subscriptionID", requestedTable.subscriptionID),
				zap.Any("span", nextSpan),
				zap.Error(err))
			backoffBeforeLoad = true
			continue
		}

		regionMetas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			if meta := region.GetMeta(); meta != nil {
				regionMetas = append(regionMetas, meta)
			}
		}
		regionMetas = regionlock.CutRegionsLeftCoverSpan(regionMetas, nextSpan)
		if len(regionMetas) == 0 {
			log.Warn("event feed load regions with holes",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("subscriptionID", requestedTable.subscriptionID),
				zap.Any("span", nextSpan))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := tablepb.Span{StartKey: regionMeta.StartKey, EndKey: regionMeta.EndKey}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = spanz.HackSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the requestedTable.span.
			// The intersection is the span that needs to be subscribed.
			intersectantSpan, err := spanz.Intersect(requestedTable.span, regionSpan)
			if err != nil {
				log.Panic("event feed check spans intersect shouldn't fail",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", requestedTable.subscriptionID))
			}

			verID := tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version)
			regionInfo := newRegionInfo(verID, intersectantSpan, nil, requestedTable)

			// Schedule a region request to subscribe the region.
			s.scheduleRegionRequest(ctx, regionInfo)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the requestedTable.span.EndKey,
			// it means all span of the requestedTable have been requested. So we return.
			if spanz.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the region's range and send the region to regionCh,
// which will be handled by handleRegionRequest.
func (s *SharedClient) scheduleRegionRequest(ctx context.Context, region regionInfo) {
	handleLockResult := func(res regionlock.LockRangeResult) {
		switch res.Status {
		case regionlock.LockRangeStatusSuccess:
			region.lockedRange = res.LockedRange
			lockTime := time.Since(region.lockedRange.Created).Milliseconds()
			s.metrics.regionLockDuration.Observe(float64(lockTime))
			select {
			case s.regionCh.In() <- region:
			case <-ctx.Done():
			}
		case regionlock.LockRangeStatusStale:
			for _, r := range res.RetryRanges {
				s.scheduleRangeRequest(ctx, r, region.requestedTable)
			}
		default:
			return
		}
	}

	rangeLock := region.requestedTable.rangeLock
	res := rangeLock.LockRange(ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())
	if res.Status == regionlock.LockRangeStatusWait {
		res = res.WaitFn()
	}
	handleLockResult(res)
}

func (s *SharedClient) scheduleRangeRequest(
	ctx context.Context, span tablepb.Span,
	requestedTable *requestedTable,
) {
	select {
	case s.rangeTaskCh.In() <- rangeTask{span: span, requestedTable: requestedTable}:
	case <-ctx.Done():
	}
}

func (s *SharedClient) handleErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case errInfo := <-s.errCh.Out():
			if err := s.handleError(ctx, errInfo); err != nil {
				return err
			}
		}
	}
}

func (s *SharedClient) handleError(ctx context.Context, errInfo regionErrorInfo) error {
	if errInfo.requestedTable.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		s.onTableDrained(errInfo.requestedTable)
		return nil
	}

	err := errors.Cause(errInfo.err)
	switch eerr := err.(type) {
	case *eventError:
		innerErr := eerr.err
		s.logRegionDetails("cdc region error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", errInfo.requestedTable.subscriptionID),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.requestedTable)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.requestedTable)
			return nil
		}
		if innerErr.GetServerIsBusy() != nil {
			metricKvIsBusyCounter.Inc()
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			metricFeedDuplicateRequestCounter.Inc()
			// TODO(qupeng): It's better to add a new machanism to deregister one region.
			return errors.New("duplicate request")
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		}
		if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		}

		log.Warn("empty or unknown cdc error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", errInfo.requestedTable.subscriptionID),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleRangeRequest(ctx, errInfo.span, errInfo.requestedTable)
		return nil
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	default:
		// TODO(qupeng): for some errors it's better to just deregister the region from TiKVs.
		log.Warn("event feed meets an internal error, fail the changefeed",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", errInfo.requestedTable.subscriptionID),
			zap.Error(err))
		return err
	}
}

func (s *SharedClient) resolveLock(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)

	gcResolveLastRun := func() {
		if len(resolveLastRun) > 1024 {
			copied := make(map[uint64]time.Time)
			now := time.Now()
			for regionID, lastRun := range resolveLastRun {
				if now.Sub(lastRun) < resolveLockMinInterval {
					resolveLastRun[regionID] = lastRun
				}
			}
			resolveLastRun = copied
		}
	}

	doResolve := func(regionID uint64, state *regionlock.LockedRange, maxVersion uint64) {
		if state.ResolvedTs.Load() > maxVersion || !state.Initialzied.Load() {
			return
		}
		if lastRun, ok := resolveLastRun[regionID]; ok {
			if time.Since(lastRun) < resolveLockMinInterval {
				return
			}
		}
		start := time.Now()
		defer func() {
			s.metrics.lockResolveRunDuration.Observe(float64(time.Since(start).Milliseconds()))
		}()

		if err := s.lockResolver.Resolve(ctx, regionID, maxVersion); err != nil {
			log.Warn("event feed resolve lock fail",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Error(err))
		}
		resolveLastRun[regionID] = time.Now()
	}

	gcTicker := time.NewTicker(resolveLockMinInterval * 3 / 2)
	defer gcTicker.Stop()
	for {
		var task resolveLockTask
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			gcResolveLastRun()
		case task = <-s.resolveLockCh.Out():
			s.metrics.lockResolveWaitDuration.Observe(float64(time.Since(task.enter).Milliseconds()))
			doResolve(task.regionID, task.state, task.maxVersion)
		}
	}
}

func (s *SharedClient) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
		log.Info("event feed starts to check locked regions",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))

		currTime := s.pdClock.CurrentTime()
		s.totalSpans.RLock()
		slowInitializeRegion := 0
		for subscriptionID, rt := range s.totalSpans.v {
			attr := rt.rangeLock.CollectLockedRangeAttrs(nil)
			ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.ResolvedTs)
			if attr.SlowestRegion.Initialized {
				if currTime.Sub(ckptTime) > 2*resolveLockMinInterval {
					log.Info("event feed finds a initialized slow region",
						zap.String("namespace", s.changefeed.Namespace),
						zap.String("changefeed", s.changefeed.ID),
						zap.Any("subscriptionID", subscriptionID),
						zap.Any("slowRegion", attr.SlowestRegion))
				}
			} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
				slowInitializeRegion += 1
				log.Info("event feed initializes a region too slow",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("slowRegion", attr.SlowestRegion))
			} else if currTime.Sub(ckptTime) > 10*time.Minute {
				log.Info("event feed finds a uninitialized slow region",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
			if len(attr.Holes) > 0 {
				log.Info("event feed holes exist",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("holes", attr.Holes))
			}
		}
		s.totalSpans.RUnlock()
		s.metrics.slowInitializeRegion.Set(float64(slowInitializeRegion))
	}
}

func (s *SharedClient) newRequestedTable(
	subID SubscriptionID, span tablepb.Span, startTs uint64,
	eventCh chan<- MultiplexingEvent,
) *requestedTable {
	cfName := s.changefeed.String()
	rangeLock := regionlock.NewRegionRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs, cfName)

	rt := &requestedTable{
		subscriptionID: subID,
		span:           span,
		startTs:        startTs,
		rangeLock:      rangeLock,
		eventCh:        eventCh,
	}

	rt.postUpdateRegionResolvedTs = func(regionID, _ uint64, state *regionlock.LockedRange, _ tablepb.Span) {
		maxVersion := rt.staleLocksVersion.Load()
		if state.ResolvedTs.Load() <= maxVersion && state.Initialzied.Load() {
			enter := time.Now()
			s.resolveLockCh.In() <- resolveLockTask{regionID, maxVersion, state, enter}
		}
	}
	return rt
}

func (r *requestedTable) associateSubscriptionID(event model.RegionFeedEvent) MultiplexingEvent {
	return MultiplexingEvent{
		RegionFeedEvent: event,
		SubscriptionID:  r.subscriptionID,
		Start:           time.Now(),
	}
}

func (r *requestedTable) updateStaleLocks(s *SharedClient, maxVersion uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksVersion, maxVersion)

	res := r.rangeLock.CollectLockedRangeAttrs(r.postUpdateRegionResolvedTs)
	log.Warn("event feed finds slow locked ranges",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", r.subscriptionID),
		zap.Any("ranges", res))
}

type sharedClientMetrics struct {
	regionLockDuration      prometheus.Observer
	regionLocateDuration    prometheus.Observer
	regionConnectDuration   prometheus.Observer
	batchResolvedSize       prometheus.Observer
	lockResolveWaitDuration prometheus.Observer
	lockResolveRunDuration  prometheus.Observer
	slowInitializeRegion    prometheus.Gauge
}

func (s *SharedClient) initMetrics() {
	eventFeedGauge.Inc()

	s.metrics.regionLockDuration = regionConnectDuration.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID, "lock")
	s.metrics.regionLocateDuration = regionConnectDuration.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID, "locate")
	s.metrics.regionConnectDuration = regionConnectDuration.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID, "connect")

	s.metrics.lockResolveWaitDuration = lockResolveDuration.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID, "wait")
	s.metrics.lockResolveRunDuration = lockResolveDuration.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID, "run")

	s.metrics.batchResolvedSize = batchResolvedEventSize.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)

	s.metrics.slowInitializeRegion = slowInitializeRegion.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)
}

func (s *SharedClient) clearMetrics() {
	eventFeedGauge.Dec()

	regionConnectDuration.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID, "lock")
	regionConnectDuration.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID, "locate")
	regionConnectDuration.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID, "connect")

	lockResolveDuration.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID, "wait")
	lockResolveDuration.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID, "run")

	batchResolvedEventSize.DeleteLabelValues(s.changefeed.Namespace, s.changefeed.ID)
}

func hashRegionID(regionID uint64, slots int) int {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, regionID)
	return int(seahash.Sum64(b) % uint64(slots))
}

var (
	// To generate an ID for a new subscription. And the subscription ID will also be used as
	// `RequestId` in region requests of the table.
	subscriptionIDGen atomic.Uint64

	// To generate a streamID in `newStream`.
	streamIDGen atomic.Uint64
)

const (
	loadRegionRetryInterval time.Duration  = 100 * time.Millisecond
	resolveLockMinInterval  time.Duration  = 10 * time.Second
	invalidSubscriptionID   SubscriptionID = SubscriptionID(0)
)
