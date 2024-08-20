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
	"github.com/pingcap/tiflow/pkg/util/seahash"
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
	// Maximum total sleep time(in ms), 20 seconds.
	tikvRequestMaxBackoff = 20000

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	scanRegionsConcurrency = 1024

	loadRegionRetryInterval time.Duration  = 100 * time.Millisecond
	resolveLockMinInterval  time.Duration  = 10 * time.Second
	invalidSubscriptionID   SubscriptionID = SubscriptionID(0)
)

var (
	// To generate an ID for a new subscription. And the subscription ID will also be used as
	// `RequestId` in region requests of the table.
	subscriptionIDGen atomic.Uint64
	// To generate a streamID in `newStream`.
	streamIDGen atomic.Uint64
)

var (
	// unreachable error, only used in unit test
	errUnreachable = errors.New("kv client unreachable error")
	logPanic       = log.Panic
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

// MultiplexingEvent wrap a region event with
// SubscriptionID to indicate which subscription it belongs to.
type MultiplexingEvent struct {
	model.RegionFeedEvent
	SubscriptionID SubscriptionID
	Start          time.Time
}

// newMultiplexingEvent creates a new MultiplexingEvent.
func newMultiplexingEvent(e model.RegionFeedEvent, table *subscribedTable) MultiplexingEvent {
	return MultiplexingEvent{
		RegionFeedEvent: e,
		SubscriptionID:  table.subscriptionID,
		Start:           time.Now(),
	}
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

	totalSpans struct {
		sync.RWMutex
		v map[SubscriptionID]*subscribedTable
	}

	workers []*sharedRegionWorker
	// Note: stores is only motified in handleRegion goroutine,
	// so it is not protected by a lock.
	stores map[string]*requestedStore

	// rangeTaskCh is used to receive range tasks.
	// The tasks will be handled in `handleRangeTask` goroutine.
	rangeTaskCh *chann.DrainableChann[rangeTask]
	// regionCh is used to receive region tasks have been locked in rangeLock.
	// The region will be handled in `handleRegions` goroutine.
	regionCh *chann.DrainableChann[regionInfo]
	// resolveLockTaskCh is used to receive resolve lock tasks.
	// The tasks will be handled in `handleResolveLockTasks` goroutine.
	resolveLockTaskCh *chann.DrainableChann[resolveLockTask]
	errCh             *chann.DrainableChann[regionErrorInfo]

	logRegionDetails func(msg string, fields ...zap.Field)
}

type resolveLockTask struct {
	regionID uint64
	targetTs uint64
	state    *regionlock.LockedRangeState
	create   time.Time
}

// rangeTask represents a task to subscribe a range span of a table.
// It can be a part of a table or a whole table, it also can be a part of a region.
type rangeTask struct {
	span            tablepb.Span
	subscribedTable *subscribedTable
}

// requestedStore represents a store that has been connected.
// A store may have multiple streams.
type requestedStore struct {
	storeID   uint64
	storeAddr string
	// Use to select a stream to send request.
	nextStream atomic.Uint32
	streams    []*requestedStream
}

func (rs *requestedStore) getStream() *requestedStream {
	index := rs.nextStream.Add(1) % uint32(len(rs.streams))
	return rs.streams[index]
}

// subscribedTable represents a table to subscribe.
// It contains the span of the table, the startTs of the table, and the output event channel.
type subscribedTable struct {
	subscriptionID SubscriptionID
	startTs        model.Ts

	// The whole span of the table.
	span tablepb.Span
	// The range lock of the table,
	// it is used to prevent duplicate requests to the same region range,
	// and it also used to calculate this table's resolvedTs.
	rangeLock *regionlock.RangeLock
	// The output event channel of the table.
	eventCh chan<- MultiplexingEvent

	// To handle table removing.
	stopped atomic.Bool

	// To handle stale lock resolvings.
	tryResolveLock     func(regionID uint64, state *regionlock.LockedRangeState)
	staleLocksTargetTs atomic.Uint64

	lastAdvanceTime atomic.Int64
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

		rangeTaskCh:       chann.NewAutoDrainChann[rangeTask](),
		regionCh:          chann.NewAutoDrainChann[regionInfo](),
		resolveLockTaskCh: chann.NewAutoDrainChann[resolveLockTask](),
		errCh:             chann.NewAutoDrainChann[regionErrorInfo](),

		stores: make(map[string]*requestedStore),
	}
	s.totalSpans.v = make(map[SubscriptionID]*subscribedTable)
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
// It new a subscribedTable and store it in `s.totalSpans`,
// and send a rangeTask to `s.rangeTaskCh`.
// The rangeTask will be handled in `handleRangeTasks` goroutine.
func (s *SharedClient) Subscribe(subID SubscriptionID, span tablepb.Span, startTs uint64, eventCh chan<- MultiplexingEvent) {
	if span.TableID == 0 {
		log.Panic("event feed subscribe with zero tablepb.Span.TableID",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))
	}

	rt := s.newSubscribedTable(subID, span, startTs, eventCh)
	s.totalSpans.Lock()
	s.totalSpans.v[subID] = rt
	s.totalSpans.Unlock()

	s.rangeTaskCh.In() <- rangeTask{span: span, subscribedTable: rt}
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
func (s *SharedClient) ResolveLock(subID SubscriptionID, targetTs uint64) {
	s.totalSpans.Lock()
	rt := s.totalSpans.v[subID]
	s.totalSpans.Unlock()
	if rt != nil {
		rt.resolveStaleLocks(s, targetTs)
	}
}

// RegionCount returns subscribed region count for the span.
func (s *SharedClient) RegionCount(subID SubscriptionID) uint64 {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	if rt := s.totalSpans.v[subID]; rt != nil {
		return uint64(rt.rangeLock.Len())
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

	g.Go(func() error { return s.handleRangeTasks(ctx) })
	g.Go(func() error { return s.handleRegions(ctx, g) })
	g.Go(func() error { return s.handleErrors(ctx) })
	g.Go(func() error { return s.handleResolveLockTasks(ctx) })
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
	s.resolveLockTaskCh.CloseAndDrain()
	s.errCh.CloseAndDrain()
	s.clearMetrics()

	for _, rs := range s.stores {
		for _, stream := range rs.streams {
			stream.requests.CloseAndDrain()
		}
	}
}

func (s *SharedClient) setTableStopped(rt *subscribedTable) {
	log.Info("event feed starts to stop table",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Any("subscriptionID", rt.subscriptionID))

	// Set stopped to true so we can stop handling region events from the table.
	// Then send a special singleRegionInfo to regionRouter to deregister the table
	// from all TiKV instances.
	if rt.stopped.CompareAndSwap(false, true) {
		s.regionCh.In() <- regionInfo{subscribedTable: rt}
		if rt.rangeLock.Stop() {
			s.onTableDrained(rt)
		}
	}
}

func (s *SharedClient) onTableDrained(rt *subscribedTable) {
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

// handleRegions receives regionInfo from regionCh and attch rpcCtx to them,
// then send them to corresponding requestedStore.
func (s *SharedClient) handleRegions(ctx context.Context, eg *errgroup.Group) error {
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case region := <-s.regionCh.Out():
			if region.isStoped() {
				for _, rs := range s.stores {
					s.broadcastRequest(rs, region)
				}
				continue
			}

			region, ok := s.attachRPCContextForRegion(ctx, region)
			// If attachRPCContextForRegion fails, the region will be re-scheduled.
			if !ok {
				continue
			}

			store := s.getStore(ctx, eg, region.rpcCtx.Peer.StoreId, region.rpcCtx.Addr)
			stream := store.getStream()
			stream.requests.In() <- region

			s.logRegionDetails("event feed will request a region",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("streamID", stream.streamID),
				zap.Any("subscriptionID", region.subscribedTable.subscriptionID),
				zap.Uint64("regionID", region.verID.GetID()),
				zap.Uint64("storeID", store.storeID),
				zap.String("addr", store.storeAddr))
		}
	}
}

func (s *SharedClient) attachRPCContextForRegion(ctx context.Context, region regionInfo) (regionInfo, bool) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, region.verID, kvclientv2.ReplicaReadLeader, 0)
	if rpcCtx != nil {
		region.rpcCtx = rpcCtx
		locateTime := time.Since(region.lockedRangeState.Created).Milliseconds()
		s.metrics.regionLocateDuration.Observe(float64(locateTime))
		return region, true
	}
	if err != nil {
		log.Debug("event feed get RPC context fail",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", region.subscribedTable.subscriptionID),
			zap.Uint64("regionID", region.verID.GetID()),
			zap.Error(err))
	}
	s.onRegionFail(newRegionErrorInfo(region, &rpcCtxUnavailableErr{verID: region.verID}))
	return region, false
}

// getStore gets a requestedStore from requestedStores by storeAddr.
func (s *SharedClient) getStore(
	ctx context.Context, g *errgroup.Group,
	storeID uint64, storeAddr string,
) *requestedStore {
	var rs *requestedStore
	if rs = s.stores[storeAddr]; rs != nil {
		return rs
	}
	rs = &requestedStore{storeID: storeID, storeAddr: storeAddr}
	s.stores[storeAddr] = rs
	for i := uint(0); i < s.config.KVClient.GrpcStreamConcurrent; i++ {
		stream := newStream(ctx, s, g, rs)
		rs.streams = append(rs.streams, stream)
	}

	return rs
}

func (s *SharedClient) createRegionRequest(region regionInfo) *cdcpb.ChangeDataRequest {
	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     region.verID.GetID(),
		RequestId:    uint64(region.subscribedTable.subscriptionID),
		RegionEpoch:  region.rpcCtx.Meta.RegionEpoch,
		CheckpointTs: region.resolvedTs(),
		StartKey:     region.span.StartKey,
		EndKey:       region.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   s.filterLoop,
	}
}

func (s *SharedClient) broadcastRequest(r *requestedStore, region regionInfo) {
	for _, stream := range r.streams {
		stream.requests.In() <- region
	}
}

func (s *SharedClient) handleRangeTasks(ctx context.Context) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(scanRegionsConcurrency)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.rangeTaskCh.Out():
			g.Go(func() error { return s.divideSpanAndScheduleRegionRequests(ctx, task.span, task.subscribedTable) })
		}
	}
}

// divideSpanAndScheduleRegionRequests processes the specified span by dividing it into
// manageable regions and schedules requests to subscribe to these regions.
// 1. Load regions from PD.
// 2. Find the intersection of each region.span and the subscribedTable.span.
// 3. Schedule a region request to subscribe the region.
func (s *SharedClient) divideSpanAndScheduleRegionRequests(
	ctx context.Context,
	span tablepb.Span,
	subscribedTable *subscribedTable,
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
			zap.Any("subscriptionID", subscribedTable.subscriptionID),
			zap.Any("span", nextSpan))

		backoff := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err := s.regionCache.BatchLoadRegionsWithKeyRange(backoff, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("event feed load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("subscriptionID", subscribedTable.subscriptionID),
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
				zap.Any("subscriptionID", subscribedTable.subscriptionID),
				zap.Any("span", nextSpan))
			backoffBeforeLoad = true
			continue
		}

		for _, regionMeta := range regionMetas {
			regionSpan := tablepb.Span{StartKey: regionMeta.StartKey, EndKey: regionMeta.EndKey}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			// So we need to fix it by calling spanz.HackSpan.
			regionSpan = spanz.HackSpan(regionSpan)

			// Find the intersection of the regionSpan returned by PD and the subscribedTable.span.
			// The intersection is the span that needs to be subscribed.
			intersectantSpan, err := spanz.Intersect(subscribedTable.span, regionSpan)
			if err != nil {
				log.Panic("event feed check spans intersect shouldn't fail",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", subscribedTable.subscriptionID))
			}

			verID := tikv.NewRegionVerID(regionMeta.Id, regionMeta.RegionEpoch.ConfVer, regionMeta.RegionEpoch.Version)
			regionInfo := newRegionInfo(verID, intersectantSpan, nil, subscribedTable)

			// Schedule a region request to subscribe the region.
			s.scheduleRegionRequest(ctx, regionInfo)

			nextSpan.StartKey = regionMeta.EndKey
			// If the nextSpan.StartKey is larger than the subscribedTable.span.EndKey,
			// it means all span of the subscribedTable have been requested. So we return.
			if spanz.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

// scheduleRegionRequest locks the region's range and send the region to regionCh,
// which will be handled by handleRegions.
func (s *SharedClient) scheduleRegionRequest(ctx context.Context, region regionInfo) {
	lockRangeResult := region.subscribedTable.rangeLock.LockRange(
		ctx, region.span.StartKey, region.span.EndKey, region.verID.GetID(), region.verID.GetVer())

	if lockRangeResult.Status == regionlock.LockRangeStatusWait {
		lockRangeResult = lockRangeResult.WaitFn()
	}

	switch lockRangeResult.Status {
	case regionlock.LockRangeStatusSuccess:
		region.lockedRangeState = lockRangeResult.LockedRangeState
		lockTime := time.Since(region.lockedRangeState.Created).Milliseconds()
		s.metrics.regionLockDuration.Observe(float64(lockTime))
		select {
		case s.regionCh.In() <- region:
		case <-ctx.Done():
		}
	case regionlock.LockRangeStatusStale:
		for _, r := range lockRangeResult.RetryRanges {
			s.scheduleRangeRequest(ctx, r, region.subscribedTable)
		}
	default:
		return
	}
}

func (s *SharedClient) scheduleRangeRequest(
	ctx context.Context, span tablepb.Span,
	subscribedTable *subscribedTable,
) {
	select {
	case s.rangeTaskCh.In() <- rangeTask{span: span, subscribedTable: subscribedTable}:
	case <-ctx.Done():
	}
}

func (s *SharedClient) handleErrors(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case errInfo := <-s.errCh.Out():
			if err := s.doHandleError(ctx, errInfo); err != nil {
				return err
			}
		}
	}
}

func (s *SharedClient) doHandleError(ctx context.Context, errInfo regionErrorInfo) error {
	if errInfo.subscribedTable.rangeLock.UnlockRange(
		errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs()) {
		s.onTableDrained(errInfo.subscribedTable)
		return nil
	}

	err := errors.Cause(errInfo.err)
	switch eerr := err.(type) {
	case *eventError:
		innerErr := eerr.err
		s.logRegionDetails("cdc region error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Any("subscriptionID", errInfo.subscribedTable.subscriptionID),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			s.scheduleRegionRequest(ctx, errInfo.regionInfo)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedTable)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedTable)
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
			zap.Any("subscriptionID", errInfo.subscribedTable.subscriptionID),
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.scheduleRegionRequest(ctx, errInfo.regionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleRangeRequest(ctx, errInfo.span, errInfo.subscribedTable)
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
			zap.Any("subscriptionID", errInfo.subscribedTable.subscriptionID),
			zap.Error(err))
		return err
	}
}

func (s *SharedClient) handleResolveLockTasks(ctx context.Context) error {
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

	doResolve := func(regionID uint64, state *regionlock.LockedRangeState, targetTs uint64) {
		if state.ResolvedTs.Load() > targetTs || !state.Initialzied.Load() {
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

		if err := s.lockResolver.Resolve(ctx, regionID, targetTs); err != nil {
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
		case task = <-s.resolveLockTaskCh.Out():
			s.metrics.lockResolveWaitDuration.Observe(float64(time.Since(task.create).Milliseconds()))
			doResolve(task.regionID, task.state, task.targetTs)
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
			attr := rt.rangeLock.IterAll(nil)
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
			if len(attr.UnLockedRanges) > 0 {
				log.Info("event feed holes exist",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Any("subscriptionID", subscriptionID),
					zap.Any("holes", attr.UnLockedRanges))
			}
		}
		s.totalSpans.RUnlock()
		s.metrics.slowInitializeRegion.Set(float64(slowInitializeRegion))
	}
}

func (s *SharedClient) newSubscribedTable(
	subID SubscriptionID, span tablepb.Span, startTs uint64,
	eventCh chan<- MultiplexingEvent,
) *subscribedTable {
	cfName := s.changefeed.String()
	rangeLock := regionlock.NewRangeLock(uint64(subID), span.StartKey, span.EndKey, startTs, cfName)

	rt := &subscribedTable{
		subscriptionID: subID,
		span:           span,
		startTs:        startTs,
		rangeLock:      rangeLock,
		eventCh:        eventCh,
	}

	rt.tryResolveLock = func(regionID uint64, state *regionlock.LockedRangeState) {
		targetTs := rt.staleLocksTargetTs.Load()
		if state.ResolvedTs.Load() < targetTs && state.Initialzied.Load() {
			s.resolveLockTaskCh.In() <- resolveLockTask{
				regionID: regionID,
				targetTs: targetTs,
				state:    state,
				create:   time.Now(),
			}
		}
	}
	return rt
}

func (r *subscribedTable) resolveStaleLocks(s *SharedClient, targetTs uint64) {
	util.MustCompareAndMonotonicIncrease(&r.staleLocksTargetTs, targetTs)
	res := r.rangeLock.IterAll(r.tryResolveLock)
	s.logRegionDetails("event feed finds slow locked ranges",
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
