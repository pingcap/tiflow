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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"blainsmith.com/go/seahash"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
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
	"google.golang.org/grpc"
	grpcerror "google.golang.org/grpc/codes"
	grpcmeta "google.golang.org/grpc/metadata"
)

// SubscriptionID comes from `SharedClient.AllocSubscriptionID`.
type SubscriptionID uint64

// MultiplexingEvent is like model.RegionFeedEvent.
type MultiplexingEvent struct {
	model.RegionFeedEvent
	SubscriptionID SubscriptionID
	Start          time.Time
}

// SharedClient is shared in many tables. Methods are thread-safe.
type SharedClient struct {
	changefeed model.ChangeFeedID
	config     *config.KVClientConfig
	metrics    sharedClientMetrics

	clusterID  uint64
	filterLoop bool

	pd           pd.Client
	grpcPool     GrpcPool
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	tikvStorage  tidbkv.Storage
	lockResolver txnutil.LockResolver

	// requestRangeCh is used to retrieve subscribed table tasks.
	requestRangeCh *chann.DrainableChann[rangeTask]
	// regionCh is used to cache region tasks after ranges locked.
	regionCh *chann.DrainableChann[singleRegionInfo]
	// regionRouter is used to cache region tasks with rpcCtx attached.
	regionRouter *chann.DrainableChann[singleRegionInfo]
	// resolveLockCh is used to retrieve resolve lock tasks.
	resolveLockCh *chann.DrainableChann[resolveLockTask]
	errCh         *chann.DrainableChann[regionErrorInfo]

	workers []*sharedRegionWorker

	// only modified in requestRegionToStore so lock is unnecessary.
	requestedStores map[string]*requestedStore

	totalSpans struct {
		sync.RWMutex
		m *spanz.HashMap[*requestedTable]
	}
}

type resolveLockTask struct {
	regionID   uint64
	maxVersion uint64
	enter      time.Time
}

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

type requestedStream struct {
	// init is for initializing client and conn asynchronously.
	init func() error
	// stream send/recv goroutines are maintained with wg.
	wg sync.WaitGroup

	streamID         uint64
	client           cdcpb.ChangeData_EventFeedClient
	conn             *sharedConn
	requests         *chann.DrainableChann[singleRegionInfo]
	requestedRegions struct {
		sync.RWMutex
		// whether the stream is in running or not.
		running bool
		// map[requestID]map[regionID]*regionFeedState
		m map[uint64]map[uint64]*regionFeedState
	}
}

type requestedTable struct {
	span      tablepb.Span
	startTs   model.Ts
	rangeLock *regionlock.RegionRangeLock
	eventCh   chan<- MultiplexingEvent
	requestID uint64

	// To handle table removings.
	removed    atomic.Bool
	deregister sync.Map // map[streamID]time.Time

	// To handle lock resolvings.
	staleLocks struct {
		maxVersion atomic.Uint64

		sync.RWMutex
		registered time.Time
	}
}

// NewSharedClient creates a client.
func NewSharedClient(
	changefeed model.ChangeFeedID,
	cfg *config.KVClientConfig,
	filterLoop bool,
	pd pd.Client,
	grpcPool GrpcPool,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	kvStorage tidbkv.Storage,
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
		tikvStorage:  kvStorage,
		lockResolver: nil,

		requestRangeCh: chann.NewAutoDrainChann[rangeTask](),
		regionCh:       chann.NewAutoDrainChann[singleRegionInfo](),
		regionRouter:   chann.NewAutoDrainChann[singleRegionInfo](),
		resolveLockCh:  chann.NewAutoDrainChann[resolveLockTask](),
		errCh:          chann.NewAutoDrainChann[regionErrorInfo](),

		requestedStores: make(map[string]*requestedStore),
	}
	s.totalSpans.m = spanz.NewHashMap[*requestedTable]()
	s.initMetrics()
	return s
}

// AllocSubscriptionID gets an ID can be used in `Subscribe`.
func (s *SharedClient) AllocSubscriptionID() SubscriptionID {
	return SubscriptionID(requestIDGen.Add(1))
}

// Subscribe the given table span.
// NOTE: `span.TableID` must be set correctly.
func (s *SharedClient) Subscribe(
	subID SubscriptionID, span tablepb.Span, startTs uint64,
	eventCh chan<- MultiplexingEvent,
) (existSubID SubscriptionID, success bool) {
	if span.TableID == 0 {
		log.Panic("event feed subscribe with zero tablepb.Span.TableID",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))
	}

	var requestedTable *requestedTable
	s.totalSpans.Lock()
	if requestedTable, _ = s.totalSpans.m.Get(span); requestedTable == nil {
		requestedTable = s.newRequestedTable(subID, span, startTs, eventCh)
		s.totalSpans.m.ReplaceOrInsert(span, requestedTable)
		s.totalSpans.Unlock()

		s.requestRangeCh.In() <- rangeTask{span: span, requestedTable: requestedTable}
		log.Info("event feed subscribes table success",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", span.TableID),
			zap.Uint64("requestID", requestedTable.requestID))
		success = true
	} else {
		s.totalSpans.Unlock()
		existSubID = SubscriptionID(requestedTable.requestID)
		success = false
	}
	return
}

// Unsubscribe the given table span. All covered regions will be deregistered asynchronously.
// NOTE: `span.TableID` must be set correctly.
func (s *SharedClient) Unsubscribe(span tablepb.Span) (subID SubscriptionID, success bool) {
	if span.TableID == 0 {
		log.Panic("event feed unsubscribe with zero tablepb.Span.TableID",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))
	}

	s.totalSpans.Lock()
	if rt, ok := s.totalSpans.m.Get(span); ok {
		s.totalSpans.m.Delete(span)
		s.totalSpans.Unlock()

		rt.removed.Store(true)
		log.Info("event feed unsubscribes table is began",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", rt.span.TableID),
			zap.Uint64("requestID", rt.requestID))
		if rt.rangeLock.Stop() {
			log.Info("event feed unsubscribes table is finished",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.String("span", rt.span.String()))
		}
		subID = SubscriptionID(rt.requestID)
		success = true
	} else {
		s.totalSpans.Unlock()
	}
	return
}

// ResolveLock is a function. If outsider subscribers find a span resolved timestamp is
// advanced slowly or stopped, they can try to resolve locks in the given span.
func (s *SharedClient) ResolveLock(span tablepb.Span) (success bool) {
	if span.TableID == 0 {
		log.Panic("event feed unsubscribe with zero tablepb.Span.TableID",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID))
	}

	var requestedTable *requestedTable
	s.totalSpans.Lock()
	requestedTable, _ = s.totalSpans.m.Get(span)
	if requestedTable == nil {
		s.totalSpans.Unlock()
		return false
	}
	s.totalSpans.Unlock()

	if requestedTable.updateStaleLocks(s) {
		res := requestedTable.rangeLock.CheckLockedRanges()
		log.Warn("event feed finds slow locked ranges",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", requestedTable.span.TableID),
			zap.Uint64("requestID", requestedTable.requestID),
			zap.Any("LockedRanges", res))
	}
	return true
}

// RegionCount returns subscribed region count for the span.
func (s *SharedClient) RegionCount(span tablepb.Span) uint64 {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	if t, ok := s.totalSpans.m.Get(span); ok {
		return t.rangeLock.RefCount()
	}
	return 0
}

// Run the client.
func (s *SharedClient) Run(ctx context.Context) error {
	s.clusterID = s.pd.GetClusterID(ctx)
	s.lockResolver = txnutil.NewLockerResolver(s.tikvStorage.(tikv.Storage), s.changefeed)

	g, ctx := errgroup.WithContext(ctx)

	s.workers = make([]*sharedRegionWorker, 0, s.config.WorkerConcurrent)
	for i := uint(0); i < s.config.WorkerConcurrent; i++ {
		worker := newSharedRegionWorker(s)
		g.Go(func() error { return worker.run(ctx) })
		s.workers = append(s.workers, worker)
	}

	g.Go(func() error { return s.handleRequestRanges(ctx, g) })
	g.Go(func() error { return s.dispatchRequest(ctx) })
	g.Go(func() error { return s.requestRegionToStore(ctx) })
	g.Go(func() error { return s.handleErrors(ctx) })
	g.Go(func() error { return s.resolveLock(ctx) })

	log.Info("event feed started",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID))

	err := g.Wait()
	for _, rs := range s.requestedStores {
		for _, stream := range rs.streams {
			stream.wg.Wait()
		}
	}
	return err
}

// Close closes the client. Must be called after `Run` returns.
func (s *SharedClient) Close() {
	s.requestRangeCh.CloseAndDrain()
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

func (s *SharedClient) onRegionFail(ctx context.Context, errInfo regionErrorInfo) {
	select {
	case s.errCh.In() <- errInfo:
	case <-ctx.Done():
	}
}

func (s *SharedClient) dispatchRequest(ctx context.Context) error {
	attachCtx := func(ctx context.Context, sri singleRegionInfo) {
		rpcCtx, err := s.getRPCContextForRegion(ctx, sri.verID)
		if rpcCtx != nil {
			sri.rpcCtx = rpcCtx
			s.metrics.regionLocateDuration.Observe(float64(time.Since(sri.createTime).Milliseconds()))
			s.regionRouter.In() <- sri
			return
		}
		if err != nil {
			// NOTE: retry later instead of failing the changefeed.
			log.Warn("event feed get RPC context fail",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Error(err))
		}
		s.onRegionFail(ctx, newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID}))
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

func (s *SharedClient) requestRegionToStore(ctx context.Context) error {
	for {
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Out():
			storeID := sri.rpcCtx.Peer.StoreId
			storeAddr := sri.rpcCtx.Addr
			rs := s.requestStore(ctx, storeID, storeAddr)

			requestID := sri.requestedTable.requestID
			regionID := sri.verID.GetID()
			state := newRegionFeedState(sri, requestID)
			state.start()
			s.metrics.regionConnectDuration.Observe(float64(time.Since(state.sri.createTime).Milliseconds()))

			offset := rs.nextStream.Add(1) % uint32(len(rs.streams))
			rs.streams[offset].requests.In() <- sri
			s.requestRegion(ctx, rs, rs.streams[offset], requestID, regionID, state)
		}
	}
}

func (s *SharedClient) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, id, kvclientv2.ReplicaReadLeader, 0)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGetTiKVRPCContext, err)
	}
	return rpcCtx, nil
}

func (s *SharedClient) requestStore(
	ctx context.Context,
	storeID uint64, storeAddr string,
) *requestedStore {
	var rs *requestedStore
	if rs = s.requestedStores[storeAddr]; rs != nil {
		return rs
	}

	rs = &requestedStore{storeID: storeID, storeAddr: storeAddr}
	s.requestedStores[storeAddr] = rs
	for i := uint(0); i < s.config.GrpcStreamConcurrent; i++ {
		stream := s.newStream(ctx, rs)
		rs.streams = append(rs.streams, stream)
	}

	return rs
}

func (s *SharedClient) createRegionRequest(sri singleRegionInfo) *cdcpb.ChangeDataRequest {
	rpcCtx := sri.rpcCtx
	regionID := rpcCtx.Meta.GetId()
	regionEpoch := rpcCtx.Meta.RegionEpoch
	requestID := sri.requestedTable.requestID

	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     regionID,
		RequestId:    requestID,
		RegionEpoch:  regionEpoch,
		CheckpointTs: sri.resolvedTs(),
		StartKey:     sri.span.StartKey,
		EndKey:       sri.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   s.filterLoop,
	}
}

func (s *SharedClient) sendToStream(ctx context.Context, rs *requestedStore, stream *requestedStream) (err error) {
	doSend := func(req *cdcpb.ChangeDataRequest) error {
		if err := stream.client.Send(req); err != nil {
			log.Warn("event feed send request to grpc stream failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Error(err))
			_ = stream.client.CloseSend()
			return errors.Trace(err)
		}
		return nil
	}

	for {
		var sri singleRegionInfo
		var req *cdcpb.ChangeDataRequest
		select {
		case sri = <-stream.requests.Out():
		case <-ctx.Done():
			return ctx.Err()
		}

		if sri.requestedTable.removed.Load() {
			req = &cdcpb.ChangeDataRequest{
				RequestId: sri.requestedTable.requestID,
				Request:   &cdcpb.ChangeDataRequest_Deregister_{},
			}
			if err = doSend(req); err != nil {
				return err
			}
			for _, state := range stream.takeStates(sri.requestedTable.requestID) {
				state.markStopped(&sendRequestToStoreErr{})
				slot := hashRegionID(state.sri.verID.GetID(), len(s.workers))
				sfEvent := statefulEvent{eventItem: eventItem{state: state}}
				if err = s.workers[slot].sendEvent(ctx, sfEvent); err != nil {
					return errors.Trace(err)
				}
			}
			continue
		}

		req = s.createRegionRequest(sri)
		if err = doSend(req); err != nil {
			return err
		}

	}
}

func (s *SharedClient) receiveFromStream(ctx context.Context, rs *requestedStore, stream *requestedStream) (err error) {
	for {
		cevent, err := stream.client.Recv()
		if err != nil {
			code := grpc.Code(err)
			if code != grpcerror.OK && code != grpcerror.Canceled {
				log.Warn("event feed receive from grpc stream failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("storeID", rs.storeID),
					zap.String("addr", rs.storeAddr),
					zap.Error(err))
				return errors.Trace(err)
			}
			return context.Canceled
		}

		if len(cevent.Events) > 0 {
			if err = s.sendRegionChangeEvents(ctx, cevent.Events, stream); err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			s.metrics.batchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			if err = s.sendResolvedTs(ctx, cevent.ResolvedTs, stream); err != nil {
				return err
			}
		}
	}
}

func (s *SharedClient) sendRegionChangeEvents(ctx context.Context, events []*cdcpb.Event, rs *requestedStream) error {
	for _, event := range events {
		regionID := event.RegionId
		requestID := event.RequestId
		if state := rs.getState(requestID, regionID); state != nil {
			sfEvent := newEventItem(event, state, rs)
			slot := hashRegionID(regionID, len(s.workers))
			if err := s.workers[slot].sendEvent(ctx, sfEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (s *SharedClient) sendResolvedTs(ctx context.Context, resolvedTs *cdcpb.ResolvedTs, rs *requestedStream) error {
	requestID := resolvedTs.RequestId
	sfEvents := make([]statefulEvent, len(s.workers))
	log.Debug("event feed get a ResolvedTs",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Uint64("ResolvedTs", resolvedTs.Ts),
		zap.Uint64("requestID", resolvedTs.RequestId),
		zap.Int("regionCount", len(resolvedTs.Regions)))

	for _, regionID := range resolvedTs.Regions {
		slot := hashRegionID(regionID, len(s.workers))
		if sfEvents[slot].stream == nil {
			sfEvents[slot] = newResolvedTsBatch(resolvedTs.Ts, rs)
		}
		x := &sfEvents[slot].resolvedTsBatch
		if state := rs.getState(requestID, regionID); state != nil {
			x.regions = append(x.regions, state)
		}
	}

	for i, sfEvent := range sfEvents {
		if len(sfEvent.resolvedTsBatch.regions) > 0 {
			sfEvent.stream = rs
			if err := s.workers[i].sendEvent(ctx, sfEvent); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SharedClient) newStream(ctx context.Context, r *requestedStore) *requestedStream {
	stream := &requestedStream{
		streamID: streamIDGen.Add(1),
		requests: chann.NewAutoDrainChann[singleRegionInfo](),
	}

	stream.requestedRegions.m = make(map[uint64]map[uint64]*regionFeedState)
	stream.init = func() (err error) {
		if stream.conn, err = s.grpcPool.GetConn(r.storeAddr); err != nil {
			return errors.Trace(err)
		}
		if err = version.CheckStoreVersion(ctx, s.pd, r.storeID); err != nil {
			return errors.Trace(err)
		}
		rpc := cdcpb.NewChangeDataClient(stream.conn.ClientConn)
		ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
		if stream.client, err = rpc.EventFeedV2(ctx); err != nil {
			return errors.Trace(err)
		}
		return nil
	}
	return stream
}

func (s *SharedClient) clearStream(r *requestedStore, stream *requestedStream) {
	if stream.conn != nil {
		s.grpcPool.ReleaseConn(stream.conn, r.storeAddr)
		stream.conn = nil
	}
}

func (s *SharedClient) runStream(
	ctx context.Context,
	rs *requestedStore,
	stream *requestedStream,
) {
	defer s.clearStream(rs, stream)
	log.Info("event feed going to create grpc stream",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Uint64("storeID", rs.storeID),
		zap.String("addr", rs.storeAddr))

	if err := stream.init(); err != nil {
		log.Warn("event feed create grpc stream failed",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Error(err))
	} else {
		g, ctx := errgroup.WithContext(ctx)
		g.Go(func() (err error) { return s.sendToStream(ctx, rs, stream) })
		g.Go(func() (err error) { return s.receiveFromStream(ctx, rs, stream) })
		if err := g.Wait(); err == nil || errors.Cause(err) == context.Canceled {
			return
		}
	}

	// If any error happens and it's not canceled, clear pending region states
	// so we can re-schedule them later.
	for _, m := range stream.clearStates() {
		for _, state := range m {
			state.markStopped(&sendRequestToStoreErr{})
			slot := hashRegionID(state.sri.verID.GetID(), len(s.workers))
			sfEvent := statefulEvent{eventItem: eventItem{state: state}}
			_ = s.workers[slot].sendEvent(ctx, sfEvent)
		}
	}
}

func (s *SharedClient) requestRegion(
	ctx context.Context,
	rs *requestedStore,
	stream *requestedStream,
	requestID, regionID uint64,
	state *regionFeedState,
) {
	stream.requestedRegions.Lock()
	defer stream.requestedRegions.Unlock()

	var m map[uint64]*regionFeedState
	if m = stream.requestedRegions.m[requestID]; m == nil {
		m = make(map[uint64]*regionFeedState)
		stream.requestedRegions.m[requestID] = m
	}
	m[regionID] = state

	if !stream.requestedRegions.running {
		stream.requestedRegions.running = true
		stream.wg.Add(1)
		go func() {
			defer stream.wg.Done()
			s.runStream(ctx, rs, stream)
		}()
	}
}

func (r *requestedStream) handleRemovedTable(requestedTable *requestedTable) {
	if requestedTable.removed.Load() {
		now := time.Now()
		value, loaded := requestedTable.deregister.LoadOrStore(r.streamID, now)
		if !loaded || now.Sub(value.(time.Time)) > 10*time.Second {
			if requestedTable.deregister.CompareAndSwap(r.streamID, value, now) {
				r.requests.In() <- singleRegionInfo{requestedTable: requestedTable}
			}
		}
	}
}

func (r *requestedStream) getState(requestID, regionID uint64) (state *regionFeedState) {
	r.requestedRegions.RLock()
	defer r.requestedRegions.RUnlock()
	if m, ok := r.requestedRegions.m[requestID]; ok {
		state = m[regionID]
	}
	if state != nil && state.sri.requestedTable.removed.Load() {
		r.handleRemovedTable(state.sri.requestedTable)
		return nil
	}
	return state
}

// NOTE: SharedClient.onRegionFail can only be called with the state.
func (r *requestedStream) takeState(requestID, regionID uint64) (state *regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	if m, ok := r.requestedRegions.m[requestID]; ok {
		state = m[regionID]
		delete(m, regionID)
	}
	return
}

func (r *requestedStream) takeStates(requestID uint64) (v map[uint64]*regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	v = r.requestedRegions.m[requestID]
	delete(r.requestedRegions.m, requestID)
	return
}

func (r *requestedStream) clearStates() (v map[uint64]map[uint64]*regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	r.requestedRegions.running = false
	v = r.requestedRegions.m
	r.requestedRegions.m = make(map[uint64]map[uint64]*regionFeedState)
	return
}

func (s *SharedClient) handleRequestRanges(ctx context.Context, g *errgroup.Group) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.requestRangeCh.Out():
			g.Go(func() error { return s.divideAndRequestRegions(ctx, task.span, task.requestedTable) })
		}
	}
}

func (s *SharedClient) divideAndRequestRegions(
	ctx context.Context,
	span tablepb.Span,
	requestedTable *requestedTable,
) (err error) {
	limit := 1024
	nextSpan := span
	var regions []*tikv.Region
	for {
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err = s.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("event feed load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("span", nextSpan),
				zap.Error(err))

			// Re-schedule the span with a simple backoff.
			if err = util.Hang(ctx, 3*time.Second); err != nil {
				return err
			}
			s.scheduleDivideRegionAndRequest(ctx, nextSpan, requestedTable)
			return
		}

		metas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			metas = append(metas, region.GetMeta())
		}
		if !regionlock.CheckRegionsLeftCover(metas, nextSpan) {
			log.Panic("event feed check span left cover shouldn't fail",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID))
		}

		for _, region := range regions {
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			regionSpan := tablepb.Span{StartKey: region.GetMeta().StartKey, EndKey: region.GetMeta().EndKey}
			regionSpan = spanz.HackSpan(regionSpan)
			partialSpan, err := spanz.Intersect(requestedTable.span, regionSpan)
			if err != nil {
				log.Panic("event feed check spans intersect shouldn't fail",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID))
			}
			sri := newSingleRegionInfo(region.VerID(), partialSpan, nil)
			sri.requestedTable = requestedTable
			s.scheduleRegionRequest(ctx, sri)

			nextSpan.StartKey = region.GetMeta().EndKey
			if spanz.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

func (s *SharedClient) scheduleRegionRequest(ctx context.Context, sri singleRegionInfo) {
	sri.createTime = time.Now()
	handleResult := func(res regionlock.LockRangeResult) {
		switch res.Status {
		case regionlock.LockRangeStatusSuccess:
			sri.lockedRange = res.LockedRange
			s.metrics.regionLockDuration.Observe(float64(time.Since(sri.createTime).Milliseconds()))
			select {
			case s.regionCh.In() <- sri:
			case <-ctx.Done():
			}
		case regionlock.LockRangeStatusStale:
			for _, r := range res.RetryRanges {
				s.scheduleDivideRegionAndRequest(ctx, r, sri.requestedTable)
			}
		default:
			return
		}
	}

	rangeLock := sri.requestedTable.rangeLock
	res := rangeLock.LockRange(ctx, sri.span.StartKey, sri.span.EndKey, sri.verID.GetID(), sri.verID.GetVer())
	if res.Status == regionlock.LockRangeStatusWait {
		res = res.WaitFn()
	}
	handleResult(res)
}

func (s *SharedClient) scheduleDivideRegionAndRequest(
	ctx context.Context, span tablepb.Span,
	requestedTable *requestedTable,
) {
	select {
	case s.requestRangeCh.In() <- rangeTask{span: span, requestedTable: requestedTable}:
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
	rangeLock := errInfo.requestedTable.rangeLock
	tableRemoved := rangeLock.UnlockRange(errInfo.span.StartKey, errInfo.span.EndKey,
		errInfo.verID.GetID(), errInfo.verID.GetVer(), errInfo.resolvedTs())
	if tableRemoved {
		log.Info("event feed unsubscribes table is finished",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.String("span", errInfo.requestedTable.span.String()))
		return nil
	}

	err := errInfo.err
	switch eerr := errors.Cause(err).(type) {
	case *eventError:
		innerErr := eerr.err
		log.Debug("cdc error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			s.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
			s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
			return nil
		}
		if innerErr.GetEpochNotMatch() != nil {
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.requestedTable)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.requestedTable)
			return nil
		}
		if duplicated := innerErr.GetDuplicateRequest(); duplicated != nil {
			metricFeedDuplicateRequestCounter.Inc()
			return cerror.ErrDuplicatedRegionRequest.GenWithStackByArgs(duplicated.RegionId)
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
			zap.Stringer("error", innerErr))
		metricFeedUnknownErrorCounter.Inc()
		s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
		return nil
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.requestedTable)
		return nil
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
		s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
		return nil
	default:
		log.Warn("changefeed client or worker meets internal error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Error(err))
		s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
		return nil

	}
}

func (s *SharedClient) resolveLock(ctx context.Context) error {
	resolveLastRun := make(map[uint64]time.Time)
	gcTicker := time.NewTicker(resolveLockFence * 3 / 2)
	defer gcTicker.Stop()

	gcResolveLastRun := func() {
		if len(resolveLastRun) > 1024 {
			copied := make(map[uint64]time.Time)
			now := time.Now()
			for regionID, lastRun := range resolveLastRun {
				if now.Sub(lastRun) < resolveLockFence {
					resolveLastRun[regionID] = lastRun
				}
			}
			resolveLastRun = copied
		}
	}

	doResolve := func(regionID uint64, maxVersion uint64) {
		start := time.Now()
		defer s.metrics.lockResolveRunDuration.Observe(float64(time.Since(start).Milliseconds()))
		if lastRun, ok := resolveLastRun[regionID]; ok {
			if time.Since(lastRun) < resolveLockFence {
				return
			}
		}
		if err := s.lockResolver.Resolve(ctx, regionID, maxVersion); err != nil {
			log.Warn("event feed resolve lock fail",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Error(err))
		}
		resolveLastRun[regionID] = time.Now()
	}
LOOP:
	for {
		var task resolveLockTask
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-gcTicker.C:
			gcResolveLastRun()
			continue LOOP
		case task = <-s.resolveLockCh.Out():
			s.metrics.lockResolveWaitDuration.Observe(float64(time.Since(task.enter).Milliseconds()))
			doResolve(task.regionID, task.maxVersion)
		}
	}
}

func (s *SharedClient) newRequestedTable(
	subID SubscriptionID, span tablepb.Span, startTs uint64,
	eventCh chan<- MultiplexingEvent,
) *requestedTable {
	requestedTable := &requestedTable{
		span:      span,
		startTs:   startTs,
		eventCh:   eventCh,
		requestID: uint64(subID),
	}

	requestedTable.rangeLock = regionlock.NewRegionRangeLock(
		span.StartKey, span.EndKey, startTs,
		s.changefeed.Namespace+"."+s.changefeed.ID,
		func(regionID uint64, state *regionlock.LockedRange) {
			maxVersion := requestedTable.staleLocks.maxVersion.Load()
			if state.CheckpointTs.Load() <= maxVersion {
				enter := time.Now()
				s.resolveLockCh.In() <- resolveLockTask{regionID, maxVersion, enter}
			}
		},
	)

	return requestedTable
}

func (r *requestedTable) associateSubscriptionID(event model.RegionFeedEvent) MultiplexingEvent {
	return MultiplexingEvent{
		RegionFeedEvent: event,
		SubscriptionID:  SubscriptionID(r.requestID),
		Start:           time.Now(),
	}
}

func (r *requestedTable) updateStaleLocks(s *SharedClient) (success bool) {
	currentTime := s.pdClock.CurrentTime()
	r.staleLocks.Lock()
	if currentTime.Sub(r.staleLocks.registered) < resolveLockFence/2 {
		r.staleLocks.Unlock()
		return false
	}
	r.staleLocks.registered = currentTime
	r.staleLocks.Unlock()

	maxVersion := oracle.ComposeTS(oracle.GetPhysical(currentTime.Add(-1*resolveLockFence)), 0)
	r.staleLocks.maxVersion.Store(maxVersion)
	r.rangeLock.CallPostLockSuccess()
	return true
}

type sharedClientMetrics struct {
	regionLockDuration      prometheus.Observer
	regionLocateDuration    prometheus.Observer
	regionConnectDuration   prometheus.Observer
	batchResolvedSize       prometheus.Observer
	lockResolveWaitDuration prometheus.Observer
	lockResolveRunDuration  prometheus.Observer
}

func (s *SharedClient) initMetrics() {
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
}

func (s *SharedClient) clearMetrics() {
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
	// To generate a requestID in `newRequestedTable`.
	requestIDGen atomic.Uint64

	// To generate a streamID in `newStream`.
	streamIDGen atomic.Uint64
)

const (
	rpcMetaFeaturesKey               string = "features"
	rpcMetaFeatureStreamMultiplexing string = "stream-multiplexing"
	rpcMetaFeaturesSep               string = ","

	resolveLockFence time.Duration = 10 * time.Second
)

func getContextFromFeatures(ctx context.Context, features []string) context.Context {
	return grpcmeta.NewOutgoingContext(
		ctx,
		grpcmeta.New(map[string]string{rpcMetaFeaturesKey: strings.Join(features, rpcMetaFeaturesSep)}),
	)
}
