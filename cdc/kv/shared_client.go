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
	"io"
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
	"github.com/pingcap/tiflow/cdc/contextutil"
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
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// SharedClient is shared in many tables. Methods are thread-safe.
type SharedClient struct {
	changefeed model.ChangeFeedID
	startTs    model.Ts
	config     *config.KVClientConfig

	clusterID  uint64
	filterLoop bool

	pd           pd.Client
	grpcPool     GrpcPool
	regionCache  *tikv.RegionCache
	pdClock      pdutil.Clock
	tikvStorage  tidbkv.Storage
	lockResolver txnutil.LockResolver

	requestRangeCh *chann.DrainableChann[rangeRequestTask]
	regionCh       *chann.DrainableChann[singleRegionInfo]
	regionRouter   *chann.DrainableChann[singleRegionInfo]
	errCh          *chann.DrainableChann[regionErrorInfo]

	workers []*sharedRegionWorker

	totalSpans struct {
		sync.RWMutex
		m map[model.TableID]*requestedTable
	}

	// only modified in requestRegionToStore so lock is unnecessary.
	requestedStores map[string]*requestedStore
}

type requestedStore struct {
	storeID   uint64
	storeAddr string
	requests  *chann.DrainableChann[singleRegionInfo]

	requestedRegions struct {
		sync.RWMutex
		m map[requestedRegion]*regionFeedState
	}

	streams []*requestedStream
}

type requestedStream struct {
	client  cdcpb.ChangeData_EventFeedClient
	conn    *sharedConn
	regions []requestedRegion
}

type requestedRegion struct {
	regionID  uint64
	requestID uint64
}

type requestedTable struct {
	lock    *regionlock.RegionRangeLock
	span    tablepb.Span
	eventCh chan<- model.RegionFeedEvent
}

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
		filterLoop: filterLoop,

		pd:          pd,
		grpcPool:    grpcPool,
		regionCache: regionCache,
		pdClock:     pdClock,
		tikvStorage: kvStorage,

		requestedStores: make(map[string]*requestedStore),
	}
	s.totalSpans.m = make(map[model.TableID]*requestedTable)

	return s
}

func (s *SharedClient) Subscribe(span tablepb.Span, ts uint64, eventCh chan<- model.RegionFeedEvent) error {
	s.totalSpans.Lock()
	defer s.totalSpans.Unlock()
	if _, ok := s.totalSpans.m[span.TableID]; !ok {
		lock := regionlock.NewEmptyRegionRangeLock(s.changefeed.Namespace + "." + s.changefeed.ID)
		requestedTable := &requestedTable{lock, span, eventCh}
		s.totalSpans.m[span.TableID] = requestedTable
		s.requestRangeCh.In() <- rangeRequestTask{span, ts, requestedTable}
		return nil
	}
	return errors.New("redundant table subscription")
}

func (s *SharedClient) Run(ctx context.Context) error {
	s.clusterID = s.pd.GetClusterID(ctx)

	tikvStorage := s.tikvStorage.(tikv.Storage)
	role := contextutil.RoleFromCtx(ctx)
	s.lockResolver = txnutil.NewLockerResolver(tikvStorage, s.changefeed, role)

	s.requestRangeCh = chann.NewAutoDrainChann[rangeRequestTask]()
	s.regionCh = chann.NewAutoDrainChann[singleRegionInfo]()
	s.regionRouter = chann.NewAutoDrainChann[singleRegionInfo]()
	s.errCh = chann.NewAutoDrainChann[regionErrorInfo]()
	defer func() {
		s.requestRangeCh.CloseAndDrain()
		s.regionCh.CloseAndDrain()
		s.regionRouter.CloseAndDrain()
		s.errCh.CloseAndDrain()
	}()

	g, ctx := errgroup.WithContext(ctx)

	s.workers = make([]*sharedRegionWorker, 0, s.config.WorkerConcurrent)
	metrics := newWorkerMetrics(s.changefeed)
	for i := 0; i < s.config.WorkerConcurrent; i++ {
		worker := newSharedRegionWorker(s, metrics)
		g.Go(func() error { return worker.run(ctx) })
		s.workers = append(s.workers, worker)
	}

	g.Go(func() error { return s.dispatchRequest(ctx) })
	g.Go(func() error { return s.requestRegionToStore(ctx, g) })
	g.Go(func() error { return s.handleRequestRanges(ctx, g) })
	g.Go(func() error { return s.handleErrors(ctx) })

	log.Info("event feed started",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID))

	return g.Wait()
}

func (s *SharedClient) onRegionFail(ctx context.Context, errorInfo regionErrorInfo) {
	rangeLock := errorInfo.requestedTable.lock
	rangeLock.UnlockRange(errorInfo.span.StartKey, errorInfo.span.EndKey,
		errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.resolvedTs)
	s.enqueueError(ctx, errorInfo)
}

func (s *SharedClient) dispatchRequest(ctx context.Context) error {
	for {
		// Note that when a region is received from the channel, it's range has been already locked.
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionCh.Out():
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
		case sri.requestedTable.eventCh <- resolvedEv:
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
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", &sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs))
			// NOTE: The region hasn't be attached with a regionFeedState.
			s.onRegionFail(ctx, newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID}))
			continue
		}
		sri.rpcCtx = rpcCtx
		s.regionRouter.In() <- sri
	}
}

func (s *SharedClient) requestRegionToStore(ctx context.Context, g *errgroup.Group) error {
	for {
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Out():
			storeID := sri.rpcCtx.Peer.StoreId
			storeAddr := sri.rpcCtx.Addr
			rs := s.requestStore(ctx, g, storeID, storeAddr)
			rs.requests.In() <- sri
		}
	}
}

func (s *SharedClient) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	// todo: add metrics to track rpc cost
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.regionCache.GetTiKVRPCContext(bo, id, kvclientv2.ReplicaReadLeader, 0)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGetTiKVRPCContext, err)
	}
	return rpcCtx, nil
}

func (s *SharedClient) enqueueError(ctx context.Context, errorInfo regionErrorInfo) {
	select {
	case s.errCh.In() <- errorInfo:
	case <-ctx.Done():
	}
}

func (s *SharedClient) requestStore(
	ctx context.Context,
	g *errgroup.Group,
	storeID uint64, storeAddr string,
) *requestedStore {
	var rs *requestedStore
	if rs = s.requestedStores[storeAddr]; rs != nil {
		return rs
	}

	rs = &requestedStore{
		storeID:   storeID,
		storeAddr: storeAddr,
		requests:  chann.NewAutoDrainChann[singleRegionInfo](),
	}
	rs.requestedRegions.m = make(map[requestedRegion]*regionFeedState)
	rs.streams = make([]*requestedStream, s.config.GrpcStreamConcurrent)
	s.requestedStores[storeAddr] = rs

	streamID := atomic.Uint32{}
	for i := 0; i < s.config.GrpcStreamConcurrent; i++ {
		g.Go(func() (err error) {
			selfStreamID := streamID.Add(1) - 1
			for {
				log.Info("going to create grpc stream",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("storeID", storeID),
					zap.Uint32("streamID", selfStreamID),
					zap.String("addr", storeAddr))

				if err = rs.newStream(ctx, s, selfStreamID); err != nil {
					log.Warn("create grpc stream failed",
						zap.String("namespace", s.changefeed.Namespace),
						zap.String("changefeed", s.changefeed.ID),
						zap.Uint64("storeID", storeID),
						zap.String("addr", storeAddr),
						zap.Uint32("streamID", selfStreamID),
						zap.Error(err))
				} else {
					g, ctx := errgroup.WithContext(ctx)
					g.Go(func() (err error) { return s.sendToStream(ctx, rs, selfStreamID) })
					g.Go(func() (err error) { return s.receiveFromStream(ctx, rs, selfStreamID) })
					if err = g.Wait(); err == nil || errors.Cause(err) == context.Canceled {
						return nil
					}
				}

				states := make([]*regionFeedState, 0)
				chs := make([]<-chan struct{}, 0)
				for _, rr := range rs.clearStream(s, selfStreamID) {
					state := rs.takeState(rr.regionID, rr.requestID)
					state.markStopped()
					states = append(states, state)

					slot := hashRegionID(rr.regionID, uint64(len(s.workers)))
					ch, err := s.workers[slot].checkRegionStopped(ctx, state)
					if err != nil {
						return err
					}
					chs = append(chs, ch)
				}
				for _, ch := range chs {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ch:
					}
				}

				for _, state := range states {
					state.setRegionInfoResolvedTs()
					s.onRegionFail(ctx, newRegionErrorInfo(state.sri, &sendRequestToStoreErr{}))
				}

				if err = util.Hang(ctx, 5*time.Second); err != nil {
					return err
				}
			}
		})
	}

	return rs
}

func (s *SharedClient) createRegionRequest(sri singleRegionInfo) *cdcpb.ChangeDataRequest {
	rpcCtx := sri.rpcCtx
	regionID := rpcCtx.Meta.GetId()
	regionEpoch := rpcCtx.Meta.RegionEpoch
	requestID := allocID()

	return &cdcpb.ChangeDataRequest{
		Header:       &cdcpb.Header{ClusterId: s.clusterID, TicdcVersion: version.ReleaseSemver()},
		RegionId:     regionID,
		RequestId:    requestID,
		RegionEpoch:  regionEpoch,
		CheckpointTs: sri.resolvedTs,
		StartKey:     sri.span.StartKey,
		EndKey:       sri.span.EndKey,
		ExtraOp:      kvrpcpb.ExtraOp_ReadOldValue,
		FilterLoop:   s.filterLoop,
	}
}

func (s *SharedClient) sendToStream(ctx context.Context, rs *requestedStore, offset uint32) (err error) {
	stream := rs.streams[offset]
	for {
		// TODO(qupeng): maybe round-robbin in streams is better.
		var sri singleRegionInfo
		var req *cdcpb.ChangeDataRequest
		select {
		case sri := <-rs.requests.Out():
			req = s.createRegionRequest(sri)
		case <-ctx.Done():
			return ctx.Err()
		}

		state := newRegionFeedState(sri, req.RequestId)
		rs.setState(req.RegionId, req.RequestId, state)
		stream.regions = append(stream.regions, requestedRegion{regionID: req.RegionId, requestID: req.RequestId})

		if err = stream.client.Send(req); err != nil {
			log.Warn("send request to grpc stream failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Error(err))
			_ = stream.client.CloseSend()
			return err
		}
		state.start()
	}
}

func (s *SharedClient) receiveFromStream(ctx context.Context, rs *requestedStore, offset uint32) (err error) {
	metricBatchResolvedSize := batchResolvedEventSize.WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)

	stream := rs.streams[offset]
	for {
		cevent, err := stream.client.Recv()
		if err != nil {
			if err.Error() != io.EOF.Error() && errors.Cause(err) != context.Canceled {
				log.Warn("receive from grpc stream failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("storeID", rs.storeID),
					zap.String("addr", rs.storeAddr),
					zap.Error(err))
			}
			return context.Canceled
		}

		if len(cevent.Events) > 0 {
			err = s.sendRegionChangeEvents(ctx, cevent.Events, rs)
		} else if cevent.ResolvedTs != nil {
			metricBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTs(ctx, cevent.ResolvedTs, rs)
		}
		if err != nil {
			return err
		}
	}
}

// All events will always come from one same Region.
func (s *SharedClient) sendRegionChangeEvents(ctx context.Context, events []*cdcpb.Event, rs *requestedStore) error {
	if len(events) == 0 {
		return nil
	}

	regionID := events[0].RegionId
	requestID := events[0].RequestId
	state := rs.getState(regionID, requestID)

	var sfEvent statefulEvent
	sfEvent.rs = rs
	sfEvent.eventBatch.state = state
	sfEvent.eventBatch.items = events

	slot := hashRegionID(regionID, uint64(len(s.workers)))
	return s.workers[slot].sendEvent(ctx, sfEvent)
}

func (s *SharedClient) sendResolvedTs(ctx context.Context, resolvedTs *cdcpb.ResolvedTs, rs *requestedStore) error {
	requestID := resolvedTs.RequestId
	sfEvents := make([]statefulEvent, len(s.workers))

	for _, regionID := range resolvedTs.Regions {
		slot := hashRegionID(regionID, uint64(len(s.workers)))
		state := rs.getState(regionID, requestID)

		x := &sfEvents[slot].resolvedTsBatch
		x.ts = resolvedTs.Ts
		x.regions = append(x.regions, state)
	}

	for i, sfEvent := range sfEvents {
		if sfEvent.resolvedTsBatch.ts != 0 {
			sfEvent.rs = rs
			if err := s.workers[i].sendEvent(ctx, sfEvent); err != nil {
				return err
			}
		}
	}

	return nil
}

func (r *requestedStore) newStream(ctx context.Context, s *SharedClient, offset uint32) (err error) {
	stream := &requestedStream{}

	if stream.conn, err = s.grpcPool.GetConn(r.storeAddr); err != nil {
		return errors.Trace(err)
	}
	if err = version.CheckStoreVersion(ctx, s.pd, r.storeID); err != nil {
		return errors.Trace(err)
	}
	client := cdcpb.NewChangeDataClient(stream.conn.ClientConn)
	if stream.client, err = client.EventFeed(ctx); err != nil {
		return errors.Trace(err)
	}

	r.streams[offset] = stream
	return
}

func (r *requestedStore) clearStream(s *SharedClient, offset uint32) []requestedRegion {
	stream := r.streams[offset]
	if stream != nil {
		s.grpcPool.ReleaseConn(stream.conn, r.storeAddr)
		regions := stream.regions
		r.streams[offset] = nil
		return regions
	}
	return nil
}

func (r *requestedStore) getState(regionID, requestID uint64) *regionFeedState {
	r.requestedRegions.RLock()
	defer r.requestedRegions.RUnlock()

	key := requestedRegion{regionID, requestID}
	return r.requestedRegions.m[key]
}

func (r *requestedStore) setState(regionID, requestID uint64, state *regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()

	key := requestedRegion{regionID, requestID}
	r.requestedRegions.m[key] = state
}

func (r *requestedStore) takeState(regionID, requestID uint64) *regionFeedState {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()

	key := requestedRegion{regionID, requestID}
	state := r.requestedRegions.m[key]
	delete(r.requestedRegions.m, key)
	return state
}

func (r *requestedStore) takeAllStates() map[requestedRegion]*regionFeedState {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	regions := r.requestedRegions.m
	r.requestedRegions.m = make(map[requestedRegion]*regionFeedState)
	return regions
}

func (s *SharedClient) handleRequestRanges(ctx context.Context, g *errgroup.Group) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-s.requestRangeCh.Out():
			if s.getRequestedTable(task.span.TableID) != nil {
				// divideAndSendEventFeedToRegions could be blocked for some time,
				// since it must wait for the region lock available. In order to
				// consume region range request from `requestRangeCh` as soon as
				// possible, we create a new goroutine to handle it.
				// The sequence of region range we process is not matter, the
				// region lock keeps the region access sequence.
				// Besides the count or frequency of range request is limited,
				// we use ephemeral goroutine instead of permanent goroutine.
				g.Go(func() error {
					return s.divideAndRequestRegions(ctx, task.span, task.ts)
				})
			}
		}
	}
}

func (s *SharedClient) divideAndRequestRegions(ctx context.Context, span tablepb.Span, ts uint64) (err error) {
	rt := s.getRequestedTable(span.TableID)
	if rt == nil {
		return nil
	}

	limit := 1024
	nextSpan := span
	var regions []*tikv.Region
	for {
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		regions, err = s.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.StartKey, nextSpan.EndKey, limit)
		if err != nil {
			log.Warn("load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("span", nextSpan),
				zap.Error(err))
			return cerror.WrapError(cerror.ErrPDBatchLoadRegions, err)
		}

		metas := make([]*metapb.Region, 0, len(regions))
		for _, region := range regions {
			metas = append(metas, region.GetMeta())
		}
		if !regionlock.CheckRegionsLeftCover(metas, nextSpan) {
			return cerror.ErrRegionsNotCoverSpan.FastGenByArgs(nextSpan, metas)
		}

		for _, region := range regions {
			regionSpan := tablepb.Span{StartKey: region.GetMeta().StartKey, EndKey: region.GetMeta().EndKey}
			partialSpan, err := spanz.Intersect(rt.span, regionSpan)
			if err != nil {
				return errors.Trace(err)
			}
			// NOTE: the End key return by the PD API will be nil to represent the biggest key.
			partialSpan = spanz.HackSpan(partialSpan)
			sri := newSingleRegionInfo(region.VerID(), partialSpan, ts, nil)
			sri.requestedTable = rt
			s.scheduleRegionRequest(ctx, sri)

			nextSpan.StartKey = region.GetMeta().EndKey
			if spanz.EndCompare(nextSpan.StartKey, span.EndKey) >= 0 {
				return nil
			}
		}
	}
}

func (s *SharedClient) getRequestedTable(tableID model.TableID) *requestedTable {
	s.totalSpans.RLock()
	defer s.totalSpans.RUnlock()
	return s.totalSpans.m[tableID]
}

func (s *SharedClient) scheduleRegionRequest(ctx context.Context, sri singleRegionInfo) {
	handleResult := func(res regionlock.LockRangeResult) {
		switch res.Status {
		case regionlock.LockRangeStatusSuccess:
			sri.resolvedTs = res.CheckpointTs
			select {
			case s.regionCh.In() <- sri:
			case <-ctx.Done():
			}
		case regionlock.LockRangeStatusStale:
			log.Info("request expired",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", &sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs),
				zap.Any("retrySpans", res.RetryRanges))
			for _, r := range res.RetryRanges {
				s.scheduleDivideRegionAndRequest(ctx, r, sri.resolvedTs, sri.requestedTable)
			}
		case regionlock.LockRangeStatusCancel:
			return
		default:
			panic("unreachable")
		}
	}

	rangeLock := sri.requestedTable.lock
	res := rangeLock.LockRange(ctx, sri.span.StartKey, sri.span.EndKey, sri.verID.GetID(), sri.verID.GetVer())
	if res.Status == regionlock.LockRangeStatusWait {
		res = res.WaitFn()
	}
	handleResult(res)
}

func (s *SharedClient) scheduleDivideRegionAndRequest(
	ctx context.Context, span tablepb.Span, ts uint64,
	requestedTable *requestedTable,
) {
	select {
	case s.requestRangeCh.In() <- rangeRequestTask{span, ts, requestedTable}:
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
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs, errInfo.requestedTable)
			return nil
		}
		if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs, errInfo.requestedTable)
			return nil
		}
		if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			metricFeedDuplicateRequestCounter.Inc()
			s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
			return nil
		}
		if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			// TODO(qupeng): Currently compatibility check on TiKV is wrong.
			s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
			return nil
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
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span, errInfo.resolvedTs, errInfo.requestedTable)
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

func (s *SharedClient) recycleRegionStatefulEvents(_ ...*regionStatefulEvent) {}

func hashRegionID(regionID uint64, slots uint64) uint64 {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, regionID)
	return seahash.Sum64(b) % slots
}
