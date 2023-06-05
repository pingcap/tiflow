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
	"io"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
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

	pd          pd.Client
	grpcPool    GrpcPool
	regionCache *tikv.RegionCache
	pdClock     pdutil.Clock
	tikvStorage tidbkv.Storage

	requestRangeCh *chann.DrainableChann[rangeRequestTask]
	regionCh       *chann.DrainableChann[singleRegionInfo]
	regionRouter   *chann.DrainableChann[singleRegionInfo]
	errCh          *chann.DrainableChann[regionErrorInfo]

	eventCh      chan<- model.RegionFeedEvent
	rangeLock    *regionlock.RegionRangeLock
	lockResolver txnutil.LockResolver

	// only modified in requestRegionToStore so lock is unnecessary.
	requestedStores map[string]*requestedStore
}

type requestedStore struct {
	storeID          uint64
	storeAddr        string
	requests         *chann.DrainableChann[singleRegionInfo]
	requestedRegions map[requestedRegion]*regionFeedState
	worker           *regionWorker
	stream           eventFeedStream
}

type requestedRegion struct {
	regionID  uint64
	requestID uint64
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
	return &SharedClient{
		changefeed: changefeed,
		config:     cfg,
		filterLoop: filterLoop,

		pd:          pd,
		grpcPool:    grpcPool,
		regionCache: regionCache,
		pdClock:     pdClock,
		tikvStorage: kvStorage,

		eventCh:   make(chan model.RegionFeedEvent, 128), // FIXME: size.
		rangeLock: regionlock.NewEmptyRegionRangeLock(changefeed.Namespace + "." + changefeed.ID),

		requestedStores: make(map[string]*requestedStore),
	}
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
	g.Go(func() error { return s.dispatchRequest(ctx) })
	g.Go(func() error { return s.requestRegionToStore(ctx, g) })
	return g.Wait()
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
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", &sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs))
			errInfo := newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID})
			s.onRegionFail(ctx, errInfo)
			continue
		}
		sri.rpcCtx = rpcCtx
		s.regionRouter.In() <- sri
	}
}

func (s *SharedClient) requestRegionToStore(ctx context.Context, g *errgroup.Group) error {
	for {
		var sri singleRegionInfo
		var storeID uint64
		var storeAddr string
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Out():
			storeID = sri.rpcCtx.Peer.StoreId
			storeAddr = sri.rpcCtx.Addr
		}

		rs := s.requestStore(ctx, g, storeID, storeAddr)
		rs.requests.In() <- sri
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
	if rs, ok := s.requestedStores[storeAddr]; ok {
		return rs
	}

	rs := &requestedStore{
		storeID:          storeID,
		storeAddr:        storeAddr,
		requests:         chann.NewAutoDrainChann[singleRegionInfo](),
		requestedRegions: make(map[requestedRegion]*regionFeedState),
		worker:           newRegionWorker(s.changefeed, storeAddr, s.config.WorkerConcurrent, s),
	}
	s.requestedStores[storeAddr] = rs

	g.Go(func() (err error) { return rs.worker.run(ctx) })
	g.Go(func() (err error) {
		for {
			log.Info("going to create grpc stream",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("storeID", storeID),
				zap.String("addr", storeAddr))

			if err = rs.newStream(ctx, s); err != nil {
				log.Warn("create grpc stream failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("storeID", storeID),
					zap.String("addr", storeAddr),
					zap.Error(err))
			} else {
				g, ctx := errgroup.WithContext(ctx)
				g.Go(func() (err error) { return s.sendToStream(ctx, rs) })
				g.Go(func() (err error) { return s.receiveFromStream(ctx, rs) })
				if err = g.Wait(); err == nil || errors.Cause(err) == context.Canceled {
					return nil
				}
			}

			rs.clearStream(s)
			for _, state := range rs.requestedRegions {
				errInfo := newRegionErrorInfo(state.sri, &sendRequestToStoreErr{})
				s.onRegionFail(ctx, errInfo)
			}
			if err = util.Hang(ctx, 5*time.Second); err != nil {
				return err
			}
		}
	})

	return rs
}

func (s *SharedClient) delRequestedStore(ctx context.Context, storeAddr string) {
	if rs, ok := s.requestedStores[storeAddr]; ok {
		_ = rs.worker.sendEvents(ctx, []*regionStatefulEvent{nil})
		rs.worker.evictAllRegions()
		// TODO(qupeng): wait the store goroutines.
	}
	delete(s.requestedStores, storeAddr)
}

func (s *SharedClient) sendToStream(ctx context.Context, rs *requestedStore) (err error) {
	for {
		var sri singleRegionInfo
		var req *cdcpb.ChangeDataRequest
		select {
		case sri := <-rs.requests.Out():
			req = s.createRegionRequest(sri)
		case <-ctx.Done():
			return ctx.Err()
		}
		key := requestedRegion{regionID: req.RegionId, requestID: req.RequestId}
		rs.requestedRegions[key] = newRegionFeedState(sri, req.RequestId)
		if err = rs.stream.client.Send(req); err != nil {
			log.Warn("send request to grpc stream failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Error(err))
			_ = rs.stream.client.CloseSend()
			return err
		}
	}
}

func (s *SharedClient) receiveFromStream(ctx context.Context, rs *requestedStore) (err error) {
	metricBatchResolvedSize := batchResolvedEventSize.WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)

	for {
		cevent, err := rs.stream.client.Recv()
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
			// TODO(quepng): fix it.
			// err = s.sendRegionChangeEvents(ctx, cevent.Events, worker, pendingRegions, addr)
			// if err != nl {
			//     return errors.Trace(err)
			// }
		}
		if cevent.ResolvedTs != nil {
			metricBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			// err = s.sendResolvedTs(ctx, cevent.ResolvedTs, worker)
			// if err != nl {
			//     return errors.Trace(err)
			// }
		}
	}
}

func (r *requestedStore) newStream(ctx context.Context, s *SharedClient) (err error) {
	r.stream.conn, err = s.grpcPool.GetConn(r.storeAddr)
	if err != nil {
		return errors.Trace(err)
	}

	if err = version.CheckStoreVersion(ctx, s.pd, r.storeID); err != nil {
		return errors.Trace(err)
	}

	client := cdcpb.NewChangeDataClient(r.stream.conn.ClientConn)
	r.stream.client, err = client.EventFeed(ctx)
	return
}

func (r *requestedStore) clearStream(s *SharedClient) {
	r.stream.client = nil
	if r.stream.conn != nil {
		s.grpcPool.ReleaseConn(r.stream.conn, r.storeAddr)
		r.stream.conn = nil
	}
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

func (s *SharedClient) getStartTs() model.Ts {
	return s.startTs
}

func (s *SharedClient) getPDClock() pdutil.Clock {
	return s.pdClock
}

func (s *SharedClient) getLockResolver() txnutil.LockResolver {
	return s.lockResolver
}

func (s *SharedClient) getEventCh() chan<- model.RegionFeedEvent {
	return s.eventCh
}

func (s *SharedClient) onRegionFail(ctx context.Context, errorInfo regionErrorInfo) {
	s.rangeLock.UnlockRange(errorInfo.span.StartKey, errorInfo.span.EndKey,
		errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.resolvedTs)
	s.enqueueError(ctx, errorInfo)
}

func (s *SharedClient) getStreamCancel(string) context.CancelFunc {
	// FIXME(qupeng): add it.
	return nil
}

func (s *SharedClient) recycleRegionStatefulEvents(_ ...*regionStatefulEvent) {}
