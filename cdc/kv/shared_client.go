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
	"sync"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tiflow/cdc/contextutil"
	"github.com/pingcap/tiflow/pkg/version"
	"github.com/pingcap/tiflow/cdc/kv/regionlock"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	"github.com/pingcap/tiflow/pkg/util"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/txnutil"
	kvclientv2 "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

// SharedClient is shared in many tables. Methods are thread-safe.
type SharedClient struct {
	changefeed model.ChangeFeedID
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

	eventCh   chan<- model.RegionFeedEvent
	rangeLock *regionlock.RegionRangeLock
    lockResolver txnutil.LockResolver

	tableStoreStats struct {
		sync.RWMutex
		// map[table_id/store_id] -> *tableStoreStat.
		v map[string]*tableStoreStat
	}
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

        eventCh: make(chan model.RegionFeedEvent, 128), // FIXME: size.
		rangeLock: regionlock.NewEmptyRegionRangeLock(changefeed.Namespace + "." + changefeed.ID),
	}
	s.tableStoreStats.v = make(map[string]*tableStoreStat)
	return s
}

func (s *SharedClient) Run(ctx context.Context) error {
	s.clusterID := s.pd.GetClusterID(ctx)

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

func (s *eventFeedSession) requestRegionToStore(ctx context.Context, g *errgroup.Group) error {
    streams := make(map[string]*eventFeedStream)
	storePendingRegions := make(map[string]*syncRegionFeedStateMap)

	var sri singleRegionInfo
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Out():
		}
		requestID := allocID()

		rpcCtx := sri.rpcCtx
		regionID := rpcCtx.Meta.GetId()
		regionEpoch := rpcCtx.Meta.RegionEpoch
		storeAddr := rpcCtx.Addr
		storeID := rpcCtx.Peer.GetStoreId()

		req := &cdcpb.ChangeDataRequest{
			Header:       &cdcpb.Header{ ClusterId:    s.clusterID, TicdcVersion: version.ReleaseSemver() },
			RegionId:     regionID,
			RequestId:    requestID,
			RegionEpoch:  regionEpoch,
			CheckpointTs: sri.resolvedTs,
			StartKey:     sri.span.StartKey,
			EndKey:       sri.span.EndKey,
			ExtraOp:      extraOp := kvrpcpb.ExtraOp_ReadOldValue,
			FilterLoop:   s.client.filterLoop,
		}

		failpoint.Inject("kvClientPendingRegionDelay", nil)

		stream, ok := streams[storeAddr]
		pendingRegions, _ := storePendingRegions[storeAddr]
		if !ok {
			stream, err = s.newStream(ctx, storeAddr, storeID)
			if err != nil {
				log.Warn("get grpc stream client failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", requestID),
					zap.Uint64("storeID", storeID),
					zap.Error(err))
				if cerror.ErrVersionIncompatible.Equal(err) {
                    _ = util.Hang(ctx, 20*time.Second)
                } else {
                    bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
                    s.regionCache.OnSendFail(bo, rpcCtx, regionScheduleReload, err)
                }
				errInfo := newRegionErrorInfo(sri, &connectToStoreErr{})
				s.onRegionFail(ctx, errInfo)
				continue
			}
            streams[storeAddr] = stream
			pendingRegions = newSyncRegionFeedStateMap()
			storePendingRegions[storeAddr] = pendingRegions

			log.Info("creating new stream to store to send request",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", requestID),
				zap.Uint64("storeID", storeID),
				zap.String("addr", storeAddr))

			g.Go(func() error {
				return s.receiveFromStream(ctx, g, storeAddr, storeID, stream.client, pendingRegions)
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
				continue
			}

			errInfo := newRegionErrorInfo(sri, &sendRequestToStoreErr{})
			s.onRegionFail(ctx, errInfo)
		}
	}
}

func (s *eventFeedSession) receiveFromStream(
	ctx context.Context,
	g *errgroup.Group,
	addr string,
	storeID uint64,
	stream cdcpb.ChangeData_EventFeedClient,
	pendingRegions *syncRegionFeedStateMap,
) error {
	var tsStat *tableStoreStat
	s.client.tableStoreStats.Lock()
	key := fmt.Sprintf("%d_%d", s.totalSpan.TableID, storeID)
	if tsStat = s.client.tableStoreStats.v[key]; tsStat == nil {
		tsStat = new(tableStoreStat)
		s.client.tableStoreStats.v[key] = tsStat
	}
	s.client.tableStoreStats.Unlock()

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
			s.onRegionFail(ctx, errInfo)
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
			// NOTE(qupeng): what if all regions are removed from the store?
			// TiKV send resolved ts events every second by default.
			// We check and update region count here to save CPU.
			tsStat.regionCount.Store(uint64(worker.statesManager.regionCount()))
			tsStat.resolvedTs.Store(cevent.ResolvedTs.Ts)
			if maxCommitTs == 0 {
				// In case, there is no write for the table,
				// we use resolved ts as maxCommitTs to make the stats meaningful.
				tsStat.commitTs.Store(cevent.ResolvedTs.Ts)
			} else {
				tsStat.commitTs.Store(maxCommitTs)
			}
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

func (s *SharedClient) onRegionFail(ctx context.Context, errorInfo regionErrorInfo) {
	s.rangeLock.UnlockRange(errorInfo.span.StartKey, errorInfo.span.EndKey,
		errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.resolvedTs)
	s.enqueueError(ctx, errorInfo)
}

func (s *SharedClient) enqueueError(ctx context.Context, errorInfo regionErrorInfo) {
	select {
	case s.errCh.In() <- errorInfo:
	case <-ctx.Done():
	}
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
