// Copyright 2021 PingCAP, Inc.
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
	"math/rand"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"github.com/pingcap/ticdc/pkg/util"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type regionStatefulEvent struct {
	changeEvent *cdcpb.Event
	resolvedTs  *cdcpb.ResolvedTs
	state       *regionFeedState
}

func (s *eventFeedSession) sendRegionChangeEventV2(
	ctx context.Context,
	g *errgroup.Group,
	event *cdcpb.Event,
	worker *regionWorker,
	pendingRegions *syncRegionFeedStateMap,
	addr string,
	limiter *rate.Limiter,
) error {
	state, ok := worker.getRegionState(event.RegionId)
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
		worker.setRegionState(event.RegionId, state)

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

func (s *eventFeedSession) sendResolvedTsV2(
	ctx context.Context,
	g *errgroup.Group,
	resolvedTs *cdcpb.ResolvedTs,
	worker *regionWorker,
	addr string,
) error {
	for _, regionID := range resolvedTs.Regions {
		state, ok := worker.getRegionState(regionID)
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

// receiveFromStreamV2 receives gRPC messages from a stream continuously and sends
// messages to region worker, if `stream.Recv` meets error, this routine will exit
// silently. As for regions managed by this routine, there are two situations:
// 1. established regions: a `nil` event will be sent to region worker, and region
//    worker call `s.onRegionFail` to re-establish these regions.
// 2. pending regions: call `s.onRegionFail` for each pending region before this
//    routine exits to establish these regions.
func (s *eventFeedSession) receiveFromStreamV2(
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

		remainingRegions := pendingRegions.takeAll()
		for _, state := range remainingRegions {
			err := s.onRegionFail(ctx, regionErrorInfo{
				singleRegionInfo: state.sri,
				err:              cerror.ErrPendingRegionCancel.GenWithStackByArgs(),
			}, false /* initialized */)
			if err != nil {
				// The only possible is that the ctx is cancelled. Simply return.
				return
			}
		}
		s.workersLock.Lock()
		delete(s.workers, addr)
		s.workersLock.Unlock()
	}()

	captureAddr := util.CaptureAddrFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	metricSendEventBatchResolvedSize := batchResolvedEventSize.WithLabelValues(captureAddr, changefeedID)

	// always create a new region worker, because `receiveFromStreamV2` is ensured
	// to call exactly once from outter code logic
	worker := newRegionWorker(s, limiter)
	s.workersLock.Lock()
	s.workers[addr] = worker
	s.workersLock.Unlock()

	g.Go(func() error {
		return worker.run(ctx)
	})

	for {
		cevent, err := stream.Recv()

		failpoint.Inject("kvClientRegionReentrantError", func(op failpoint.Value) {
			if op.(string) == "error" {
				worker.inputCh <- nil
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
			err = s.sendRegionChangeEventV2(ctx, g, event, worker, pendingRegions, addr, limiter)
			if err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			metricSendEventBatchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			err = s.sendResolvedTsV2(ctx, g, cevent.ResolvedTs, worker, addr)
			if err != nil {
				return err
			}
		}
	}
}
