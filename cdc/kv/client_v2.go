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

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/cdc/model"
	cerror "github.com/pingcap/ticdc/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
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
	state, ok := worker.regionStates[event.RegionId]
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
		worker.regionStates[event.RegionId] = state

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
		state, ok := worker.regionStates[regionID]
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
