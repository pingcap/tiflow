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
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/kv/sharedconn"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/util"
	"github.com/pingcap/tiflow/pkg/version"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	grpcstatus "google.golang.org/grpc/status"
)

type requestedStream struct {
	streamID uint64

	// To trigger a connect action lazily.
	preFetchForConnecting *singleRegionInfo
	requests              *chann.DrainableChann[singleRegionInfo]

	requestedRegions struct {
		sync.RWMutex
		// map[SubscriptionID]map[RegionID]*regionFeedState
		m map[SubscriptionID]map[uint64]*regionFeedState
	}

	// multiplexing is for sharing one GRPC stream in many tables.
	multiplexing *sharedconn.ConnAndClient

	// tableExclusives means one GRPC stream is exclusive by one table.
	tableExclusives struct {
		m         sync.Map // map[SubscriptionID]*sharedconn.ConnAndClient
		receivers chan SubscriptionID
	}
}

func newStream(ctx context.Context, c *SharedClient, g *errgroup.Group, r *requestedStore) *requestedStream {
	stream := &requestedStream{
		streamID: streamIDGen.Add(1),
		requests: chann.NewAutoDrainChann[singleRegionInfo](),
	}
	stream.requestedRegions.m = make(map[SubscriptionID]map[uint64]*regionFeedState)

	waitForPreFetching := func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case sri := <-stream.requests.Out():
				if sri.lockedRange != nil {
					stream.preFetchForConnecting = new(singleRegionInfo)
					*stream.preFetchForConnecting = sri
					return nil
				}
			}
		}
	}

	g.Go(func() error {
		for {
			if err := waitForPreFetching(); err != nil {
				return err
			}
			if canceled := stream.run(ctx, c, r); canceled {
				return nil
			}
			for _, m := range stream.clearStates() {
				for _, state := range m {
					state.markStopped(&sendRequestToStoreErr{})
					slot := hashRegionID(state.sri.verID.GetID(), len(c.workers))
					sfEvent := statefulEvent{eventItem: eventItem{state: state}}
					_ = c.workers[slot].sendEvent(ctx, sfEvent)
				}
			}
			// Why we need to re-schedule pending regions? This because the store can
			// fail forever, and all regions are scheduled to other stores.
			for _, sri := range stream.clearPendingRegions() {
				c.onRegionFail(newRegionErrorInfo(sri, &sendRequestToStoreErr{}))
			}
			if err := util.Hang(ctx, time.Second); err != nil {
				return err
			}
		}
	})

	return stream
}

func (s *requestedStream) run(ctx context.Context, c *SharedClient, rs *requestedStore) (canceled bool) {
	isCanceled := func() bool {
		select {
		case <-ctx.Done():
			return true
		default:
			return false
		}
	}

	if err := version.CheckStoreVersion(ctx, c.pd, rs.storeID); err != nil {
		log.Info("event feed check store version fails",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Uint64("streamID", s.streamID),
			zap.Error(err))
		return isCanceled()
	}

	log.Info("event feed going to create grpc stream",
		zap.String("namespace", c.changefeed.Namespace),
		zap.String("changefeed", c.changefeed.ID),
		zap.Uint64("storeID", rs.storeID),
		zap.String("addr", rs.storeAddr),
		zap.Uint64("streamID", s.streamID))

	defer func() {
		log.Info("event feed grpc stream exits",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Uint64("streamID", s.streamID),
			zap.Bool("canceled", canceled))
		if s.multiplexing != nil {
			s.multiplexing.Release()
		} else {
			s.tableExclusives.m.Range(func(_, value any) bool {
				value.(*sharedconn.ConnAndClient).Release()
				return true
			})
			s.tableExclusives.m = sync.Map{}
			close(s.tableExclusives.receivers)
		}
	}()

	cc, err := c.grpcPool.Connect(ctx, rs.storeAddr)
	if err != nil {
		log.Warn("event feed create grpc stream failed",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Uint64("streamID", s.streamID),
			zap.Error(err))
		return isCanceled()
	}

	g, ctx := errgroup.WithContext(ctx)
	if cc.Multiplexing() {
		s.multiplexing = cc
		g.Go(func() error { return s.receive(ctx, c, rs, s.multiplexing, invalidSubscriptionID) })
	} else {
		log.Info("event feed stream multiplexing is not supported, will fallback",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Uint64("streamID", s.streamID))
		cc.Release()

		s.tableExclusives.receivers = make(chan SubscriptionID, 8)
		g.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case subscriptionID := <-s.tableExclusives.receivers:
					if value, ok := s.tableExclusives.m.Load(subscriptionID); ok {
						cc := value.(*sharedconn.ConnAndClient)
						g.Go(func() error { return s.receive(ctx, c, rs, cc, subscriptionID) })
					}
				}
			}
		})
	}
	g.Go(func() error { return s.send(ctx, c, rs) })
	_ = g.Wait()
	return isCanceled()
}

func (s *requestedStream) receive(
	ctx context.Context,
	c *SharedClient,
	rs *requestedStore,
	cc *sharedconn.ConnAndClient,
	subscriptionID SubscriptionID,
) error {
	client := cc.Client()
	for {
		cevent, err := client.Recv()
		if err != nil {
			log.Info("event feed receive from grpc stream failed",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Uint64("streamID", s.streamID),
				zap.String("code", grpcstatus.Code(err).String()),
				zap.Error(err))
			if sharedconn.StatusIsEOF(grpcstatus.Convert(err)) {
				return nil
			}
			return errors.Trace(err)
		}
		if len(cevent.Events) > 0 {
			if err := s.sendRegionChangeEvents(ctx, c, cevent.Events, subscriptionID); err != nil {
				return err
			}
		}
		if cevent.ResolvedTs != nil {
			c.metrics.batchResolvedSize.Observe(float64(len(cevent.ResolvedTs.Regions)))
			if err := s.sendResolvedTs(ctx, c, cevent.ResolvedTs, subscriptionID); err != nil {
				return err
			}
		}
	}
}

func (s *requestedStream) send(ctx context.Context, c *SharedClient, rs *requestedStore) (err error) {
	doSend := func(cc *sharedconn.ConnAndClient, req *cdcpb.ChangeDataRequest) error {
		if err := cc.Client().Send(req); err != nil {
			log.Warn("event feed send request to grpc stream failed",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Any("subscriptionID", SubscriptionID(req.RequestId)),
				zap.Uint64("regionID", req.RegionId),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Uint64("streamID", s.streamID),
				zap.Error(err))
			return errors.Trace(err)
		}
		log.Debug("event feed send request to grpc stream success",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Any("subscriptionID", SubscriptionID(req.RequestId)),
			zap.Uint64("regionID", req.RegionId),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Uint64("streamID", s.streamID))
		return nil
	}

	fetchMoreReq := func() (sri singleRegionInfo, _ error) {
		waitReqTicker := time.NewTicker(60 * time.Second)
		defer waitReqTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return sri, ctx.Err()
			case sri = <-s.requests.Out():
				return sri, nil
			case <-waitReqTicker.C:
				// The stream is idle now, will be re-established when necessary.
				if s.countStates() == 0 {
					return sri, errors.New("closed as idle")
				}
			}
		}
	}

	getTableExclusiveConn := func(subscriptionID SubscriptionID) (cc *sharedconn.ConnAndClient, err error) {
		value, ok := s.tableExclusives.m.Load(subscriptionID)
		if !ok {
			if cc, err = c.grpcPool.Connect(ctx, rs.storeAddr); err != nil {
				return
			}
			if cc.Multiplexing() {
				cc.Release()
				cc, err = nil, errors.New("multiplexing is enabled, will re-establish the stream")
				return
			}
			s.tableExclusives.m.Store(subscriptionID, cc)
			select {
			case <-ctx.Done():
			case s.tableExclusives.receivers <- subscriptionID:
			}
		} else {
			cc = value.(*sharedconn.ConnAndClient)
		}
		return
	}

	sri := *s.preFetchForConnecting
	s.preFetchForConnecting = nil
	for {
		subscriptionID := sri.requestedTable.subscriptionID
		// It means it's a special task for stopping the table.
		if sri.lockedRange == nil {
			if s.multiplexing != nil {
				req := &cdcpb.ChangeDataRequest{
					RequestId: uint64(subscriptionID),
					Request:   &cdcpb.ChangeDataRequest_Deregister_{},
				}
				if err = doSend(s.multiplexing, req); err != nil {
					return err
				}
			} else {
				if value, ok := s.tableExclusives.m.LoadAndDelete(subscriptionID); ok {
					value.(*sharedconn.ConnAndClient).Release()
				}
			}
			// NOTE: some principles to help understand deregistering a table:
			// 1. after a Deregister(requestID) message is sent out, no more region requests
			//    with the same requestID will be sent out in the same GRPC stream;
			// 2. so it's OK to clear all pending states in the GRPC stream;
			// 3. is it possible that TiKV is keeping to send events belong to a removed state?
			//    I guess no because internal errors will cause the changefeed or table stopped,
			//    and then those regions from the bad requestID will be unsubscribed finally.
			for _, state := range s.takeStates(subscriptionID) {
				state.markStopped(&sendRequestToStoreErr{})
				slot := hashRegionID(state.sri.verID.GetID(), len(c.workers))
				sfEvent := statefulEvent{eventItem: eventItem{state: state}}
				if err = c.workers[slot].sendEvent(ctx, sfEvent); err != nil {
					return errors.Trace(err)
				}
			}
		} else if sri.requestedTable.stopped.Load() {
			// It can be skipped directly because there must be no pending states from
			// the stopped requestedTable, or the special singleRegionInfo for stopping
			// the table will be handled later.
			c.onRegionFail(newRegionErrorInfo(sri, &sendRequestToStoreErr{}))
		} else {
			connectTime := time.Since(sri.lockedRange.Created).Milliseconds()
			c.metrics.regionConnectDuration.Observe(float64(connectTime))

			state := newRegionFeedState(sri, uint64(subscriptionID))
			state.start()
			s.setState(subscriptionID, sri.verID.GetID(), state)

			var cc *sharedconn.ConnAndClient
			if s.multiplexing != nil {
				cc = s.multiplexing
			} else if cc, err = getTableExclusiveConn(subscriptionID); err != nil {
				return err
			}
			if err = doSend(cc, c.createRegionRequest(sri)); err != nil {
				return err
			}
		}

		if sri, err = fetchMoreReq(); err != nil {
			return err
		}
	}
}

func (r *requestedStream) countStates() (sum int) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	for _, mm := range r.requestedRegions.m {
		sum += len(mm)
	}
	return
}

func (r *requestedStream) setState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	var m map[uint64]*regionFeedState
	if m = r.requestedRegions.m[subscriptionID]; m == nil {
		m = make(map[uint64]*regionFeedState)
		r.requestedRegions.m[subscriptionID] = m
	}
	m[regionID] = state
}

func (r *requestedStream) getState(subscriptionID SubscriptionID, regionID uint64) (state *regionFeedState) {
	r.requestedRegions.RLock()
	defer r.requestedRegions.RUnlock()
	if m, ok := r.requestedRegions.m[subscriptionID]; ok {
		state = m[regionID]
	}
	return state
}

func (r *requestedStream) takeState(subscriptionID SubscriptionID, regionID uint64) (state *regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	if m, ok := r.requestedRegions.m[subscriptionID]; ok {
		state = m[regionID]
		delete(m, regionID)
	}
	return
}

func (r *requestedStream) takeStates(subscriptionID SubscriptionID) (v map[uint64]*regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	v = r.requestedRegions.m[subscriptionID]
	delete(r.requestedRegions.m, subscriptionID)
	return
}

func (r *requestedStream) clearStates() (v map[SubscriptionID]map[uint64]*regionFeedState) {
	r.requestedRegions.Lock()
	defer r.requestedRegions.Unlock()
	v = r.requestedRegions.m
	r.requestedRegions.m = make(map[SubscriptionID]map[uint64]*regionFeedState)
	return
}

func (r *requestedStream) clearPendingRegions() []singleRegionInfo {
	regions := make([]singleRegionInfo, 0, r.requests.Len()+1)
	if r.preFetchForConnecting != nil {
		sri := *r.preFetchForConnecting
		r.preFetchForConnecting = nil
		regions = append(regions, sri)
	}
	for i := 1; i < cap(regions); i++ {
		regions = append(regions, <-r.requests.Out())
	}
	return regions
}

func (s *requestedStream) sendRegionChangeEvents(
	ctx context.Context, c *SharedClient, events []*cdcpb.Event,
	tableSubID SubscriptionID,
) error {
	for _, event := range events {
		regionID := event.RegionId
		subscriptionID := tableSubID
		if subscriptionID == invalidSubscriptionID {
			subscriptionID = SubscriptionID(event.RequestId)
		}
		if state := s.getState(subscriptionID, regionID); state != nil {
			sfEvent := newEventItem(event, state, s)
			slot := hashRegionID(regionID, len(c.workers))
			if err := c.workers[slot].sendEvent(ctx, sfEvent); err != nil {
				return errors.Trace(err)
			}
		}
	}
	return nil
}

func (s *requestedStream) sendResolvedTs(
	ctx context.Context, c *SharedClient, resolvedTs *cdcpb.ResolvedTs,
	tableSubID SubscriptionID,
) error {
	subscriptionID := tableSubID
	if subscriptionID == invalidSubscriptionID {
		subscriptionID = SubscriptionID(resolvedTs.RequestId)
	}
	sfEvents := make([]statefulEvent, len(c.workers))
	log.Debug("event feed get a ResolvedTs",
		zap.String("namespace", c.changefeed.Namespace),
		zap.String("changefeed", c.changefeed.ID),
		zap.Any("subscriptionID", subscriptionID),
		zap.Uint64("ResolvedTs", resolvedTs.Ts),
		zap.Int("regionCount", len(resolvedTs.Regions)))

	for _, regionID := range resolvedTs.Regions {
		slot := hashRegionID(regionID, len(c.workers))
		if sfEvents[slot].stream == nil {
			sfEvents[slot] = newResolvedTsBatch(resolvedTs.Ts, s)
		}
		x := &sfEvents[slot].resolvedTsBatch
		if state := s.getState(subscriptionID, regionID); state != nil {
			x.regions = append(x.regions, state)
		}
	}

	for i, sfEvent := range sfEvents {
		if len(sfEvent.resolvedTsBatch.regions) > 0 {
			sfEvent.stream = s
			if err := c.workers[i].sendEvent(ctx, sfEvent); err != nil {
				return err
			}
		}
	}
	return nil
}
