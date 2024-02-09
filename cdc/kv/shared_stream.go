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

	logRegionDetails func(msg string, fields ...zap.Field)

	// multiplexing is for sharing one GRPC stream in many tables.
	multiplexing *sharedconn.ConnAndClient

	// tableExclusives means one GRPC stream is exclusive by one table.
	tableExclusives chan tableExclusive
}

type tableExclusive struct {
	subscriptionID SubscriptionID
	cc             *sharedconn.ConnAndClient
}

func newStream(ctx context.Context, c *SharedClient, g *errgroup.Group, r *requestedStore) *requestedStream {
	stream := newRequestedStream(streamIDGen.Add(1))
	stream.logRegionDetails = c.logRegionDetails
	stream.requests = chann.NewAutoDrainChann[singleRegionInfo]()

	waitForPreFetching := func() error {
		if stream.preFetchForConnecting != nil {
			log.Panic("preFetchForConnecting should be nil",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Uint64("streamID", stream.streamID),
				zap.Uint64("storeID", r.storeID),
				zap.String("addr", r.storeAddr))
		}
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
					sfEvent := newEventItem(nil, state, stream)
					slot := hashRegionID(state.sri.verID.GetID(), len(c.workers))
					_ = c.workers[slot].sendEvent(ctx, sfEvent)
				}
			}
			// Why we need to re-schedule pending regions? This because the store can
			// fail forever, and all regions are scheduled to other stores.
			for _, sri := range stream.clearPendingRegions() {
				if sri.lockedRange == nil {
					// It means it's a special task for stopping the table.
					continue
				}
				c.onRegionFail(newRegionErrorInfo(sri, &sendRequestToStoreErr{}))
			}
			if err := util.Hang(ctx, time.Second); err != nil {
				return err
			}
		}
	})

	return stream
}

func newRequestedStream(streamID uint64) *requestedStream {
	stream := &requestedStream{streamID: streamID}
	stream.requestedRegions.m = make(map[SubscriptionID]map[uint64]*regionFeedState)
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
			zap.Uint64("streamID", s.streamID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Error(err))
		return isCanceled()
	}

	log.Info("event feed going to create grpc stream",
		zap.String("namespace", c.changefeed.Namespace),
		zap.String("changefeed", c.changefeed.ID),
		zap.Uint64("streamID", s.streamID),
		zap.Uint64("storeID", rs.storeID),
		zap.String("addr", rs.storeAddr))

	defer func() {
		log.Info("event feed grpc stream exits",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("streamID", s.streamID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Bool("canceled", canceled))
		if s.multiplexing != nil {
			s.multiplexing = nil
		} else if s.tableExclusives != nil {
			close(s.tableExclusives)
			s.tableExclusives = nil
		}
	}()

	cc, err := c.grpcPool.Connect(ctx, rs.storeAddr)
	if err != nil {
		log.Warn("event feed create grpc stream failed",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("streamID", s.streamID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr),
			zap.Error(err))
		return isCanceled()
	}

	g, gctx := errgroup.WithContext(ctx)
	if cc.Multiplexing() {
		s.multiplexing = cc
		g.Go(func() error { return s.receive(gctx, c, rs, s.multiplexing, invalidSubscriptionID) })
	} else {
		log.Info("event feed stream multiplexing is not supported, will fallback",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("streamID", s.streamID),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr))
		cc.Release()

		s.tableExclusives = make(chan tableExclusive, 8)
		g.Go(func() error {
			for {
				select {
				case <-gctx.Done():
					return gctx.Err()
				case tableExclusive := <-s.tableExclusives:
					subscriptionID := tableExclusive.subscriptionID
					cc := tableExclusive.cc
					g.Go(func() error { return s.receive(gctx, c, rs, cc, subscriptionID) })
				}
			}
		})
	}
	g.Go(func() error { return s.send(gctx, c, rs) })
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
			s.logRegionDetails("event feed receive from grpc stream failed",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Uint64("streamID", s.streamID),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
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
	doSend := func(cc *sharedconn.ConnAndClient, req *cdcpb.ChangeDataRequest, subscriptionID SubscriptionID) error {
		if err := cc.Client().Send(req); err != nil {
			log.Warn("event feed send request to grpc stream failed",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Uint64("streamID", s.streamID),
				zap.Any("subscriptionID", subscriptionID),
				zap.Uint64("regionID", req.RegionId),
				zap.Uint64("storeID", rs.storeID),
				zap.String("addr", rs.storeAddr),
				zap.Error(err))
			return errors.Trace(err)
		}
		log.Debug("event feed send request to grpc stream success",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("streamID", s.streamID),
			zap.Any("subscriptionID", subscriptionID),
			zap.Uint64("regionID", req.RegionId),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr))
		return nil
	}

	fetchMoreReq := func() (singleRegionInfo, error) {
		waitReqTicker := time.NewTicker(60 * time.Second)
		defer waitReqTicker.Stop()
		for {
			var sri singleRegionInfo
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

	tableExclusives := make(map[SubscriptionID]*sharedconn.ConnAndClient)
	getTableExclusiveConn := func(subscriptionID SubscriptionID) (cc *sharedconn.ConnAndClient, err error) {
		if cc = tableExclusives[subscriptionID]; cc == nil {
			if cc, err = c.grpcPool.Connect(ctx, rs.storeAddr); err != nil {
				return
			}
			if cc.Multiplexing() {
				cc.Release()
				cc, err = nil, errors.New("multiplexing is enabled, will re-establish the stream")
				return
			}
			tableExclusives[subscriptionID] = cc
			select {
			case <-ctx.Done():
			case s.tableExclusives <- tableExclusive{subscriptionID, cc}:
			}
		}
		return
	}
	defer func() {
		if s.multiplexing != nil {
			s.multiplexing.Release()
		}
		for _, cc := range tableExclusives {
			cc.Release()
		}
	}()

	sri := *s.preFetchForConnecting
	s.preFetchForConnecting = nil
	for {
		subscriptionID := sri.requestedTable.subscriptionID
		log.Debug("event feed gets a singleRegionInfo",
			zap.String("namespace", c.changefeed.Namespace),
			zap.String("changefeed", c.changefeed.ID),
			zap.Uint64("streamID", s.streamID),
			zap.Any("subscriptionID", subscriptionID),
			zap.Uint64("regionID", sri.verID.GetID()),
			zap.Uint64("storeID", rs.storeID),
			zap.String("addr", rs.storeAddr))
		// It means it's a special task for stopping the table.
		if sri.lockedRange == nil {
			if s.multiplexing != nil {
				req := &cdcpb.ChangeDataRequest{
					RequestId: uint64(subscriptionID),
					Request:   &cdcpb.ChangeDataRequest_Deregister_{},
				}
				if err = doSend(s.multiplexing, req, subscriptionID); err != nil {
					return err
				}
			} else if cc := tableExclusives[subscriptionID]; cc != nil {
				delete(tableExclusives, subscriptionID)
				cc.Release()
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
				sfEvent := newEventItem(nil, state, s)
				slot := hashRegionID(state.sri.verID.GetID(), len(c.workers))
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
			if err = doSend(cc, c.createRegionRequest(sri), subscriptionID); err != nil {
				return err
			}
		}

		if sri, err = fetchMoreReq(); err != nil {
			return err
		}
	}
}

func (s *requestedStream) countStates() (sum int) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	for _, mm := range s.requestedRegions.m {
		sum += len(mm)
	}
	return
}

func (s *requestedStream) setState(subscriptionID SubscriptionID, regionID uint64, state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	var m map[uint64]*regionFeedState
	if m = s.requestedRegions.m[subscriptionID]; m == nil {
		m = make(map[uint64]*regionFeedState)
		s.requestedRegions.m[subscriptionID] = m
	}
	m[regionID] = state
}

func (s *requestedStream) getState(subscriptionID SubscriptionID, regionID uint64) (state *regionFeedState) {
	s.requestedRegions.RLock()
	defer s.requestedRegions.RUnlock()
	if m, ok := s.requestedRegions.m[subscriptionID]; ok {
		state = m[regionID]
	}
	return state
}

func (s *requestedStream) takeState(subscriptionID SubscriptionID, regionID uint64) (state *regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	if m, ok := s.requestedRegions.m[subscriptionID]; ok {
		state = m[regionID]
		delete(m, regionID)
		if len(m) == 0 {
			delete(s.requestedRegions.m, subscriptionID)
		}
	}
	return
}

func (s *requestedStream) takeStates(subscriptionID SubscriptionID) (v map[uint64]*regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	v = s.requestedRegions.m[subscriptionID]
	delete(s.requestedRegions.m, subscriptionID)
	return
}

func (s *requestedStream) clearStates() (v map[SubscriptionID]map[uint64]*regionFeedState) {
	s.requestedRegions.Lock()
	defer s.requestedRegions.Unlock()
	v = s.requestedRegions.m
	s.requestedRegions.m = make(map[SubscriptionID]map[uint64]*regionFeedState)
	return
}

func (s *requestedStream) clearPendingRegions() []singleRegionInfo {
	regions := make([]singleRegionInfo, 0, s.requests.Len()+1)
	if s.preFetchForConnecting != nil {
		sri := *s.preFetchForConnecting
		s.preFetchForConnecting = nil
		regions = append(regions, sri)
	}
	for i := 1; i < cap(regions); i++ {
		regions = append(regions, <-s.requests.Out())
	}
	return regions
}

func (s *requestedStream) sendRegionChangeEvents(
	ctx context.Context, c *SharedClient, events []*cdcpb.Event,
	tableSubID SubscriptionID,
) error {
	for _, event := range events {
		regionID := event.RegionId
		var subscriptionID SubscriptionID
		if tableSubID == invalidSubscriptionID {
			subscriptionID = SubscriptionID(event.RequestId)
		} else {
			subscriptionID = tableSubID
		}

		state := s.getState(subscriptionID, regionID)
		switch x := event.Event.(type) {
		case *cdcpb.Event_Error:
			s.logRegionDetails("event feed receives a region error",
				zap.String("namespace", c.changefeed.Namespace),
				zap.String("changefeed", c.changefeed.ID),
				zap.Uint64("streamID", s.streamID),
				zap.Any("subscriptionID", subscriptionID),
				zap.Uint64("regionID", event.RegionId),
				zap.Bool("stateIsNil", state == nil),
				zap.Any("error", x.Error))
		}

		if state != nil {
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
	var subscriptionID SubscriptionID
	if tableSubID == invalidSubscriptionID {
		subscriptionID = SubscriptionID(resolvedTs.RequestId)
	} else {
		subscriptionID = tableSubID
	}
	sfEvents := make([]statefulEvent, len(c.workers))
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
