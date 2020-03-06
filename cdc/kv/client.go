// Copyright 2019 PingCAP, Inc.
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
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	pd "github.com/pingcap/pd/client"
	"github.com/pingcap/ticdc/cdc/model"
	"github.com/pingcap/ticdc/pkg/retry"
	"github.com/pingcap/ticdc/pkg/util"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	gbackoff "google.golang.org/grpc/backoff"
	"google.golang.org/grpc/keepalive"
)

const (
	dialTimeout               = 10 * time.Second
	maxRetry                  = 10
	tikvRequestMaxBackoff     = 20000 // Maximum total sleep time(in ms)
	grpcInitialWindowSize     = 1 << 30
	grpcInitialConnWindowSize = 1 << 30
)

type singleRegionInfo struct {
	verID        tikv.RegionVerID
	span         util.Span
	ts           uint64
	failStoreIDs map[uint64]struct{}
	rpcCtx       *tikv.RPCContext
}

func newSingleRegionInfo(verID tikv.RegionVerID, span util.Span, ts uint64, rpcCtx *tikv.RPCContext) singleRegionInfo {
	return singleRegionInfo{
		verID:        verID,
		span:         span,
		ts:           ts,
		failStoreIDs: make(map[uint64]struct{}),
		rpcCtx:       rpcCtx,
	}
}

type regionErrorInfo struct {
	singleRegionInfo
	err error
}

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	clusterID uint64

	mu struct {
		sync.Mutex
		conns map[string]*grpc.ClientConn
	}

	regionCache *tikv.RegionCache
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(pd pd.Client) (c *CDCClient, err error) {
	clusterID := pd.GetClusterID(context.Background())
	log.Info("get clusterID", zap.Uint64("id", clusterID))

	c = &CDCClient{
		clusterID:   clusterID,
		pd:          pd,
		regionCache: tikv.NewRegionCache(pd),
		mu: struct {
			sync.Mutex
			conns map[string]*grpc.ClientConn
		}{
			conns: make(map[string]*grpc.ClientConn),
		},
	}

	return
}

// Close CDCClient
func (c *CDCClient) Close() error {
	c.mu.Lock()
	for _, conn := range c.mu.conns {
		conn.Close()
	}
	c.mu.Unlock()
	c.regionCache.Close()

	return nil
}

func (c *CDCClient) getConn(
	ctx context.Context, addr string,
) (conn *grpc.ClientConn, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.mu.conns[addr]; ok {
		return conn, nil
	}

	ctx, cancel := context.WithTimeout(ctx, dialTimeout)

	conn, err = grpc.DialContext(
		ctx,
		addr,
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithInsecure(),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: gbackoff.Config{
				BaseDelay:  time.Second,
				Multiplier: 1.1,
				Jitter:     0.1,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: 3 * time.Second,
		}),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(32*1024*1024)),
	)
	cancel()

	if err != nil {
		return nil, errors.Annotatef(err, "dial %s fail", addr)
	}

	c.mu.conns[addr] = conn

	return
}

func (c *CDCClient) getStream(ctx context.Context, addr string) (stream cdcpb.ChangeData_EventFeedClient, err error) {
	conn, err := c.getConn(ctx, addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	client := cdcpb.NewChangeDataClient(conn)
	stream, err = client.EventFeed(ctx)
	log.Debug("created stream to store", zap.String("addr", addr))
	return
}

// EventFeed divides a EventFeed request on range boundaries and establishes
// a EventFeed to each of the individual region. It streams back result on the
// provided channel.
// The `Start` and `End` field in input span must be memcomparable encoded.
func (c *CDCClient) EventFeed(
	ctx context.Context, span util.Span, ts uint64, eventCh chan<- *model.RegionFeedEvent,
) error {
	eventFeedGauge.Inc()
	defer eventFeedGauge.Dec()

	log.Debug("event feed started", zap.Reflect("span", span), zap.Uint64("ts", ts))

	g, ctx := errgroup.WithContext(ctx)

	regionCh := make(chan singleRegionInfo, 16)
	errCh := make(chan regionErrorInfo, 16)

	g.Go(func() error {
		return c.dispatchRequest(ctx, g, regionCh, errCh, eventCh)
	})

	g.Go(func() error {
		err := c.divideAndSendEventFeedToRegions(ctx, span, ts, regionCh)
		if err != nil {
			return errors.Trace(err)
		}
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case errInfo := <-errCh:
				err = c.handleError(ctx, errInfo, regionCh)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	})

	return g.Wait()
}

func (c *CDCClient) dispatchRequest(
	ctx context.Context,
	g *errgroup.Group,
	regionCh chan singleRegionInfo,
	errCh chan<- regionErrorInfo,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	streams := make(map[string]cdcpb.ChangeData_EventFeedClient)
	// Stores regionHandlerSet for each stream, and each regionHandlerSet keeps the channels of each regions in a stream.
	regionInfoMap := make(map[uint64]singleRegionInfo)
	regionInfoMapMu := &sync.RWMutex{}

MainLoop:
	for {
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sri = <-regionCh:
		}

		// Loop for retrying in case the stream has disconnected.
		// TODO: Should we break if retries and fails too many times?
		for {
			rpcCtx, err := c.getRPCContextForRegion(ctx, sri.verID)
			if err != nil {
				return errors.Trace(err)
			}
			if rpcCtx == nil {
				// The region info is invalid. Retry the span.
				log.Debug("cannot get rpcCtx, retry span", zap.Reflect("span", sri.span))
				err = c.divideAndSendEventFeedToRegions(ctx, sri.span, sri.ts, regionCh)
				if err != nil {
					return errors.Trace(err)
				}
				continue MainLoop
			}
			sri.rpcCtx = rpcCtx

			req := &cdcpb.ChangeDataRequest{
				Header: &cdcpb.Header{
					ClusterId: c.clusterID,
				},
				RegionId:     rpcCtx.Meta.GetId(),
				RegionEpoch:  rpcCtx.Meta.RegionEpoch,
				CheckpointTs: sri.ts,
				StartKey:     sri.span.Start,
				EndKey:       sri.span.End,
			}

			// The receiver thread need to know the span, which is only known in the sender thread. So create the
			// receiver thread for region here so that it can know the span.
			// TODO: Find a better way to handle this.
			// TODO: Make sure there will not be goroutine leak.
			// TODO: Here we use region id to index the regionInfo. However, in case that region merge is enabled, there
			// may be multiple streams to the same regions. Maybe we need to add a requestID field to the protocol for it.
			regionInfoMapMu.Lock()
			if _, ok := regionInfoMap[sri.verID.GetID()]; ok {
				log.Error("region is already pending for the first response while trying to send another request. region merge mast have happened which we didn't support yet",
					zap.Uint64("regionID", sri.verID.GetID()))
			}
			regionInfoMap[sri.verID.GetID()] = sri
			regionInfoMapMu.Unlock()

			stream, ok := streams[rpcCtx.Addr]
			// Establish the stream if it has not been connected yet.
			if !ok {
				stream, err = c.getStream(ctx, rpcCtx.Addr)
				if err != nil {
					return errors.Trace(err)
				}
				streams[rpcCtx.Addr] = stream

				g.Go(func() error {
					return c.receiveFromStream(ctx, g, rpcCtx.Addr, rpcCtx.GetStoreID(), stream, regionCh, eventCh, errCh, regionInfoMap, regionInfoMapMu)
				})
			}

			log.Debug("start new request", zap.Reflect("request", req))
			err = stream.Send(req)

			// If Send error, the receiver should have received error too or will receive error soon. So we doesn't need
			// to do extra work here.
			if err != nil {
				log.Error("send request to stream failed",
					zap.String("addr", rpcCtx.Addr),
					zap.Uint64("storeID", rpcCtx.GetStoreID()),
					zap.Error(err))
				err1 := stream.CloseSend()
				if err1 != nil {
					log.Error("failed to close stream", zap.Error(err1))
				}
				delete(streams, rpcCtx.Addr)

				// Wait for a while and retry sending the request
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				// Break if ctx has been canceled.
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				continue
			}

			break
		}
	}
}

func needReloadRegion(failStoreIDs map[uint64]struct{}, rpcCtx *tikv.RPCContext) (need bool) {
	failStoreIDs[rpcCtx.GetStoreID()] = struct{}{}
	need = len(failStoreIDs) == len(rpcCtx.Meta.GetPeers())
	if need {
		for k := range failStoreIDs {
			delete(failStoreIDs, k)
		}
	}
	return
}

// divideAndSendEventFeedToRegions split up the input span
// into non-overlapping spans aligned to region boundaries.
func (c *CDCClient) divideAndSendEventFeedToRegions(
	ctx context.Context, span util.Span, ts uint64, regionCh chan<- singleRegionInfo,
) error {
	limit := 20

	nextSpan := span
	captureID := util.CaptureIDFromCtx(ctx)

	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Run(func() error {
			scanT0 := time.Now()
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			regions, err = c.regionCache.BatchLoadRegionsWithKeyRange(bo, nextSpan.Start, nextSpan.End, limit)
			scanRegionsDuration.WithLabelValues(captureID).Observe(time.Since(scanT0).Seconds())
			if err != nil {
				return errors.Trace(err)
			}
			metas := make([]*metapb.Region, 0, len(regions))
			for _, region := range regions {
				if region.GetMeta() == nil {
					err = errors.New("meta not exists in region")
					log.Warn("batch load region", zap.Reflect("span", nextSpan), zap.Reflect("regions", regions), zap.Error(err))
					return err
				}
				metas = append(metas, region.GetMeta())
			}
			if !util.CheckRegionsLeftCover(metas, nextSpan) {
				err = errors.New("regions not completely left cover span")
				log.Warn("ScanRegions", zap.Reflect("span", nextSpan), zap.Reflect("regions", regions), zap.Error(err))
				return err
			}
			log.Debug("ScanRegions", zap.Reflect("span", nextSpan), zap.Reflect("regions", regions))
			return nil
		}, maxRetry)

		if retryErr != nil {
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := util.Intersect(nextSpan, util.Span{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("get partialSpan", zap.Reflect("span", partialSpan))

			nextSpan.Start = region.EndKey

			select {
			case regionCh <- newSingleRegionInfo(tiRegion.VerID(), partialSpan, ts, nil):
			case <-ctx.Done():
				return ctx.Err()
			}

			// return if no more regions
			if util.EndCompare(nextSpan.Start, span.End) >= 0 {
				return nil
			}
		}
	}
}

func (c *CDCClient) handleError(ctx context.Context, errInfo regionErrorInfo, regionCh chan<- singleRegionInfo) error {
	err := errInfo.err
	switch eerr := errors.Cause(err).(type) {
	case *eventError:
		innerErr := eerr.err
		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			eventFeedErrorCounter.WithLabelValues("NotLeader").Inc()
			// TODO: Handle the case that notleader.GetLeader() is nil.
			c.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader().GetStoreId(), errInfo.rpcCtx.PeerIdx)
		} else if innerErr.GetEpochNotMatch() != nil {
			// TODO: If only confver is updated, we don't need to reload the region from region cache.
			eventFeedErrorCounter.WithLabelValues("EpochNotMatch").Inc()
			return c.divideAndSendEventFeedToRegions(ctx, errInfo.span, errInfo.ts, regionCh)
		} else if innerErr.GetRegionNotFound() != nil {
			eventFeedErrorCounter.WithLabelValues("RegionNotFound").Inc()
			return c.divideAndSendEventFeedToRegions(ctx, errInfo.span, errInfo.ts, regionCh)
		} else if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			eventFeedErrorCounter.WithLabelValues("DuplicateRequest").Inc()
			log.Error("tikv reported duplicated request to the same region. region merge should happened which is not supported",
				zap.Uint64("regionID", duplicatedRequest.RegionId))
			return nil
		} else {
			eventFeedErrorCounter.WithLabelValues("Unknown").Inc()
			log.Warn("receive empty or unknown error msg", zap.Stringer("error", innerErr))
		}
	default:
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		if errInfo.rpcCtx.Meta != nil {
			c.regionCache.OnSendFail(bo, errInfo.rpcCtx, needReloadRegion(errInfo.failStoreIDs, errInfo.rpcCtx), err)
		}
	}

	regionCh <- errInfo.singleRegionInfo

	return nil
}

func (c *CDCClient) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := c.regionCache.GetTiKVRPCContext(bo, id, tidbkv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, backoff.Permanent(errors.Trace(err))
	}
	return rpcCtx, nil
}

func (c *CDCClient) receiveFromStream(
	ctx context.Context,
	g *errgroup.Group,
	addr string,
	storeID uint64,
	stream cdcpb.ChangeData_EventFeedClient,
	regionCh <-chan singleRegionInfo,
	eventCh chan<- *model.RegionFeedEvent,
	errCh chan<- regionErrorInfo,
	regionInfoMap map[uint64]singleRegionInfo,
	regionInfoMapMu *sync.RWMutex,
) error {
	regionStates := make(map[uint64]*regionFeedState)

	for {
		cevent, err := stream.Recv()

		//log.Debug("recv ChangeDataEvent", zap.Stringer("event", cevent))

		// TODO: Should we have better way to handle the errors?
		if err != nil {
			for _, s := range regionStates {
				// The region must be stopped after sending an error
				_ = s.onReceiveEvent(nil, err)
			}

			if err == io.EOF {
				return nil
			}

			log.Error(
				"failed to receive from stream",
				zap.String("addr", addr),
				zap.Uint64("storeID", storeID),
				zap.Error(err))

			// Do no return error but gracefully stop the goroutine here. Then the whole job will not be canceled and
			// connection will be retried.
			return nil
		}

		for _, event := range cevent.Events {
			state, ok := regionStates[event.RegionId]
			if !ok {
				// Fetch the region info
				regionInfoMapMu.Lock()
				sri, ok := regionInfoMap[event.RegionId]
				if !ok {
					regionInfoMapMu.Unlock()
					log.Warn("drop event due to region stopped", zap.Uint64("regionID", event.RegionId))
					continue
				}
				delete(regionInfoMap, event.RegionId)
				regionInfoMapMu.Unlock()

				state, err = newRegionFeedState(ctx, sri, errCh, eventCh)
				if err != nil {
					return errors.Trace(err)
				}

				regionStates[event.RegionId] = state
			}

			stopped := state.onReceiveEvent(event, nil)
			if stopped {
				delete(regionStates, event.RegionId)
			}
		}
	}
}

type regionFeedState struct {
	ctx          context.Context
	captureID    string
	changefeedID string
	started      bool
	sri          singleRegionInfo
	errCh        chan<- regionErrorInfo
	eventCh      chan<- *model.RegionFeedEvent

	initialized  uint32
	checkpointTs uint64
	matcher      *matcher
	sorter       *sorter
}

func newRegionFeedState(
	ctx context.Context,
	sri singleRegionInfo,
	errCh chan<- regionErrorInfo,
	eventCh chan<- *model.RegionFeedEvent,
) (*regionFeedState, error) {
	rl := rate.NewLimiter(0.1, 5)

	if !rl.Allow() {
		return nil, errors.New("onReceiveEvent exceeds rate limit")
	}

	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)
	log.Debug("region feed state created", zap.Uint64("regionID", sri.verID.GetID()))

	s := &regionFeedState{
		ctx:          ctx,
		captureID:    captureID,
		changefeedID: changefeedID,
		started:      false,
		sri:          sri,
		errCh:        errCh,
		eventCh:      eventCh,

		matcher: newMatcher(),
	}

	maxItemFn := func(item sortItem) {
		if atomic.LoadUint32(&s.initialized) == 0 {
			return
		}

		// emit a checkpointTs
		revent := &model.RegionFeedEvent{
			Resolved: &model.ResolvedSpan{
				Span:       sri.span,
				ResolvedTs: item.commit,
			},
		}
		updateCheckpointTS(&s.checkpointTs, item.commit)
		select {
		case eventCh <- revent:
			sendEventCounter.WithLabelValues("sorter resolved", captureID, changefeedID).Inc()
		case <-ctx.Done():
		}
	}

	// TODO: drop this if we totally depends on the ResolvedTs event from
	// tikv to emit the ResolvedSpan.
	s.sorter = newSorter(maxItemFn)

	return s, nil
}

// onReceiveEvent establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection, this may involve retry this region EventFeed or subdividing the region
// further in the event of a split.
func (s *regionFeedState) onReceiveEvent(
	event *cdcpb.Event,
	err error,
) bool {
	maxTs, stopped, err := s.singleEventFeed(event, err)
	if !stopped && err == nil {
		// If not stopped and no error occurs, the region will continue receiving events.
		return false
	}

	// TODO: `close` does `wg.Wait`. Do we need to avoid blocking the current thread?
	s.sorter.close()

	log.Debug("singleEventFeed quit")

	if err == nil || errors.Cause(err) == context.Canceled {
		return true
	}

	if maxTs > s.sri.ts {
		s.sri.ts = maxTs
	}

	log.Info("EventFeed disconnected",
		zap.Reflect("span", s.sri.span),
		zap.Uint64("checkpoint", s.sri.ts),
		zap.Error(err))

	s.errCh <- regionErrorInfo{
		singleRegionInfo: s.sri,
		err:              err,
	}

	//	// avoid too many kv clients retry at the same time, we should have a better solution
	//	time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))

	return true
}

// singleEventFeed makes a EventFeed RPC call.
// Results will be send to eventCh
// EventFeed RPC will not return checkpoint event directly
// Resolved event is generate while there's not non-match pre-write
// Return the maximum checkpoint
func (s *regionFeedState) singleEventFeed(
	event *cdcpb.Event,
	err error,
) (uint64, bool, error) {
	if err != nil {
		return atomic.LoadUint64(&s.checkpointTs), true, err
	}

	if _, isEntry := event.Event.(*cdcpb.Event_Entries_); !isEntry {
		log.Debug("singleEventFeed got event", zap.Stringer("event", event))
	}

	eventSize.WithLabelValues(s.captureID).Observe(float64(event.Event.Size()))
	switch x := event.Event.(type) {
	case *cdcpb.Event_Entries_:
		for _, entry := range x.Entries.GetEntries() {
			pullEventCounter.WithLabelValues(entry.Type.String(), s.captureID, s.changefeedID).Inc()
			switch entry.Type {
			case cdcpb.Event_INITIALIZED:
				atomic.StoreUint32(&s.initialized, 1)
			case cdcpb.Event_COMMITTED:
				var opType model.OpType
				switch entry.GetOpType() {
				case cdcpb.Event_Row_DELETE:
					opType = model.OpTypeDelete
				case cdcpb.Event_Row_PUT:
					opType = model.OpTypePut
				default:
					return atomic.LoadUint64(&s.checkpointTs), true, errors.Errorf("unknown tp: %v", entry.GetOpType())
				}

				revent := &model.RegionFeedEvent{
					Val: &model.RawKVEntry{
						OpType: opType,
						Key:    entry.Key,
						Value:  entry.GetValue(),
						Ts:     entry.CommitTs,
					},
				}
				select {
				case s.eventCh <- revent:
					sendEventCounter.WithLabelValues("committed", s.captureID, s.changefeedID).Inc()
				case <-s.ctx.Done():
					return atomic.LoadUint64(&s.checkpointTs), true, errors.Trace(s.ctx.Err())
				}
			case cdcpb.Event_PREWRITE:
				s.matcher.putPrewriteRow(entry)
				s.sorter.pushTsItem(sortItem{
					start: entry.GetStartTs(),
					tp:    cdcpb.Event_PREWRITE,
				})
			case cdcpb.Event_COMMIT:
				// emit a value
				value, err := s.matcher.matchRow(entry)
				if err != nil {
					// FIXME: need a better event match mechanism
					log.Warn("match entry error", zap.Error(err), zap.Stringer("entry", entry))
				}

				var opType model.OpType
				switch entry.GetOpType() {
				case cdcpb.Event_Row_DELETE:
					opType = model.OpTypeDelete
				case cdcpb.Event_Row_PUT:
					opType = model.OpTypePut
				default:
					return atomic.LoadUint64(&s.checkpointTs), true, errors.Errorf("unknow tp: %v", entry.GetOpType())
				}

				revent := &model.RegionFeedEvent{
					Val: &model.RawKVEntry{
						OpType: opType,
						Key:    entry.Key,
						Value:  value,
						Ts:     entry.CommitTs,
					},
				}

				select {
				case s.eventCh <- revent:
					sendEventCounter.WithLabelValues("commit", s.captureID, s.changefeedID).Inc()
				case <-s.ctx.Done():
					return atomic.LoadUint64(&s.checkpointTs), true, errors.Trace(s.ctx.Err())
				}
				s.sorter.pushTsItem(sortItem{
					start:  entry.GetStartTs(),
					commit: entry.GetCommitTs(),
					tp:     cdcpb.Event_COMMIT,
				})
			case cdcpb.Event_ROLLBACK:
				s.matcher.rollbackRow(entry)
				s.sorter.pushTsItem(sortItem{
					start:  entry.GetStartTs(),
					commit: entry.GetCommitTs(),
					tp:     cdcpb.Event_ROLLBACK,
				})
			}
		}
	case *cdcpb.Event_Admin_:
		log.Info("receive admin event", zap.Stringer("event", event))
	case *cdcpb.Event_Error:
		return atomic.LoadUint64(&s.checkpointTs), true, errors.Trace(&eventError{err: x.Error})
	case *cdcpb.Event_ResolvedTs:
		if atomic.LoadUint32(&s.initialized) == 0 {
			return atomic.LoadUint64(&s.checkpointTs), false, nil
		}
		// emit a checkpointTs
		revent := &model.RegionFeedEvent{
			Resolved: &model.ResolvedSpan{
				Span:       s.sri.span,
				ResolvedTs: x.ResolvedTs,
			},
		}

		updateCheckpointTS(&s.checkpointTs, x.ResolvedTs)
		select {
		case s.eventCh <- revent:
			sendEventCounter.WithLabelValues("native resolved", s.captureID, s.changefeedID).Inc()
		case <-s.ctx.Done():
			return atomic.LoadUint64(&s.checkpointTs), true, errors.Trace(s.ctx.Err())
		}
	}

	return atomic.LoadUint64(&s.checkpointTs), false, nil
	//}
}

func updateCheckpointTS(checkpointTs *uint64, newValue uint64) {
	for {
		oldValue := atomic.LoadUint64(checkpointTs)
		if oldValue >= newValue || atomic.CompareAndSwapUint64(checkpointTs, oldValue, newValue) {
			return
		}
	}
}

// eventError wrap cdcpb.Event_Error to implements error interface.
type eventError struct {
	err *cdcpb.Error
}

// Error implement error interface.
func (e *eventError) Error() string {
	return e.err.String()
}
