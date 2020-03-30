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
	grpcConnCount             = 10
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

type syncRegionInfoMap struct {
	mu            *sync.Mutex
	regionInfoMap map[uint64]singleRegionInfo
}

func newSyncRegionInfoMap() *syncRegionInfoMap {
	return &syncRegionInfoMap{
		mu:            &sync.Mutex{},
		regionInfoMap: make(map[uint64]singleRegionInfo),
	}
}

func (m *syncRegionInfoMap) replace(regionID uint64, sri singleRegionInfo) (singleRegionInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldSri, ok := m.regionInfoMap[regionID]
	m.regionInfoMap[regionID] = sri
	return oldSri, ok
}

func (m *syncRegionInfoMap) take(regionID uint64) (singleRegionInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	sri, ok := m.regionInfoMap[regionID]
	if ok {
		delete(m.regionInfoMap, regionID)
	}
	return sri, ok
}

func (m *syncRegionInfoMap) takeAll() map[uint64]singleRegionInfo {
	m.mu.Lock()
	defer m.mu.Unlock()

	oldMap := m.regionInfoMap
	m.regionInfoMap = make(map[uint64]singleRegionInfo)
	return oldMap
}

type connArray struct {
	target string
	index  uint32
	v      []*grpc.ClientConn
}

func newConnArray(ctx context.Context, maxSize uint, addr string) (*connArray, error) {
	a := &connArray{
		target: addr,
		index:  0,
		v:      make([]*grpc.ClientConn, maxSize),
	}
	err := a.Init(ctx)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return a, nil
}

func (a *connArray) Init(ctx context.Context) error {
	for i := range a.v {
		ctx, cancel := context.WithTimeout(ctx, dialTimeout)

		conn, err := grpc.DialContext(
			ctx,
			a.target,
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
			a.Close()
			return errors.Trace(err)
		}
		a.v[i] = conn
	}
	return nil
}

func (a *connArray) Get() *grpc.ClientConn {
	next := atomic.AddUint32(&a.index, 1) % uint32(len(a.v))
	return a.v[next]
}

func (a *connArray) Close() {
	for i, c := range a.v {
		if c != nil {
			err := c.Close()
			if err != nil {
				log.Warn("close grpc conn", zap.Error(err))
			}
		}
		a.v[i] = nil
	}
}

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	clusterID uint64

	mu struct {
		sync.Mutex
		conns map[string]*connArray
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
			conns map[string]*connArray
		}{
			conns: make(map[string]*connArray),
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

func (c *CDCClient) getConn(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conns, ok := c.mu.conns[addr]; ok {
		return conns.Get(), nil
	}
	ca, err := newConnArray(ctx, grpcConnCount, addr)
	if err != nil {
		return nil, errors.Trace(err)
	}
	c.mu.conns[addr] = ca
	return ca.Get(), nil
}

func (c *CDCClient) getStream(ctx context.Context, addr string) (stream cdcpb.ChangeData_EventFeedClient, err error) {
	err = retry.Run(500*time.Millisecond, 10, func() error {
		conn, err := c.getConn(ctx, addr)
		if err != nil {
			return errors.Trace(err)
		}
		client := cdcpb.NewChangeDataClient(conn)
		stream, err = client.EventFeed(ctx)
		if err != nil {
			return errors.Trace(err)
		}
		log.Debug("created stream to store", zap.String("addr", addr))
		return nil
	})
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

// dispatchRequest manages a set of streams and dispatch event feed requests
// to these streams. Streams to each store will be created on need. After
// establishing new stream, a goroutine will be spawned to handle events from
// the stream.
// Regions from `regionCh` will be connected. If any error happens to a
// region, the error will be send to `errCh` and the receiver of `errCh` is
// responsible for handling the error.
func (c *CDCClient) dispatchRequest(
	ctx context.Context,
	g *errgroup.Group,
	regionCh chan singleRegionInfo,
	errCh chan<- regionErrorInfo,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	streams := make(map[string]cdcpb.ChangeData_EventFeedClient)
	// Stores pending regions info for each stream. After sending a new request, the region info wil be put to the map,
	// and it will be loaded by the receiver thread when it receives the first response from that region. We need this
	// to pass the region info to the receiver since the region info cannot be inferred from the response from TiKV.
	storePendingRegions := make(map[string]*syncRegionInfoMap)

MainLoop:
	for {
		var sri singleRegionInfo
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sri = <-regionCh:
		}

		log.Debug("dispatching region", zap.Uint64("regionID", sri.verID.GetID()))

		// Loop for retrying in case the stream has disconnected.
		// TODO: Should we break if retries and fails too many times?
		for {
			rpcCtx, err := c.getRPCContextForRegion(ctx, sri.verID)
			if err != nil {
				return errors.Trace(err)
			}
			if rpcCtx == nil {
				// The region info is invalid. Retry the span.
				log.Info("cannot get rpcCtx, retry span",
					zap.Uint64("regionID", sri.verID.GetID()),
					zap.Reflect("span", sri.span))
				// Workaround: spawn to a new goroutine, otherwise the function may blocks when sending to regionCh but
				// regionCh can only be received from `dispatchRequest`.
				// TODO: Find better solution after a refactoring
				g.Go(func() error {
					err := c.divideAndSendEventFeedToRegions(ctx, sri.span, sri.ts, regionCh)
					return errors.Trace(err)
				})
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

			// Get region info collection of the addr
			pendingRegions, ok := storePendingRegions[rpcCtx.Addr]
			if !ok {
				pendingRegions = newSyncRegionInfoMap()
				storePendingRegions[rpcCtx.Addr] = pendingRegions
			}

			_, hasOld := pendingRegions.replace(sri.verID.GetID(), sri)
			if hasOld {
				log.Error("region is already pending for the first response while trying to send another request."+
					"region merge may have happened which is not supported yet",
					zap.Uint64("regionID", sri.verID.GetID()))
			}

			stream, ok := streams[rpcCtx.Addr]
			// Establish the stream if it has not been connected yet.
			if !ok {
				stream, err = c.getStream(ctx, rpcCtx.Addr)
				if err != nil {
					// if get stream failed, maybe the store is down permanently, we should try to relocate the active store
					log.Warn("get grpc stream client failed", zap.Error(err))
					bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
					c.regionCache.OnSendFail(bo, rpcCtx, needReloadRegion(sri.failStoreIDs, rpcCtx), err)
					continue MainLoop
				}
				streams[rpcCtx.Addr] = stream

				g.Go(func() error {
					return c.receiveFromStream(ctx, g, rpcCtx.Addr, rpcCtx.GetStoreID(), stream, regionCh, eventCh, errCh, pendingRegions)
				})
			}

			log.Info("start new request", zap.Reflect("request", req), zap.String("addr", rpcCtx.Addr))
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
				// Delete the stream from the map so that the next time the store is accessed, the stream will be
				// re-established.
				delete(streams, rpcCtx.Addr)

				// Remove the region from pendingRegions. If it's already removed, it should be already retried by
				// `receiveFromStream`, so no need to retry here.
				_, ok := pendingRegions.take(sri.verID.GetID())
				if !ok {
					break
				}

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

// partialRegionFeed establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection. If any error happens (region split, leader change, etc), the region
// and error info will be sent to `errCh`, and the receiver of `errCh` is
// responsible for handling the error and re-establish the connection to the region.
func (c *CDCClient) partialRegionFeed(
	ctx context.Context,
	regionInfo singleRegionInfo,
	receiver <-chan *cdcpb.Event,
	errCh chan<- regionErrorInfo,
	eventCh chan<- *model.RegionFeedEvent,
	isStopped *int32,
) error {
	defer func() {
		atomic.StoreInt32(isStopped, 1)
		// Workaround to avoid remaining messages in the channel blocks the receiver thread.
		// TODO: Find a better solution.
		go func() {
			timer := time.After(time.Second * 2)
			for {
				select {
				case <-receiver:
				case <-timer:
					return
				}
			}
		}()
	}()

	ts := regionInfo.ts
	rl := rate.NewLimiter(0.1, 5)

	if !rl.Allow() {
		return errors.New("partialRegionFeed exceeds rate limit")
	}

	maxTs, err := c.singleEventFeed(ctx, regionInfo.span, regionInfo.ts, receiver, eventCh)
	log.Debug("singleEventFeed quit")

	if err == nil || errors.Cause(err) == context.Canceled {
		return nil
	}

	if maxTs > ts {
		ts = maxTs
	}

	log.Info("EventFeed disconnected",
		zap.Reflect("regionID", regionInfo.verID.GetID()),
		zap.Reflect("span", regionInfo.span),
		zap.Uint64("checkpoint", ts),
		zap.Error(err))

	regionInfo.ts = ts

	// We need to ensure when the error is handled, `isStopped` must be set. So set it before sending the error.
	atomic.StoreInt32(isStopped, 1)
	errCh <- regionErrorInfo{
		singleRegionInfo: regionInfo,
		err:              err,
	}

	return nil
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
		retryErr := retry.Run(500*time.Millisecond, maxRetry,
			func() error {
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
			})

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

// handleError handles error returned by a region. If some new EventFeed connection should be established, the region
// info will be sent to `regionCh`.
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
		return nil, errors.Trace(err)
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
	pendingRegions *syncRegionInfoMap,
) error {
	// Cancel the pending regions if the stream failed. Otherwise it will remain unhandled in the pendingRegions list
	// however not registered in the new reconnected stream.
	defer func() {
		log.Info("stream to store closed", zap.String("addr", addr), zap.Uint64("storeID", storeID))

		remainingRegions := pendingRegions.takeAll()

		for _, r := range remainingRegions {
			select {
			case <-ctx.Done():
				return
			case errCh <- regionErrorInfo{
				singleRegionInfo: r,
				err:              errors.New("pending region cancelled due to stream disconnecting"),
			}:
			}
		}
	}()

	// Each region has it's own goroutine to handle its messages. `regionHandlers` stores the channels to these
	// channels.
	// Maps from regionID to the channel.
	regionHandlers := make(map[uint64]chan *cdcpb.Event)
	// If an error occurs on a region, the goroutine to process messages of that region should exit, and therefore
	// the channel to that goroutine is invalidated. We need to know whether it's exited here. If it exited,
	// `regionStopped` will be set to false.
	regionStopped := make(map[uint64]*int32)

	for {
		cevent, err := stream.Recv()

		// TODO: Should we have better way to handle the errors?
		if err == io.EOF {
			for _, ch := range regionHandlers {
				close(ch)
			}
			return nil
		}
		if err != nil {
			log.Error(
				"failed to receive from stream",
				zap.String("addr", addr),
				zap.Uint64("storeID", storeID),
				zap.Error(err))

			for _, ch := range regionHandlers {
				select {
				case ch <- nil:
				case <-ctx.Done():
					return ctx.Err()
				}
			}

			// Do no return error but gracefully stop the goroutine here. Then the whole job will not be canceled and
			// connection will be retried.
			return nil
		}

		for _, event := range cevent.Events {
			isStopped := false
			if pIsStopped, ok := regionStopped[event.RegionId]; ok {
				isStopped = atomic.LoadInt32(pIsStopped) > 0
			}

			ch, ok := regionHandlers[event.RegionId]
			if !ok || isStopped {
				// It's the first response for this region. If the region is newly connected, the region info should
				// have been put in `pendingRegions`. So here we load the region info from `pendingRegions` and start
				// a new goroutine to handle messages from this region.
				// Firstly load the region info.
				sri, ok := pendingRegions.take(event.RegionId)
				if !ok {
					log.Warn("drop event due to region stopped", zap.Uint64("regionID", event.RegionId))
					continue
				}

				// Then spawn the goroutine to process messages of this region.
				ch = make(chan *cdcpb.Event, 16)
				regionHandlers[event.RegionId] = ch

				isStopped := new(int32)
				regionStopped[event.RegionId] = isStopped
				g.Go(func() error {
					return c.partialRegionFeed(ctx, sri, ch, errCh, eventCh, isStopped)
				})
			}

			select {
			case ch <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// singleEventFeed handles events of a single EventFeed stream.
// Results will be send to eventCh
// EventFeed RPC will not return checkpoint event directly
// Resolved event is generate while there's not non-match pre-write
// Return the maximum checkpoint
func (c *CDCClient) singleEventFeed(
	ctx context.Context,
	span util.Span,
	checkpointTs uint64,
	receiverCh <-chan *cdcpb.Event,
	eventCh chan<- *model.RegionFeedEvent,
) (uint64, error) {
	captureID := util.CaptureIDFromCtx(ctx)
	changefeedID := util.ChangefeedIDFromCtx(ctx)

	var initialized uint32

	matcher := newMatcher()

	for {

		var event *cdcpb.Event
		var ok bool
		select {
		case <-ctx.Done():
			return atomic.LoadUint64(&checkpointTs), ctx.Err()
		case event, ok = <-receiverCh:
		}

		if !ok {
			log.Debug("singleEventFeed receiver closed")
			return atomic.LoadUint64(&checkpointTs), nil
		}

		if event == nil {
			log.Debug("singleEventFeed closed by error")
			return atomic.LoadUint64(&checkpointTs), errors.New("single event feed aborted")
		}

		eventSize.WithLabelValues(captureID).Observe(float64(event.Event.Size()))
		switch x := event.Event.(type) {
		case *cdcpb.Event_Entries_:
			for _, entry := range x.Entries.GetEntries() {
				pullEventCounter.WithLabelValues(entry.Type.String(), captureID, changefeedID).Inc()
				switch entry.Type {
				case cdcpb.Event_INITIALIZED:
					atomic.StoreUint32(&initialized, 1)
				case cdcpb.Event_COMMITTED:
					var opType model.OpType
					switch entry.GetOpType() {
					case cdcpb.Event_Row_DELETE:
						opType = model.OpTypeDelete
					case cdcpb.Event_Row_PUT:
						opType = model.OpTypePut
					default:
						return atomic.LoadUint64(&checkpointTs), errors.Errorf("unknown tp: %v", entry.GetOpType())
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
					case eventCh <- revent:
						sendEventCounter.WithLabelValues("committed", captureID, changefeedID).Inc()
					case <-ctx.Done():
						return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
					}
				case cdcpb.Event_PREWRITE:
					matcher.putPrewriteRow(entry)
				case cdcpb.Event_COMMIT:
					// emit a value
					value, err := matcher.matchRow(entry)
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
						return atomic.LoadUint64(&checkpointTs), errors.Errorf("unknow tp: %v", entry.GetOpType())
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
					case eventCh <- revent:
						sendEventCounter.WithLabelValues("commit", captureID, changefeedID).Inc()
					case <-ctx.Done():
						return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
					}
				case cdcpb.Event_ROLLBACK:
					matcher.rollbackRow(entry)
				}
			}
		case *cdcpb.Event_Admin_:
			log.Info("receive admin event", zap.Stringer("event", event))
		case *cdcpb.Event_Error:
			return atomic.LoadUint64(&checkpointTs), errors.Trace(&eventError{err: x.Error})
		case *cdcpb.Event_ResolvedTs:
			if atomic.LoadUint32(&initialized) == 0 {
				continue
			}
			// emit a checkpointTs
			revent := &model.RegionFeedEvent{
				Resolved: &model.ResolvedSpan{
					Span:       span,
					ResolvedTs: x.ResolvedTs,
				},
			}

			updateCheckpointTS(&checkpointTs, x.ResolvedTs)
			select {
			case eventCh <- revent:
				sendEventCounter.WithLabelValues("native resolved", captureID, changefeedID).Inc()
			case <-ctx.Done():
				return atomic.LoadUint64(&checkpointTs), errors.Trace(ctx.Err())
			}
		}

	}
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
