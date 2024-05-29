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
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/cdc/model"
	"github.com/pingcap/tiflow/pkg/chann"
	"github.com/pingcap/tiflow/pkg/config"
	cerror "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/pdutil"
	"github.com/pingcap/tiflow/pkg/regionspan"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/txnutil"
	"github.com/pingcap/tiflow/pkg/version"
	tidbkv "github.com/tikv/client-go/v2/kv"
	"github.com/tikv/client-go/v2/oracle"
	"github.com/tikv/client-go/v2/tikv"
	pd "github.com/tikv/pd/client"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout           = 10 * time.Second
	tikvRequestMaxBackoff = 20000 // Maximum total sleep time(in ms)

	// TiCDC may open numerous gRPC streams,
	// with 65535 bytes window size, 10K streams takes about 27GB memory.
	//
	// 65535 bytes, the initial window size in http2 spec.
	grpcInitialWindowSize = (1 << 16) - 1
	// 8 MB The value for initial window size on a connection
	grpcInitialConnWindowSize = 1 << 23
	// 256 MB The maximum message size the client can receive
	grpcMaxCallRecvMsgSize = 1 << 28

	// The threshold of warning a message is too large. TiKV split events into 6MB per-message.
	warnRecvMsgSizeThreshold = 12 * 1024 * 1024

	// TiCDC always interacts with region leader, every time something goes wrong,
	// failed region will be reloaded via `BatchLoadRegionsWithKeyRange` API. So we
	// don't need to force reload region anymore.
	regionScheduleReload = false

	resolveLockMinInterval = 10 * time.Second

	scanRegionsConcurrency = 1024
)

// time interval to force kv client to terminate gRPC stream and reconnect
var reconnectInterval = 60 * time.Minute

// streamAlterInterval is the interval to limit the frequency of creating/deleting streams.
// Make it a variable so that we can change it in unit test.
var streamAlterInterval = 1 * time.Second

type regionStatefulEvent struct {
	changeEvent     *cdcpb.Event
	resolvedTsEvent *resolvedTsEvent
	state           *regionFeedState

	// regionID is used for load balancer, we don't use fields in state to reduce lock usage
	regionID uint64

	// finishedCallbackCh is used to mark events that are sent from a give region
	// worker to this worker(one of the workers in worker pool) are all processed.
	finishedCallbackCh chan struct{}
}

type resolvedTsEvent struct {
	resolvedTs uint64
	regions    []*regionFeedState
}

var (
	metricFeedNotLeaderCounter        = eventFeedErrorCounter.WithLabelValues("NotLeader")
	metricFeedEpochNotMatchCounter    = eventFeedErrorCounter.WithLabelValues("EpochNotMatch")
	metricFeedRegionNotFoundCounter   = eventFeedErrorCounter.WithLabelValues("RegionNotFound")
	metricFeedDuplicateRequestCounter = eventFeedErrorCounter.WithLabelValues("DuplicateRequest")
	metricFeedUnknownErrorCounter     = eventFeedErrorCounter.WithLabelValues("Unknown")
	metricFeedRPCCtxUnavailable       = eventFeedErrorCounter.WithLabelValues("RPCCtxUnavailable")
	metricStoreSendRequestErr         = eventFeedErrorCounter.WithLabelValues("SendRequestToStore")
	metricConnectToStoreErr           = eventFeedErrorCounter.WithLabelValues("ConnectToStore")
)

var (
	// unreachable error, only used in unit test
	errUnreachable = errors.New("kv client unreachable error")
	// internal error, force the gPRC stream terminate and reconnect
	errReconnect = errors.New("internal error, reconnect all regions")
	logPanic     = log.Panic
)

type regionErrorInfo struct {
	singleRegionInfo
	err error
}

func newRegionErrorInfo(info singleRegionInfo, err error) regionErrorInfo {
	return regionErrorInfo{
		singleRegionInfo: info,
		err:              err,
	}
}

type eventFeedStream struct {
	client cdcpb.ChangeData_EventFeedClient
	conn   *sharedConn
	// regions is used to store the state of the regions that are being processed by the stream.
	regions *syncRegionFeedStateMap
	// addr is the address of the TiKV store
	addr string
	// storeID is the ID of the TiKV store
	storeID uint64
	// id is the stream ID, which is used to identify the stream.
	id uint64
	// cancel is used to cancel the gRPC stream
	cancel     context.CancelFunc
	isCanceled atomic.Bool
	createTime time.Time
}

func (s *eventFeedStream) close() {
	if s.isCanceled.CompareAndSwap(false, true) {
		s.cancel()
	}
}

// CDCKVClient is an interface to receives kv changed logs from TiKV
type CDCKVClient interface {
	EventFeed(
		ctx context.Context,
		span regionspan.ComparableSpan,
		ts uint64,
		lockResolver txnutil.LockResolver,
		eventCh chan<- model.RegionFeedEvent,
	) error

	// RegionCount returns the number of captured regions.
	RegionCount() uint64
	// ResolvedTs returns the current ingress resolved ts.
	ResolvedTs() model.Ts
	// CommitTs returns the current ingress commit ts.
	CommitTs() model.Ts
}

// NewCDCKVClient is the constructor of CDC KV client
var NewCDCKVClient = NewCDCClient

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	config    *config.ServerConfig
	clusterID uint64

	grpcPool GrpcPool

	regionCache *tikv.RegionCache
	pdClock     pdutil.Clock

	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string

	tableStoreStats struct {
		sync.RWMutex
		// map[table_id/store_id] -> *tableStoreStat.
		v map[string]*tableStoreStat
	}

	// filterLoop is used in BDR mode, when it is true, tikv cdc component
	// will filter data that are written by another TiCDC.
	filterLoop bool
}

type tableStoreStat struct {
	regionCount atomic.Uint64
	resolvedTs  atomic.Uint64
	commitTs    atomic.Uint64
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(
	ctx context.Context,
	pd pd.Client,
	grpcPool GrpcPool,
	regionCache *tikv.RegionCache,
	pdClock pdutil.Clock,
	cfg *config.ServerConfig,
	changefeed model.ChangeFeedID,
	tableID model.TableID,
	tableName string,
	filterLoop bool,
) CDCKVClient {
	clusterID := pd.GetClusterID(ctx)

	c := &CDCClient{
		clusterID:   clusterID,
		config:      cfg,
		pd:          pd,
		grpcPool:    grpcPool,
		regionCache: regionCache,
		pdClock:     pdClock,

		changefeed: changefeed,
		tableID:    tableID,
		tableName:  tableName,
		filterLoop: filterLoop,
	}
	c.tableStoreStats.v = make(map[string]*tableStoreStat)
	return c
}

func (c *CDCClient) newStream(
	ctx context.Context,
	addr string,
	storeID uint64,
) (stream *eventFeedStream, err error) {
	var conn *sharedConn
	defer func() {
		if err != nil && conn != nil {
			c.grpcPool.ReleaseConn(conn, addr)
		}
	}()

	conn, err = c.grpcPool.GetConn(addr)
	if err != nil {
		return nil, errors.Trace(err)
	}

	ctx, cancel := context.WithCancel(ctx)

	err = version.CheckStoreVersion(ctx, c.pd, storeID)
	if err != nil {
		cancel()
		return nil, errors.Trace(err)
	}

	client := cdcpb.NewChangeDataClient(conn.ClientConn)
	var streamClient cdcpb.ChangeData_EventFeedClient
	streamClient, err = client.EventFeed(ctx)
	if err != nil {
		cancel()
		return nil, cerror.WrapError(cerror.ErrTiKVEventFeed, err)
	}

	stream = &eventFeedStream{
		client:     streamClient,
		conn:       conn,
		regions:    newSyncRegionFeedStateMap(),
		addr:       addr,
		storeID:    storeID,
		id:         allocateStreamID(),
		cancel:     cancel,
		isCanceled: atomic.Bool{},
		createTime: time.Now(),
	}

	log.Info("created stream to store",
		zap.String("namespace", c.changefeed.Namespace),
		zap.String("changefeed", c.changefeed.ID),
		zap.Int64("tableID", c.tableID),
		zap.String("tableName", c.tableName),
		zap.String("store", addr),
		zap.Uint64("storeID", storeID),
		zap.Uint64("streamID", stream.id))

	return stream, nil
}

// EventFeed divides a EventFeed request on range boundaries and establishes
// a EventFeed to each of the individual region. It streams back result on the
// provided channel.
// The `Start` and `End` field in input span must be memcomparable encoded.
func (c *CDCClient) EventFeed(
	ctx context.Context, span regionspan.ComparableSpan, ts uint64,
	lockResolver txnutil.LockResolver,
	eventCh chan<- model.RegionFeedEvent,
) error {
	s := newEventFeedSession(c, span, lockResolver, ts, eventCh)
	return s.eventFeed(ctx)
}

// RegionCount returns the number of captured regions.
func (c *CDCClient) RegionCount() (totalCount uint64) {
	c.tableStoreStats.RLock()
	defer c.tableStoreStats.RUnlock()
	for _, v := range c.tableStoreStats.v {
		totalCount += v.regionCount.Load()
	}
	return totalCount
}

// ResolvedTs returns the current ingress resolved ts.
func (c *CDCClient) ResolvedTs() model.Ts {
	c.tableStoreStats.RLock()
	defer c.tableStoreStats.RUnlock()
	ingressResolvedTs := uint64(0)
	for _, v := range c.tableStoreStats.v {
		curr := v.resolvedTs.Load()
		if curr > ingressResolvedTs {
			ingressResolvedTs = curr
		}
	}
	return ingressResolvedTs
}

// CommitTs returns the current ingress commit ts.
func (c *CDCClient) CommitTs() model.Ts {
	c.tableStoreStats.RLock()
	defer c.tableStoreStats.RUnlock()
	ingressCommitTs := uint64(0)
	for _, v := range c.tableStoreStats.v {
		curr := v.commitTs.Load()
		if curr > ingressCommitTs {
			ingressCommitTs = curr
		}
	}
	return ingressCommitTs
}

var currentRequestID uint64 = 0

func allocateRequestID() uint64 {
	return atomic.AddUint64(&currentRequestID, 1)
}

// used in test only
func getCurrentRequestID() uint64 {
	return atomic.LoadUint64(&currentRequestID)
}

var currentStreamID uint64 = 0

func allocateStreamID() uint64 {
	return atomic.AddUint64(&currentStreamID, 1)
}

type eventFeedSession struct {
	client     *CDCClient
	startTs    model.Ts
	changefeed model.ChangeFeedID
	tableID    model.TableID
	tableName  string

	lockResolver txnutil.LockResolver

	// The whole range that is being subscribed.
	totalSpan regionspan.ComparableSpan

	// The channel to send the processed events.
	eventCh      chan<- model.RegionFeedEvent
	regionRouter *chann.DrainableChann[singleRegionInfo]
	// The channel to put the region that will be sent requests.
	regionCh *chann.DrainableChann[singleRegionInfo]
	// The channel to notify that an error is happening, so that the error will be handled and the affected region
	// will be re-requested.
	errCh *chann.DrainableChann[regionErrorInfo]
	// The channel to schedule scanning and requesting regions in a specified range.
	requestRangeCh *chann.DrainableChann[rangeRequestTask]

	rangeLock *regionspan.RegionRangeLock

	// storeStreamsCache is used to cache the established gRPC streams to TiKV stores.
	// Note: The cache is not thread-safe, so it should be accessed in the same goroutine.
	// For now, it is only accessed in the `requestRegionToStore` goroutine.
	storeStreamsCache map[string]*eventFeedStream

	// use sync.Pool to store resolved ts event only, because resolved ts event
	// has the same size and generate cycle.
	resolvedTsPool sync.Pool
}

type rangeRequestTask struct {
	span regionspan.ComparableSpan
}

func newEventFeedSession(
	client *CDCClient,
	totalSpan regionspan.ComparableSpan,
	lockResolver txnutil.LockResolver,
	startTs uint64,
	eventCh chan<- model.RegionFeedEvent,
) *eventFeedSession {
	id := allocateRequestID()
	rangeLock := regionspan.NewRegionRangeLock(
		id, totalSpan.Start, totalSpan.End, startTs,
		client.changefeed.Namespace+"."+client.changefeed.ID)

	return &eventFeedSession{
		client:            client,
		startTs:           startTs,
		changefeed:        client.changefeed,
		tableID:           client.tableID,
		tableName:         client.tableName,
		storeStreamsCache: make(map[string]*eventFeedStream),
		totalSpan:         totalSpan,
		eventCh:           eventCh,
		rangeLock:         rangeLock,
		lockResolver:      lockResolver,
		resolvedTsPool: sync.Pool{
			New: func() any {
				return &regionStatefulEvent{
					resolvedTsEvent: &resolvedTsEvent{},
				}
			},
		},
	}
}

func (s *eventFeedSession) eventFeed(ctx context.Context) error {
	s.requestRangeCh = chann.NewDrainableChann[rangeRequestTask]()
	s.regionCh = chann.NewDrainableChann[singleRegionInfo]()
	s.regionRouter = chann.NewDrainableChann[singleRegionInfo]()
	s.errCh = chann.NewDrainableChann[regionErrorInfo]()

	eventFeedGauge.Inc()
	defer func() {
		eventFeedGauge.Dec()
		s.regionRouter.CloseAndDrain()
		s.regionCh.CloseAndDrain()
		s.errCh.CloseAndDrain()
		s.requestRangeCh.CloseAndDrain()
	}()

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(scanRegionsConcurrency)

	g.Go(func() error { return s.dispatchRequest(ctx) })

	g.Go(func() error { return s.requestRegionToStore(ctx, g) })

	g.Go(func() error { return s.logSlowRegions(ctx) })

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case task := <-s.requestRangeCh.Out():
				// divideAndSendEventFeedToRegions could be blocked for some time,
				// since it must wait for the region lock available. In order to
				// consume region range request from `requestRangeCh` as soon as
				// possible, we create a new goroutine to handle it.
				// The sequence of region range we process is not matter, the
				// region lock keeps the region access sequence.
				// Besides the count or frequency of range request is limited,
				// we use ephemeral goroutine instead of permanent goroutine.
				g.Go(func() error {
					return s.divideAndSendEventFeedToRegions(ctx, task.span)
				})
			}
		}
	})

	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case errInfo := <-s.errCh.Out():
				if err := s.handleError(ctx, errInfo); err != nil {
					return err
				}
				continue
			}
		}
	})

	s.requestRangeCh.In() <- rangeRequestTask{span: s.totalSpan}

	log.Info("event feed started",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Int64("tableID", s.tableID),
		zap.String("tableName", s.tableName),
		zap.Uint64("startTs", s.startTs),
		zap.Stringer("span", s.totalSpan))

	return g.Wait()
}

// scheduleDivideRegionAndRequest schedules a range to be divided by regions,
// and these regions will be then scheduled to send ChangeData requests.
func (s *eventFeedSession) scheduleDivideRegionAndRequest(
	ctx context.Context, span regionspan.ComparableSpan,
) {
	task := rangeRequestTask{span: span}
	select {
	case s.requestRangeCh.In() <- task:
	case <-ctx.Done():
	}
}

// scheduleRegionRequest locks the region's range and schedules sending ChangeData request to the region.
// This function is blocking until the region range is locked successfully
func (s *eventFeedSession) scheduleRegionRequest(ctx context.Context, sri singleRegionInfo) {
	handleResult := func(res regionspan.LockRangeResult) {
		switch res.Status {
		case regionspan.LockRangeStatusSuccess:
			sri.lockedRange = res.LockedRange
			select {
			case s.regionCh.In() <- sri:
			case <-ctx.Done():
			}
		case regionspan.LockRangeStatusStale:
			log.Info("request expired",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Any("retrySpans", res.RetryRanges))
			for _, r := range res.RetryRanges {
				// This call is always blocking, otherwise if scheduling in a new
				// goroutine, it won't block the caller of `schedulerRegionRequest`.
				s.scheduleDivideRegionAndRequest(ctx, r)
			}
		case regionspan.LockRangeStatusCancel:
			return
		default:
			panic("unreachable")
		}
	}

	res := s.rangeLock.LockRange(ctx, sri.span.Start, sri.span.End, sri.verID.GetID(), sri.verID.GetVer())
	failpoint.Inject("kvClientMockRangeLock", func(val failpoint.Value) {
		// short sleep to wait region has split
		time.Sleep(time.Second)
		s.rangeLock.UnlockRange(sri.span.Start, sri.span.End,
			sri.verID.GetID(), sri.verID.GetVer())
		regionNum := val.(int)
		retryRanges := make([]regionspan.ComparableSpan, 0, regionNum)
		start := []byte("a")
		end := []byte("b1001")
		for i := 0; i < regionNum; i++ {
			span := regionspan.Span{Start: start, End: end}
			retryRanges = append(retryRanges, regionspan.ToComparableSpan(span))
			start = end
			end = []byte(fmt.Sprintf("b%d", 1002+i))
		}
		res = regionspan.LockRangeResult{
			Status:      regionspan.LockRangeStatusStale,
			RetryRanges: retryRanges,
		}
	})

	if res.Status == regionspan.LockRangeStatusWait {
		res = res.WaitFn()
	}

	handleResult(res)
}

// onRegionFail handles a region's failure, which means, unlock the region's range and send the error to the errCh for
// error handling. This function is non-blocking even if error channel is full.
// CAUTION: Note that this should only be called in a context that the region has locked its range.
func (s *eventFeedSession) onRegionFail(ctx context.Context, errorInfo regionErrorInfo) {
	s.rangeLock.UnlockRange(errorInfo.span.Start, errorInfo.span.End,
		errorInfo.verID.GetID(), errorInfo.verID.GetVer(), errorInfo.resolvedTs())
	log.Info("region failed", zap.Stringer("span", &errorInfo.span),
		zap.Any("regionId", errorInfo.verID.GetID()),
		zap.Error(errorInfo.err))
	select {
	case s.errCh.In() <- errorInfo:
	case <-ctx.Done():
	}
}

// requestRegionToStore gets singleRegionInfo from regionRouter, which is a token
// based limiter, sends request to TiKV.
// If the send request to TiKV returns error, fail the region with sendRequestToStoreErr
// and kv client will redispatch the region.
// If initialize gPRC stream with an error, fail the region with connectToStoreErr
// and kv client will also redispatch the region.
func (s *eventFeedSession) requestRegionToStore(
	ctx context.Context,
	g *errgroup.Group,
) error {
	header := &cdcpb.Header{
		ClusterId:    s.client.clusterID,
		TicdcVersion: version.ReleaseSemver(),
	}
	// Always read old value.
	extraOp := kvrpcpb.ExtraOp_ReadOldValue

	var sri singleRegionInfo
	for {
		select {
		case <-ctx.Done():
			return errors.Trace(ctx.Err())
		case sri = <-s.regionRouter.Out():
		}
		requestID := allocateRequestID()

		rpcCtx := sri.rpcCtx
		regionID := rpcCtx.Meta.GetId()
		regionEpoch := rpcCtx.Meta.RegionEpoch
		req := &cdcpb.ChangeDataRequest{
			Header:       header,
			RegionId:     regionID,
			RequestId:    requestID,
			RegionEpoch:  regionEpoch,
			CheckpointTs: sri.resolvedTs(),
			StartKey:     sri.span.Start,
			EndKey:       sri.span.End,
			ExtraOp:      extraOp,
			FilterLoop:   s.client.filterLoop,
		}

		failpoint.Inject("kvClientPendingRegionDelay", nil)

		// each TiKV store has an independent pendingRegions.
		storeAddr := rpcCtx.Addr
		storeID := rpcCtx.Peer.GetStoreId()
		var (
			stream *eventFeedStream
			err    error
		)
		stream, ok := s.storeStreamsCache[storeAddr]
		if !ok || stream.isCanceled.Load() {
			if ok {
				// If the stream is canceled, we need to delete it from the cache and close it.
				s.deleteStream(stream)
			}
			stream, err = s.client.newStream(ctx, storeAddr, storeID)
			if err != nil {
				// get stream failed, maybe the store is down permanently, we should try to relocate the active store
				log.Warn("get grpc stream client failed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", requestID),
					zap.Uint64("storeID", storeID),
					zap.Error(err))
				if cerror.ErrVersionIncompatible.Equal(err) {
					// It often occurs on rolling update. Sleep 20s to reduce logs.
					delay := 20 * time.Second
					failpoint.Inject("kvClientDelayWhenIncompatible", func() {
						delay = 100 * time.Millisecond
					})
					time.Sleep(delay)
				}
				bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
				s.client.regionCache.OnSendFail(bo, rpcCtx, regionScheduleReload, err)
				errInfo := newRegionErrorInfo(sri, &connectToStoreErr{})
				s.onRegionFail(ctx, errInfo)
				continue
			}
			s.addStream(stream)
			log.Info("creating new stream to store to send request",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", requestID),
				zap.Uint64("storeID", storeID),
				zap.String("store", storeAddr),
				zap.Uint64("streamID", stream.id))

			g.Go(func() error {
				return s.receiveFromStream(ctx, stream)
			})
		}

		state := newRegionFeedState(sri, requestID)
		stream.regions.setByRequestID(requestID, state)
		log.Debug("start new request",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Uint64("regionID", sri.verID.GetID()),
			zap.String("addr", storeAddr))

		err = stream.client.Send(req)
		// If Send returns an error, the stream.client.Recv (In s.receiveFromStream)
		// would also receive an error.
		if err != nil {
			log.Warn("send request to stream failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.String("addr", storeAddr),
				zap.Uint64("storeID", storeID),
				zap.String("store", storeAddr),
				zap.Uint64("streamID", stream.id),
				zap.Uint64("regionID", regionID),
				zap.Uint64("requestID", requestID),
				zap.Error(err))
			if err := stream.client.CloseSend(); err != nil {
				log.Warn("failed to close stream",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Int64("tableID", s.tableID),
					zap.String("tableName", s.tableName),
					zap.Uint64("streamID", stream.id),
					zap.Uint64("storeID", storeID),
					zap.Uint64("regionID", regionID),
					zap.Uint64("requestID", requestID),
					zap.Error(err))
			}

			// Delete the stream from the cache so that when next time a region of
			// this store is requested, a new stream to this store will be created.
			s.deleteStream(stream)
			// Remove the region from pendingRegions. If it's already removed, it should be already retried by
			// `receiveFromStream`, so no need to retry here.
			_, ok := stream.regions.takeByRequestID(requestID)
			if !ok {
				continue
			}
			log.Debug("region send to store failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Any("regionId", sri.verID.GetID()),
				zap.Stringer("span", &sri.span))
			errInfo := newRegionErrorInfo(sri, &sendRequestToStoreErr{})
			s.onRegionFail(ctx, errInfo)
		}
	}
}

// dispatchRequest manages a set of streams and dispatch event feed requests
// to these streams. Streams to each store will be created on need. After
// establishing new stream, a goroutine will be spawned to handle events from
// the stream.
// Regions from `regionCh` will be connected. If any error happens to a
// region, the error will be sent to `errCh` and the receiver of `errCh` is
// responsible for handling the error.
func (s *eventFeedSession) dispatchRequest(ctx context.Context) error {
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
				ResolvedTs: sri.resolvedTs(),
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
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Uint64("regionID", sri.verID.GetID()),
				zap.Stringer("span", sri.span),
				zap.Uint64("resolvedTs", sri.resolvedTs()))
			errInfo := newRegionErrorInfo(sri, &rpcCtxUnavailableErr{verID: sri.verID})
			s.onRegionFail(ctx, errInfo)
			continue
		}
		sri.rpcCtx = rpcCtx
		s.regionRouter.In() <- sri
	}
}

// divideAndSendEventFeedToRegions split up the input span into spans aligned
// to region boundaries. When region merging happens, it's possible that it
// will produce some overlapping spans.
func (s *eventFeedSession) divideAndSendEventFeedToRegions(
	ctx context.Context, span regionspan.ComparableSpan,
) error {
	limit := 20
	nextSpan := span

	for {
		var (
			regions []*tikv.Region
			err     error
		)
		retryErr := retry.Do(ctx, func() error {
			bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
			start := time.Now()
			regions, err = s.client.regionCache.BatchLoadRegionsWithKeyRange(
				bo, nextSpan.Start, nextSpan.End, limit)
			scanRegionsDuration.Observe(time.Since(start).Seconds())
			if err != nil {
				return cerror.WrapError(cerror.ErrPDBatchLoadRegions, err)
			}
			metas := make([]*metapb.Region, 0, len(regions))
			for _, region := range regions {
				if region.GetMeta() == nil {
					return cerror.ErrMetaNotInRegion.FastGenByArgs()
				}
				metas = append(metas, region.GetMeta())
			}
			if !regionspan.CheckRegionsLeftCover(metas, nextSpan) {
				return cerror.ErrRegionsNotCoverSpan.FastGenByArgs(nextSpan, metas)
			}
			return nil
		}, retry.WithBackoffMaxDelay(500),
			retry.WithTotalRetryDuratoin(time.Duration(s.client.config.KVClient.RegionRetryDuration)))
		if retryErr != nil {
			log.Warn("load regions failed",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Any("span", nextSpan),
				zap.Error(retryErr))
			return retryErr
		}

		for _, tiRegion := range regions {
			region := tiRegion.GetMeta()
			partialSpan, err := regionspan.Intersect(
				s.totalSpan, regionspan.ComparableSpan{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			nextSpan.Start = region.EndKey
			// the End key return by the PD API will be nil to represent the biggest key,
			partialSpan = partialSpan.Hack()

			sri := newSingleRegionInfo(tiRegion.VerID(), partialSpan, nil)
			s.scheduleRegionRequest(ctx, sri)
			// return if no more regions
			if regionspan.EndCompare(nextSpan.Start, span.End) >= 0 {
				return nil
			}
		}
	}
}

// handleError handles error returned by a region. If some new EventFeed connection should be established, the region
// info will be sent to `regionCh`. Note if region channel is full, this function will be blocked.
// CAUTION: Note that this should only be invoked in a context that the region is not locked, otherwise use onRegionFail
// instead.
func (s *eventFeedSession) handleError(ctx context.Context, errInfo regionErrorInfo) error {
	err := errInfo.err
	switch eerr := errors.Cause(err).(type) {
	case *eventError:
		innerErr := eerr.err
		log.Info("cdc region error",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Stringer("error", innerErr))

		if notLeader := innerErr.GetNotLeader(); notLeader != nil {
			metricFeedNotLeaderCounter.Inc()
			s.client.regionCache.UpdateLeader(errInfo.verID, notLeader.GetLeader(), errInfo.rpcCtx.AccessIdx)
		} else if innerErr.GetEpochNotMatch() != nil {
			// TODO: If only confver is updated, we don't need to reload the region from region cache.
			metricFeedEpochNotMatchCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span)
			return nil
		} else if innerErr.GetRegionNotFound() != nil {
			metricFeedRegionNotFoundCounter.Inc()
			s.scheduleDivideRegionAndRequest(ctx, errInfo.span)
			return nil
		} else if duplicatedRequest := innerErr.GetDuplicateRequest(); duplicatedRequest != nil {
			metricFeedDuplicateRequestCounter.Inc()
			logPanic("tikv reported duplicated request to the same region, which is not expected",
				zap.Uint64("regionID", duplicatedRequest.RegionId))
			return errUnreachable
		} else if compatibility := innerErr.GetCompatibility(); compatibility != nil {
			log.Error("tikv reported compatibility error, which is not expected",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.String("rpcCtx", errInfo.rpcCtx.String()),
				zap.Stringer("error", compatibility))
			return cerror.ErrVersionIncompatible.GenWithStackByArgs(compatibility)
		} else if mismatch := innerErr.GetClusterIdMismatch(); mismatch != nil {
			log.Error("tikv reported the request cluster ID mismatch error, which is not expected",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("tikvCurrentClusterID", mismatch.Current),
				zap.Uint64("requestClusterID", mismatch.Request))
			return cerror.ErrClusterIDMismatch.GenWithStackByArgs(mismatch.Current, mismatch.Request)
		} else {
			metricFeedUnknownErrorCounter.Inc()
			log.Warn("receive empty or unknown error msg",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Stringer("error", innerErr))
		}
	case *rpcCtxUnavailableErr:
		metricFeedRPCCtxUnavailable.Inc()
		s.scheduleDivideRegionAndRequest(ctx, errInfo.span)
		return nil
	case *connectToStoreErr:
		metricConnectToStoreErr.Inc()
	case *sendRequestToStoreErr:
		metricStoreSendRequestErr.Inc()
	default:
		//[TODO] Move all OnSendFail logic here
		// We expect some unknown error to trigger RegionCache recheck its store state and change leader to peer to
		// make some detection(peer may tell us where new leader is)
		// RegionCache.OnSendFail is thread_safe inner.
		bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
		s.client.regionCache.OnSendFail(bo, errInfo.rpcCtx, regionScheduleReload, err)
	}

	failpoint.Inject("kvClientRegionReentrantErrorDelay", nil)
	s.scheduleRegionRequest(ctx, errInfo.singleRegionInfo)
	return nil
}

func (s *eventFeedSession) getRPCContextForRegion(ctx context.Context, id tikv.RegionVerID) (*tikv.RPCContext, error) {
	// todo: add metrics to track rpc cost
	bo := tikv.NewBackoffer(ctx, tikvRequestMaxBackoff)
	rpcCtx, err := s.client.regionCache.GetTiKVRPCContext(bo, id, tidbkv.ReplicaReadLeader, 0)
	if err != nil {
		return nil, cerror.WrapError(cerror.ErrGetTiKVRPCContext, err)
	}
	return rpcCtx, nil
}

// receiveFromStream receives gRPC messages from a stream continuously and sends
// messages to region worker, if `stream.Recv` meets error, this routine will exit
// silently. As for regions managed by this routine, there are two situations:
//  1. established regions: a `nil` event will be sent to region worker, and region
//     worker call `s.onRegionFail` to re-establish these regions.
//  2. pending regions: call `s.onRegionFail` for each pending region before this
//     routine exits to establish these regions.
func (s *eventFeedSession) receiveFromStream(
	parentCtx context.Context,
	stream *eventFeedStream,
) error {
	var tsStat *tableStoreStat
	s.client.tableStoreStats.Lock()
	key := fmt.Sprintf("%d_%d", s.client.tableID, stream.storeID)
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
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.String("store", stream.addr),
			zap.Uint64("storeID", stream.storeID),
			zap.Uint64("streamID", stream.id))

		failpoint.Inject("kvClientStreamCloseDelay", nil)

		remainingRegions := stream.regions.takeAll()
		for _, state := range remainingRegions {
			errInfo := newRegionErrorInfo(state.sri, cerror.ErrPendingRegionCancel.FastGenByArgs())
			s.onRegionFail(parentCtx, errInfo)
		}
	}()

	metricSendEventBatchResolvedSize := batchResolvedEventSize.
		WithLabelValues(s.changefeed.Namespace, s.changefeed.ID)
	// always create a new region worker, because `receiveFromStream` is ensured
	// to call exactly once from outer code logic
	worker := newRegionWorker(parentCtx, stream, s)
	defer worker.evictAllRegions()

	ctx, cancel := context.WithCancel(parentCtx)
	var retErr error
	once := sync.Once{}
	handleExit := func(err error) error {
		once.Do(func() {
			cancel()
			retErr = err
		})
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		err := handleExit(worker.run())
		if err != nil {
			log.Error("region worker exited with error",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.String("store", stream.addr),
				zap.Uint64("storeID", stream.storeID),
				zap.Uint64("streamID", stream.id),
				zap.Error(err))
		}
		return err
	})

	receiveEvents := func() error {
		maxCommitTs := model.Ts(0)
		for {
			cevent, err := stream.client.Recv()

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
					log.Info(
						"receive from stream canceled",
						zap.String("namespace", s.changefeed.Namespace),
						zap.String("changefeed", s.changefeed.ID),
						zap.Int64("tableID", s.tableID),
						zap.String("tableName", s.tableName),
						zap.String("store", stream.addr),
						zap.Uint64("storeID", stream.storeID),
						zap.Uint64("streamID", stream.id))
				} else {
					log.Warn(
						"failed to receive from stream",
						zap.String("namespace", s.changefeed.Namespace),
						zap.String("changefeed", s.changefeed.ID),
						zap.Int64("tableID", s.tableID),
						zap.String("tableName", s.tableName),
						zap.String("store", stream.addr),
						zap.Uint64("storeID", stream.storeID),
						zap.Uint64("streamID", stream.id),
						zap.Error(err))
					// Note that pd need at lease 10s+ to tag a kv node as disconnect if kv node down
					// tikv raft need wait (raft-base-tick-interval * raft-election-timeout-ticks) 10s to start a new
					// election
				}

				// Use the same delay mechanism as `stream.Send` error handling, since
				// these two errors often mean upstream store suffers an accident, which
				// needs time to recover, kv client doesn't need to retry frequently.
				// TODO: add a better retry backoff or rate limitter
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(100)))
				// send nil regionStatefulEvent to signal worker exit
				// worker.sendEvents will return error if ctx is canceled
				// In this case, we should return the error to the caller to cancel the whole job.
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
				if entries, ok := cevent.Events[0].Event.(*cdcpb.Event_Entries_); ok && len(entries.Entries.Entries) > 0 {
					commitTs := entries.Entries.Entries[0].CommitTs
					if maxCommitTs < commitTs {
						maxCommitTs = commitTs
					}
				}
			}
			err = s.sendRegionChangeEvents(ctx, cevent.Events, worker)
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
	eg.Go(func() error {
		return handleExit(receiveEvents())
	})

	_ = eg.Wait()
	return retErr
}

func (s *eventFeedSession) sendRegionChangeEvents(
	ctx context.Context,
	events []*cdcpb.Event,
	worker *regionWorker,
) error {
	statefulEvents := make([][]*regionStatefulEvent, worker.concurrency)
	for i := 0; i < worker.concurrency; i++ {
		// Allocate a buffer with 2x length than average to reduce reallocate.
		buffLen := len(events) / worker.concurrency * 3 / 2
		statefulEvents[i] = make([]*regionStatefulEvent, 0, buffLen)
	}

	for _, event := range events {
		state, valid := worker.getRegionState(event.RegionId)
		// Every region's range is locked before sending requests and unlocked after exiting, and the requestID
		// is allocated while holding the range lock. Therefore the requestID is always incrementing. If a region
		// is receiving messages with different requestID, only the messages with the larges requestID is valid.
		if valid {
			if state.requestID < event.RequestId {
				log.Debug("region state entry will be replaced because received message of newer requestID",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("oldRequestID", state.requestID),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", worker.stream.addr),
					zap.Uint64("streamID", worker.stream.id),
				)
				valid = false
			} else if state.requestID > event.RequestId {
				log.Warn("drop event due to event belongs to a stale request",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.Uint64("currRequestID", state.requestID),
					zap.String("addr", worker.stream.addr),
					zap.Uint64("streamID", worker.stream.id),
				)
				continue
			}
		}

		if !valid {
			// It's the first response for this region. If the region is newly connected, the region info should
			// have been put in `pendingRegions`. So here we load the region info from `pendingRegions` and start
			// a new goroutine to handle messages from this region.
			// Firstly load the region info.
			state, valid = worker.stream.regions.takeByRequestID(event.RequestId)
			if !valid {
				log.Warn("drop event due to region feed is removed",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Uint64("regionID", event.RegionId),
					zap.Uint64("requestID", event.RequestId),
					zap.String("addr", worker.stream.addr),
					zap.Uint64("streamID", worker.stream.id),
				)
				continue
			}
			state.start()
			worker.setRegionState(event.RegionId, state)
		} else if state.isStale() {
			log.Warn("drop event due to region feed stopped",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Uint64("regionID", event.RegionId),
				zap.Uint64("requestID", event.RequestId),
				zap.String("addr", worker.stream.addr),
				zap.Uint64("streamID", worker.stream.id),
			)
			continue
		}

		switch x := event.Event.(type) {
		case *cdcpb.Event_Error:
			log.Info("event feed receives a region error",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Uint64("regionID", event.RegionId),
				zap.String("addr", worker.stream.addr),
				zap.Uint64("streamID", worker.stream.id),
				zap.Any("error", x.Error))
		}

		slot := worker.inputCalcSlot(event.RegionId)
		statefulEvents[slot] = append(statefulEvents[slot], &regionStatefulEvent{
			changeEvent: event,
			regionID:    event.RegionId,
			state:       state,
		})
	}
	for _, events := range statefulEvents {
		if len(events) > 0 {
			err := worker.sendEvents(ctx, events)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *eventFeedSession) sendResolvedTs(
	ctx context.Context,
	resolvedTs *cdcpb.ResolvedTs,
	worker *regionWorker,
) error {
	statefulEvents := make([]*regionStatefulEvent, worker.concurrency)
	// split resolved ts
	for i := 0; i < worker.concurrency; i++ {
		// Allocate a buffer with 1.5x length than average to reduce reallocate.
		buffLen := len(resolvedTs.Regions) / worker.concurrency * 2
		ev := s.resolvedTsPool.Get().(*regionStatefulEvent)
		// must reset fields to prevent dirty data
		ev.resolvedTsEvent.resolvedTs = resolvedTs.Ts
		ev.resolvedTsEvent.regions = make([]*regionFeedState, 0, buffLen)
		ev.finishedCallbackCh = nil
		statefulEvents[i] = ev
	}

	for _, regionID := range resolvedTs.Regions {
		state, ok := worker.getRegionState(regionID)
		if ok {
			slot := worker.inputCalcSlot(regionID)
			statefulEvents[slot].resolvedTsEvent.regions = append(
				statefulEvents[slot].resolvedTsEvent.regions, state,
			)
			// regionID is just an slot index
			statefulEvents[slot].regionID = regionID
		}
	}
	for _, event := range statefulEvents {
		if len(event.resolvedTsEvent.regions) > 0 {
			err := worker.sendEvents(ctx, []*regionStatefulEvent{event})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// addStream adds a stream to the session.streams.
// Note: It must be called with deleteStream in a same goroutine.
func (s *eventFeedSession) addStream(stream *eventFeedStream) {
	oldStream, ok := s.storeStreamsCache[stream.addr]
	if ok {
		failpoint.Inject("kvClientAddDuplicatedStream", func() {
			log.Error("A stream to a same store already exists, it shouldn't happen, please report a bug",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Uint64("oldStreamID", oldStream.id),
				zap.Uint64("newStreamID", stream.id))
		})
		// There is no need to return an error here because even if it happens,
		// it does not harm the data correctness, but may only cause some lag spikes.
		// Log it to help us improve the code.
		log.Error("A stream to a same store already exists, it shouldn't happen, please report a bug",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Uint64("oldStreamID", oldStream.id),
			zap.Uint64("newStreamID", stream.id))
	}
	s.storeStreamsCache[stream.addr] = stream
}

// deleteStream deletes a stream from the session.streams.
// If the stream is not found, it takes no effect.
func (s *eventFeedSession) deleteStream(streamToDelete *eventFeedStream) {
	streamInMap, ok := s.storeStreamsCache[streamToDelete.addr]
	if !ok {
		// This should not happen, but it will be no harm if it happens.
		// Log a warning message to help us diagnose the problem.
		log.Warn("delete stream failed, stream not found, ignore it",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Uint64("streamID", streamToDelete.id),
			zap.Uint64("streamIDInMap", streamInMap.id))
		return
	}
	if streamInMap.id != streamToDelete.id {
		// This should not happen, but it will be no harm if it happens.
		// Log a warning message to help us diagnose the problem.
		log.Warn("delete stream failed, stream id mismatch, ignore it",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Uint64("streamID", streamToDelete.id),
			zap.Uint64("streamIDInMap", streamInMap.id))
		return
	}

	if time.Since(streamToDelete.createTime) < streamAlterInterval {
		log.Warn("It's too soon to delete a stream, wait for a while",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName),
			zap.Uint64("streamID", streamToDelete.id),
			zap.Duration("sinceCreateDuration", time.Since(streamToDelete.createTime)))
		time.Sleep(streamAlterInterval - time.Since(streamToDelete.createTime))
	}

	s.client.grpcPool.ReleaseConn(streamToDelete.conn, streamToDelete.addr)
	streamToDelete.close()
	delete(s.storeStreamsCache, streamToDelete.addr)
	log.Info("A stream to store has been removed",
		zap.String("namespace", s.changefeed.Namespace),
		zap.String("changefeed", s.changefeed.ID),
		zap.Int64("tableID", s.tableID),
		zap.String("tableName", s.tableName),
		zap.String("store", streamToDelete.addr),
		zap.Uint64("storeID", streamToDelete.storeID),
		zap.Uint64("streamID", streamToDelete.id),
		zap.Duration("sinceCreateDuration", time.Since(streamToDelete.createTime)))
}

func (s *eventFeedSession) logSlowRegions(ctx context.Context) error {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		attr := s.rangeLock.CollectLockedRangeAttrs(nil)
		ckptTime := oracle.GetTimeFromTS(attr.SlowestRegion.CheckpointTs)
		currTime := s.client.pdClock.CurrentTime()
		log.Info("event feed starts to check locked regions",
			zap.String("namespace", s.changefeed.Namespace),
			zap.String("changefeed", s.changefeed.ID),
			zap.Int64("tableID", s.tableID),
			zap.String("tableName", s.tableName))

		if attr.SlowestRegion.Initialized {
			if currTime.Sub(ckptTime) > 2*resolveLockMinInterval {
				log.Info("event feed finds a initialized slow region",
					zap.String("namespace", s.changefeed.Namespace),
					zap.String("changefeed", s.changefeed.ID),
					zap.Int64("tableID", s.tableID),
					zap.String("tableName", s.tableName),
					zap.Any("slowRegion", attr.SlowestRegion))
			}
		} else if currTime.Sub(attr.SlowestRegion.Created) > 10*time.Minute {
			log.Info("event feed initializes a region too slow",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Any("slowRegion", attr.SlowestRegion))
		} else if currTime.Sub(ckptTime) > 10*time.Minute {
			log.Info("event feed finds a uninitialized slow region",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.Any("slowRegion", attr.SlowestRegion))
		}
		if len(attr.Holes) > 0 {
			holes := make([]string, 0, len(attr.Holes))
			for _, hole := range attr.Holes {
				holes = append(holes, fmt.Sprintf("[%s,%s)", hole.Start, hole.End))
			}
			log.Info("event feed holes exist",
				zap.String("namespace", s.changefeed.Namespace),
				zap.String("changefeed", s.changefeed.ID),
				zap.Int64("tableID", s.tableID),
				zap.String("tableName", s.tableName),
				zap.String("holes", strings.Join(holes, ", ")))
		}
	}
}

func assembleRowEvent(regionID uint64, entry *cdcpb.Event_Row) (model.RegionFeedEvent, error) {
	var opType model.OpType
	switch entry.GetOpType() {
	case cdcpb.Event_Row_DELETE:
		opType = model.OpTypeDelete
	case cdcpb.Event_Row_PUT:
		opType = model.OpTypePut
	default:
		return model.RegionFeedEvent{}, cerror.ErrUnknownKVEventType.GenWithStackByArgs(entry.GetOpType(), entry)
	}

	revent := model.RegionFeedEvent{
		RegionID: regionID,
		Val: &model.RawKVEntry{
			OpType:   opType,
			Key:      entry.Key,
			Value:    entry.GetValue(),
			StartTs:  entry.StartTs,
			CRTs:     entry.CommitTs,
			RegionID: regionID,
			OldValue: entry.GetOldValue(),
		},
	}

	return revent, nil
}

// eventError wrap cdcpb.Event_Error to implements error interface.
type eventError struct {
	err *cdcpb.Error
}

// Error implement error interface.
func (e *eventError) Error() string {
	return e.err.String()
}

type rpcCtxUnavailableErr struct {
	verID tikv.RegionVerID
}

func (e *rpcCtxUnavailableErr) Error() string {
	return fmt.Sprintf("cannot get rpcCtx for region %v. ver:%v, confver:%v",
		e.verID.GetID(), e.verID.GetVer(), e.verID.GetConfVer())
}

type connectToStoreErr struct{}

func (e *connectToStoreErr) Error() string { return "connect to store error" }

type sendRequestToStoreErr struct{}

func (e *sendRequestToStoreErr) Error() string { return "send request to store error" }
