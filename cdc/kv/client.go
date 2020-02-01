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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

const (
	dialTimeout = 10 * time.Second
	maxRetry    = 10
)

type singleRegionInfo struct {
	meta *metapb.Region
	span util.Span
	ts   uint64
}

// CDCClient to get events from TiKV
type CDCClient struct {
	pd pd.Client

	clusterID uint64

	mu struct {
		sync.Mutex
		conns map[string]*grpc.ClientConn
	}

	storeMu struct {
		sync.Mutex
		stores map[uint64]*metapb.Store
	}
}

// NewCDCClient creates a CDCClient instance
func NewCDCClient(pd pd.Client) (c *CDCClient, err error) {
	clusterID := pd.GetClusterID(context.Background())
	log.Info("get clusterID", zap.Uint64("id", clusterID))

	c = &CDCClient{
		clusterID: clusterID,
		pd:        pd,
		mu: struct {
			sync.Mutex
			conns map[string]*grpc.ClientConn
		}{
			conns: make(map[string]*grpc.ClientConn),
		},
		storeMu: struct {
			sync.Mutex
			stores map[uint64]*metapb.Store
		}{
			stores: make(map[uint64]*metapb.Store),
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
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
	)
	cancel()

	if err != nil {
		return nil, errors.Annotatef(err, "dial %s fail", addr)
	}

	c.mu.conns[addr] = conn

	return
}

func (c *CDCClient) getConnByMeta(
	ctx context.Context, meta *metapb.Region,
) (conn *grpc.ClientConn, err error) {
	if len(meta.Peers) == 0 {
		return nil, errors.New("no peer")
	}

	// the first is leader in Peers?
	peer := meta.Peers[0]

	store, err := c.getStore(ctx, peer.GetStoreId())
	if err != nil {
		return nil, err
	}

	return c.getConn(ctx, store.Address)
}

func (c *CDCClient) getStore(
	ctx context.Context, id uint64,
) (store *metapb.Store, err error) {
	c.storeMu.Lock()
	defer c.storeMu.Unlock()

	if store, ok := c.storeMu.stores[id]; ok {
		return store, nil
	}

	store, err = c.pd.GetStore(ctx, id)
	if err != nil {
		return nil, errors.Annotatef(err, "get store %d failed", id)
	}

	c.storeMu.stores[id] = store

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

	g, ctx := errgroup.WithContext(ctx)

	regionCh := make(chan singleRegionInfo, 16)
	g.Go(func() error {
		for {
			select {
			case sri := <-regionCh:
				g.Go(func() error {
					return c.partialRegionFeed(ctx, &sri, regionCh, eventCh)
				})
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	g.Go(func() error {
		return c.divideAndSendEventFeedToRegions(ctx, span, ts, regionCh)
	})

	return g.Wait()
}

// partialRegionFeed establishes a EventFeed to the region specified by regionInfo.
// It manages lifecycle events of the region in order to maintain the EventFeed
// connection, this may involve retry this region EventFeed or subdividing the region
// further in the event of a split.
func (c *CDCClient) partialRegionFeed(
	ctx context.Context,
	regionInfo *singleRegionInfo,
	regionCh chan<- singleRegionInfo,
	eventCh chan<- *model.RegionFeedEvent,
) error {
	ts := regionInfo.ts

	berr := retry.Run(func() error {
		var err error

		if regionInfo.meta == nil {
			regionInfo.meta, _, err = c.pd.GetRegion(ctx, regionInfo.span.Start)
		}

		if err != nil {
			log.Warn("get meta failed", zap.Error(err))
			return errors.Trace(err)
		}

		maxTs, err := c.singleEventFeed(ctx, regionInfo.span, regionInfo.ts, regionInfo.meta, eventCh)
		log.Debug("singleEventFeed quit")

		if maxTs > ts {
			ts = maxTs
		}

		if err != nil {
			log.Info("EventFeed disconnected",
				zap.Reflect("span", regionInfo.span),
				zap.Uint64("checkpoint", ts),
				zap.Error(err))

			switch eerr := errors.Cause(err).(type) {
			case *eventError:
				if eerr.GetNotLeader() != nil {
					eventFeedErrorCounter.WithLabelValues("NotLeader").Inc()
					regionInfo.meta = nil
					return errors.Trace(err)
				} else if eerr.GetEpochNotMatch() != nil {
					eventFeedErrorCounter.WithLabelValues("EpochNotMatch").Inc()
					return c.divideAndSendEventFeedToRegions(ctx, regionInfo.span, ts, regionCh)
				} else if eerr.GetRegionNotFound() != nil {
					eventFeedErrorCounter.WithLabelValues("RegionNotFound").Inc()
					regionInfo.meta = nil
					return errors.Trace(err)
				} else {
					eventFeedErrorCounter.WithLabelValues("Unknown").Inc()
					log.Warn("receive empty or unknown error msg", zap.Stringer("error", eerr))
					return errors.Annotate(err, "receive empty or unknow error msg")
				}
			default:
				if status.Code(err) == codes.Unavailable {
					regionInfo.meta = nil
					return errors.Trace(err)
				}

				return backoff.Permanent(err)

			}
		}

		return nil
	}, maxRetry)

	if errors.Cause(berr) == context.Canceled {
		return nil
	}
	return errors.Trace(berr)
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
			regions []*metapb.Region
			err     error
		)
		retryErr := retry.Run(func() error {
			scanT0 := time.Now()
			regions, _, err = c.pd.ScanRegions(ctx, nextSpan.Start, nextSpan.End, limit)
			scanRegionsDuration.WithLabelValues(captureID).Observe(time.Since(scanT0).Seconds())
			if err != nil {
				return errors.Trace(err)
			}
			if !util.CheckRegionsLeftCover(regions, nextSpan) {
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

		for _, region := range regions {
			partialSpan, err := util.Intersect(nextSpan, util.Span{Start: region.StartKey, End: region.EndKey})
			if err != nil {
				return errors.Trace(err)
			}
			log.Debug("get partialSpan", zap.Reflect("span", partialSpan))

			nextSpan.Start = region.EndKey

			select {
			case regionCh <- singleRegionInfo{
				meta: region,
				span: partialSpan,
				ts:   ts,
			}:
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

// singleEventFeed makes a EventFeed RPC call.
// Results will be send to eventCh
// EventFeed RPC will not return checkpoint event directly
// Checkpoint event is generate while there's not non-match pre-write
// Return the maximum checkpoint
func (c *CDCClient) singleEventFeed(
	ctx context.Context,
	span util.Span,
	ts uint64,
	meta *metapb.Region,
	eventCh chan<- *model.RegionFeedEvent,
) (checkpointTs uint64, err error) {
	req := &cdcpb.ChangeDataRequest{
		Header: &cdcpb.Header{
			ClusterId: c.clusterID,
		},
		RegionId:     meta.GetId(),
		RegionEpoch:  meta.RegionEpoch,
		CheckpointTs: ts,
		StartKey:     span.Start,
		EndKey:       span.End,
	}

	var initialized uint32

	conn, err := c.getConnByMeta(ctx, meta)
	if err != nil {
		return req.CheckpointTs, err
	}

	client := cdcpb.NewChangeDataClient(conn)
	matcher := newMatcher()

	log.Debug("start new request", zap.Reflect("request", req))
	stream, err := client.EventFeed(ctx, req)

	if err != nil {
		log.Error("RPC error", zap.Error(err))
		return req.CheckpointTs, errors.Trace(err)
	}

	maxItemFn := func(item sortItem) {
		if atomic.LoadUint32(&initialized) == 0 {
			return
		}

		// emit a checkpoint
		revent := &model.RegionFeedEvent{
			Checkpoint: &model.RegionFeedCheckpoint{
				Span:       span,
				ResolvedTs: item.commit,
			},
		}
		updateCheckpointTS(&req.CheckpointTs, item.commit)
		select {
		case eventCh <- revent:
		case <-ctx.Done():
		}
	}

	// TODO: drop this if we totally depends on the ResolvedTs event from
	// tikv to emit the RegionFeedCheckpoint.
	sorter := newSorter(maxItemFn)
	defer sorter.close()

	for {
		cevent, err := stream.Recv()
		if err == io.EOF {
			return atomic.LoadUint64(&req.CheckpointTs), nil
		}

		if err != nil {
			return atomic.LoadUint64(&req.CheckpointTs), errors.Trace(err)
		}

		// log.Debug("recv ChangeDataEvent", zap.Stringer("event", cevent))
		captureID := util.CaptureIDFromCtx(ctx)

		for _, event := range cevent.Events {
			eventSize.WithLabelValues(captureID).Observe(float64(event.Event.Size()))
			switch x := event.Event.(type) {
			case *cdcpb.Event_Entries_:
				for _, row := range x.Entries.GetEntries() {
					switch row.Type {
					case cdcpb.Event_INITIALIZED:
						atomic.StoreUint32(&initialized, 1)
					case cdcpb.Event_COMMITTED:
						var opType model.OpType
						switch row.GetOpType() {
						case cdcpb.Event_Row_DELETE:
							opType = model.OpTypeDelete
						case cdcpb.Event_Row_PUT:
							opType = model.OpTypePut
						default:
							return atomic.LoadUint64(&req.CheckpointTs), errors.Errorf("unknow tp: %v", row.GetOpType())
						}

						revent := &model.RegionFeedEvent{
							Val: &model.RegionFeedValue{
								OpType: opType,
								Key:    row.Key,
								Value:  row.GetValue(),
								Ts:     row.CommitTs,
							},
						}
						select {
						case eventCh <- revent:
						case <-ctx.Done():
							return atomic.LoadUint64(&req.CheckpointTs), errors.Trace(ctx.Err())
						}
					case cdcpb.Event_PREWRITE:
						matcher.putPrewriteRow(row)
						sorter.pushTsItem(sortItem{
							start: row.GetStartTs(),
							tp:    cdcpb.Event_PREWRITE,
						})
					case cdcpb.Event_COMMIT:
						// emit a value
						value, err := matcher.matchRow(row)
						if err != nil {
							log.Warn("match row error", zap.Error(err), zap.Stringer("row", row))
						}

						var opType model.OpType
						switch row.GetOpType() {
						case cdcpb.Event_Row_DELETE:
							opType = model.OpTypeDelete
						case cdcpb.Event_Row_PUT:
							opType = model.OpTypePut
						default:
							return atomic.LoadUint64(&req.CheckpointTs), errors.Errorf("unknow tp: %v", row.GetOpType())
						}

						revent := &model.RegionFeedEvent{
							Val: &model.RegionFeedValue{
								OpType: opType,
								Key:    row.Key,
								Value:  value,
								Ts:     row.CommitTs,
							},
						}

						select {
						case eventCh <- revent:
						case <-ctx.Done():
							return atomic.LoadUint64(&req.CheckpointTs), errors.Trace(ctx.Err())
						}
						sorter.pushTsItem(sortItem{
							start:  row.GetStartTs(),
							commit: row.GetCommitTs(),
							tp:     cdcpb.Event_COMMIT,
						})
					case cdcpb.Event_ROLLBACK:
						matcher.rollbackRow(row)
						sorter.pushTsItem(sortItem{
							start:  row.GetStartTs(),
							commit: row.GetCommitTs(),
							tp:     cdcpb.Event_ROLLBACK,
						})
					}
				}
			case *cdcpb.Event_Admin_:
				log.Info("receive admin event", zap.Stringer("event", event))
			case *cdcpb.Event_Error_:
				return atomic.LoadUint64(&req.CheckpointTs), errors.Trace(&eventError{Event_Error: x.Error})
			case *cdcpb.Event_ResolvedTs:
				if atomic.LoadUint32(&initialized) == 1 {
					// emit a checkpoint
					revent := &model.RegionFeedEvent{
						Checkpoint: &model.RegionFeedCheckpoint{
							Span:       span,
							ResolvedTs: x.ResolvedTs,
						},
					}

					updateCheckpointTS(&req.CheckpointTs, x.ResolvedTs)
					select {
					case eventCh <- revent:
					case <-ctx.Done():
						return atomic.LoadUint64(&req.CheckpointTs), errors.Trace(ctx.Err())
					}

				}
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
	*cdcpb.Event_Error
}

// Error implement error interface.
func (e *eventError) Error() string {
	return e.Event_Error.String()
}
