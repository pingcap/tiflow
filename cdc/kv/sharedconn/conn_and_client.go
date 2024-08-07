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

package sharedconn

import (
	"context"
	"io"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/pkg/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
)

const (
	grpcInitialWindowSize     = (1 << 16) - 1
	grpcInitialConnWindowSize = 1 << 23
	grpcMaxCallRecvMsgSize    = 1 << 28

	rpcMetaFeaturesKey string = "features"
	rpcMetaFeaturesSep string = ","

	// this feature supports these interactions with TiKV sides:
	// 1. in one GRPC stream, TiKV will merge resolved timestamps into several buckets based on
	//    `RequestId`s. For example, region 100 and 101 have been subscribed twice with `RequestId`
	//    1 and 2, TiKV will sends a ResolvedTs message
	//    [{"RequestId": 1, "regions": [100, 101]}, {"RequestId": 2, "regions": [100, 101]}]
	//    to the TiCDC client.
	// 2. TiCDC can deregister all regions with a same request ID by specifying the `RequestId`.
	rpcMetaFeatureStreamMultiplexing string = "stream-multiplexing"
)

// grpcConn is a wrapper for a gRPC client connection. It contains the number of streams that are
// sharing the connection.
type grpcConn struct {
	*grpc.ClientConn
	multiplexing bool
	// streamCount is the number of streams that are sharing the connection.
	streamCount int
}

// storeConns represent a group of conns of a same store.
type storeConns struct {
	// pool is the ConnAndClientPool that manages this connArray.
	pool *GRPCPool
	// addr is the address of the store that this connArray connects to.
	addr         string
	inConnecting atomic.Bool

	sync.Mutex
	// conns is the list of connections in this connArray.
	conns []*grpcConn
}

// newConnect creates a new connection to the store of the connArray.
func (c *storeConns) newConnect(ctx context.Context) (conn *grpcConn, err error) {
	if c.inConnecting.CompareAndSwap(false, true) {
		defer c.inConnecting.Store(false)
		var clientConn *grpc.ClientConn
		if clientConn, err = c.pool.connect(ctx, c.addr); err != nil {
			return
		}

		rpc := cdcpb.NewChangeDataClient(clientConn)
		ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
		var client cdcpb.ChangeData_EventFeedV2Client
		if client, err = rpc.EventFeedV2(ctx); err == nil {
			_ = client.CloseSend()
			_, err = client.Recv()
		}

		status := grpcstatus.Convert(err)
		if StatusIsEOF(status) {
			conn = new(grpcConn)
			conn.ClientConn = clientConn
			conn.multiplexing = true
			err = nil
		} else if status.Code() == grpccodes.Unimplemented {
			conn = new(grpcConn)
			conn.ClientConn = clientConn
			conn.multiplexing = false
			err = nil
		} else {
			_ = clientConn.Close()
		}
	}
	return
}

// peek returns the connection with the fewest streams.
func (c *storeConns) peek() *grpcConn {
	c.Lock()
	defer c.Unlock()
	c.sort()
	return c.conns[0]
}

// push adds a connection to the storeConns.
func (c *storeConns) push(conn *grpcConn) {
	c.Lock()
	defer c.Unlock()
	c.conns = append(c.conns, conn)
	c.sort()
}

func (c *storeConns) release(conn *grpcConn) {
	c.Lock()
	defer c.Unlock()
	conn.streamCount -= 1
	if conn.streamCount == 0 {
		for i := range c.conns {
			if c.conns[i] == conn {
				c.conns[i] = c.conns[len(c.conns)-1]
				c.conns = c.conns[:len(c.conns)-1]
				break
			}
		}
		if len(c.conns) == 0 {
			c.pool.Lock()
			delete(c.pool.stores, c.addr)
			c.pool.Unlock()
		}
		_ = conn.ClientConn.Close()
	}
	c.sort()
}

// sort sorts the connections in the connArray, so that the connections with
// multiplexing and fewer streams come first.
// Note: This function must be called with the lock of the storeConns held.
func (c *storeConns) sort() {
	sort.Slice(c.conns, func(i, j int) bool {
		return (c.conns[i].multiplexing && !c.conns[j].multiplexing) ||
			(c.conns[i].multiplexing == c.conns[j].multiplexing && c.conns[i].streamCount < c.conns[j].streamCount)
	})
}

func (c *storeConns) connAvailable() bool {
	c.Lock()
	defer c.Unlock()
	return len(c.conns) > 0 && c.conns[0].streamCount < c.pool.maxStreamsPerConn
}

// EventFeedClient represents a EventFeedV2 client.
// It is used to create grpc streams for the EventFeedV2 service.
// It holds the underlying grpc connection and the storeConns that the connection belongs to.
type EventFeedClient struct {
	// conn is the underlying connection of the client.
	// Multiple EventFeedClients can share a same grpcConn.
	conn *grpcConn
	// storeConns is the storeConns that the conn belongs to.
	// It is used to release the conn when the client is closed.
	storeConns *storeConns
	client     cdcpb.ChangeData_EventFeedV2Client
	closed     atomic.Bool
}

// Client returns the underlying EventFeedV2 client.
func (c *EventFeedClient) Client() cdcpb.ChangeData_EventFeedV2Client {
	return c.client
}

// IsMultiplexing indicates whether the client can be used for multiplexing or not.
func (c *EventFeedClient) IsMultiplexing() bool {
	return c.conn.multiplexing
}

// Release releases the EventFeedClient from its underlying connection.
func (c *EventFeedClient) Release() {
	if c.client != nil && !c.closed.Load() {
		_ = c.client.CloseSend()
		c.closed.Store(true)
	}
	if c.conn != nil && c.storeConns != nil {
		c.storeConns.release(c.conn)
		c.conn = nil
		c.storeConns = nil
	}
}

// GRPCPool is a pool for EventFeedClient.
type GRPCPool struct {
	credential        *security.Credential
	grpcMetrics       *grpc_prometheus.ClientMetrics
	maxStreamsPerConn int

	sync.Mutex
	stores map[string]*storeConns
}

// NewGRPCPool creates a new ConnAndClientPool.
func NewGRPCPool(
	credential *security.Credential,
	grpcMetrics *grpc_prometheus.ClientMetrics,
	maxStreamsPerConn ...int,
) *GRPCPool {
	return newGRPCPool(credential, grpcMetrics, 1000)
}

func newGRPCPool(
	credential *security.Credential,
	grpcMetrics *grpc_prometheus.ClientMetrics,
	maxStreamsPerConn int,
) *GRPCPool {
	stores := make(map[string]*storeConns, 64)
	return &GRPCPool{
		credential:        credential,
		grpcMetrics:       grpcMetrics,
		maxStreamsPerConn: maxStreamsPerConn,
		stores:            stores,
	}
}

// Connect gets a ConnAndClient object for the store with the specified address.
// It will create a new connection if there is no available connection.
func (c *GRPCPool) Connect(ctx context.Context, addr string) (cc *EventFeedClient, err error) {
	var storeConnections *storeConns
	c.Lock()
	if storeConnections = c.stores[addr]; storeConnections == nil {
		storeConnections = &storeConns{pool: c, addr: addr}
		c.stores[addr] = storeConnections
	}
	c.Unlock()

	// Wait for an available connection.
	for !storeConnections.connAvailable() {
		var conn *grpcConn
		if conn, err = storeConnections.newConnect(ctx); err != nil {
			return
		}

		if conn != nil {
			storeConnections.push(conn)
		}

		// If the connection is not established, we should wait for a while and retry to connect.
		if err = util.Hang(ctx, time.Second); err != nil {
			return
		}
	}

	cc = &EventFeedClient{conn: storeConnections.peek(), storeConns: storeConnections}
	cc.conn.streamCount += 1
	defer func() {
		if err != nil && cc != nil {
			cc.Release()
			cc = nil
		}
	}()

	rpc := cdcpb.NewChangeDataClient(cc.conn.ClientConn)
	if cc.conn.multiplexing {
		ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
		cc.client, err = rpc.EventFeedV2(ctx)
	} else {
		cc.client, err = rpc.EventFeed(ctx)
	}
	return
}

func (c *GRPCPool) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := c.credential.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}

	dialOptions := []grpc.DialOption{
		grpcTLSOption,
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
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
	}

	if c.grpcMetrics != nil {
		dialOptions = append(dialOptions, grpc.WithUnaryInterceptor(c.grpcMetrics.UnaryClientInterceptor()))
		dialOptions = append(dialOptions, grpc.WithStreamInterceptor(c.grpcMetrics.StreamClientInterceptor()))
	}

	return grpc.DialContext(ctx, target, dialOptions...)
}

func getContextFromFeatures(ctx context.Context, features []string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			rpcMetaFeaturesKey: strings.Join(features, rpcMetaFeaturesSep),
		}),
	)
}

// StatusIsEOF checks whether status is caused by client send closing.
func StatusIsEOF(status *grpcstatus.Status) bool {
	return status == nil ||
		status.Code() == grpccodes.Canceled ||
		(status.Code() == grpccodes.Unknown && status.Message() == io.EOF.Error())
}
