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

// StatusIsEOF checks whether status is caused by client send closing.
func StatusIsEOF(status *grpcstatus.Status) bool {
	return status == nil ||
		status.Code() == grpccodes.Canceled ||
		(status.Code() == grpccodes.Unknown && status.Message() == io.EOF.Error())
}

// ConnAndClientPool is a pool of ConnAndClient.
type ConnAndClientPool struct {
	credential        *security.Credential
	grpcMetrics       *grpc_prometheus.ClientMetrics
	maxStreamsPerConn int

	sync.Mutex
	stores map[string]*connArray
}

// ConnAndClient indicates a connection and a EventFeedV2 client.
type ConnAndClient struct {
	conn   *Conn
	array  *connArray
	client cdcpb.ChangeData_EventFeedV2Client
	closed atomic.Bool
}

// Conn is a connection.
type Conn struct {
	*grpc.ClientConn
	multiplexing bool
	streams      int
}

type connArray struct {
	pool         *ConnAndClientPool
	addr         string
	inConnecting atomic.Bool

	sync.Mutex
	conns []*Conn
}

// NewConnAndClientPool creates a new ConnAndClientPool.
func NewConnAndClientPool(
	credential *security.Credential,
	grpcMetrics *grpc_prometheus.ClientMetrics,
	maxStreamsPerConn ...int,
) *ConnAndClientPool {
	return newConnAndClientPool(credential, grpcMetrics, 1000)
}

func newConnAndClientPool(
	credential *security.Credential,
	grpcMetrics *grpc_prometheus.ClientMetrics,
	maxStreamsPerConn int,
) *ConnAndClientPool {
	stores := make(map[string]*connArray, 64)
	return &ConnAndClientPool{
		credential:        credential,
		grpcMetrics:       grpcMetrics,
		maxStreamsPerConn: maxStreamsPerConn,
		stores:            stores,
	}
}

// Connect connects to addr.
func (c *ConnAndClientPool) Connect(ctx context.Context, addr string) (cc *ConnAndClient, err error) {
	var conns *connArray
	c.Lock()
	if conns = c.stores[addr]; conns == nil {
		conns = &connArray{pool: c, addr: addr}
		c.stores[addr] = conns
	}
	c.Unlock()

	for {
		conns.Lock()
		if len(conns.conns) > 0 && conns.conns[0].streams < c.maxStreamsPerConn {
			break
		}

		conns.Unlock()
		var conn *Conn
		if conn, err = conns.connect(ctx); err != nil {
			return
		}
		if conn != nil {
			conns.Lock()
			conns.push(conn, true)
			conns.sort(true)
			break
		}
		if err = util.Hang(ctx, time.Second); err != nil {
			return
		}
	}

	cc = &ConnAndClient{conn: conns.conns[0], array: conns}
	cc.conn.streams += 1
	defer func() {
		conns.Unlock()
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

// Client gets an EventFeedV2 client.
func (c *ConnAndClient) Client() cdcpb.ChangeData_EventFeedV2Client {
	return c.client
}

// Multiplexing indicates whether the client can be used for multiplexing or not.
func (c *ConnAndClient) Multiplexing() bool {
	return c.conn.multiplexing
}

// Release releases a ConnAndClient object.
func (c *ConnAndClient) Release() {
	if c.client != nil && !c.closed.Load() {
		_ = c.client.CloseSend()
		c.closed.Store(true)
	}
	if c.conn != nil && c.array != nil {
		c.array.release(c.conn, false)
		c.conn = nil
		c.array = nil
	}
}

func (c *connArray) connect(ctx context.Context) (conn *Conn, err error) {
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
			conn = new(Conn)
			conn.ClientConn = clientConn
			conn.multiplexing = true
			err = nil
		} else if status.Code() == grpccodes.Unimplemented {
			conn = new(Conn)
			conn.ClientConn = clientConn
			conn.multiplexing = false
			err = nil
		} else {
			_ = clientConn.Close()
		}
	}
	return
}

func (c *connArray) push(conn *Conn, locked bool) {
	if !locked {
		c.Lock()
		defer c.Unlock()
	}
	c.conns = append(c.conns, conn)
	c.sort(true)
}

func (c *connArray) release(conn *Conn, locked bool) {
	if !locked {
		c.Lock()
		defer c.Unlock()
	}
	conn.streams -= 1
	if conn.streams == 0 {
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
	c.sort(true)
}

func (c *connArray) sort(locked bool) {
	if !locked {
		c.Lock()
		defer c.Unlock()
	}
	sort.Slice(c.conns, func(i, j int) bool {
		return (c.conns[i].multiplexing && !c.conns[j].multiplexing) ||
			(c.conns[i].multiplexing == c.conns[j].multiplexing && c.conns[i].streams < c.conns[j].streams)
	})
}

func (c *ConnAndClientPool) connect(ctx context.Context, target string) (*grpc.ClientConn, error) {
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

func getContextFromFeatures(ctx context.Context, features []string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			rpcMetaFeaturesKey: strings.Join(features, rpcMetaFeaturesSep),
		}),
	)
}
