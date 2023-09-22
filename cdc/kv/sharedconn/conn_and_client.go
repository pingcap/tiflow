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

type ConnAndClientPool struct {
	credential *security.Credential

	sync.Mutex
	stores map[string]*connArray
}

type ConnAndClient struct {
	conn   *Conn
	array  *connArray
	client cdcpb.ChangeData_EventFeedV2Client
}

type Conn struct {
	*grpc.ClientConn
	multiplexing bool
	streams      int
}

type connArray struct {
	addr         string
	inConnecting atomic.Bool

	sync.Mutex
	conns []*Conn
}

func NewConnAndClientPool(credential *security.Credential) *ConnAndClientPool {
	stores := make(map[string]*connArray, 64)
	return &ConnAndClientPool{credential: credential, stores: stores}
}

func (c *ConnAndClientPool) Connect(ctx context.Context, addr string) (cc *ConnAndClient, err error) {
	var conns *connArray
	c.Lock()
	if conns = c.stores[addr]; conns == nil {
		conns = &connArray{addr: addr}
	}
	c.Unlock()

	conns.Lock()
	for len(conns.conns) == 0 || conns.conns[0].streams >= grpcConnCapacity {
		conns.Unlock()
		var conn *Conn
		if conn, err = conns.connect(ctx, c.credential); conn != nil {
			conns.Lock()
			conns.push(conn, true)
			break
		}
		if err == nil {
			err = util.Hang(ctx, time.Second)
		}
		if err != nil {
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

func (c *ConnAndClient) Client() cdcpb.ChangeData_EventFeedV2Client {
	return c.client
}

func (c *ConnAndClient) Multiplexing() bool {
	return c.conn.multiplexing
}

func (c *ConnAndClient) Release() {
	if c.client != nil {
		_ = c.client.CloseSend()
		c.client = nil
	}
	if c.conn != nil && c.array != nil {
		c.array.release(c.conn, false)
		c.conn = nil
		c.array = nil
	}
}

func (c *connArray) connect(ctx context.Context, credential *security.Credential) (conn *Conn, err error) {
	if c.inConnecting.CompareAndSwap(false, true) {
		defer c.inConnecting.Store(false)
		var clientConn *grpc.ClientConn
		if clientConn, err = connect(ctx, credential, c.addr); err != nil {
			return
		}

		rpc := cdcpb.NewChangeDataClient(clientConn)
		ctx = getContextFromFeatures(ctx, []string{rpcMetaFeatureStreamMultiplexing})
		var client cdcpb.ChangeData_EventFeedV2Client
		if client, err = rpc.EventFeedV2(ctx); err == nil {
			_ = client.CloseSend()
			_, err = client.Recv()
		}
		if err == nil {
			conn = new(Conn)
			conn.ClientConn = clientConn
			conn.multiplexing = true
		} else if grpcstatus.Code(err) == grpccodes.Unimplemented {
			conn = new(Conn)
			conn.ClientConn = clientConn
			conn.multiplexing = false
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

func connect(ctx context.Context, credential *security.Credential, target string) (*grpc.ClientConn, error) {
	grpcTLSOption, err := credential.ToGRPCDialOption()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	return grpc.DialContext(
		ctx,
		target,
		grpcTLSOption,
		grpc.WithInitialWindowSize(grpcInitialWindowSize),
		grpc.WithInitialConnWindowSize(grpcInitialConnWindowSize),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(grpcMaxCallRecvMsgSize)),
		grpc.WithUnaryInterceptor(grpcMetrics.UnaryClientInterceptor()),
		grpc.WithStreamInterceptor(grpcMetrics.StreamClientInterceptor()),
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
	)
}

const (
	dialTimeout = 10 * time.Second

	grpcInitialWindowSize     = (1 << 16) - 1
	grpcInitialConnWindowSize = 1 << 23
	grpcMaxCallRecvMsgSize    = 1 << 28

	grpcConnCapacity = 1000

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

var grpcMetrics = grpc_prometheus.NewClientMetrics()

func getContextFromFeatures(ctx context.Context, features []string) context.Context {
	return metadata.NewOutgoingContext(
		ctx,
		metadata.New(map[string]string{
			rpcMetaFeaturesKey: strings.Join(features, rpcMetaFeaturesSep),
		}),
	)
}
