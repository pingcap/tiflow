// Copyright 2021 PingCAP, Inc.
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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// Use clientSuite for some special reasons, the embed etcd uses zap as the only candidate
// logger and in the logger initialization it also initializes the grpclog/loggerv2, which
// is not a thread-safe operation and it must be called before any gRPC functions
// ref: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L67-L72
func TestConnArray(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer pool.Close()
	addr := "127.0.0.1:20161"
	conn, err := pool.GetConn(addr)
	require.Nil(t, err)
	require.Equal(t, int64(1), conn.active)
	pool.ReleaseConn(conn, addr)
	require.Equal(t, int64(0), conn.active)

	lastConn := conn
	// First grpcConnCapacity*2 connections will use initial two connections.
	for i := 0; i < grpcConnCapacity*2; i++ {
		conn, err := pool.GetConn(addr)
		require.Nil(t, err)
		require.NotSame(t, conn.ClientConn, lastConn.ClientConn)
		require.Equal(t, int64(i)/2+1, conn.active)
		lastConn = conn
	}
	// The following grpcConnCapacity*2 connections will trigger resize of connection array.
	for i := 0; i < grpcConnCapacity*2; i++ {
		conn, err := pool.GetConn(addr)
		require.Nil(t, err)
		require.NotSame(t, conn.ClientConn, lastConn.ClientConn)
		require.Equal(t, int64(i)/2+1, conn.active)
		lastConn = conn
	}
}

func TestConnArrayRecycle(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool := NewGrpcPoolImpl(ctx, &security.Credential{})
	defer pool.Close()
	addr := "127.0.0.1:20161"

	bucket := 6
	// sharedConns will store SharedConn with the same index according to connArray bucket.
	sharedConns := make([]*sharedConn, bucket)
	// get conn for 6000 times, and grpc pool will create 6 buckets
	for i := 0; i < grpcConnCapacity*bucket; i++ {
		conn, err := pool.GetConn(addr)
		require.Nil(t, err)
		if i%(grpcConnCapacity*resizeBucketStep) == 0 {
			sharedConns[i/grpcConnCapacity] = conn
		}
		if i%(grpcConnCapacity*resizeBucketStep) == 1 {
			sharedConns[i/grpcConnCapacity+1] = conn
		}
	}
	for i := 2; i < bucket; i++ {
		require.Equal(t, int64(grpcConnCapacity), sharedConns[i].active)
		for j := 0; j < grpcConnCapacity; j++ {
			pool.ReleaseConn(sharedConns[i], addr)
		}
	}
	empty := pool.bucketConns[addr].recycle()
	require.False(t, empty)
	require.Len(t, pool.bucketConns[addr].conns, 2)

	for i := 0; i < 2; i++ {
		require.Equal(t, int64(grpcConnCapacity), sharedConns[i].active)
		for j := 0; j < grpcConnCapacity; j++ {
			pool.ReleaseConn(sharedConns[i], addr)
		}
	}
	empty = pool.bucketConns[addr].recycle()
	require.True(t, empty)
	require.Len(t, pool.bucketConns[addr].conns, 0)
}

func TestConnectToUnavailable(t *testing.T) {
	targets := []string{"127.0.0.1:9999", "9.9.9.9:9999"}
	for _, target := range targets {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(3*time.Second))

		conn, err := createClientConn(ctx, &security.Credential{}, target)
		require.NotNil(t, conn)
		require.Nil(t, err)

		rpc := cdcpb.NewChangeDataClient(conn)
		_, err = rpc.EventFeedV2(ctx)
		require.NotNil(t, err)
		cancel()
	}

	service := make(chan *grpc.Server, 1)
	var wg sync.WaitGroup
	var addr string
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Nil(t, runGrpcService(&addr, service))
	}()

	if svc := <-service; svc != nil {
		time.Sleep(time.Second)
		conn, err := createClientConn(context.Background(), &security.Credential{}, addr)
		require.NotNil(t, conn)
		require.Nil(t, err)

		rpc := cdcpb.NewChangeDataClient(conn)
		client, err := rpc.EventFeedV2(context.Background())
		require.Nil(t, err)

		_, err = client.Header()
		require.Nil(t, err)

		err = client.Send(&cdcpb.ChangeDataRequest{
			RequestId: 0,
			RegionId:  0,
			Request:   &cdcpb.ChangeDataRequest_Deregister_{},
		})
		require.Equal(t, "EOF", err.Error())

		_, err = client.Recv()
		require.Equal(t, codes.Unimplemented, status.Code(err))

		svc.Stop()
	}
	wg.Wait()
}

func runGrpcService(addr *string, service chan<- *grpc.Server) error {
	defer close(service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}

	kaep := keepalive.EnforcementPolicy{
		MinTime:             3 * time.Second,
		PermitWithoutStream: true,
	}
	kasp := keepalive.ServerParameters{
		MaxConnectionIdle:     10 * time.Second,
		MaxConnectionAge:      10 * time.Second,
		MaxConnectionAgeGrace: 5 * time.Second,
		Time:                  3 * time.Second,
		Timeout:               1 * time.Second,
	}
	grpcServer := grpc.NewServer(grpc.KeepaliveEnforcementPolicy(kaep), grpc.KeepaliveParams(kasp))
	service <- grpcServer
	*addr = lis.Addr().String()
	return grpcServer.Serve(lis)
}
