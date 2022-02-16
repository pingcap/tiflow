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
	"testing"

	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
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
