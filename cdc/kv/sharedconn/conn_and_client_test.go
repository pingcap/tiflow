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
	"net"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/cdcpb"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
	grpcstatus "google.golang.org/grpc/status"
)

func TestConnAndClientPool(t *testing.T) {
	service := make(chan *grpc.Server, 1)
	var addr string

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Nil(t, runGrpcService(&srv{}, &addr, service))
	}()

	svc := <-service
	require.NotNil(t, svc)
	defer svc.GracefulStop()

	pool := newConnAndClientPool(&security.Credential{}, nil, 2)
	cc1, err := pool.Connect(context.Background(), addr)
	require.Nil(t, err)
	require.NotNil(t, cc1)
	require.Equal(t, 1, len(cc1.array.conns))
	require.Equal(t, 1, cc1.conn.streams)
	require.False(t, cc1.Multiplexing())

	cc2, err := pool.Connect(context.Background(), addr)
	require.Nil(t, err)
	require.NotNil(t, cc2)
	require.Equal(t, 1, len(cc2.array.conns))
	require.Equal(t, 2, cc2.conn.streams)
	require.False(t, cc2.Multiplexing())

	cc3, err := pool.Connect(context.Background(), addr)
	require.Nil(t, err)
	require.NotNil(t, cc3)
	require.Equal(t, 2, len(cc3.array.conns))
	require.Equal(t, 1, cc3.conn.streams)
	require.False(t, cc3.Multiplexing())

	cc1.Release()
	cc1.Release()
	cc2.Release()
	require.Equal(t, 1, len(cc3.array.conns))
	require.Equal(t, 1, cc3.conn.streams)

	cc3.Release()
	require.Equal(t, 0, len(pool.stores))
}

func TestConnAndClientPoolForV2(t *testing.T) {
	service := make(chan *grpc.Server, 1)
	var addr string

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Nil(t, runGrpcService(&srv{v2: true}, &addr, service))
	}()

	svc := <-service
	require.NotNil(t, svc)
	defer svc.GracefulStop()

	pool := newConnAndClientPool(&security.Credential{}, nil, 2)
	cc1, err := pool.Connect(context.Background(), addr)
	require.Nil(t, err)
	require.NotNil(t, cc1)
	require.True(t, cc1.Multiplexing())

	cc1.Release()
	require.Equal(t, 0, len(pool.stores))
}

func TestConnectToUnavailable(t *testing.T) {
	pool := newConnAndClientPool(&security.Credential{}, nil, 1)

	targets := []string{"127.0.0.1:9999", "2.2.2.2:9999"}
	for _, target := range targets {
		ctx := context.Background()
		conn, err := pool.connect(ctx, target)
		require.NotNil(t, conn)
		require.Nil(t, err)

		rpc := cdcpb.NewChangeDataClient(conn)
		_, err = rpc.EventFeedV2(ctx)
		require.NotNil(t, err)

		require.Nil(t, conn.Close())
	}

	service := make(chan *grpc.Server, 1)
	var addr string

	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Nil(t, runGrpcService(&srv{}, &addr, service))
	}()

	svc := <-service
	require.NotNil(t, svc)
	defer svc.GracefulStop()

	conn, err := pool.connect(context.Background(), addr)
	require.NotNil(t, conn)
	require.Nil(t, err)

	rpc := cdcpb.NewChangeDataClient(conn)
	client, err := rpc.EventFeedV2(context.Background())
	require.Nil(t, err)
	_ = client.CloseSend()

	_, err = client.Recv()
	require.Equal(t, codes.Unimplemented, status.Code(err))

	require.Nil(t, conn.Close())
}

func TestCancelStream(t *testing.T) {
	service := make(chan *grpc.Server, 1)
	var addr string
	var wg sync.WaitGroup
	defer wg.Wait()
	wg.Add(1)
	go func() {
		defer wg.Done()
		require.Nil(t, runGrpcService(&srv{}, &addr, service))
	}()

	svc := <-service
	require.NotNil(t, svc)
	defer svc.GracefulStop()

	connCtx, connCancel := context.WithCancel(context.Background())
	defer connCancel()

	pool := newConnAndClientPool(&security.Credential{}, nil, 1)
	conn, err := pool.connect(connCtx, addr)
	require.NotNil(t, conn)
	require.Nil(t, err)

	rpcCtx, rpcCancel := context.WithCancel(context.Background())
	rpc := cdcpb.NewChangeDataClient(conn)
	client, err := rpc.EventFeed(rpcCtx)
	require.Nil(t, err)

	rpcCancel()
	_, err = client.Recv()
	require.Equal(t, grpccodes.Canceled, grpcstatus.Code(err))
	require.Nil(t, conn.Close())
}

func runGrpcService(srv cdcpb.ChangeDataServer, addr *string, service chan<- *grpc.Server) error {
	defer close(service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	defer lis.Close()

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
	cdcpb.RegisterChangeDataServer(grpcServer, srv)
	*addr = lis.Addr().String()
	service <- grpcServer
	return grpcServer.Serve(lis)
}

type srv struct {
	v2 bool
}

func (s *srv) EventFeed(server cdcpb.ChangeData_EventFeedServer) error {
	for {
		if _, err := server.Recv(); err != nil {
			return err
		}
	}
}

func (s *srv) EventFeedV2(server cdcpb.ChangeData_EventFeedV2Server) error {
	if !s.v2 {
		return grpcstatus.Error(grpccodes.Unimplemented, "srv")
	}
	for {
		if _, err := server.Recv(); err != nil {
			return err
		}
	}
}
