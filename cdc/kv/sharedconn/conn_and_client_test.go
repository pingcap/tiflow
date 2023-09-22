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
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

func TestConnectToUnavailable(t *testing.T) {
	targets := []string{"127.0.0.1:9999", "9.9.9.9:9999"}
	for _, target := range targets {
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(3*time.Second))

		conn, err := connect(ctx, &security.Credential{}, target)
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
		time.Sleep(100 * time.Millisecond)
		conn, err := connect(context.Background(), &security.Credential{}, addr)
		require.NotNil(t, conn)
		require.Nil(t, err)

		rpc := cdcpb.NewChangeDataClient(conn)
		client, err := rpc.EventFeedV2(context.Background())
		require.Nil(t, err)
		_ = client.CloseSend()

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
