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

package p2p

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type mockService struct {
	callCount int32
}

func (s *mockService) SendMessage(server p2p.CDCPeerToPeer_SendMessageServer) error {
	atomic.AddInt32(&s.callCount, 1)
	return nil
}

func TestClientConnector(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	mockService := &mockService{}
	grpcServer := grpc.NewServer()

	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	p2p.RegisterCDCPeerToPeerServer(grpcServer, mockService)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	cc := newClientConnector()

	client, release, err := cc.Connect(clientConnectOptions{
		network:    "tcp",
		addr:       addr,
		credential: &security.Credential{},
	})
	require.NoError(t, err)
	defer release()

	_, err = client.SendMessage(ctx)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&mockService.callCount) == 1
	}, time.Second*5, time.Millisecond*100)

	cancel()
	grpcServer.Stop()
	wg.Wait()
}
