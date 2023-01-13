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
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type mockGrpcService struct {
	mock.Mock
	t           *testing.T
	streamCount int64
}

func (s *mockGrpcService) SendMessage(stream MessageServerStream) error {
	atomic.AddInt64(&s.streamCount, 1)
	defer atomic.AddInt64(&s.streamCount, -1)

	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				log.Info("error received", zap.Error(err))
				return
			}
			s.Mock.MethodCalled("OnNewMessage", msg)
		}
	}()

	<-stream.Context().Done()
	return status.Error(codes.Canceled, stream.Context().Err().Error())
}

func newServerWrapperForTesting(t *testing.T) (server *ServerWrapper, newClient func() (p2p.CDCPeerToPeerClient, func()), cancel func()) {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	cfg := &MessageServerConfig{
		MaxRecvMsgSize: 4 * 1024 * 1024, // 4MB
	}
	server = NewServerWrapper(cfg)
	p2p.RegisterCDCPeerToPeerServer(grpcServer, server)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	cancel = func() {
		grpcServer.Stop()
		wg.Wait()
	}

	newClient = func() (p2p.CDCPeerToPeerClient, func()) {
		conn, err := grpc.Dial(
			addr,
			grpc.WithInsecure(),
			grpc.WithContextDialer(func(_ context.Context, s string) (net.Conn, error) {
				return net.Dial("tcp", addr)
			}))
		require.NoError(t, err)

		cancel2 := func() {
			_ = conn.Close()
		}
		return p2p.NewCDCPeerToPeerClient(conn), cancel2
	}
	return
}

func TestServerWrapperBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	serverWrapper, newClient, cancelServer := newServerWrapperForTesting(t)
	defer cancelServer()

	client, closeClient := newClient()
	defer closeClient()

	// initiates a stream to an empty server
	clientStream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	_, err = clientStream.Recv()
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Unavailable, st.Code())

	innerServer := &mockGrpcService{t: t}

	serverWrapper.Reset(innerServer)

	clientStream, err = client.SendMessage(ctx)
	require.NoError(t, err)

	innerServer.On("OnNewMessage", &p2p.MessagePacket{})
	err = clientStream.Send(&p2p.MessagePacket{})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	innerServer.AssertExpectations(t)

	require.Equal(t, int64(1), atomic.LoadInt64(&innerServer.streamCount))

	serverWrapper.Reset(nil)
	_, err = clientStream.Recv()
	require.Error(t, err)

	st, ok = status.FromError(err)
	require.True(t, ok)
	require.Equal(t, codes.Canceled, st.Code())
}

func TestServerWrapperDelayed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	serverWrapper, newClient, cancelServer := newServerWrapperForTesting(t)
	defer cancelServer()

	client, closeClient := newClient()
	defer closeClient()

	err := failpoint.Enable("github.com/pingcap/tiflow/pkg/p2p/ServerWrapperSendMessageDelay", "pause")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/ServerWrapperSendMessageDelay")
	}()

	innerServer := &mockGrpcService{t: t}

	serverWrapper.Reset(innerServer)
	innerServer.On("OnNewMessage", &p2p.MessagePacket{})

	// initiates a stream to an empty server
	clientStream, err := client.SendMessage(ctx)
	require.NoError(t, err)

	err = clientStream.Send(&p2p.MessagePacket{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		serverWrapper.wrappedStreamsMu.Lock()
		defer serverWrapper.wrappedStreamsMu.Unlock()
		return len(serverWrapper.wrappedStreams) > 0
	}, time.Second*1, time.Millisecond*20)

	serverWrapper.Reset(nil)

	_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/ServerWrapperSendMessageDelay")

	// It is enough for this test case to finish without panicking.
}
