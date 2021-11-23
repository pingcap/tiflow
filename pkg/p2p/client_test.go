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
	"github.com/pingcap/ticdc/pkg/security"
	"github.com/pingcap/ticdc/proto/p2p"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockClientBatchSender struct {
	mock.Mock
}

func (s *mockClientBatchSender) Append(msg messageEntry) error {
	args := s.Called(msg)
	return args.Error(0)
}

func (s *mockClientBatchSender) Flush() error {
	args := s.Called()
	return args.Error(0)
}

type mockClientConnector struct {
	mock.Mock
}

func (c *mockClientConnector) Connect(opts clientConnectOptions) (p2p.CDCPeerToPeerClient, cancelFn, error) {
	args := c.Called(opts)
	return args.Get(0).(p2p.CDCPeerToPeerClient), args.Get(1).(cancelFn), args.Error(2)
}

type mockCDCPeerToPeerClient struct {
	mock.Mock
}

func (c *mockCDCPeerToPeerClient) SendMessage(
	ctx context.Context, opts ...grpc.CallOption,
) (p2p.CDCPeerToPeer_SendMessageClient, error) {
	args := c.Called(ctx, opts)
	return args.Get(0).(p2p.CDCPeerToPeer_SendMessageClient), args.Error(1)
}

type mockSendMessageClient struct {
	mock.Mock
	// embeds an empty interface
	p2p.CDCPeerToPeer_SendMessageClient
	ctx context.Context
}

func (s *mockSendMessageClient) Send(packet *p2p.MessagePacket) error {
	args := s.Called(packet)
	return args.Error(0)
}

func (s *mockSendMessageClient) Recv() (*p2p.SendMessageResponse, error) {
	args := s.Called()
	return args.Get(0).(*p2p.SendMessageResponse), args.Error(1)
}

func (s *mockSendMessageClient) Context() context.Context {
	return s.ctx
}

var clientConfigForUnitTesting = &MessageClientConfig{
	SendChannelSize:         0, // unbuffered channel to make tests more reliable
	BatchSendInterval:       time.Second,
	MaxBatchBytes:           math.MaxInt64,
	MaxBatchCount:           math.MaxInt64,
	RetryRateLimitPerSecond: 999.0,
}

func TestMessageClientBasics(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	client := NewMessageClient("node-1", clientConfigForUnitTesting)
	sender := &mockClientBatchSender{}
	client.newSenderFn = func(stream clientStream) clientBatchSender {
		return sender
	}
	connector := &mockClientConnector{}
	grpcClient := &mockCDCPeerToPeerClient{}
	client.connector = connector
	var closed int32
	connector.On("Connect", mock.Anything).Return(
		grpcClient,
        func() {
			require.Equal(t, 0, atomic.LoadInt32(&closed))
			atomic.StoreInt32(&closed, 1)
		},
        nil,
    )

	grpcStream := &mockSendMessageClient{
		ctx: context.Background(),
	}
	grpcClient.On("SendMessage", mock.Anything).Return(
		grpcStream,
        nil,
    )
	grpcStream.On("Send", mock.Anything).Return(nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := client.Run(ctx, "", "", "node-2", &security.Credential{})
		require.Error(t, err)
		require.Regexp(t, "context canceled", err.Error())
	}()

	
}
