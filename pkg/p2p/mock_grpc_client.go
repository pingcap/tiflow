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
	"sync"
	"sync/atomic"

	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
)

type mockSendMessageClient struct {
	mu sync.Mutex
	mock.Mock
	// embeds an empty interface
	p2p.CDCPeerToPeer_SendMessageClient
	ctx context.Context

	msgCount int32
	replyCh  chan *p2p.SendMessageResponse
}

func newMockSendMessageClient(ctx context.Context) *mockSendMessageClient {
	return &mockSendMessageClient{
		ctx:     ctx,
		replyCh: make(chan *p2p.SendMessageResponse), // unbuffered channel
	}
}

func (s *mockSendMessageClient) Send(packet *p2p.MessagePacket) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	args := s.Called(packet)
	atomic.AddInt32(&s.msgCount, 1)
	return args.Error(0)
}

func (s *mockSendMessageClient) Recv() (*p2p.SendMessageResponse, error) {
	var args mock.Arguments
	func() {
		// We use a deferred Unlock in case `s.Called()` panics.
		s.mu.Lock()
		defer s.mu.Unlock()

		args = s.MethodCalled("Recv")
	}()

	if err := args.Error(1); err != nil {
		return nil, err
	}
	if args.Get(0) != nil {
		return args.Get(0).(*p2p.SendMessageResponse), nil
	}
	select {
	case <-s.ctx.Done():
		return nil, s.ctx.Err()
	case resp := <-s.replyCh:
		return resp, nil
	}
}

func (s *mockSendMessageClient) Context() context.Context {
	return s.ctx
}

func (s *mockSendMessageClient) ResetMock() {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.ExpectedCalls = nil
	s.Calls = nil
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
