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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pingcap/failpoint"
	"github.com/pingcap/tiflow/pkg/security"
	"github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/require"
	"github.com/tikv/pd/pkg/utils/tempurl"
	"google.golang.org/grpc"
)

type messageRouterTestSuite struct {
	servers       map[NodeID]*MessageServer
	cancels       map[NodeID]context.CancelFunc
	messageRouter MessageRouter
	wg            sync.WaitGroup
}

// read only
var clientConfig4TestingMessageRouter = &MessageClientConfig{
	SendChannelSize:         128,
	BatchSendInterval:       time.Millisecond * 200,
	MaxBatchCount:           128,
	MaxBatchBytes:           8192,
	RetryRateLimitPerSecond: 10.0, // using 10.0 instead of 1.0 to accelerate testing
	DialTimeout:             time.Second * 3,
	MaxRecvMsgSize:          4 * 1024 * 1024, // 4MB
}

func newMessageRouterTestSuite() *messageRouterTestSuite {
	return &messageRouterTestSuite{
		servers: map[NodeID]*MessageServer{},
		cancels: map[NodeID]context.CancelFunc{},
		messageRouter: NewMessageRouter(
			"test-client-1",
			&security.Credential{},
			clientConfig4TestingMessageRouter),
	}
}

func (s *messageRouterTestSuite) getServer(id NodeID) *MessageServer {
	return s.servers[id]
}

func (s *messageRouterTestSuite) addServer(ctx context.Context, t *testing.T, id NodeID) {
	addr := strings.TrimPrefix(tempurl.Alloc(), "http://")
	lis, err := net.Listen("tcp", addr)
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	newServer := NewMessageServer(id, defaultServerConfig4Testing)
	p2p.RegisterCDCPeerToPeerServer(grpcServer, newServer)

	ctx, cancel := context.WithCancel(ctx)
	s.cancels[id] = cancel
	s.servers[id] = newServer

	s.messageRouter.AddPeer(id, addr)

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		_ = grpcServer.Serve(lis)
	}()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer grpcServer.Stop()
		defer s.messageRouter.RemovePeer(id)
		err := newServer.Run(ctx)
		if err != nil {
			require.Regexp(t, ".*context canceled.*", err.Error())
		}
	}()
}

func (s *messageRouterTestSuite) close() {
	for _, cancel := range s.cancels {
		cancel()
	}

	s.messageRouter.Close()
}

func (s *messageRouterTestSuite) wait() {
	s.wg.Wait()
	s.messageRouter.Wait()
}

func TestMessageRouterBasic(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	suite := newMessageRouterTestSuite()
	suite.addServer(ctx, t, "server-1")
	suite.addServer(ctx, t, "server-2")
	suite.addServer(ctx, t, "server-3")

	noClient := suite.messageRouter.GetClient("server-4")
	require.Nilf(t, noClient, "no client should have been created")

	var lastIndex [3]int64
	mustAddHandler(ctx, t, suite.getServer("server-1"), "test-topic", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index, lastIndex[0]+1)
		lastIndex[0] = content.Index
		return nil
	})

	mustAddHandler(ctx, t, suite.getServer("server-2"), "test-topic", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index, lastIndex[1]+1)
		lastIndex[1] = content.Index
		return nil
	})

	mustAddHandler(ctx, t, suite.getServer("server-3"), "test-topic", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index, lastIndex[2]+1)
		lastIndex[2] = content.Index
		return nil
	})

	var lastSeq [3]Seq
	for i := 0; i < defaultMessageBatchSizeLarge; i++ {
		serverIdx := i % 3
		serverID := fmt.Sprintf("server-%d", serverIdx+1)
		Seq, err := suite.messageRouter.GetClient(serverID).SendMessage(ctx, "test-topic", &testTopicContent{int64(i/3) + 1})
		require.NoError(t, err)
		lastSeq[serverIdx] = Seq
	}

	require.Eventually(t, func() bool {
		seq, ok := suite.messageRouter.GetClient("server-1").CurrentAck("test-topic")
		if !ok {
			return false
		}
		return seq >= lastSeq[0]
	}, time.Second*10, time.Millisecond*20)

	require.Eventually(t, func() bool {
		seq, ok := suite.messageRouter.GetClient("server-2").CurrentAck("test-topic")
		if !ok {
			return false
		}
		return seq >= lastSeq[1]
	}, time.Second*10, time.Millisecond*20)

	require.Eventually(t, func() bool {
		seq, ok := suite.messageRouter.GetClient("server-3").CurrentAck("test-topic")
		if !ok {
			return false
		}
		return seq >= lastSeq[2]
	}, time.Second*10, time.Millisecond*20)

	suite.close()
	suite.close() // double close: should not panic
	suite.wait()
	suite.close() // triple close: should not panic
}

func TestMessageRouterRemovePeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	suite := newMessageRouterTestSuite()
	suite.addServer(ctx, t, "server-1")
	suite.addServer(ctx, t, "server-2")

	var lastIndex [3]int64
	mustAddHandler(ctx, t, suite.getServer("server-1"), "test-topic", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index, lastIndex[0]+1)
		lastIndex[0] = content.Index
		return nil
	})

	mustAddHandler(ctx, t, suite.getServer("server-2"), "test-topic", &testTopicContent{}, func(senderID string, i interface{}) error {
		require.Equal(t, "test-client-1", senderID)
		require.IsType(t, &testTopicContent{}, i)
		content := i.(*testTopicContent)
		require.Equal(t, content.Index, lastIndex[1]+1)
		lastIndex[1] = content.Index
		return nil
	})

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		var lastSeq Seq
		for i := 0; i < defaultMessageBatchSizeLarge; i++ {
			var err error
			lastSeq, err = suite.messageRouter.GetClient("server-1").
				SendMessage(ctx, "test-topic", &testTopicContent{int64(i + 1)})
			require.NoError(t, err)
		}
		require.Eventually(t, func() bool {
			seq, ok := suite.messageRouter.GetClient("server-1").CurrentAck("test-topic")
			if !ok {
				return false
			}
			return seq >= lastSeq
		}, time.Second*10, time.Millisecond*20)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		client := suite.messageRouter.GetClient("server-2")
		require.NotNil(t, client)
		for i := 0; i < defaultMessageBatchSizeSmall; i++ {
			var err error
			_, err = client.SendMessage(ctx, "test-topic", &testTopicContent{int64(i + 1)})
			require.NoError(t, err)
		}
		suite.messageRouter.RemovePeer("server-2")

		var err error
		require.Eventually(t, func() bool {
			_, err = client.SendMessage(ctx, "test-topic", &testTopicContent{0})
			return err != nil
		}, time.Millisecond*500, time.Millisecond*50)
		require.Regexp(t, ".*ErrPeerMessageClientClosed.*", err.Error())
	}()

	wg.Wait()
	suite.close()
	suite.wait()
}

func TestMessageRouterClientFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.TODO(), defaultTimeout)
	defer cancel()

	suite := newMessageRouterTestSuite()
	suite.addServer(ctx, t, "server-1")

	// FIXME remove the failpoint test, and use a mock client instead
	// But we should make MessageClient an interface first.
	err := failpoint.Enable("github.com/pingcap/tiflow/pkg/p2p/InjectClientPermanentFailure", "return(true)")
	require.NoError(t, err)
	defer func() {
		_ = failpoint.Disable("github.com/pingcap/tiflow/pkg/p2p/InjectClientPermanentFailure")
	}()

	client := suite.messageRouter.GetClient("server-1")
	require.NotNil(t, client)

	select {
	case <-ctx.Done():
		require.Fail(t, "TestMessageRouterClientFailure timed out")
	case err := <-suite.messageRouter.Err():
		require.NotNil(t, err)
		require.Regexp(t, ".*ErrPeerMessageClientPermanentFail.*", err.Error())
	}

	suite.close()
	suite.wait()
}
