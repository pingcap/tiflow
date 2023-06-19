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
	"testing"
	"time"

	"github.com/phayes/freeport"
	"github.com/pingcap/tiflow/pkg/security"
	proto "github.com/pingcap/tiflow/proto/p2p"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

// MockCluster mocks the whole peer-messaging cluster.
type MockCluster struct {
	Nodes map[NodeID]*MockNode
}

// MockNode represents one mock node.
type MockNode struct {
	Addr string
	ID   NodeID

	Server *MessageServer
	Router MessageRouter

	cancel func()
	wg     sync.WaitGroup
}

// read only
var serverConfig4MockCluster = &MessageServerConfig{
	MaxPendingMessageCountPerTopic:       256,
	MaxPendingTaskCount:                  102400,
	SendChannelSize:                      1,
	AckInterval:                          time.Millisecond * 100,
	WorkerPoolSize:                       4,
	MaxPeerCount:                         1024,
	WaitUnregisterHandleTimeoutThreshold: time.Millisecond * 100,
}

// read only
var clientConfig4MockCluster = &MessageClientConfig{
	SendChannelSize:         1,
	BatchSendInterval:       time.Millisecond * 100,
	MaxBatchCount:           128,
	MaxBatchBytes:           8192,
	RetryRateLimitPerSecond: 10.0, // using 10.0 instead of 1.0 to accelerate testing
	DialTimeout:             time.Second * 3,
	MaxRecvMsgSize:          4 * 1024 * 1024, // 4MB
}

func newMockNode(t *testing.T, id NodeID) *MockNode {
	port := freeport.GetPort()
	addr := fmt.Sprintf("127.0.0.1:%d", port)

	ctx, cancel := context.WithCancel(context.Background())

	ret := &MockNode{
		Addr:   addr,
		ID:     id,
		Server: NewMessageServer(id, serverConfig4MockCluster),
		// Note that TLS is disabled.
		Router: NewMessageRouter(id, &security.Credential{}, clientConfig4MockCluster),
		cancel: cancel,
	}

	grpcServer := grpc.NewServer()
	proto.RegisterCDCPeerToPeerServer(grpcServer, ret.Server)

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		lis, err := net.Listen("tcp", addr)
		require.NoError(t, err)
		_ = grpcServer.Serve(lis)
	}()

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		err := ret.Server.Run(ctx)
		require.Error(t, err)
		require.Regexp(t, ".*context canceled.*", err.Error())
	}()

	ret.wg.Add(1)
	go func() {
		defer ret.wg.Done()
		<-ctx.Done()
		grpcServer.Stop()
	}()

	return ret
}

// Close closes the mock node.
func (n *MockNode) Close() {
	n.Router.Close()
	n.cancel()
	n.wg.Wait()
}

// NewMockCluster creates a mock cluster.
func NewMockCluster(t *testing.T, nodeCount int) *MockCluster {
	ret := &MockCluster{
		Nodes: make(map[NodeID]*MockNode),
	}

	for i := 0; i < nodeCount; i++ {
		id := fmt.Sprintf("capture-%d", i)
		ret.Nodes[id] = newMockNode(t, id)
	}

	for _, sourceNode := range ret.Nodes {
		for _, targetNode := range ret.Nodes {
			sourceNode.Router.AddPeer(targetNode.ID, targetNode.Addr)
		}
	}

	return ret
}

// Close closes the mock cluster.
func (c *MockCluster) Close() {
	for _, node := range c.Nodes {
		node.Close()
	}
}
