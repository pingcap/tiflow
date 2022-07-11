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

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/atomic"
	"go.uber.org/zap"
)

// MessageRouter is used to maintain clients to all the peers in the cluster
// that the local node needs to communicate with.
type MessageRouter interface {
	// AddPeer should be invoked when a new peer is discovered.
	AddPeer(id NodeID, addr string)
	// RemovePeer should be invoked when a peer is determined to
	// be permanently unavailable.
	RemovePeer(id NodeID)
	// GetClient returns a MessageClient for `target`. It returns
	// nil if the target peer does not exist. The returned client
	// is canceled if RemovePeer is called on `target`.
	GetClient(target NodeID) *MessageClient
	// Close cancels all clients maintained internally.
	Close()
	// Wait waits for all clients to exit.
	Wait()
	// Err returns a channel to receive errors from.
	Err() <-chan error
}

type messageRouterImpl struct {
	mu         sync.RWMutex
	addressMap map[NodeID]string
	clients    map[NodeID]clientWrapper

	wg       sync.WaitGroup
	isClosed atomic.Bool
	errCh    chan error

	// read only field
	credentials  *security.Credential
	selfID       NodeID
	clientConfig *MessageClientConfig
}

// NewMessageRouter creates a new MessageRouter
func NewMessageRouter(selfID NodeID, credentials *security.Credential, clientConfig *MessageClientConfig) MessageRouter {
	return &messageRouterImpl{
		addressMap:   make(map[NodeID]string),
		clients:      make(map[NodeID]clientWrapper),
		errCh:        make(chan error, 1), // one error at most
		credentials:  credentials,
		selfID:       selfID,
		clientConfig: clientConfig,
	}
}

type clientWrapper struct {
	*MessageClient
	cancelFn context.CancelFunc
}

// AddPeer implements MessageRouter.
func (m *messageRouterImpl) AddPeer(id NodeID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addressMap[id] = addr
}

// RemovePeer implements MessageRouter.
func (m *messageRouterImpl) RemovePeer(id NodeID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.addressMap, id)
	// The client is removed from m.clients only after it is successfully
	// canceled, to prevent duplicate clients to the same target.
	if clientWrapper, ok := m.clients[id]; ok {
		clientWrapper.cancelFn()
	}
}

// GetClient implements MessageRouter. The client will be created lazily.
// It returns nil if the target peer does not exist.
func (m *messageRouterImpl) GetClient(target NodeID) *MessageClient {
	m.mu.RLock()
	// fast path
	if cliWrapper, ok := m.clients[target]; ok {
		m.mu.RUnlock()
		return cliWrapper.MessageClient
	}

	// There is no ready-to-use client for target
	m.mu.RUnlock()
	// escalate the lock
	m.mu.Lock()
	defer m.mu.Unlock()

	// repeats the logic in fast path after escalating the lock, since
	// the lock was briefly released.
	if cliWrapper, ok := m.clients[target]; ok {
		return cliWrapper.MessageClient
	}

	addr, ok := m.addressMap[target]
	if !ok {
		log.Warn("failed to create client, no peer",
			zap.String("target", target),
			zap.StackSkip("stack", 1))
		// There is no address for this target. We are not able to create a client.
		// The client is expected to retry if the target peer is added later.
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := NewMessageClient(m.selfID, m.clientConfig)
	cliWrapper := clientWrapper{
		MessageClient: client,
		cancelFn:      cancel,
	}

	m.clients[target] = cliWrapper
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer cancel()
		err := client.Run(ctx, "tcp", addr, target, m.credentials)
		log.Warn("p2p client exited with error",
			zap.String("addr", addr),
			zap.String("targetCapture", target),
			zap.Error(err))

		if errors.Cause(err) != context.Canceled {
			// Send the error to the error channel.
			select {
			case m.errCh <- err:
			default:
				// We allow an error to be lost in case the channel is full.
			}
		}

		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.clients, target)
	}()
	return client
}

func (m *messageRouterImpl) Close() {
	if m.isClosed.Swap(true) {
		// the messageRouter is already closed
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cliWrapper := range m.clients {
		cliWrapper.cancelFn()
	}
}

func (m *messageRouterImpl) Wait() {
	m.wg.Wait()
}

func (m *messageRouterImpl) Err() <-chan error {
	return m.errCh
}
