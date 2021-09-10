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

	"github.com/pingcap/log"
	"github.com/pingcap/ticdc/pkg/security"
	"go.uber.org/zap"
)

// MessageRouter is used to maintain clients to all the peers in the cluster
// that the local node needs to communicate with.
type MessageRouter interface {
	// AddPeer should be invoked when a new peer is discovered.
	AddPeer(id SenderID, addr string)
	// RemovePeer should be invoked when a peer is determined to
	// be permanently unavailable.
	RemovePeer(id SenderID)
	// GetClient returns a MessageClient for `target`. It returns
	// nil if the target peer does not exist. The returned client
	// is canceled if RemovePeer is called on `target`.
	GetClient(target SenderID) *MessageClient
	// Close cancels all clients maintained internally.
	Close()
	// Wait waits for all clients to exit.
	Wait()
}

// NewMessageRouter creates a new MessageRouter
func NewMessageRouter(selfID SenderID, credentials *security.Credential) MessageRouter {
	return &messageRouterImpl{
		addressMap:  make(map[SenderID]string),
		clients:     make(map[SenderID]clientWrapper),
		credentials: credentials,
		selfID:      selfID,
	}
}

type messageRouterImpl struct {
	mu         sync.RWMutex
	addressMap map[SenderID]string
	clients    map[SenderID]clientWrapper

	wg sync.WaitGroup

	// read only field
	credentials *security.Credential
	selfID      SenderID
}

type clientWrapper struct {
	*MessageClient
	cancelFn context.CancelFunc
}

// AddPeer implements MessageRouter.
func (m *messageRouterImpl) AddPeer(id SenderID, addr string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.addressMap[id] = addr
}

// RemovePeer implements MessageRouter.
func (m *messageRouterImpl) RemovePeer(id SenderID) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.addressMap, id)
	if clientWrapper, ok := m.clients[id]; ok {
		clientWrapper.cancelFn()
	}
}

// GetClient implements MessageRouter. The client will be created lazily.
func (m *messageRouterImpl) GetClient(target SenderID) *MessageClient {
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
		log.Info("failed to create client, no peer", zap.String("target", string(target)))
		// there is no address for this target. We are not able to create a client.
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	client := NewMessageClient(m.selfID)
	cliWrapper := clientWrapper{
		MessageClient: client,
		cancelFn:      cancel,
	}

	m.clients[target] = cliWrapper

	// TODO add metrics for active client count
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer cancel()
		err := client.Run(ctx, "tcp", addr, target, m.credentials)
		log.Warn("p2p client exited with error",
			zap.String("addr", addr),
			zap.String("target-capture", string(target)),
			zap.Error(err))

		m.mu.Lock()
		defer m.mu.Unlock()
		delete(m.clients, target)
	}()
	return client
}

func (m *messageRouterImpl) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, cliWrapper := range m.clients {
		cliWrapper.cancelFn()
	}
}

func (m *messageRouterImpl) Wait() {
	m.wg.Wait()
}
