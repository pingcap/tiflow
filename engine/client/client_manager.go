// Copyright 2022 PingCAP, Inc.
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

package client

import (
	"context"
	"sync"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/model"
	"go.uber.org/zap"
)

// ClientsManager defines interface to manage all clients, including master client
// and executor clients.
type ClientsManager interface {
	MasterClient() MasterClient
	ExecutorClient(id model.ExecutorID) ExecutorClient
	AddExecutor(id model.ExecutorID, addr string) error
}

func NewClientManager() *Manager {
	return &Manager{
		executors: make(map[model.ExecutorID]ExecutorClient),
	}
}

// TODO: We need to consider when to remove executor client and how to process transilient error.
type Manager struct {
	mu sync.RWMutex

	master    *MasterClientImpl
	executors map[model.ExecutorID]ExecutorClient
}

func (c *Manager) MasterClient() MasterClient {
	return c.master
}

func (c *Manager) ExecutorClient(id model.ExecutorID) ExecutorClient {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.executors[id]
}

// TODO Right now the interface and params are not consistent. We should abstract a "grpc pool"
// interface to maintain a pool of grpc connections.
func (c *Manager) AddMasterClient(ctx context.Context, addrs []string) error {
	if c.master != nil {
		return nil
	}
	var err error
	c.master, err = NewMasterClient(ctx, addrs)
	return err
}

func (c *Manager) AddExecutor(id model.ExecutorID, addr string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.executors[id]; ok {
		return nil
	}
	log.L().Info("client manager adds executor", zap.String("id", string(id)), zap.String("addr", addr))
	client, err := newExecutorClient(addr)
	if err != nil {
		return err
	}
	c.executors[id] = client
	return nil
}

func (c *Manager) AddExecutorClient(id model.ExecutorID, client ExecutorClient) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.executors[id] = client
	return nil
}
