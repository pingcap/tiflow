package client

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/model"
	"github.com/pingcap/tiflow/dm/pkg/log"
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
