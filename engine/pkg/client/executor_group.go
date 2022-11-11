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
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/retry"
	"github.com/pingcap/tiflow/pkg/security"
	"go.uber.org/zap"
)

const (
	// getExecutorBlockTimeout indicates the maximum wait time
	// when we would like to retrieve the client for a given executor ID.
	//
	// The scenarios where we may need to wait is that
	// 1) Due to an inconsistency of the servermaster's and the executor's states,
	//    the executor has not received the information of another peer yet,
	// 2) The target executor has gone offline.
	//
	// Here we do not want to set getExecutorBlockTimeout to avoid a large
	// tail latency for scheduling a worker.
	getExecutorBlockTimeout = 1 * time.Second

	// tombstoneKeepTime indicates how long we should keep tombstone records.
	tombstoneKeepTime = 5 * time.Minute
)

// ExecutorGroup holds a group of ExecutorClients.
// It is used by any component that would like to invoke RPC
// methods on the executors.
type ExecutorGroup interface {
	// GetExecutorClient tries to get the ExecutorClient for the given executor.
	// It will return (nil, false) immediately if no such executor is found.
	GetExecutorClient(id model.ExecutorID) (ExecutorClient, bool)

	// GetExecutorClientB tries to get the ExecutorClient for the given executor.
	// It blocks until either the context has been canceled or the executor ID becomes valid.
	GetExecutorClientB(ctx context.Context, id model.ExecutorID) (ExecutorClient, error)
}

// A tombstoneEntry records when a given executor has been
// removed from an ExecutorGroup.
type tombstoneEntry struct {
	id         model.ExecutorID
	removeTime time.Time
}

// DefaultExecutorGroup is the default implementation for ExecutorGroup.
type DefaultExecutorGroup struct {
	mu      sync.RWMutex
	clients map[model.ExecutorID]ExecutorClient
	// tombstoneList stores a list of offline executors to reduce
	// the tail latency of trying to get client for a non-existent executor.
	tombstoneList *list.List // stores tombstoneEntry

	logger            *zap.Logger
	clientFactory     executorClientFactory
	tombstoneKeepTime time.Duration
}

// NewExecutorGroup creates a new ExecutorGroup.
func NewExecutorGroup(
	credentials *security.Credential,
	logger *zap.Logger,
) *DefaultExecutorGroup {
	return newExecutorGroupWithClientFactory(
		logger,
		newExecutorClientFactory(credentials, logger))
}

func newExecutorGroupWithClientFactory(
	logger *zap.Logger,
	factory executorClientFactory,
) *DefaultExecutorGroup {
	if logger == nil {
		logger = zap.L()
	}
	return &DefaultExecutorGroup{
		clients:       make(map[model.ExecutorID]ExecutorClient),
		clientFactory: factory,
		tombstoneList: list.New(),

		logger:            logger,
		tombstoneKeepTime: tombstoneKeepTime,
	}
}

// GetExecutorClient tries to get the ExecutorClient for the given executor.
func (g *DefaultExecutorGroup) GetExecutorClient(id model.ExecutorID) (ExecutorClient, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	if client, ok := g.clients[id]; ok {
		return client, true
	}
	return nil, false
}

// GetExecutorClientB tries to get the ExecutorClient for the given executor.
// It blocks until either the context has been canceled or the executor ID becomes valid.
//
// When an Executor goes offline, the ID is added to a tombstone list so that
// we can fail fast. The assumption here is that executor IDs will not be reused.
func (g *DefaultExecutorGroup) GetExecutorClientB(ctx context.Context, id model.ExecutorID) (ExecutorClient, error) {
	ctx, cancel := context.WithTimeout(ctx, getExecutorBlockTimeout)
	defer cancel()

	g.cleanTombstones()

	var ret ExecutorClient
	err := retry.Do(ctx, func() error {
		g.mu.RLock()
		defer g.mu.RUnlock()

		if client, ok := g.clients[id]; ok {
			ret = client
			return nil
		}
		for item := g.tombstoneList.Front(); item != nil; item = item.Next() {
			tombstone := item.Value.(tombstoneEntry)
			if tombstone.id == id {
				return errors.ErrTombstoneExecutor.GenWithStackByArgs(id)
			}
		}
		return errors.ErrExecutorNotFound.GenWithStackByArgs(id)
	},
		retry.WithBackoffMaxDelay(10),
		retry.WithIsRetryableErr(func(err error) bool {
			return errors.Is(err, errors.ErrExecutorNotFound)
		}))
	if err != nil {
		if errors.IsContextCanceledError(err) || errors.IsContextDeadlineExceededError(err) {
			return nil, errors.ErrExecutorNotFound.GenWithStackByArgs(id)
		}
		return nil, err
	}
	return ret, nil
}

// UpdateExecutorList updates the stored clients using a map from executor IDs
// to their addresses.
// Note: This method will not wait for the clients to be fully connected.
// In the rare case where grpc.Dial does fail, an error will be returned to the caller
// of this method, and the caller should retry appropriately.
func (g *DefaultExecutorGroup) UpdateExecutorList(executors map[model.ExecutorID]string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	// Remove outdated clients.
	for executorID, client := range g.clients {
		if _, exists := executors[executorID]; exists {
			continue
		}
		// The executor does not exist in the new list.
		client.Close()

		delete(g.clients, executorID)
		g.logger.Info("executor client removed",
			zap.String("executor-id", string(executorID)))
	}

	for executorID, addr := range executors {
		if _, exists := g.clients[executorID]; exists {
			// The executor already exists.
			continue
		}

		// NewExecutorClient should be non-blocking.
		client, err := g.clientFactory.NewExecutorClient(addr)
		if err != nil {
			g.logger.Warn("failed to create new client",
				zap.String("executor-id", string(executorID)),
				zap.String("address", addr),
				zap.Error(err))
			return err
		}

		g.clients[executorID] = client
		g.logger.Info("executor client added",
			zap.String("executor-id", string(executorID)),
			zap.String("address", addr))
	}
	return nil
}

// AddExecutor adds an executor to the executor group. A new ExecutorClient will
// be created by this method.
// Note that since we are using asynchronous Dial, this method usually does not fail
// even if a bad address is provided.
func (g *DefaultExecutorGroup) AddExecutor(executorID model.ExecutorID, addr string) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	if _, exists := g.clients[executorID]; exists {
		return errors.ErrExecutorAlreadyExists.GenWithStackByArgs(executorID)
	}

	// NewExecutorClient should be non-blocking.
	client, err := g.clientFactory.NewExecutorClient(addr)
	if err != nil {
		g.logger.Warn("failed to create new client",
			zap.String("executor-id", string(executorID)),
			zap.String("address", addr),
			zap.Error(err))
		return err
	}

	g.clients[executorID] = client
	g.logger.Info("executor client added",
		zap.String("executor-id", string(executorID)),
		zap.String("address", addr))
	return nil
}

// RemoveExecutor removes an executor from the group.
// Note that the ExecutorClient maintained will be closed.
func (g *DefaultExecutorGroup) RemoveExecutor(executorID model.ExecutorID) error {
	g.mu.Lock()
	defer g.mu.Unlock()

	client, exists := g.clients[executorID]
	if !exists {
		g.logger.Info("trying to remove non-existent executor",
			zap.String("executor-id", string(executorID)))
		return errors.ErrExecutorNotFound.GenWithStackByArgs(executorID)
	}

	client.Close()

	delete(g.clients, executorID)
	g.tombstoneList.PushFront(tombstoneEntry{
		id:         executorID,
		removeTime: time.Now(),
	})
	g.logger.Info("executor client removed",
		zap.String("executor-id", string(executorID)))

	return nil
}

func (g *DefaultExecutorGroup) cleanTombstones() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for g.tombstoneList.Len() > 0 {
		elem := g.tombstoneList.Back()
		tombstone := elem.Value.(tombstoneEntry)
		if time.Since(tombstone.removeTime) > g.tombstoneKeepTime {
			g.tombstoneList.Remove(elem)
			continue
		}
		break
	}
}
