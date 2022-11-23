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

package master

import (
	"context"

	"github.com/pingcap/log"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"go.uber.org/zap"
)

// BaseHandle provides some common api of a worker, no matter it is running or dead.
type BaseHandle interface {
	Status() *frameModel.WorkerStatus
	ID() frameModel.WorkerID
}

// WorkerHandle defines the interface of a worker, businiss logic can use this
// handler to get RunningHandle or TombstoneHandle
type WorkerHandle interface {
	BaseHandle

	GetTombstone() TombstoneHandle
	Unwrap() RunningHandle
}

// RunningHandle represents a running worker
type RunningHandle interface {
	BaseHandle

	SendMessage(
		ctx context.Context,
		topic p2p.Topic,
		message interface{},
		nonblocking bool,
	) error
}

// TombstoneHandle represents a dead worker.
type TombstoneHandle interface {
	BaseHandle

	// CleanTombstone cleans the metadata from the metastore,
	// and cleans the state managed by the framework.
	// Do not call any other methods on this handle after
	// CleanTombstone is called.
	CleanTombstone(ctx context.Context) error
}

type runningHandleImpl struct {
	workerID   frameModel.WorkerID
	executorID model.ExecutorID
	manager    *WorkerManager
}

func (h *runningHandleImpl) Status() *frameModel.WorkerStatus {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.Status()
}

func (h *runningHandleImpl) ID() frameModel.WorkerID {
	return h.workerID
}

func (h *runningHandleImpl) GetTombstone() TombstoneHandle {
	return nil
}

func (h *runningHandleImpl) Unwrap() RunningHandle {
	return h
}

func (h *runningHandleImpl) SendMessage(
	ctx context.Context,
	topic p2p.Topic,
	message interface{},
	nonblocking bool,
) error {
	var err error
	if nonblocking {
		_, err = h.manager.messageSender.SendToNode(ctx, p2p.NodeID(h.executorID), topic, message)
	} else {
		err = h.manager.messageSender.SendToNodeB(ctx, p2p.NodeID(h.executorID), topic, message)
	}

	return err
}

type tombstoneHandleImpl struct {
	workerID frameModel.WorkerID
	manager  *WorkerManager
}

func (h *tombstoneHandleImpl) Status() *frameModel.WorkerStatus {
	h.manager.mu.Lock()
	defer h.manager.mu.Unlock()

	entry, exists := h.manager.workerEntries[h.workerID]
	if !exists {
		log.Panic("Using a stale handle", zap.String("worker-id", h.workerID))
	}

	return entry.Status()
}

func (h *tombstoneHandleImpl) ID() frameModel.WorkerID {
	return h.workerID
}

func (h *tombstoneHandleImpl) GetTombstone() TombstoneHandle {
	return h
}

func (h *tombstoneHandleImpl) Unwrap() RunningHandle {
	return nil
}

func (h *tombstoneHandleImpl) CleanTombstone(ctx context.Context) error {
	ok, err := h.manager.workerMetaClient.Remove(ctx, h.workerID)
	if err != nil {
		return err
	}
	if !ok {
		log.Info("Tombstone already cleaned", zap.String("worker-id", h.workerID))
		// Idempotent for robustness.
		return nil
	}
	log.Info("Worker tombstone is cleaned", zap.String("worker-id", h.workerID))
	h.manager.removeTombstoneEntry(h.workerID)

	return nil
}
