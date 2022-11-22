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

package example

import (
	"context"
	"sync"

	"github.com/pingcap/log"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"go.uber.org/zap"
)

const (
	exampleWorkerType = 999
	exampleWorkerCfg  = "config"
)

var _ framework.Master = &exampleMaster{}

type exampleMaster struct {
	*framework.DefaultBaseMaster

	worker struct {
		mu sync.Mutex

		id          frameModel.WorkerID
		handle      framework.WorkerHandle
		online      bool
		statusCode  frameModel.WorkerState
		receivedErr error
	}

	tickCount int
}

func (e *exampleMaster) InitImpl(ctx context.Context) (err error) {
	log.Info("InitImpl")
	e.worker.mu.Lock()
	e.worker.id, err = e.CreateWorker(exampleWorkerType, exampleWorkerCfg)
	e.worker.mu.Unlock()
	return
}

func (e *exampleMaster) Tick(ctx context.Context) error {
	log.Info("Tick")
	e.tickCount++

	e.worker.mu.Lock()
	defer e.worker.mu.Unlock()
	handle := e.worker.handle
	if handle == nil {
		return nil
	}
	e.worker.statusCode = handle.Status().State
	log.Info("status", zap.Any("status", handle.Status()))
	return nil
}

func (e *exampleMaster) OnMasterRecovered(ctx context.Context) error {
	log.Info("OnMasterRecovered")
	return nil
}

func (e *exampleMaster) OnWorkerDispatched(worker framework.WorkerHandle, result error) error {
	log.Info("OnWorkerDispatched")
	e.worker.mu.Lock()
	e.worker.handle = worker
	e.worker.receivedErr = result
	e.worker.mu.Unlock()
	return nil
}

func (e *exampleMaster) OnWorkerOnline(worker framework.WorkerHandle) error {
	log.Info("OnWorkerOnline")
	e.worker.mu.Lock()
	e.worker.handle = worker
	e.worker.online = true
	e.worker.mu.Unlock()
	return nil
}

func (e *exampleMaster) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	log.Info("OnWorkerOffline")
	return nil
}

func (e *exampleMaster) OnWorkerMessage(worker framework.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.Info("OnWorkerMessage")
	return nil
}

func (e *exampleMaster) CloseImpl(ctx context.Context) {
	log.Info("CloseImpl")
}

func (e *exampleMaster) StopImpl(ctx context.Context) {
	log.Info("StopImpl")
}

func (e *exampleMaster) OnWorkerStatusUpdated(worker framework.WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	log.Info("OnWorkerStatusUpdated")
	return nil
}
