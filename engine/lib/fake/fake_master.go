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

package fake

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
	"golang.org/x/time/rate"

	"github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

/*
Fake job has two business logic in each fake worker

- By default the worker will increase a counter by one in each tick. In the task
  config it receives a target tick count, if the counter reaches target, the task
  will stop.
- Besides if `EtcdWatchEnable` is true in task config, for each worker will watch
  the key change of `WatchPrefix+WorkerIndex`, record the changed count of this
  key and latest value of this key.

- WorkerIndex: fake job always spawns `WorkerCount` workers, each worker has a
  unique index from 0 to `WorkerCount-1`
*/

// Config represents the job config of fake master
type Config struct {
	JobName     string `json:"job-name"`
	WorkerCount int    `json:"worker-count"`
	TargetTick  int    `json:"target-tick"`

	EtcdWatchEnable bool     `json:"etcd-watch-enable"`
	EtcdEndpoints   []string `json:"etcd-endpoints"`
	EtcdWatchPrefix string   `json:"etcd-watch-prefix"`

	InjectErrorInterval time.Duration `json:"inject-error-interval"`
}

// Checkpoint defines the checkpoint of fake job
type Checkpoint struct {
	Ticks       map[int]int64            `json:"ticks"`
	Checkpoints map[int]workerCheckpoint `json:"checkpoints"`
}

// String implements fmt.Stringer
func (cp *Checkpoint) String() string {
	data, err := json.Marshal(cp)
	if err != nil {
		log.L().Warn("checkpoint marshal failed", zap.Error(err))
	}
	return string(data)
}

// workerCheckpoint is used to resume a new worker from old checkpoint
type workerCheckpoint struct {
	Tick      int64  `json:"tick"`
	Revision  int64  `json:"revision"`
	MvccCount int    `json:"mvcc-count"`
	Value     string `json:"value"`
}

func zeroWorkerCheckpoint() workerCheckpoint {
	return workerCheckpoint{}
}

var _ lib.BaseJobMaster = (*Master)(nil)

// Master defines the job master implementation of fake job.
type Master struct {
	lib.BaseJobMaster

	config  *Config
	bStatus *businessStatus

	// workerID stores the ID of the Master AS A WORKER.
	workerID libModel.WorkerID

	workerListMu        sync.Mutex
	workerList          []lib.WorkerHandle
	workerID2BusinessID map[libModel.WorkerID]int
	pendingWorkerSet    map[libModel.WorkerID]int
	finishedSet         map[libModel.WorkerID]int

	// worker status
	statusRateLimiter *rate.Limiter
	statusCode        struct {
		sync.RWMutex
		code libModel.WorkerStatusCode
	}

	ctx         context.Context
	clocker     clock.Clock
	initialized bool
}

type businessStatus struct {
	sync.RWMutex
	status map[libModel.WorkerID]*dummyWorkerStatus
}

// OnJobManagerMessage implements JobMasterImpl.OnJobManagerMessage
func (m *Master) OnJobManagerMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("FakeMaster: OnJobManagerMessage", zap.Any("message", message))
	switch msg := message.(type) {
	case *libModel.StatusChangeRequest:
		switch msg.ExpectState {
		case libModel.WorkerStatusStopped:
			m.setStatusCode(libModel.WorkerStatusStopped)
			m.workerListMu.Lock()
			for _, worker := range m.workerList {
				if worker == nil {
					continue
				}
				wTopic := libModel.WorkerStatusChangeRequestTopic(m.BaseJobMaster.ID(), worker.ID())
				wMessage := &libModel.StatusChangeRequest{
					SendTime:     m.clocker.Mono(),
					FromMasterID: m.BaseJobMaster.ID(),
					Epoch:        m.BaseJobMaster.CurrentEpoch(),
					ExpectState:  libModel.WorkerStatusStopped,
				}
				ctx, cancel := context.WithTimeout(m.ctx, time.Second*2)
				runningHandle := worker.Unwrap()
				if runningHandle == nil {
					cancel()
					continue
				}
				if err := runningHandle.SendMessage(ctx, wTopic, wMessage, false /*nonblocking*/); err != nil {
					cancel()
					m.workerListMu.Unlock()
					return err
				}
				cancel()
			}
			m.workerListMu.Unlock()
		default:
			log.L().Info("FakeMaster: ignore status change state", zap.Int32("state", int32(msg.ExpectState)))
		}
	default:
		log.L().Info("unsupported message", zap.Any("message", message))
	}
	return nil
}

// IsJobMasterImpl implements JobMasterImpl.IsJobMasterImpl
func (m *Master) IsJobMasterImpl() {
	panic("unreachable")
}

// ID implements BaseJobMaster.ID
func (m *Master) ID() worker.RunnableID {
	return m.workerID
}

// Workload implements BaseJobMaster.Workload
func (m *Master) Workload() model.RescUnit {
	return 0
}

// InitImpl implements BaseJobMaster.InitImpl
func (m *Master) InitImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Init", zap.Any("config", m.config))
	return m.initWorkers()
}

// This function is not thread safe, it must be called with m.workerListMu locked
func (m *Master) createWorker(wcfg *WorkerConfig) error {
	workerID, err := m.CreateWorker(lib.FakeTask, wcfg, 1)
	if err != nil {
		return errors.Trace(err)
	}
	log.L().Info("CreateWorker called",
		zap.Int("BusinessID", wcfg.ID),
		zap.String("worker-id", workerID))
	m.pendingWorkerSet[workerID] = wcfg.ID
	m.workerID2BusinessID[workerID] = wcfg.ID
	return nil
}

func (m *Master) initWorkers() error {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
OUT:
	for i, handle := range m.workerList {
		if handle == nil {
			for _, idx := range m.pendingWorkerSet {
				if idx == i {
					continue OUT
				}
			}
			wcfg := m.genWorkerConfig(i, zeroWorkerCheckpoint())
			err := m.createWorker(wcfg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Master) tickedCheckWorkers(ctx context.Context) error {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()

	// handle failover if needed
	if !m.initialized {
		if !m.IsMasterReady() {
			if m.statusRateLimiter.Allow() {
				log.L().Info("master is not ready, wait")
			}
			return nil
		}
		m.initialized = true
		for _, worker := range m.GetWorkers() {
			if worker.GetTombstone() != nil {
				continue
			}
			var businessID int
			if _, ok := m.workerID2BusinessID[worker.ID()]; ok {
				businessID = m.workerID2BusinessID[worker.ID()]
			} else {
				// worker status could have not been updated
				if worker.Status().ExtBytes == nil {
					continue
				}
				ws, err := parseExtBytes(worker.Status().ExtBytes)
				if err != nil {
					return errors.Trace(err)
				}
				businessID = ws.BusinessID
			}
			// found active worker after fake_master failover
			if m.workerList[businessID] == nil {
				m.workerList[businessID] = worker
			}
		}
		// load checkpoint if it exists
		ckpt := &Checkpoint{}
		resp, metaErr := m.MetaKVClient().Get(ctx, CheckpointKey(m.workerID))
		if metaErr != nil {
			log.L().Warn("failed to load checkpoint", zap.Error(metaErr))
		} else {
			if len(resp.Kvs) > 0 {
				if err := json.Unmarshal(resp.Kvs[0].Value, ckpt); err != nil {
					return errors.Trace(err)
				}
			}
		}
		for i, worker := range m.workerList {
			// create new worker for non-active worker
			if worker == nil {
				workerCkpt := zeroWorkerCheckpoint()
				if tick, ok := ckpt.Ticks[i]; ok {
					workerCkpt.Tick = tick
				}
				if etcdCkpt, ok := ckpt.Checkpoints[i]; ok {
					workerCkpt.Revision = etcdCkpt.Revision
					workerCkpt.MvccCount = etcdCkpt.MvccCount
				}
				wcfg := m.genWorkerConfig(i, workerCkpt)
				if err := m.createWorker(wcfg); err != nil {
					return errors.Trace(err)
				}
			}
		}
	}

	// load worker status from Status API
	for _, worker := range m.workerList {
		if worker != nil {
			status := worker.Status()
			dws := &dummyWorkerStatus{}
			if status.ExtBytes != nil {
				var err error
				dws, err = parseExtBytes(status.ExtBytes)
				if err != nil {
					return err
				}
			}
			m.bStatus.Lock()
			m.bStatus.status[worker.ID()] = dws
			m.bStatus.Unlock()
		}
	}

	return nil
}

func (m *Master) tickedCheckStatus(ctx context.Context) error {
	if m.statusRateLimiter.Allow() {
		m.bStatus.RLock()
		log.L().Info("FakeMaster: Tick", zap.Any("status", m.bStatus.status))
		m.bStatus.RUnlock()
		// save checkpoint, which is used in business only
		_, metaErr := m.MetaKVClient().Put(ctx, CheckpointKey(m.workerID), m.genCheckpoint().String())
		if metaErr != nil {
			log.L().Warn("update checkpoint with error", zap.Error(metaErr))
		}
		// update status via framework provided API
		err := m.BaseJobMaster.UpdateJobStatus(ctx, m.Status())
		if derrors.ErrWorkerUpdateStatusTryAgain.Equal(err) {
			log.L().Warn("update status try again later", zap.String("error", err.Error()))
			return nil
		}
		return err
	}

	// check for special worker status
	if m.getStatusCode() == libModel.WorkerStatusStopped {
		log.L().Info("FakeMaster: received pause command, stop now")
		m.setStatusCode(libModel.WorkerStatusStopped)
		return m.Exit(ctx, m.Status(), nil)
	}
	if len(m.finishedSet) == m.config.WorkerCount {
		log.L().Info("FakeMaster: all worker finished, job master exits now")
		m.setStatusCode(libModel.WorkerStatusFinished)
		return m.Exit(ctx, m.Status(), nil)
	}

	return nil
}

// Tick implements MasterImpl.Tick
func (m *Master) Tick(ctx context.Context) error {
	if err := m.tickedCheckWorkers(ctx); err != nil {
		return err
	}
	return m.tickedCheckStatus(ctx)
}

// OnMasterRecovered implements MasterImpl.OnMasterRecovered
func (m *Master) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("FakeMaster: OnMasterRecovered")
	return nil
}

// OnWorkerDispatched implements MasterImpl.OnWorkerDispatched
func (m *Master) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	if result != nil {
		log.L().Error("FakeMaster: OnWorkerDispatched", zap.Error(result))
		return errors.Trace(result)
	}

	return nil
}

// OnWorkerOnline implements MasterImpl.OnWorkerOnline
func (m *Master) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("FakeMaster: OnWorkerOnline",
		zap.String("worker-id", worker.ID()))

	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()

	idx, ok := m.pendingWorkerSet[worker.ID()]
	if !ok {
		log.L().Panic("OnWorkerOnline is called with an unknown workerID",
			zap.String("worker-id", worker.ID()))
	}
	delete(m.pendingWorkerSet, worker.ID())
	m.workerList[idx] = worker

	return nil
}

// OnWorkerOffline implements MasterImpl.OnWorkerOffline
func (m *Master) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	index := -1
	m.workerListMu.Lock()
	for i, handle := range m.workerList {
		if handle == nil {
			continue
		}
		if handle.ID() == worker.ID() {
			index = i
			m.workerList[i] = nil
			break
		}
	}
	m.workerListMu.Unlock()
	if index < 0 {
		return errors.Errorf("worker(%s) is not found in worker list", worker.ID())
	}

	m.bStatus.Lock()
	delete(m.bStatus.status, worker.ID())
	m.bStatus.Unlock()

	if derrors.ErrWorkerFinish.Equal(reason) {
		log.L().Info("FakeMaster: OnWorkerOffline: worker finished", zap.String("worker-id", worker.ID()))
		m.finishedSet[worker.ID()] = index
		return nil
	}

	log.L().Info("FakeMaster: OnWorkerOffline",
		zap.String("worker-id", worker.ID()), zap.Error(reason))
	workerCkpt := zeroWorkerCheckpoint()
	if ws, err := parseExtBytes(worker.Status().ExtBytes); err != nil {
		log.L().Warn("failed to parse worker ext bytes", zap.Error(err))
	} else {
		workerCkpt.Tick = ws.Tick
		if ws.Checkpoint != nil {
			workerCkpt.Revision = ws.Checkpoint.Revision
			workerCkpt.MvccCount = ws.Checkpoint.MvccCount
		}
	}
	wcfg := m.genWorkerConfig(index, workerCkpt)
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
	return m.createWorker(wcfg)
}

// OnWorkerMessage implements MasterImpl.OnWorkerMessage
func (m *Master) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("FakeMaster: OnWorkerMessage",
		zap.String("topic", topic),
		zap.Any("message", message))
	return nil
}

// OnWorkerStatusUpdated implements MasterImpl.OnWorkerStatusUpdated
func (m *Master) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
	log.L().Info("FakeMaster: worker status updated",
		zap.String("worker-id", worker.ID()),
		zap.Any("worker-status", newStatus))
	return nil
}

// CloseImpl implements MasterImpl.CloseImpl
func (m *Master) CloseImpl(ctx context.Context) error {
	log.L().Info("FakeMaster: Close", zap.Stack("stack"))
	return nil
}

// OnMasterMessage implements MasterImpl.OnMasterMessage
func (m *Master) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	log.L().Info("FakeMaster: OnMasterMessage", zap.Any("message", message))
	return nil
}

func (m *Master) marshalBusinessStatus() []byte {
	m.bStatus.RLock()
	defer m.bStatus.RUnlock()
	bytes, err := json.Marshal(m.bStatus.status)
	if err != nil {
		log.L().Panic("unexpected marshal error", zap.Error(err))
	}
	return bytes
}

// Status implements
func (m *Master) Status() libModel.WorkerStatus {
	extBytes := m.marshalBusinessStatus()
	return libModel.WorkerStatus{
		Code:     m.getStatusCode(),
		ExtBytes: extBytes,
	}
}

func (m *Master) setStatusCode(code libModel.WorkerStatusCode) {
	m.statusCode.Lock()
	defer m.statusCode.Unlock()
	m.statusCode.code = code
}

func (m *Master) getStatusCode() libModel.WorkerStatusCode {
	m.statusCode.RLock()
	defer m.statusCode.RUnlock()
	return m.statusCode.code
}

func parseExtBytes(data []byte) (*dummyWorkerStatus, error) {
	dws := &dummyWorkerStatus{}
	err := json.Unmarshal(data, dws)
	return dws, err
}

// CheckpointKey returns key path used in etcd for checkpoint
func CheckpointKey(id libModel.MasterID) string {
	return strings.Join([]string{"fake-master", "checkpoint", id}, "/")
}

func (m *Master) genCheckpoint() *Checkpoint {
	m.workerListMu.Lock()
	defer m.workerListMu.Unlock()
	cp := &Checkpoint{
		Ticks:       make(map[int]int64),
		Checkpoints: make(map[int]workerCheckpoint),
	}
	m.bStatus.RLock()
	defer m.bStatus.RUnlock()
	for wid, status := range m.bStatus.status {
		if businessID, ok := m.workerID2BusinessID[wid]; ok {
			cp.Ticks[businessID] = status.Tick
			if status.Checkpoint != nil {
				cp.Checkpoints[businessID] = *status.Checkpoint
			} else {
				cp.Checkpoints[businessID] = workerCheckpoint{}
			}
		}
	}
	return cp
}

func (m *Master) genWorkerConfig(index int, checkpoint workerCheckpoint) *WorkerConfig {
	return &WorkerConfig{
		ID: index,

		// generated from fake master config
		TargetTick:      int64(m.config.TargetTick),
		EtcdWatchEnable: m.config.EtcdWatchEnable,
		EtcdEndpoints:   m.config.EtcdEndpoints,
		EtcdWatchPrefix: m.config.EtcdWatchPrefix,

		// loaded from checkpoint if exists
		Checkpoint:          checkpoint,
		InjectErrorInterval: m.config.InjectErrorInterval,
	}
}

// NewFakeMaster creates a new fake master instance
func NewFakeMaster(ctx *dcontext.Context, workerID libModel.WorkerID, masterID libModel.MasterID, config lib.WorkerConfig) *Master {
	log.L().Info("new fake master", zap.Any("config", config))
	masterConfig := config.(*Config)
	ret := &Master{
		workerID:            workerID,
		pendingWorkerSet:    make(map[libModel.WorkerID]int),
		workerList:          make([]lib.WorkerHandle, masterConfig.WorkerCount),
		workerID2BusinessID: make(map[libModel.WorkerID]int),
		config:              masterConfig,
		statusRateLimiter:   rate.NewLimiter(rate.Every(100*time.Millisecond), 1),
		bStatus:             &businessStatus{status: make(map[libModel.WorkerID]*dummyWorkerStatus)},
		finishedSet:         make(map[libModel.WorkerID]int),
		ctx:                 ctx.Context,
		clocker:             clock.New(),
		initialized:         false,
	}
	ret.setStatusCode(libModel.WorkerStatusNormal)
	return ret
}
