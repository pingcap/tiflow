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

package cvs

import (
	"context"
	"encoding/json"
	"sync"
	"time"
	"unsafe"

	"github.com/gin-gonic/gin"
	"github.com/pingcap/log"
	cvsTask "github.com/pingcap/tiflow/engine/executor/cvs"
	"github.com/pingcap/tiflow/engine/executor/worker"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/engine/framework/registry"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/atomic"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// Config records all configurations of cvs job
type Config struct {
	SrcHost string `toml:"srcHost" json:"srcHost"`
	SrcDir  string `toml:"srcDir" json:"srcDir"`
	DstHost string `toml:"dstHost" json:"dstHost"`
	DstDir  string `toml:"dstDir" json:"dstDir"`
	FileNum int    `toml:"fileNum" json:"fileNum"`
}

// SyncFileInfo records sync file progress
type SyncFileInfo struct {
	Idx      int    `json:"idx"`
	Location string `json:"loc"`
}

// Status records worker status of cvs job master
type Status struct {
	*Config `json:"cfg"`

	FileInfos map[int]*SyncFileInfo `json:"files"`
}

// WorkerInfo holds handler of worker
type WorkerInfo struct {
	handle     atomic.UnsafePointer // a handler to get information
	needCreate atomic.Bool
}

// JobMaster defines cvs job master
type JobMaster struct {
	sync.Mutex

	framework.BaseJobMaster
	jobStatus         *Status
	syncFilesInfo     map[int]*WorkerInfo
	counter           int64
	workerID          frameModel.WorkerID
	statusRateLimiter *rate.Limiter

	launchedWorkers sync.Map
	statusCode      struct {
		sync.RWMutex
		code frameModel.WorkerState
	}
	ctx     context.Context
	clocker clock.Clock
}

var _ framework.JobMasterImpl = (*JobMaster)(nil)

// RegisterWorker is used to register cvs job master into global registry
func RegisterWorker() {
	factory := registry.NewSimpleWorkerFactory(NewCVSJobMaster)
	registry.GlobalWorkerRegistry().MustRegisterWorkerType(frameModel.CvsJobMaster, factory)
}

// NewCVSJobMaster creates a new cvs job master
func NewCVSJobMaster(ctx *dcontext.Context, workerID frameModel.WorkerID, masterID frameModel.MasterID, conf *Config) *JobMaster {
	jm := &JobMaster{}
	jm.workerID = workerID
	jm.jobStatus = &Status{
		FileInfos: make(map[int]*SyncFileInfo),
	}
	jm.jobStatus.Config = conf
	jm.syncFilesInfo = make(map[int]*WorkerInfo)
	jm.statusRateLimiter = rate.NewLimiter(rate.Every(time.Second*2), 1)
	jm.ctx = ctx.Context
	jm.clocker = clock.New()
	log.Info("new cvs jobmaster ", zap.Any("id :", jm.workerID))
	return jm
}

// InitImpl implements JobMasterImpl.InitImpl
func (jm *JobMaster) InitImpl(ctx context.Context) (err error) {
	log.Info("initializing the cvs jobmaster  ", zap.Any("id :", jm.workerID))
	jm.setState(frameModel.WorkerStateInit)
	filesNum := jm.jobStatus.Config.FileNum
	if filesNum == 0 {
		return errors.New("no file found under the folder")
	}
	log.Info("cvs jobmaster list file success", zap.Any("id", jm.workerID), zap.Any("file number", filesNum))
	// todo: store the jobmaster information into the metastore
	for idx := 0; idx < filesNum; idx++ {
		jm.jobStatus.FileInfos[idx] = &SyncFileInfo{Idx: idx}
		jm.syncFilesInfo[idx] = &WorkerInfo{
			needCreate: *atomic.NewBool(true),
			handle:     *atomic.NewUnsafePointer(unsafe.Pointer(nil)),
		}
	}

	// Then persist the checkpoint for recovery
	// This persistence has to succeed before we set this master to normal status.
	statusBytes, err := json.Marshal(jm.jobStatus)
	if err != nil {
		return err
	}
	_, err = jm.MetaKVClient().Put(ctx, jm.workerID, string(statusBytes))
	if err != nil {
		return err
	}
	jm.setState(frameModel.WorkerStateNormal)
	return nil
}

// Tick implements JobMasterImpl.Tick
func (jm *JobMaster) Tick(ctx context.Context) error {
	jm.counter = 0
	if !jm.IsMasterReady() {
		if jm.statusRateLimiter.Allow() {
			log.Info("jobmaster is not ready", zap.Any("master id", jm.workerID))
		}
		return nil
	}

	jm.Lock()
	defer jm.Unlock()
	if 0 == len(jm.jobStatus.FileInfos) {
		jm.setState(frameModel.WorkerStateFinished)
		log.Info("cvs job master finished")
		status := jm.Status()
		return jm.BaseJobMaster.Exit(ctx, framework.ExitReasonFinished, nil, status.ExtBytes)
	}
	for idx, workerInfo := range jm.syncFilesInfo {
		// check if need to recreate worker
		if workerInfo.needCreate.Load() {
			workerID, err := jm.CreateWorker(frameModel.CvsTask,
				getTaskConfig(jm.jobStatus, idx))
			if err != nil {
				log.Warn("create worker failed, try next time", zap.Any("master id", jm.workerID), zap.Error(err))
			} else {
				jm.launchedWorkers.Store(workerID, idx)
				workerInfo.needCreate.Store(false)
			}
			continue
		}

		// still awaiting online
		if workerInfo.handle.Load() == nil {
			continue
		}
		// update job status
		handle := *(*framework.WorkerHandle)(workerInfo.handle.Load())
		status := handle.Status()
		switch status.State {
		case frameModel.WorkerStateNormal, frameModel.WorkerStateFinished, frameModel.WorkerStateStopped:
			taskStatus := &cvsTask.Status{}
			err := json.Unmarshal(status.ExtBytes, taskStatus)
			if err != nil {
				return err
			}

			jm.jobStatus.FileInfos[idx].Location = taskStatus.CurrentLoc
			jm.counter += taskStatus.Count

			log.Debug("cvs job tmp num ", zap.Any("id", idx), zap.Any("status", string(status.ExtBytes)))
		case frameModel.WorkerStateError:
			log.Error("sync file failed ", zap.Any("idx", idx))
		default:
			log.Info("worker status abnormal", zap.Any("status", status))
		}
	}
	if jm.statusRateLimiter.Allow() {
		statsBytes, err := json.Marshal(jm.jobStatus)
		if err != nil {
			log.Warn("serialize job status failed, try next time", zap.Any("master id", jm.workerID), zap.Error(err))
			return err
		}
		_, err = jm.MetaKVClient().Put(ctx, jm.workerID, string(statsBytes))
		if err != nil {
			log.Warn("update job status failed, try next time", zap.Any("master id", jm.workerID), zap.Error(err))
		}
		log.Info("cvs job master status", zap.Any("id", jm.workerID), zap.Int64("counter", jm.counter), zap.Any("status", jm.getState()))
	}
	if jm.getState() == frameModel.WorkerStateStopped {
		log.Info("cvs job master stopped")
		status := jm.Status()
		return jm.BaseJobMaster.Exit(ctx, framework.ExitReasonCanceled, nil, status.ExtBytes)
	}
	return nil
}

// OnMasterRecovered implements JobMasterImpl.OnMasterRecovered
func (jm *JobMaster) OnMasterRecovered(ctx context.Context) (err error) {
	log.Info("recovering job master", zap.Any("id", jm.ID()))
	// load self status
	resp, err := jm.MetaKVClient().Get(ctx, jm.workerID)
	if err != nil {
		log.Warn("load status failed", zap.Any("master id", jm.ID), zap.Error(err))
		return err
	}
	if len(resp.Kvs) != 1 {
		log.Error("jobmaster meta unexpected result", zap.Any("master id", jm.ID()), zap.Any("meta counts", len(resp.Kvs)))
	}
	statusBytes := resp.Kvs[0].Value
	log.Info("jobmaster recover from meta", zap.Any("master id", jm.ID()), zap.String("status", string(statusBytes)))
	err = json.Unmarshal(statusBytes, jm.jobStatus)
	if err != nil {
		return err
	}
	for id := range jm.jobStatus.FileInfos {
		info := &WorkerInfo{}
		info.needCreate.Store(true)
		jm.syncFilesInfo[id] = info
	}
	return nil
}

// OnWorkerDispatched implements JobMasterImpl.OnWorkerDispatched
func (jm *JobMaster) OnWorkerDispatched(worker framework.WorkerHandle, err error) error {
	if err == nil {
		return nil
	}
	val, exist := jm.launchedWorkers.Load(worker.ID())
	log.Warn("Worker Dispatched Fail", zap.Any("master id", jm.ID()), zap.Any("worker id", err), zap.Error(err))
	if !exist {
		log.Panic("failed worker not found", zap.Any("worker", worker.ID()))
	}
	jm.launchedWorkers.Delete(worker.ID())
	id := val.(int)
	jm.Lock()
	defer jm.Unlock()
	jm.syncFilesInfo[id].needCreate.Store(true)
	jm.syncFilesInfo[id].handle.Store(nil)
	return nil
}

// OnWorkerOnline implements JobMasterImpl.OnWorkerOnline
func (jm *JobMaster) OnWorkerOnline(worker framework.WorkerHandle) error {
	id, exist := jm.launchedWorkers.Load(worker.ID())
	if !exist {
		log.Info("job master recovering and get new worker", zap.Any("id", worker.ID()), zap.Any("master id", jm.ID()))
		if jm.IsMasterReady() {
			log.Panic("job master has ready and a new worker has been created, brain split occurs!")
		}
		statusBytes := worker.Status().ExtBytes
		status := cvsTask.Status{}
		err := json.Unmarshal(statusBytes, &status)
		if err != nil {
			// bad json
			return err
		}
		id = status.TaskConfig.Idx
	} else {
		log.Info("worker online ", zap.Any("id", worker.ID()), zap.Any("master id", jm.ID()))
	}
	jm.Lock()
	defer jm.Unlock()
	jm.syncFilesInfo[id.(int)].handle.Store(unsafe.Pointer(&worker))
	jm.syncFilesInfo[id.(int)].needCreate.Store(false)
	jm.launchedWorkers.Store(worker.ID(), id.(int))
	return nil
}

func getTaskConfig(jobStatus *Status, id int) *cvsTask.Config {
	return &cvsTask.Config{
		SrcHost:  jobStatus.SrcHost,
		DstHost:  jobStatus.DstHost,
		DstDir:   jobStatus.DstDir,
		StartLoc: jobStatus.FileInfos[id].Location,
		Idx:      id,
	}
}

// OnWorkerOffline implements JobMasterImpl.OnWorkerOffline
// When offline, we should:
// 1. remove this file from map cache
// 2. update checkpoint, but note that this operation might fail.
func (jm *JobMaster) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	val, exist := jm.launchedWorkers.Load(worker.ID())
	log.Info("on worker offline ", zap.Any("worker", worker.ID()))
	if !exist {
		log.Panic("offline worker not found", zap.Any("worker", worker.ID()))
	}
	jm.launchedWorkers.Delete(worker.ID())
	id := val.(int)
	jm.Lock()
	defer jm.Unlock()
	if errors.Is(reason, errors.ErrWorkerFinish) {
		delete(jm.syncFilesInfo, id)
		delete(jm.jobStatus.FileInfos, id)
		log.Info("worker finished", zap.String("worker-id", worker.ID()), zap.Any("status", worker.Status()), zap.Error(reason))
		return nil
	}
	jm.syncFilesInfo[id].needCreate.Store(true)
	jm.syncFilesInfo[id].handle.Store(nil)
	return nil
}

// OnWorkerStatusUpdated implements JobMasterImpl.OnWorkerStatusUpdated
func (jm *JobMaster) OnWorkerStatusUpdated(worker framework.WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	return nil
}

// OnWorkerMessage implements JobMasterImpl.OnWorkerMessage
func (jm *JobMaster) OnWorkerMessage(worker framework.WorkerHandle, topic p2p.Topic, message p2p.MessageValue) error {
	return nil
}

// CloseImpl is called when the master is being closed
func (jm *JobMaster) CloseImpl(ctx context.Context) {}

// StopImpl is called when the master is being canceled
func (jm *JobMaster) StopImpl(ctx context.Context) {}

// ID implements JobMasterImpl.ID
func (jm *JobMaster) ID() worker.RunnableID {
	return jm.workerID
}

// OnMasterMessage implements JobMasterImpl.OnMasterMessage
func (jm *JobMaster) OnMasterMessage(ctx context.Context, topic p2p.Topic, message p2p.MessageValue) error {
	return nil
}

// OnCancel implements JobMasterImpl.OnCancel
func (jm *JobMaster) OnCancel(ctx context.Context) error {
	log.Info("cvs jobmaster: OnCancel")
	return jm.cancelWorkers()
}

func (jm *JobMaster) cancelWorkers() error {
	jm.setState(frameModel.WorkerStateStopped)
	for _, worker := range jm.syncFilesInfo {
		if worker.handle.Load() == nil {
			continue
		}
		handle := *(*framework.WorkerHandle)(worker.handle.Load())
		workerID := handle.ID()
		wTopic := frameModel.WorkerStatusChangeRequestTopic(jm.BaseJobMaster.ID(), handle.ID())
		wMessage := &frameModel.StatusChangeRequest{
			SendTime:     jm.clocker.Mono(),
			FromMasterID: jm.BaseJobMaster.ID(),
			Epoch:        jm.BaseJobMaster.CurrentEpoch(),
			ExpectState:  frameModel.WorkerStateStopped,
		}

		if handle := handle.Unwrap(); handle != nil {
			ctx, cancel := context.WithTimeout(jm.ctx, time.Second*2)
			if err := handle.SendMessage(ctx, wTopic, wMessage, false /*nonblocking*/); err != nil {
				cancel()
				return err
			}
			log.Info("sent message to worker", zap.String("topic", wTopic), zap.Any("message", wMessage))
			cancel()
		} else {
			log.Info("skip sending message to tombstone worker", zap.String("worker-id", workerID))
		}
	}
	return nil
}

// OnOpenAPIInitialized implements JobMasterImpl.OnOpenAPIInitialized.
func (jm *JobMaster) OnOpenAPIInitialized(apiGroup *gin.RouterGroup) {}

// Status implements JobMasterImpl.Status
func (jm *JobMaster) Status() frameModel.WorkerStatus {
	status, err := json.Marshal(jm.jobStatus)
	if err != nil {
		log.Panic("get status failed", zap.String("id", jm.workerID), zap.Error(err))
	}
	return frameModel.WorkerStatus{
		State:    jm.getState(),
		ExtBytes: status,
	}
}

// IsJobMasterImpl implements JobMasterImpl.IsJobMasterImpl
func (jm *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

func (jm *JobMaster) setState(code frameModel.WorkerState) {
	jm.statusCode.Lock()
	defer jm.statusCode.Unlock()
	jm.statusCode.code = code
}

func (jm *JobMaster) getState() frameModel.WorkerState {
	jm.statusCode.RLock()
	defer jm.statusCode.RUnlock()
	return jm.statusCode.code
}
