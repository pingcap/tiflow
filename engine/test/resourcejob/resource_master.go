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

package resourcejob

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"

	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"github.com/pingcap/tiflow/engine/lib"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
)

// JobMaster is the job master for the
// Resource-Job used to test the resource management capabilities
// of Dataflow Engine.
//
// The Resource-Job generates N files populated with random numbers
// and creates N workers to sort them. For each worker, the file is read
// from, and the sorted result is written to a temp file, and then the temp
// file is copied to overwrite the original file. Finally, the master verifies
// the result.
type JobMaster struct {
	lib.BaseJobMaster

	status *masterStatus
	config *JobConfig
}

// NewMaster creates a new JobMaster
func NewMaster(
	ctx *dcontext.Context,
	workerID libModel.WorkerID,
	masterID libModel.MasterID,
	config lib.WorkerConfig,
) lib.WorkerImpl {
	return &JobMaster{
		status: initialMasterStatus(),
		config: config.(*JobConfig),
	}
}

// JobConfig is the config for a resource test job.
type JobConfig struct {
	ResourceCount int `json:"resource_count"`
	ResourceLen   int `json:"resource_len"`
}

// InitImpl implements JobMasterImpl.
func (m *JobMaster) InitImpl(ctx context.Context) error {
	log.L().Info("ResourceJobMaster: InitImpl",
		zap.String("job-id", m.JobMasterID()),
		zap.Any("config", m.config))

	return m.persistMasterStatus(ctx)
}

// OnMasterRecovered implements JobMasterImpl.
func (m *JobMaster) OnMasterRecovered(ctx context.Context) error {
	log.L().Info("ResourceJobMaster: OnMasterRecovered",
		zap.String("job-id", m.JobMasterID()))

	dao := newMasterDAO(m.MetaKVClient(), m.JobMasterID())
	return dao.GetMasterMeta(ctx, m.status)
}

// Tick implements JobMasterImpl.
func (m *JobMaster) Tick(ctx context.Context) error {
	log.L().Info("ResourceJobMaster: Tick",
		zap.String("job-id", m.JobMasterID()),
		zap.Any("status", m.status))

	switch m.status.State {
	case masterStateUninit:
		m.createResources(m.config.ResourceCount)
		return m.persistMasterStatus(ctx)
	case masterStateInProgress:
		if err := m.createWorkerIfNecessary(); err != nil {
			return err
		}

		finishedCount := m.status.CountResourceByState(resourceStateSorted)
		if finishedCount == m.config.ResourceCount {
			m.status.OnAllResourcesSorted()
			return m.persistMasterStatus(ctx)
		}
		return nil
	case masterStateFinished:
		return m.UpdateJobStatus(ctx, libModel.WorkerStatus{
			Code: libModel.WorkerStatusFinished,
		})
	default:
		log.L().Panic("Unexpected state", zap.String("state", string(m.status.State)))
	}
	return nil
}

func (m *JobMaster) createResources(numResources int) {
	for i := 0; i < numResources; i++ {
		newResourceID := fmt.Sprintf("/local/resource-test-%d", i)
		// Since we are testing local resources, we are not actually writing
		// to the resource on the master, because this would bind all workers
		// to the current executor.
		m.status.OnResourceCreated(newResourceID)
	}
}

func (m *JobMaster) persistMasterStatus(ctx context.Context) error {
	dao := newMasterDAO(m.MetaKVClient(), m.JobMasterID())
	return dao.PutMasterMeta(ctx, m.status)
}

func (m *JobMaster) createWorkerIfNecessary() error {
	unboundResourceSet := m.status.UnboundResources()
	for unboundRes := range unboundResourceSet {
		workerID, err := m.CreateWorker(
			ResourceTestWorkerType,
			&workerConfig{ResourceID: unboundRes},
			10,
			unboundRes)
		if err != nil {
			return err
		}

		m.status.OnBindResourceToWorker(unboundRes, workerID)
	}
	return nil
}

// OnWorkerDispatched implements JobMasterImpl.
func (m *JobMaster) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	log.L().Info("ResourceJobMaster: OnWorkerDispatched",
		zap.String("job-id", m.JobMasterID()),
		zap.String("worker-id", worker.ID()))
	if result == nil {
		return nil
	}

	m.status.OnUnbindWorker(worker.ID())
	return nil
}

// OnWorkerOnline implements JobMasterImpl.
func (m *JobMaster) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("ResourceJobMaster: OnWorkerOnline",
		zap.String("job-id", m.JobMasterID()),
		zap.String("worker-id", worker.ID()))
	return nil
}

// OnWorkerOffline implements JobMasterImpl.
func (m *JobMaster) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	log.L().Info("ResourceJobMaster: OnWorkerOffline",
		zap.String("job-id", m.JobMasterID()),
		zap.String("worker-id", worker.ID()),
		zap.Error(reason))

	m.status.OnUnbindWorker(worker.ID())
	return nil
}

// OnWorkerMessage implements JobMasterImpl.
func (m *JobMaster) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	return nil
}

// OnWorkerStatusUpdated implements JobMasterImpl.
func (m *JobMaster) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
	log.L().Info("ResourceJobMaster: OnWorkerStatusUpdated",
		zap.String("job-id", m.JobMasterID()),
		zap.String("worker-id", worker.ID()),
		zap.Any("new-status", newStatus))

	workerStatus, err := newWorkerStatusFromBytes(newStatus.ExtBytes)
	if err != nil {
		return err
	}

	prevResState, ok := m.status.GetResourceStateForWorker(worker.ID())
	if !ok {
		log.L().Panic("Resource binding not found for worker",
			zap.String("worker-id", worker.ID()))
	}

	switch workerStatus.State {
	case workerStateSorting:
		return nil
	case workerStateCommitting:
		if prevResState == resourceStateUnsorted {
			m.status.OnWorkerStartedCopying(worker.ID())
		} else if prevResState == resourceStateSorted {
			log.L().Panic("OnWorkerStatusUpdated: unexpected worker status",
				zap.String("worker-id", worker.ID()),
				zap.String("prev-state", string(prevResState)))
		}
	case workerStateFinished:
		if prevResState == resourceStateCommitting {
			m.status.OnWorkerFinishedCopying(worker.ID())
		}
	default:
		panic("unreachable")
	}
	return nil
}

// CloseImpl implements JobMasterImpl.
func (m *JobMaster) CloseImpl(ctx context.Context) error {
	return nil
}

// Workload implements JobMasterImpl.
func (m *JobMaster) Workload() model.RescUnit {
	return 20
}

// OnJobManagerMessage implements JobMasterImpl.
func (m *JobMaster) OnJobManagerMessage(topic p2p.Topic, message interface{}) error {
	return nil
}

// IsJobMasterImpl implements JobMasterImpl.
func (m *JobMaster) IsJobMasterImpl() {
	panic("unreachable")
}

// OnMasterMessage implements WorkerImpl.
// TODO fix this. We shouldn't have to implement this.
func (m *JobMaster) OnMasterMessage(topic p2p.Topic, message p2p.MessageValue) error {
	return nil
}

// OnOpenAPIInitialized implements JobMaster
func (m *JobMaster) OnOpenAPIInitialized(apiGroup *gin.RouterGroup) {
	// No-op
	return
}
