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

package servermaster

import (
	"sync"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"go.uber.org/zap"
)

// JobHolder holds job meta and worker handle for a job.
type JobHolder struct {
	workerHandle framework.WorkerHandle
	masterMeta   *frameModel.MasterMeta
	// True means the job is loaded from metastore during jobmanager failover.
	// Otherwise it is added by SubmitJob.
	addFromFailover bool
}

// MasterMeta returns master meta of the job.
func (jh *JobHolder) MasterMeta() *frameModel.MasterMeta {
	return jh.masterMeta
}

// WorkerHandle returns the job master's worker handle.
func (jh *JobHolder) WorkerHandle() framework.WorkerHandle {
	return jh.workerHandle
}

// JobFsm manages state of all job masters, job master state forms a finite-state
// machine. Note job master managed in JobFsm is in running status, which means
// the job is not terminated or finished.
//
// ,-------.                   ,-------.            ,-------.       ,--------.
// |WaitAck|                   |Online |            |Pending|       |Finished|
// `---+---'                   `---+---'            `---+---'       `---+----'
//
//	|                           |                    |               |
//	| Master                    |                    |               |
//	|  .OnWorkerOnline          |                    |               |
//	|-------------------------->|                    |               |
//	|                           |                    |               |
//	|                           | Master             |               |
//	|                           |   .OnWorkerOffline |               |
//	|                           |   (failover)       |               |
//	|                           |------------------->|               |
//	|                           |                    |               |
//	|                           | Master             |               |
//	|                           |   .OnWorkerOffline |               |
//	|                           |   (finish)         |               |
//	|                           |----------------------------------->|
//	|                           |                    |               |
//	| Master                    |                    |               |
//	|  .OnWorkerOffline         |                    |               |
//	|  (failover)               |                    |               |
//	|----------------------------------------------->|               |
//	|                           |                    |               |
//	| Master                    |                    |               |
//	|  .OnWorkerOffline         |                    |               |
//	|  (finish)                 |                    |               |
//	|--------------------------------------------------------------->|
//	|                           |                    |               |
//	|                           | Master             |               |
//	|                           |   .CreateWorker    |               |
//	|<-----------------------------------------------|               |
//	|                           |                    |               |
//	| Master                    |                    |               |
//	|  .OnWorkerDispatched      |                    |               |
//	|  (with error)             |                    |               |
//	|----------------------------------------------->|               |
//	|                           |                    |               |
//	|                           |                    |               |
//	|                           |                    |               |
type JobFsm struct {
	JobStats

	jobsMu      sync.RWMutex
	pendingJobs map[frameModel.MasterID]*frameModel.MasterMeta
	waitAckJobs map[frameModel.MasterID]*JobHolder
	onlineJobs  map[frameModel.MasterID]*JobHolder
}

// JobStats defines a statistics interface for JobFsm
type JobStats interface {
	JobCount(status pb.Job_State) int
}

// NewJobFsm creates a new job fsm
func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[frameModel.MasterID]*frameModel.MasterMeta),
		waitAckJobs: make(map[frameModel.MasterID]*JobHolder),
		onlineJobs:  make(map[frameModel.MasterID]*JobHolder),
	}
}

// QueryOnlineJob queries job from online job list
func (fsm *JobFsm) QueryOnlineJob(jobID frameModel.MasterID) *JobHolder {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	return fsm.onlineJobs[jobID]
}

// QueryJob queries job with given jobID and returns QueryJobResponse
func (fsm *JobFsm) QueryJob(jobID frameModel.MasterID) *JobHolder {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	if meta, ok := fsm.pendingJobs[jobID]; ok {
		return &JobHolder{
			masterMeta: meta,
		}
	}

	if job, ok := fsm.waitAckJobs[jobID]; ok {
		return job
	}

	if job, ok := fsm.onlineJobs[jobID]; ok {
		return job
	}

	return nil
}

// JobDispatched is called when a job is firstly created or server master is failovered
func (fsm *JobFsm) JobDispatched(job *frameModel.MasterMeta, addFromFailover bool) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = &JobHolder{
		masterMeta:      job,
		addFromFailover: addFromFailover,
	}
}

// IterPendingJobs iterates all pending jobs and dispatch(via create worker) them again.
func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *frameModel.MasterMeta) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for oldJobID, job := range fsm.pendingJobs {
		id, err := dispatchJobFn(job)
		if err != nil {
			// This job is being backoff, skip it and process other jobs.
			if errors.Is(err, errors.ErrMasterCreateWorkerBackoff) {
				continue
			}
			if errors.Is(err, errors.ErrMasterCreateWorkerTerminate) {
				delete(fsm.pendingJobs, oldJobID)
				continue
			}
			return err
		}
		delete(fsm.pendingJobs, oldJobID)
		job.ID = id
		fsm.waitAckJobs[id] = &JobHolder{
			masterMeta: job,
		}
		log.Info("job master recovered", zap.Any("job", job))
	}

	return nil
}

// IterWaitAckJobs iterates wait ack jobs, failover them if they are added from failover
func (fsm *JobFsm) IterWaitAckJobs(dispatchJobFn func(job *frameModel.MasterMeta) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for id, job := range fsm.waitAckJobs {
		if !job.addFromFailover {
			continue
		}
		_, err := dispatchJobFn(job.masterMeta)
		if err != nil {
			return err
		}
		fsm.waitAckJobs[id].addFromFailover = false
		log.Info("tombstone job master doesn't receive heartbeat in time, recreate it", zap.Any("job", job))
	}

	return nil
}

// JobOnline is called when the first heartbeat of job is received
func (fsm *JobFsm) JobOnline(worker framework.WorkerHandle) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.onlineJobs[worker.ID()] = &JobHolder{
		workerHandle: worker,
		masterMeta:   job.masterMeta,
	}
	delete(fsm.waitAckJobs, worker.ID())
	return nil
}

// JobOffline is called when a job meets error or finishes
func (fsm *JobFsm) JobOffline(worker framework.WorkerHandle, needFailover bool) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.onlineJobs[worker.ID()]
	if ok {
		delete(fsm.onlineJobs, worker.ID())
	} else {
		job, ok = fsm.waitAckJobs[worker.ID()]
		if !ok {
			log.Warn("unknown worker, ignore it", zap.String("id", worker.ID()))
			return
		}
		delete(fsm.waitAckJobs, worker.ID())
	}
	if needFailover {
		fsm.pendingJobs[worker.ID()] = job.masterMeta
	}
}

// JobDispatchFailed is called when a job dispatch fails
func (fsm *JobFsm) JobDispatchFailed(worker framework.WorkerHandle) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.pendingJobs[worker.ID()] = job.masterMeta
	delete(fsm.waitAckJobs, worker.ID())
	return nil
}

// JobCount queries job count based on job status
func (fsm *JobFsm) JobCount(status pb.Job_State) int {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	switch status {
	case pb.Job_Created:
		return len(fsm.pendingJobs) + len(fsm.waitAckJobs)
	case pb.Job_Running:
		return len(fsm.onlineJobs)
	default:
		// TODO: support other job status count
		return 0
	}
}
