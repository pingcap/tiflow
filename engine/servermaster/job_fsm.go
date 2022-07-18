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

	"go.uber.org/zap"

	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/framework"
	frame "github.com/pingcap/tiflow/engine/framework"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	"github.com/pingcap/tiflow/pkg/errors"
)

type jobHolder struct {
	framework.WorkerHandle
	*frameModel.MasterMetaKVData
	// True means the job is loaded from metastore during jobmanager failover.
	// Otherwise it is added by SubmitJob.
	addFromFailover bool
}

// JobFsm manages state of all job masters, job master state forms a finite-state
// machine. Note job master managed in JobFsm is in running status, which means
// the job is not terminated or finished.
//
// ,-------.                   ,-------.            ,-------.       ,--------.
// |WaitAck|                   |Online |            |Pending|       |Finished|
// `---+---'                   `---+---'            `---+---'       `---+----'
//     |                           |                    |               |
//     | Master                    |                    |               |
//     |  .OnWorkerOnline          |                    |               |
//     |-------------------------->|                    |               |
//     |                           |                    |               |
//     |                           | Master             |               |
//     |                           |   .OnWorkerOffline |               |
//     |                           |   (failover)       |               |
//     |                           |------------------->|               |
//     |                           |                    |               |
//     |                           | Master             |               |
//     |                           |   .OnWorkerOffline |               |
//     |                           |   (finish)         |               |
//     |                           |----------------------------------->|
//     |                           |                    |               |
//     | Master                    |                    |               |
//     |  .OnWorkerOffline         |                    |               |
//     |  (failover)               |                    |               |
//     |----------------------------------------------->|               |
//     |                           |                    |               |
//     | Master                    |                    |               |
//     |  .OnWorkerOffline         |                    |               |
//     |  (finish)                 |                    |               |
//     |--------------------------------------------------------------->|
//     |                           |                    |               |
//     |                           | Master             |               |
//     |                           |   .CreateWorker    |               |
//     |<-----------------------------------------------|               |
//     |                           |                    |               |
//     | Master                    |                    |               |
//     |  .OnWorkerDispatched      |                    |               |
//     |  (with error)             |                    |               |
//     |----------------------------------------------->|               |
//     |                           |                    |               |
//     |                           |                    |               |
//     |                           |                    |               |
type JobFsm struct {
	JobStats

	jobsMu      sync.RWMutex
	pendingJobs map[frameModel.MasterID]*frameModel.MasterMetaKVData
	waitAckJobs map[frameModel.MasterID]*jobHolder
	onlineJobs  map[frameModel.MasterID]*jobHolder
}

// JobStats defines a statistics interface for JobFsm
type JobStats interface {
	JobCount(pb.QueryJobResponse_JobStatus) int
}

// NewJobFsm creates a new job fsm
func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[frameModel.MasterID]*frameModel.MasterMetaKVData),
		waitAckJobs: make(map[frameModel.MasterID]*jobHolder),
		onlineJobs:  make(map[frameModel.MasterID]*jobHolder),
	}
}

// QueryOnlineJob queries job from online job list
func (fsm *JobFsm) QueryOnlineJob(jobID frameModel.MasterID) *jobHolder {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	return fsm.onlineJobs[jobID]
}

// QueryJob queries job with given jobID and returns QueryJobResponse
// TODO: Refine me. remove the pb from JobFsm to alleviate coupling
func (fsm *JobFsm) QueryJob(jobID frameModel.MasterID) *pb.QueryJobResponse {
	checkPendingJob := func() *pb.QueryJobResponse {
		fsm.jobsMu.Lock()
		defer fsm.jobsMu.Unlock()

		meta, ok := fsm.pendingJobs[jobID]
		if !ok {
			return nil
		}
		resp := &pb.QueryJobResponse{
			Tp:     int32(frame.MustConvertWorkerType2JobType(meta.Tp)),
			Config: meta.Config,
			Status: pb.QueryJobResponse_pending,
		}
		return resp
	}

	checkWaitAckJob := func() *pb.QueryJobResponse {
		fsm.jobsMu.Lock()
		defer fsm.jobsMu.Unlock()

		job, ok := fsm.waitAckJobs[jobID]
		if !ok {
			return nil
		}
		meta := job.MasterMetaKVData
		resp := &pb.QueryJobResponse{
			Tp:     int32(frame.MustConvertWorkerType2JobType(meta.Tp)),
			Config: meta.Config,
			Status: pb.QueryJobResponse_dispatched,
		}
		return resp
	}

	checkOnlineJob := func() *pb.QueryJobResponse {
		fsm.jobsMu.Lock()
		defer fsm.jobsMu.Unlock()

		job, ok := fsm.onlineJobs[jobID]
		if !ok {
			return nil
		}
		resp := &pb.QueryJobResponse{
			Tp:     int32(frame.MustConvertWorkerType2JobType(job.Tp)),
			Config: job.Config,
			Status: pb.QueryJobResponse_online,
		}
		jobInfo, err := job.ToPB()
		// TODO (zixiong) ToPB should handle the tombstone situation gracefully.
		if err != nil {
			resp.Err = &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}
		} else if jobInfo != nil {
			resp.JobMasterInfo = jobInfo
		} else {
			// job master is just timeout but have not call OnOffline.
			return nil
		}
		return resp
	}

	if resp := checkPendingJob(); resp != nil {
		return resp
	}
	if resp := checkWaitAckJob(); resp != nil {
		return resp
	}
	return checkOnlineJob()
}

// JobDispatched is called when a job is firstly created or server master is failovered
func (fsm *JobFsm) JobDispatched(job *frameModel.MasterMetaKVData, addFromFailover bool) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = &jobHolder{
		MasterMetaKVData: job,
		addFromFailover:  addFromFailover,
	}
}

// IterPendingJobs iterates all pending jobs and dispatch(via create worker) them again.
func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *frameModel.MasterMetaKVData) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for oldJobID, job := range fsm.pendingJobs {
		id, err := dispatchJobFn(job)
		if err != nil {
			return err
		}
		delete(fsm.pendingJobs, oldJobID)
		job.ID = id
		fsm.waitAckJobs[id] = &jobHolder{
			MasterMetaKVData: job,
		}
		log.Info("job master recovered", zap.Any("job", job))
	}

	return nil
}

// IterWaitAckJobs iterates wait ack jobs, failover them if they are added from failover
func (fsm *JobFsm) IterWaitAckJobs(dispatchJobFn func(job *frameModel.MasterMetaKVData) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for id, job := range fsm.waitAckJobs {
		if !job.addFromFailover {
			continue
		}
		_, err := dispatchJobFn(job.MasterMetaKVData)
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
	fsm.onlineJobs[worker.ID()] = &jobHolder{
		WorkerHandle:     worker,
		MasterMetaKVData: job.MasterMetaKVData,
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
		fsm.pendingJobs[worker.ID()] = job.MasterMetaKVData
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
	fsm.pendingJobs[worker.ID()] = job.MasterMetaKVData
	delete(fsm.waitAckJobs, worker.ID())
	return nil
}

// JobCount queries job count based on job status
func (fsm *JobFsm) JobCount(status pb.QueryJobResponse_JobStatus) int {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	switch status {
	case pb.QueryJobResponse_pending:
		return len(fsm.pendingJobs)
	case pb.QueryJobResponse_dispatched:
		return len(fsm.waitAckJobs)
	case pb.QueryJobResponse_online:
		return len(fsm.onlineJobs)
	default:
		// TODO: support other job status count
		return 0
	}
}
