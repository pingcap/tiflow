package servermaster

import (
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobHolder struct {
	lib.WorkerHandle
	*lib.MasterMetaKVData
	waitAckStartTime time.Time
}

// JobFsm manages state of all job masters, job master state forms a finite-state
// machine. Note job master managed in JobFsm is in running status, which means
// the job is not terminated or finished.
//
// ,-------.                   ,-------.            ,-------.
// |WaitAck|                   |Online |            |Pending|
// `---+---'                   `---+---'            `---+---'
//     |                           |                    |
//     | Master                    |                    |
//     |  .OnWorkerOnline          |                    |
//     |-------------------------->|                    |
//     |                           |                    |
//     |                           |                    |
//     |                           | Master             |
//     |                           |   .OnWorkerOffline |
//     |                           |------------------->|
//     |                           |                    |
//     |                           |                    |
//     |                           | Master             |
//     |                           |   .CreateWorker    |
//     |<-----------------------------------------------|
//     |                           |                    |
//     |                           |                    |
//     | Master                    |                    |
//     |  .OnWorkerDispatched      |                    |
//     |  (with error)             |                    |
//     |----------------------------------------------->|
//     |                           |                    |
//     |                           |                    |
//     |                           |                    |
type JobFsm struct {
	JobStats

	jobsMu      sync.RWMutex
	pendingJobs map[lib.MasterID]*lib.MasterMetaKVData
	waitAckJobs map[lib.MasterID]*jobHolder
	onlineJobs  map[lib.MasterID]*jobHolder
	clocker     clock.Clock
}

// JobStats defines a statistics interface for JobFsm
type JobStats interface {
	JobCount(pb.QueryJobResponse_JobStatus) int
}

func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[lib.MasterID]*lib.MasterMetaKVData),
		waitAckJobs: make(map[lib.MasterID]*jobHolder),
		onlineJobs:  make(map[lib.MasterID]*jobHolder),
		clocker:     clock.New(),
	}
}

func (fsm *JobFsm) QueryJob(jobID lib.MasterID) *pb.QueryJobResponse {
	checkPendingJob := func() *pb.QueryJobResponse {
		fsm.jobsMu.Lock()
		defer fsm.jobsMu.Unlock()

		meta, ok := fsm.pendingJobs[jobID]
		if !ok {
			return nil
		}
		resp := &pb.QueryJobResponse{
			Tp:     int64(meta.Tp),
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
			Tp:     int64(meta.Tp),
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
			Tp:     int64(job.Tp),
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
		} else if job.IsTombStone() {
			resp.JobMasterInfo = &pb.WorkerInfo{}
			resp.JobMasterInfo.IsTombstone = true
			status := job.Status()
			if status != nil {
				var err error
				resp.JobMasterInfo.Status, err = status.Marshal()
				if err != nil {
					resp.Err = &pb.Error{
						Code:    pb.ErrorCode_UnknownError,
						Message: err.Error(),
					}
				}
			}
		} else {
			// TODO think about
			log.L().Panic("Unexpected Job Info")
		}
		return nil
	}

	if resp := checkPendingJob(); resp != nil {
		return resp
	}
	if resp := checkWaitAckJob(); resp != nil {
		return resp
	}
	return checkOnlineJob()
}

func (fsm *JobFsm) JobDispatched(job *lib.MasterMetaKVData) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = &jobHolder{
		MasterMetaKVData: job,
		waitAckStartTime: fsm.clocker.Now(),
	}
}

func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *lib.MasterMetaKVData) (string, error)) error {
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
			waitAckStartTime: fsm.clocker.Now(),
		}
		log.L().Info("job master recovered", zap.Any("job", job))
	}

	return nil
}

func (fsm *JobFsm) IterWaitAckJobs(dispatchJobFn func(job *lib.MasterMetaKVData) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for id, job := range fsm.waitAckJobs {
		duration := fsm.clocker.Since(job.waitAckStartTime)
		if duration > defaultWorkerTimeout {
			_, err := dispatchJobFn(job.MasterMetaKVData)
			if err != nil {
				return err
			}
			fsm.waitAckJobs[id].waitAckStartTime = fsm.clocker.Now()
			log.L().Info("job master doesn't receive heartbeat in time, recreate it", zap.Any("job", job))
		}
	}

	return nil
}

func (fsm *JobFsm) JobOnline(worker lib.WorkerHandle) error {
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

func (fsm *JobFsm) JobOffline(worker lib.WorkerHandle, needFailover bool) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.onlineJobs[worker.ID()]
	if !ok {
		log.L().Warn("non-online worker offline, ignore it", zap.String("id", worker.ID()))
		return
	}
	if needFailover {
		fsm.pendingJobs[worker.ID()] = job.MasterMetaKVData
	}
	delete(fsm.onlineJobs, worker.ID())
}

func (fsm *JobFsm) JobDispatchFailed(worker lib.WorkerHandle) error {
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
