package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobHolder struct {
	lib.WorkerHandle
	*lib.MasterMetaExt
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
	pendingJobs map[lib.MasterID]*lib.MasterMetaExt
	waitAckJobs map[lib.MasterID]*lib.MasterMetaExt
	onlineJobs  map[lib.MasterID]*jobHolder
}

// JobStats defines a statistics interface for JobFsm
type JobStats interface {
	JobCount(pb.QueryJobResponse_JobStatus) int
}

func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[lib.MasterID]*lib.MasterMetaExt),
		waitAckJobs: make(map[lib.MasterID]*lib.MasterMetaExt),
		onlineJobs:  make(map[lib.MasterID]*jobHolder),
	}
}

func (fsm *JobFsm) QueryJob(jobID lib.MasterID) *pb.QueryJobResponse {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	resp := &pb.QueryJobResponse{}
	meta, ok := fsm.pendingJobs[jobID]
	if ok {
		resp.Config = meta.Config
		resp.Status = pb.QueryJobResponse_pending
		return resp
	}
	meta, ok = fsm.waitAckJobs[jobID]
	if ok {
		resp.Config = meta.Config
		resp.Status = pb.QueryJobResponse_dispatched
		return resp
	}
	job, ok := fsm.onlineJobs[jobID]
	resp.Status = pb.QueryJobResponse_online
	if ok {
		resp.Config = job.Config
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
	} else {
		resp.Err = &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		}
	}
	return resp
}

func (fsm *JobFsm) JobDispatched(job *lib.MasterMetaExt) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = job
}

func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *lib.MasterMetaExt) (string, error)) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	for oldJobID, job := range fsm.pendingJobs {
		id, err := dispatchJobFn(job)
		if err != nil {
			return err
		}
		delete(fsm.pendingJobs, oldJobID)
		job.ID = id
		fsm.waitAckJobs[id] = job
		log.L().Info("job master recovered", zap.Any("job", job))
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
		WorkerHandle:  worker,
		MasterMetaExt: job,
	}
	delete(fsm.waitAckJobs, worker.ID())
	return nil
}

func (fsm *JobFsm) JobOffline(worker lib.WorkerHandle) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.onlineJobs[worker.ID()]
	if !ok {
		log.L().Warn("non-online worker offline, ignore it", zap.String("id", worker.ID()))
		return
	}
	fsm.pendingJobs[worker.ID()] = job.MasterMetaExt
	delete(fsm.onlineJobs, worker.ID())
}

func (fsm *JobFsm) JobDispatchFailed(worker lib.WorkerHandle) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	job, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.pendingJobs[worker.ID()] = job
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
