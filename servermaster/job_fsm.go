package servermaster

import (
	"sync"

	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

type jobHolder struct {
	lib.WorkerHandle
	*lib.JobMasterV2
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
	pendingJobs map[lib.MasterID]*lib.JobMasterV2
	waitAckJobs map[lib.MasterID]*lib.JobMasterV2
	onlineJobs  map[lib.MasterID]*jobHolder
}

// JobStats defines a statistics interface for JobFsm
type JobStats interface {
	PendingJobCount() int
	WaitAckJobCount() int
	OnlineJobCount() int
}

func NewJobFsm() *JobFsm {
	return &JobFsm{
		pendingJobs: make(map[lib.MasterID]*lib.JobMasterV2),
		waitAckJobs: make(map[lib.MasterID]*lib.JobMasterV2),
		onlineJobs:  make(map[lib.MasterID]*jobHolder),
	}
}

func (fsm *JobFsm) JobDispatched(job *lib.JobMasterV2) {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()
	fsm.waitAckJobs[job.ID] = job
}

func (fsm *JobFsm) IterPendingJobs(dispatchJobFn func(job *lib.JobMasterV2) (string, error)) error {
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

	cfg, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.onlineJobs[worker.ID()] = &jobHolder{
		WorkerHandle: worker,
		JobMasterV2:  cfg,
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
	fsm.pendingJobs[worker.ID()] = job.JobMasterV2
	delete(fsm.onlineJobs, worker.ID())
}

func (fsm *JobFsm) JobDispatchFailed(worker lib.WorkerHandle) error {
	fsm.jobsMu.Lock()
	defer fsm.jobsMu.Unlock()

	cfg, ok := fsm.waitAckJobs[worker.ID()]
	if !ok {
		return errors.ErrWorkerNotFound.GenWithStackByArgs(worker.ID())
	}
	fsm.pendingJobs[worker.ID()] = cfg
	delete(fsm.waitAckJobs, worker.ID())
	return nil
}

func (fsm *JobFsm) PendingJobCount() int {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	return len(fsm.pendingJobs)
}

func (fsm *JobFsm) WaitAckJobCount() int {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	return len(fsm.waitAckJobs)
}

func (fsm *JobFsm) OnlineJobCount() int {
	fsm.jobsMu.RLock()
	defer fsm.jobsMu.RUnlock()
	return len(fsm.onlineJobs)
}
