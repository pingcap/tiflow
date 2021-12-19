package master

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// JobMasterID is special and has no use.
const JobMasterID model.ID = -1

// JobManager is a special job master that manages all the job masters, and notify the offline executor to them.
type JobManager struct {
	*system.Master

	mu          sync.Mutex
	jobMasters  map[model.ID]*model.Task
	idAllocater autoid.JobIDAllocator
	clients     *client.Manager
	masterAddrs []string
}

func (j *JobManager) Start(ctx context.Context) error {
	err := j.clients.AddMasterClient(ctx, j.masterAddrs)
	if err != nil {
		return err
	}
	j.Master = system.New(ctx, JobMasterID, j.clients)
	j.Master.StartInternal()
	return nil
}

func (j *JobManager) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	j.mu.Lock()
	defer j.mu.Unlock()
	task, ok := j.jobMasters[model.ID(req.JobId)]
	if !ok {
		return &pb.CancelJobResponse{Err: &pb.Error{Message: "No such job"}}
	}
	err := j.Master.StopTasks(ctx, []*model.Task{task})
	if err != nil {
		return &pb.CancelJobResponse{Err: &pb.Error{Message: err.Error()}}
	}
	delete(j.jobMasters, model.ID(req.JobId))
	return &pb.CancelJobResponse{}
}

// SubmitJob processes "SubmitJobRequest".
func (j *JobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	var jobTask *model.Task
	log.L().Logger.Info("submit job", zap.String("config", string(req.Config)))
	resp := &pb.SubmitJobResponse{}
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		id := j.idAllocater.AllocJobID()
		// TODO: supposing job master will be running independently, then the
		// addresses of server can change because of failover, the job master
		// should have ways to detect and adapt automatically.
		masterConfig := &model.JobMaster{
			ID:          model.ID(id),
			Tp:          model.Benchmark,
			Config:      req.Config,
			MasterAddrs: j.masterAddrs,
		}
		masterConfigBytes, err := json.Marshal(masterConfig)
		if err != nil {
			resp.Err = errors.ToPBError(err)
			return resp
		}
		jobTask = &model.Task{
			ID:   model.ID(id),
			OpTp: model.JobMasterType,
			Op:   masterConfigBytes,
			Cost: 1,
		}
	default:
		err := errors.ErrBuildJobFailed.GenWithStack("unknown job type", req.Tp)
		resp.Err = errors.ToPBError(err)
		return resp
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.jobMasters[jobTask.ID] = jobTask
	err := j.Master.DispatchTasks(ctx, []*model.Task{jobTask})
	if err != nil {
		resp.Err = errors.ToPBError(err)
		return resp
	}
	log.L().Logger.Info("finished dispatch job")
	resp.JobId = int32(jobTask.ID)
	return resp
}

func NewJobManager(
	masterAddrs []string,
) *JobManager {
	return &JobManager{
		jobMasters:  make(map[model.ID]*model.Task),
		idAllocater: autoid.NewJobIDAllocator(),
		clients:     client.NewClientManager(),
		masterAddrs: masterAddrs,
	}
}
