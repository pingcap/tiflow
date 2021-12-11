package master

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosm/master/cluster"
	"github.com/hanfei1991/microcosm/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

// JobManager manages all the job masters, and notify the offline executor to them.
type JobManager struct {
	mu         sync.Mutex
	jobMasters map[model.JobID]system.JobMaster

	idAllocater    *autoid.Allocator
	resourceMgr    cluster.ResourceMgr
	executorClient cluster.ExecutorClient

	offExecutors chan model.ExecutorID

	masterAddrs []string
}

// Start the deamon goroutine.
func (j *JobManager) Start(ctx context.Context) {
	go j.startImpl(ctx)
}

func (j *JobManager) startImpl(ctx context.Context) {
	for {
		select {
		case execID := <-j.offExecutors:
			log.L().Logger.Info("notify to offline exec")
			j.mu.Lock()
			for _, jobMaster := range j.jobMasters {
				jobMaster.OfflineExecutor(execID)
			}
			j.mu.Unlock()
		case <-ctx.Done():
		}
	}
}

func (j *JobManager) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	j.mu.Lock()
	defer j.mu.Unlock()
	job, ok := j.jobMasters[model.JobID(req.JobId)]
	if !ok {
		return &pb.CancelJobResponse{Err: &pb.Error{Message: "No such job"}}
	}
	err := job.Stop(ctx)
	if err != nil {
		return &pb.CancelJobResponse{Err: &pb.Error{Message: err.Error()}}
	}
	delete(j.jobMasters, model.JobID(req.JobId))
	return &pb.CancelJobResponse{}
}

// SubmitJob processes "SubmitJobRequest".
func (j *JobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	info := model.JobInfo{
		Config:   string(req.Config),
		UserName: req.User,
	}
	var jobMaster system.JobMaster
	var err error
	log.L().Logger.Info("submit job", zap.String("config", info.Config))
	resp := &pb.SubmitJobResponse{}
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		info.Type = model.JobBenchmark
		// TODO: supposing job master will be running independently, then the
		// addresses of server can change because of failover, the job master
		// should have ways to detect and adapt automatically.
		mClient, err := NewMasterClient(ctx, j.masterAddrs)
		if err != nil {
			resp.Err = errors.ToPBError(err)
			return resp
		}
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(
			info.Config, j.idAllocater, j.resourceMgr, j.executorClient, mClient)
		if err != nil {
			resp.Err = errors.ToPBError(err)
			return resp
		}
	default:
		err = errors.ErrBuildJobFailed.GenWithStack("unknown job type", req.Tp)
		resp.Err = errors.ToPBError(err)
		return resp
	}
	log.L().Logger.Info("finished build job")
	j.mu.Lock()
	defer j.mu.Unlock()
	err = jobMaster.Start(ctx)
	if err != nil {
		resp.Err = errors.ToPBError(err)
		return resp
	}
	log.L().Logger.Info("finished dispatch job")
	j.jobMasters[jobMaster.ID()] = jobMaster

	resp.JobId = int32(jobMaster.ID())
	return resp
}

func NewJobManager(
	resource cluster.ResourceMgr,
	clt cluster.ExecutorClient,
	executorNotifier chan model.ExecutorID,
	masterAddrs []string,
) *JobManager {
	return &JobManager{
		jobMasters:     make(map[model.JobID]system.JobMaster),
		idAllocater:    autoid.NewAllocator(),
		resourceMgr:    resource,
		executorClient: clt,
		offExecutors:   executorNotifier,
		masterAddrs:    masterAddrs,
	}
}
