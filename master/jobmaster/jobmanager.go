package jobmaster

import (
	"context"
	"sync"

	"github.com/hanfei1991/microcosom/master/cluster"
	"github.com/hanfei1991/microcosom/master/jobmaster/benchmark"
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pb"
	"github.com/hanfei1991/microcosom/pkg/autoid"
	"github.com/hanfei1991/microcosom/pkg/errors"
	"github.com/pingcap/ticdc/dm/pkg/log"
	"go.uber.org/zap"
)

// JobManager manages all the job masters, and notify the offline executor to them.
type JobManager struct {
	mu         sync.Mutex
	jobMasters map[model.JobID]JobMaster

	idAllocater    *autoid.Allocator
	resourceMgr    cluster.ResourceMgr
	executorClient cluster.ExecutorClient

	offExecutors chan model.ExecutorID
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

// SubmitJob processes "SubmitJobRequest".
func (j *JobManager) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	info := model.JobInfo{
		Config:   string(req.Config),
		UserName: req.User,
	}
	var jobMaster JobMaster
	var err error
	log.L().Logger.Info("submit job", zap.String("config", info.Config))
	resp := &pb.SubmitJobResponse{}
	switch req.Tp {
	case pb.SubmitJobRequest_Benchmark:
		info.Type = model.JobBenchmark
		jobMaster, err = benchmark.BuildBenchmarkJobMaster(info.Config, j.idAllocater, j.resourceMgr, j.executorClient)
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
	err = jobMaster.DispatchJob(ctx)
	if err != nil {
		resp.Err = errors.ToPBError(err)
		return resp
	}
	log.L().Logger.Info("finished dispatch job")
	j.jobMasters[jobMaster.ID()] = jobMaster

	resp.JobId = int32(jobMaster.ID())
	return resp
}

func NewJobManager(resource cluster.ResourceMgr, clt cluster.ExecutorClient, executorNotifier chan model.ExecutorID) *JobManager {
	return &JobManager{
		jobMasters:     make(map[model.JobID]JobMaster),
		idAllocater:    autoid.NewAllocator(),
		resourceMgr:    resource,
		executorClient: clt,
		offExecutors:   executorNotifier,
	}
}
