package master

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/adapter"
	"github.com/hanfei1991/microcosm/pkg/autoid"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.etcd.io/etcd/clientv3"
	"go.uber.org/zap"
)

// JobMasterID is special and has no use.
const JobMasterID model.ID = -1

// JobManager is a special job master that manages all the job masters, and notify the offline executor to them.
type JobManager struct {
	*system.Master

	mu          sync.Mutex
	jobMasters  map[model.ID]*model.Task
	idAllocator autoid.JobIDAllocator
	clients     *client.Manager
	masterAddrs []string
}

func (j *JobManager) recover(ctx context.Context) error {
	resp, err := j.MetaKV.Get(ctx, adapter.JobKeyAdapter.Path())
	if err != nil {
		return err
	}
	result := resp.(*clientv3.GetResponse)
	for _, kv := range result.Kvs {
		jobID, err := strconv.Atoi(string(kv.Key))
		if err != nil {
			return err
		}
		jobTask := &model.Task{
			ID:   model.ID(jobID),
			OpTp: model.JobMasterType,
			Op:   kv.Value,
			Cost: 1,
		}
		err = j.RestoreTask(ctx, jobTask)
		if err != nil {
			return err
		}
		j.jobMasters[model.ID(jobID)] = jobTask
	}
	return nil
}

func (j *JobManager) persistJobInfo(ctx context.Context, jobID model.ID, jobBytes []byte) error {
	_, err := j.MetaKV.Put(ctx, adapter.JobKeyAdapter.Encode(strconv.Itoa(int(jobID))), string(jobBytes))
	return err
}

func (j *JobManager) Start(ctx context.Context, metaKV metadata.MetaKV) error {
	j.MetaKV = metaKV
	err := j.clients.AddMasterClient(ctx, j.masterAddrs)
	if err != nil {
		return err
	}
	err = j.recover(ctx)
	if err != nil {
		return err
	}
	j.Master.StartInternal(ctx)
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
		id := j.idAllocator.AllocJobID()
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
		err = j.persistJobInfo(ctx, masterConfig.ID, masterConfigBytes)
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
	resp.JobId = int32(jobTask.ID)
	j.Master.DispatchTasks(jobTask)
	log.L().Logger.Info("finished dispatch job")
	return resp
}

func NewJobManager(
	masterAddrs []string,
) *JobManager {
	clients := client.NewClientManager()
	return &JobManager{
		Master:      system.New(JobMasterID, clients),
		jobMasters:  make(map[model.ID]*model.Task),
		idAllocator: autoid.NewJobIDAllocator(),
		clients:     clients,
		masterAddrs: masterAddrs,
	}
}
