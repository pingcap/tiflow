package servermaster

import (
	"context"
	"encoding/json"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/pb"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/metadata"
	"github.com/hanfei1991/microcosm/pkg/p2p"
	"github.com/hanfei1991/microcosm/pkg/uuid"
)

// JobManager defines manager of job master
type JobManager interface {
	lib.Master
	JobStats

	SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse
	QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse
	CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse
	PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse
}

const defaultJobMasterCost = 1

// JobManagerImplV2 is a special job master that manages all the job masters, and notify the offline executor to them.
// worker state transition
// - submit new job, create job master successfully, then adds to the `waitAckJobs`.
// - receive worker online, move job from `waitAckJobs` to `onlineJobs`.
// - receive worker offline, move job from `onlineJobs` to `pendingJobs`.
// - Tick checks `pendingJobs` periodically	and reschedules the jobs.
type JobManagerImplV2 struct {
	lib.BaseMaster
	*JobFsm

	masterMetaClient *lib.MasterMetadataClient
	uuidGen          uuid.Generator
}

func (jm *JobManagerImplV2) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	panic("not implemented")
}

func (jm *JobManagerImplV2) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

func (jm *JobManagerImplV2) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	resp := jm.JobFsm.QueryJob(req.JobId)
	if resp != nil {
		return resp
	}
	mcli := lib.NewMasterMetadataClient(req.JobId, jm.MetaKVClient())
	if masterMeta, err := mcli.Load(ctx); err != nil {
		log.L().Warn("failed to load master kv meta from meta store", zap.Error(err))
	} else {
		if masterMeta != nil && masterMeta.StatusCode == lib.MasterStatusFinished {
			resp := &pb.QueryJobResponse{
				Tp:     int64(masterMeta.Tp),
				Config: masterMeta.Config,
				Status: pb.QueryJobResponse_finished,
			}
			return resp
		}
	}
	return &pb.QueryJobResponse{
		Err: &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		},
	}
}

// SubmitJob processes "SubmitJobRequest".
func (jm *JobManagerImplV2) SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse {
	log.L().Logger.Info("submit job", zap.String("config", string(req.Config)))
	resp := &pb.SubmitJobResponse{}
	var (
		id  lib.WorkerID
		err error
	)
	meta := &lib.MasterMetaKVData{
		// TODO: we can use job name provided from user, but we must check the
		// job name is unique before using it.
		ID:         jm.uuidGen.NewString(),
		Config:     req.Config,
		StatusCode: lib.MasterStatusUninit,
	}
	switch req.Tp {
	case pb.JobType_CVSDemo:
		// TODO: check config is valid, refine it later
		extConfig := &cvs.Config{}
		err = json.Unmarshal(req.Config, extConfig)
		if err != nil {
			err := derrors.ErrBuildJobFailed.GenWithStack("failed to decode config: %s", req.Config)
			resp.Err = derrors.ToPBError(err)
			return resp
		}
		meta.Tp = lib.CvsJobMaster
	case pb.JobType_DM:
		meta.Tp = lib.DMJobMaster
	case pb.JobType_FakeJob:
		meta.Tp = lib.FakeJobMaster
	default:
		err := derrors.ErrBuildJobFailed.GenWithStack("unknown job type: %s", req.Tp)
		resp.Err = derrors.ToPBError(err)
		return resp
	}

	// Store job master meta data before creating it
	err = lib.StoreMasterMeta(ctx, jm.BaseMaster.MetaKVClient(), meta)
	if err != nil {
		resp.Err = derrors.ToPBError(err)
		return resp
	}

	// CreateWorker here is to create job master actually
	// TODO: use correct worker cost
	id, err = jm.BaseMaster.CreateWorker(
		meta.Tp, meta, defaultJobMasterCost)

	if err != nil {
		log.L().Error("create job master met error", zap.Error(err))
		resp.Err = derrors.ToPBError(err)
		return resp
	}
	jm.JobFsm.JobDispatched(meta)

	resp.JobIdStr = id
	return resp
}

// NewJobManagerImplV2 creates a new JobManagerImplV2 instance
func NewJobManagerImplV2(
	dctx *dcontext.Context,
	id lib.MasterID,
) (*JobManagerImplV2, error) {
	masterMetaClient, err := dctx.Deps().Construct(func(metaKV metadata.MetaKV) (*lib.MasterMetadataClient, error) {
		return lib.NewMasterMetadataClient(id, metaKV), nil
	})
	if err != nil {
		return nil, err
	}

	impl := &JobManagerImplV2{
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: masterMetaClient.(*lib.MasterMetadataClient),
	}
	impl.BaseMaster = lib.NewBaseMaster(
		dctx,
		impl,
		id,
	)

	// Note the meta data of job manager is not used, it is safe to overwrite it
	// every time a new server master leader is elected. And we always mark the
	// Initialized to true in order to trigger OnMasterRecovered of job manager.
	meta := impl.MasterMeta()
	meta.StatusCode = lib.MasterStatusInit
	err = lib.StoreMasterMeta(dctx, impl.MetaKVClient(), meta)
	if err != nil {
		return nil, err
	}
	err = impl.BaseMaster.Init(dctx)
	if err != nil {
		return nil, err
	}
	return impl, nil
}

// InitImpl implements lib.MasterImpl.InitImpl
func (jm *JobManagerImplV2) InitImpl(ctx context.Context) error {
	return nil
}

// Tick implements lib.MasterImpl.Tick
func (jm *JobManagerImplV2) Tick(ctx context.Context) error {
	err := jm.JobFsm.IterPendingJobs(
		func(job *lib.MasterMetaKVData) (string, error) {
			return jm.BaseMaster.CreateWorker(
				job.Tp, job, defaultJobMasterCost)
		})
	if err != nil {
		return err
	}

	err = jm.JobFsm.IterWaitAckJobs(
		func(job *lib.MasterMetaKVData) (string, error) {
			return jm.BaseMaster.CreateWorker(
				job.Tp, job, defaultJobMasterCost)
		})
	if err != nil {
		return err
	}

	return nil
}

// OnMasterRecovered implements lib.MasterImpl.OnMasterRecovered
func (jm *JobManagerImplV2) OnMasterRecovered(ctx context.Context) error {
	jobs, err := jm.masterMetaClient.LoadAllMasters(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		if job.StatusCode == lib.MasterStatusFinished {
			log.L().Info("skip finished job", zap.Any("job", job))
			continue
		}
		jm.JobFsm.JobDispatched(job)
		log.L().Info("recover job, move it to WaitAck job queue", zap.Any("job", job))
	}
	return nil
}

// OnWorkerDispatched implements lib.MasterImpl.OnWorkerDispatched
func (jm *JobManagerImplV2) OnWorkerDispatched(worker lib.WorkerHandle, result error) error {
	if result != nil {
		log.L().Warn("dispatch worker met error", zap.Error(result))
		return jm.JobFsm.JobDispatchFailed(worker)
	}
	return nil
}

// OnWorkerOnline implements lib.MasterImpl.OnWorkerOnline
func (jm *JobManagerImplV2) OnWorkerOnline(worker lib.WorkerHandle) error {
	log.L().Info("on worker online", zap.Any("id", worker.ID()))
	return jm.JobFsm.JobOnline(worker)
}

// OnWorkerOffline implements lib.MasterImpl.OnWorkerOffline
func (jm *JobManagerImplV2) OnWorkerOffline(worker lib.WorkerHandle, reason error) error {
	needFailover := true
	if derrors.ErrWorkerFinish.Equal(reason) {
		log.L().Info("job master finished", zap.String("id", worker.ID()))
		needFailover = false
	} else {
		log.L().Info("on worker offline", zap.Any("id", worker.ID()), zap.Any("reason", reason))
	}
	jm.JobFsm.JobOffline(worker, needFailover)
	return nil
}

// OnWorkerMessage implements lib.MasterImpl.OnWorkerMessage
func (jm *JobManagerImplV2) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("on worker message", zap.Any("id", worker.ID()), zap.Any("topic", topic), zap.Any("message", message))
	return nil
}

// CloseImpl implements lib.MasterImpl.CloseImpl
func (jm *JobManagerImplV2) CloseImpl(ctx context.Context) error {
	return nil
}
