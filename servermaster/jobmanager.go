package servermaster

import (
	"context"
	"encoding/json"
	"time"

	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"

	cvs "github.com/hanfei1991/microcosm/jobmaster/cvsJob"
	"github.com/hanfei1991/microcosm/lib"
	"github.com/hanfei1991/microcosm/lib/metadata"
	libModel "github.com/hanfei1991/microcosm/lib/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/clock"
	dcontext "github.com/hanfei1991/microcosm/pkg/context"
	derrors "github.com/hanfei1991/microcosm/pkg/errors"
	pkgOrm "github.com/hanfei1991/microcosm/pkg/orm"
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

	masterMetaClient *metadata.MasterMetadataClient
	uuidGen          uuid.Generator
	clocker          clock.Clock
	frameMetaClient  pkgOrm.Client
	tombstoneCleaned bool
}

func (jm *JobManagerImplV2) PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse {
	job := jm.JobFsm.QueryOnlineJob(req.JobIdStr)
	if job == nil {
		return &pb.PauseJobResponse{Err: &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		}}
	}
	topic := libModel.WorkerStatusChangeRequestTopic(jm.BaseMaster.MasterID(), job.WorkerHandle.ID())
	msg := &libModel.StatusChangeRequest{
		SendTime:     jm.clocker.Mono(),
		FromMasterID: jm.BaseMaster.MasterID(),
		Epoch:        jm.BaseMaster.MasterMeta().Epoch,
		ExpectState:  libModel.WorkerStatusStopped,
	}
	if handle := job.WorkerHandle.Unwrap(); handle != nil {
		err := handle.SendMessage(ctx, topic, msg, true /*nonblocking*/)
		return &pb.PauseJobResponse{Err: derrors.ToPBError(err)}
	}
	// The job is a tombstone, which means that the job has already exited.
	return &pb.PauseJobResponse{Err: &pb.Error{
		Code: pb.ErrorCode_UnKnownJob,
	}}
}

func (jm *JobManagerImplV2) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	panic("not implemented")
}

func (jm *JobManagerImplV2) QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse {
	resp := jm.JobFsm.QueryJob(req.JobId)
	if resp != nil {
		return resp
	}

	// TODO: refine the Load method here, seems strange
	mcli := metadata.NewMasterMetadataClient(req.JobId, jm.frameMetaClient)
	if masterMeta, err := mcli.Load(ctx); err != nil {
		log.L().Warn("failed to load master kv meta from meta store", zap.Any("id", req.JobId), zap.Error(err))
	} else {
		if masterMeta != nil {
			resp := &pb.QueryJobResponse{
				Tp:     int64(masterMeta.Tp),
				Config: masterMeta.Config,
			}
			switch masterMeta.StatusCode {
			case libModel.MasterStatusFinished:
				resp.Status = pb.QueryJobResponse_finished
				return resp
			case libModel.MasterStatusStopped:
				resp.Status = pb.QueryJobResponse_stopped
				return resp
			default:
				log.L().Warn("load master kv meta from meta store, but status is not expected",
					zap.Any("id", req.JobId), zap.Any("status", masterMeta.StatusCode), zap.Any("meta", masterMeta))
			}
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
		id  libModel.WorkerID
		err error
	)

	meta := &libModel.MasterMetaKVData{
		ProjectID: req.GetUser(),
		// TODO: we can use job name provided from user, but we must check the
		// job name is unique before using it.
		ID:         jm.uuidGen.NewString(),
		Config:     req.GetConfig(),
		StatusCode: libModel.MasterStatusUninit,
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
	err = metadata.StoreMasterMeta(ctx, jm.frameMetaClient, meta)
	if err != nil {
		resp.Err = derrors.ToPBError(err)
		return resp
	}

	// CreateWorker here is to create job master actually
	// TODO: use correct worker cost
	id, err = jm.BaseMaster.CreateWorker(
		meta.Tp, meta, defaultJobMasterCost)
	if err != nil {
		err2 := metadata.DeleteMasterMeta(ctx, jm.frameMetaClient, meta.ID)
		if err2 != nil {
			// TODO: add more GC mechanism if master meta is failed to delete
			log.L().Error("failed to delete master meta", zap.Error(err2))
		}

		log.L().Error("create job master met error", zap.Error(err))
		resp.Err = derrors.ToPBError(err)
		return resp
	}

	jm.JobFsm.JobDispatched(meta, false /*addFromFailover*/)
	resp.JobIdStr = id
	return resp
}

// NewJobManagerImplV2 creates a new JobManagerImplV2 instance
func NewJobManagerImplV2(
	dctx *dcontext.Context,
	id libModel.MasterID,
) (*JobManagerImplV2, error) {
	metaCli, err := dctx.Deps().Construct(func(cli pkgOrm.Client) (pkgOrm.Client, error) {
		return cli, nil
	})
	if err != nil {
		return nil, err
	}

	metaClient := metaCli.(pkgOrm.Client)
	cli := metadata.NewMasterMetadataClient(id, metaClient)
	impl := &JobManagerImplV2{
		JobFsm:           NewJobFsm(),
		uuidGen:          uuid.NewGenerator(),
		masterMetaClient: cli,
		clocker:          clock.New(),
		frameMetaClient:  metaClient,
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
	meta.StatusCode = libModel.MasterStatusInit
	err = metadata.StoreMasterMeta(dctx, impl.frameMetaClient, meta)
	if err != nil {
		return nil, err
	}
	err = impl.BaseMaster.Init(dctx)
	if err != nil {
		_ = impl.BaseMaster.Close(dctx)
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
	filterQuotaError := func(err error) (exceedQuota bool, retErr error) {
		if err == nil {
			return false, nil
		}
		if derrors.ErrMasterConcurrencyExceeded.Equal(err) {
			log.L().Warn("create worker exceeds quota, retry later", zap.Error(err))
			return true, nil
		}
		return false, err
	}

	err := jm.JobFsm.IterPendingJobs(
		func(job *libModel.MasterMetaKVData) (string, error) {
			return jm.BaseMaster.CreateWorker(
				job.Tp, job, defaultJobMasterCost)
		})
	if _, err = filterQuotaError(err); err != nil {
		return err
	}

	if !jm.tombstoneCleaned && jm.BaseMaster.IsMasterReady() {
		for _, worker := range jm.BaseMaster.GetWorkers() {
			// clean tombstone workers from worker manager and they will be
			// re-created in the following IterWaitAckJobs
			tombstoneHandle := worker.GetTombstone()
			if tombstoneHandle != nil {
				if err := tombstoneHandle.CleanTombstone(ctx); err != nil {
					return err
				}
				continue
			}
			// mark non-tombstone workers as online
			err := jm.JobFsm.JobOnline(worker)
			// ignore worker that is not in WaitAck list
			if err != nil && derrors.ErrWorkerNotFound.NotEqual(err) {
				return err
			}
		}
		err = jm.JobFsm.IterWaitAckJobs(
			func(job *libModel.MasterMetaKVData) (string, error) {
				return jm.BaseMaster.CreateWorker(
					job.Tp, job, defaultJobMasterCost)
			})
		exceedQuota, err := filterQuotaError(err)
		if err != nil {
			return err
		}
		// if met exceed quota error, the remaining jobs need to be failover in
		// another tick
		if !exceedQuota {
			jm.tombstoneCleaned = true
		}
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
		if job.Tp == lib.JobManager {
			continue
		}
		if job.StatusCode == libModel.MasterStatusFinished || job.StatusCode == libModel.MasterStatusStopped {
			log.L().Info("skip finished or stopped job", zap.Any("job", job))
			continue
		}
		jm.JobFsm.JobDispatched(job, true /*addFromFailover*/)
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
	} else if derrors.ErrWorkerStop.Equal(reason) {
		log.L().Info("job master stopped", zap.String("id", worker.ID()))
		needFailover = false
	} else {
		log.L().Info("on worker offline", zap.Any("id", worker.ID()), zap.Any("reason", reason))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := worker.GetTombstone().CleanTombstone(ctx); err != nil {
		return err
	}
	jm.JobFsm.JobOffline(worker, needFailover)
	return nil
}

// OnWorkerMessage implements lib.MasterImpl.OnWorkerMessage
func (jm *JobManagerImplV2) OnWorkerMessage(worker lib.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.L().Info("on worker message", zap.Any("id", worker.ID()), zap.Any("topic", topic), zap.Any("message", message))
	return nil
}

func (jm *JobManagerImplV2) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
	log.L().Info("on worker status updated", zap.String("worker-id", worker.ID()), zap.Any("status", newStatus))
	return nil
}

// CloseImpl implements lib.MasterImpl.CloseImpl
func (jm *JobManagerImplV2) CloseImpl(ctx context.Context) error {
	return nil
}
