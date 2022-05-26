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
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/pingcap/errors"
	"go.uber.org/zap"

	"github.com/pingcap/tiflow/dm/pkg/log"
	cvs "github.com/pingcap/tiflow/engine/jobmaster/cvsJob"
	"github.com/pingcap/tiflow/engine/lib"
	"github.com/pingcap/tiflow/engine/lib/metadata"
	libModel "github.com/pingcap/tiflow/engine/lib/model"
	"github.com/pingcap/tiflow/engine/pb"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	dmpkg "github.com/pingcap/tiflow/engine/pkg/dm"
	"github.com/pingcap/tiflow/engine/pkg/ctxmu"
	derrors "github.com/pingcap/tiflow/engine/pkg/errors"
	resManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/uuid"
)

// JobManager defines manager of job master
type JobManager interface {
	lib.Master
	JobStats

	SubmitJob(ctx context.Context, req *pb.SubmitJobRequest) *pb.SubmitJobResponse
	QueryJob(ctx context.Context, req *pb.QueryJobRequest) *pb.QueryJobResponse
	CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse
	PauseJob(ctx context.Context, req *pb.PauseJobRequest) *pb.PauseJobResponse
	DebugJob(ctx context.Context, req *pb.DebugJobRequest) *pb.DebugJobResponse

	GetJobStatuses(ctx context.Context) (map[libModel.MasterID]libModel.MasterStatusCode, error)
	WatchJobStatuses(
		ctx context.Context,
	) (resManager.JobStatusesSnapshot, *notifier.Receiver[resManager.JobStatusChangeEvent], error)
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

	// jobStatusChangeMu must be taken when we try to create, delete,
	// pause or resume a job.
	// NOTE The concurrency management for the JobManager is not complete
	// yet. We are prioritizing implementing all features.
	// TODO We might add a pending operation queue in the future.
	jobStatusChangeMu *ctxmu.CtxMutex
	notifier          *notifier.Notifier[resManager.JobStatusChangeEvent]

	// Use for DebugJob, since we now has no openapi
	messageAgent dmpkg.MessageAgent
}

// PauseJob implements proto/Master.PauseJob
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

// DebugJob implements proto/Master.DebugJob
func (jm *JobManagerImplV2) DebugJob(ctx context.Context, req *pb.DebugJobRequest) (resp *pb.DebugJobResponse) {
	job := jm.JobFsm.QueryOnlineJob(req.JobIdStr)
	if job == nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		}}
	}
	handle := job.WorkerHandle.Unwrap()
	if handle == nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		}}
	}

	// we only call MessageAgent.Init/UpdateSender/Close in DebugJob func
	if err := jm.messageAgent.Init(ctx); err != nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}
	defer func() {
		if err := jm.messageAgent.Close(ctx); err != nil {
			resp = &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
	}()
	if err := jm.messageAgent.UpdateSender(req.JobIdStr, handle); err != nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}
	defer func() {
		if err := jm.messageAgent.UpdateSender(req.JobIdStr, nil); err != nil {
			resp = &pb.DebugJobResponse{Err: &pb.Error{
				Code:    pb.ErrorCode_UnknownError,
				Message: err.Error(),
			}}
		}
	}()

	runCtx, runCancel := context.WithCancel(ctx)
	defer runCancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			if err := jm.messageAgent.Tick(runCtx); err != nil {
				log.L().Error("failed to run message agent tick", log.ShortError(err))
				return
			}
			select {
			case <-runCtx.Done():
				return
			default:
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()

	resp2, err := jm.messageAgent.SendRequest(ctx, req.JobIdStr, "DebugJob", req)
	if err != nil {
		return &pb.DebugJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}
	runCancel()
	wg.Wait()
	return resp2.(*pb.DebugJobResponse)
}

// CancelJob implements proto/Master.CancelJob
func (jm *JobManagerImplV2) CancelJob(ctx context.Context, req *pb.CancelJobRequest) *pb.CancelJobResponse {
	// This is a draft implementation.
	// TODO:
	// (1) Handle potential race conditions.
	// (2) Refine error handling.

	job, err := jm.frameMetaClient.GetJobByID(ctx, req.GetJobIdStr())
	if pkgOrm.IsNotFoundError(err) {
		return &pb.CancelJobResponse{Err: &pb.Error{
			Code: pb.ErrorCode_UnKnownJob,
		}}
	}
	if err != nil {
		return &pb.CancelJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}

	// Only stopped (paused) jobs can be canceled.
	if job.StatusCode != libModel.MasterStatusStopped {
		return &pb.CancelJobResponse{Err: &pb.Error{
			Code: pb.ErrorCode_UnexpectedJobStatus,
		}}
	}

	if err := jm.deleteJobMeta(ctx, req.JobIdStr); err != nil {
		return &pb.CancelJobResponse{Err: &pb.Error{
			Code:    pb.ErrorCode_UnknownError,
			Message: err.Error(),
		}}
	}

	return &pb.CancelJobResponse{}
}

func (jm *JobManagerImplV2) deleteJobMeta(ctx context.Context, jobID string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if ok := jm.jobStatusChangeMu.Lock(ctx); !ok {
		return errors.Trace(ctx.Err())
	}
	defer jm.jobStatusChangeMu.Unlock()

	// Note that DeleteJob is a soft delete.
	res, err := jm.frameMetaClient.DeleteJob(ctx, jobID)
	if err != nil {
		return err
	}
	if res.RowsAffected() == 0 {
		log.L().Warn("Job not found in meta (or already deleted)",
			zap.Any("job-id", jobID))
	}

	jm.notifier.Notify(resManager.JobStatusChangeEvent{
		EventType: resManager.JobRemovedEvent,
		JobID:     jobID,
	})
	return nil
}

// QueryJob implements proto/Master.QueryJob
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
	// TODO call jm.notifier.Notify when we want to support "add job" event.
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

// GetJobStatuses returns the status code of all jobs that are not deleted.
func (jm *JobManagerImplV2) GetJobStatuses(
	ctx context.Context,
) (map[libModel.MasterID]libModel.MasterStatusCode, error) {
	jobs, err := jm.frameMetaClient.QueryJobs(ctx)
	if err != nil {
		return nil, err
	}

	ret := make(map[libModel.MasterID]libModel.MasterStatusCode, len(jobs))
	for _, jobMeta := range jobs {
		ret[jobMeta.ID] = jobMeta.StatusCode
	}
	return ret, nil
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
		JobFsm:            NewJobFsm(),
		uuidGen:           uuid.NewGenerator(),
		masterMetaClient:  cli,
		clocker:           clock.New(),
		frameMetaClient:   metaClient,
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}
	impl.BaseMaster = lib.NewBaseMaster(
		dctx,
		impl,
		id,
		lib.JobManager,
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

	// nolint:errcheck
	_, err = dctx.Deps().Construct(func(m p2p.MessageHandlerManager) (p2p.MessageHandlerManager, error) {
		impl.messageAgent = dmpkg.NewMessageAgentImpl(id, impl, m)
		return m, nil
	})
	return impl, err
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

// OnWorkerStatusUpdated implements lib.MasterImpl.OnWorkerStatusUpdated
func (jm *JobManagerImplV2) OnWorkerStatusUpdated(worker lib.WorkerHandle, newStatus *libModel.WorkerStatus) error {
	log.L().Info("on worker status updated", zap.String("worker-id", worker.ID()), zap.Any("status", newStatus))
	return nil
}

// CloseImpl implements lib.MasterImpl.CloseImpl
func (jm *JobManagerImplV2) CloseImpl(ctx context.Context) error {
	jm.notifier.Close()
	return nil
}

// WatchJobStatuses returns a snapshot of job statuses followed by a stream
// of job status changes.
func (jm *JobManagerImplV2) WatchJobStatuses(
	ctx context.Context,
) (resManager.JobStatusesSnapshot, *notifier.Receiver[resManager.JobStatusChangeEvent], error) {
	// We add an explicit deadline to make sure that
	// any potential problem will not block the JobManager forever.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	// Note that the lock is cancellable by the context.
	if ok := jm.jobStatusChangeMu.Lock(ctx); !ok {
		return nil, nil, errors.Trace(ctx.Err())
	}
	defer jm.jobStatusChangeMu.Unlock()

	snapshot, err := jm.GetJobStatuses(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Waits for pending JobStatusChangeEvents to be flushed,
	// so that the new receiver does not receive any stale data.
	err = jm.notifier.Flush(ctx)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}

	receiver := jm.notifier.NewReceiver()
	return snapshot, receiver, nil
}
