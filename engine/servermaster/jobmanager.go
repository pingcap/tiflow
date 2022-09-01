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
	"regexp"
	"sort"
	"time"

	schedModel "github.com/pingcap/tiflow/engine/servermaster/scheduler/model"
	"github.com/pingcap/tiflow/pkg/label"

	"github.com/pingcap/errors"
	"github.com/pingcap/log"
	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/executor/cvs"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/ctxmu"
	resManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
)

// JobManager defines manager of job master
type JobManager interface {
	framework.Master
	JobStats
	pb.JobManagerServer

	GetJobMasterForwardAddress(ctx context.Context, jobID string) (string, error)
	GetJobStatuses(ctx context.Context) (map[frameModel.MasterID]frameModel.MasterStatusCode, error)
	WatchJobStatuses(
		ctx context.Context,
	) (resManager.JobStatusesSnapshot, *notifier.Receiver[resManager.JobStatusChangeEvent], error)
}

const defaultJobMasterCost = 1

var jobNameRegex = regexp.MustCompile(`^\w([-.\w]{0,61}\w)?$`)

// JobManagerImpl is a special job master that manages all the job masters, and notify the offline executor to them.
// worker state transition
// - submit new job, create job master successfully, then adds to the `waitAckJobs`.
// - receive worker online, move job from `waitAckJobs` to `onlineJobs`.
// - receive worker offline, move job from `onlineJobs` to `pendingJobs`.
// - Tick checks `pendingJobs` periodically	and reschedules the jobs.
type JobManagerImpl struct {
	framework.BaseMaster
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
}

// CancelJob implements JobManagerServer.CancelJob.
func (jm *JobManagerImpl) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.Job, error) {
	// FIXME: The JobFsm's status may not be consistent with the database.
	job := jm.JobFsm.QueryOnlineJob(req.Id)
	if job == nil {
		// Check if the job is not found.
		if _, err := jm.frameMetaClient.GetJobByID(ctx, req.Id); pkgOrm.IsNotFoundError(err) {
			return nil, ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: req.Id})
		}
		return nil, ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: req.Id})
	}
	topic := frameModel.WorkerStatusChangeRequestTopic(jm.BaseMaster.MasterID(), job.WorkerHandle().ID())
	msg := &frameModel.StatusChangeRequest{
		SendTime:     jm.clocker.Mono(),
		FromMasterID: jm.BaseMaster.MasterID(),
		Epoch:        jm.BaseMaster.MasterMeta().Epoch,
		ExpectState:  frameModel.WorkerStatusStopped,
	}
	if handle := job.WorkerHandle().Unwrap(); handle != nil {
		err := handle.SendMessage(ctx, topic, msg, true /*nonblocking*/)
		if err != nil {
			return nil, err
		}
		pbJob, err := buildPBJob(job.MasterMeta())
		if err != nil {
			return nil, err
		}
		// TODO: we should persist the job status to the database.
		pbJob.Status = pb.Job_Canceling
		return pbJob, nil
	}
	// The job is a tombstone, which means that the job has already exited.
	return nil, ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: req.Id})
}

// DeleteJob implements JobManagerServer.DeleteJob.
func (jm *JobManagerImpl) DeleteJob(ctx context.Context, req *pb.DeleteJobRequest) (*emptypb.Empty, error) {
	masterMeta, err := jm.frameMetaClient.GetJobByID(ctx, req.Id)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return nil, ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: req.Id})
		}
		return nil, err
	}

	// Only stopped (canceled) jobs can be deleted.
	if masterMeta.StatusCode != frameModel.MasterStatusStopped && masterMeta.StatusCode != frameModel.MasterStatusFinished {
		return nil, ErrJobNotStopped.GenWithStack(&JobNotStoppedError{JobID: req.Id})
	}
	if err := jm.deleteJobMeta(ctx, req.Id); err != nil {
		return nil, err
	}
	return &emptypb.Empty{}, nil
}

func (jm *JobManagerImpl) deleteJobMeta(ctx context.Context, jobID string) error {
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
		log.Warn("Job not found in meta (or already deleted)",
			zap.Any("job-id", jobID))
	}

	jm.notifier.Notify(resManager.JobStatusChangeEvent{
		EventType: resManager.JobRemovedEvent,
		JobID:     jobID,
	})
	return nil
}

// GetJob implements JobManagerServer.GetJob.
func (jm *JobManagerImpl) GetJob(ctx context.Context, req *pb.GetJobRequest) (*pb.Job, error) {
	masterMeta, err := jm.frameMetaClient.GetJobByID(ctx, req.Id)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return nil, ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: req.Id})
		}
		return nil, err
	}

	return buildPBJob(masterMeta)
}

// CreateJob implements JobManagerServer.CreateJob.
func (jm *JobManagerImpl) CreateJob(ctx context.Context, req *pb.CreateJobRequest) (*pb.Job, error) {
	if err := validateCreateJobRequest(req); err != nil {
		return nil, err
	}

	selectors, err := convertSelectors(req)
	if err != nil {
		return nil, err
	}

	// TODO call jm.notifier.Notify when we want to support "add job" event.
	log.Info("create job", zap.String("config", string(req.Job.Config)),
		zap.String("tenant_id", req.TenantId), zap.String("project_id", req.ProjectId))

	job := req.Job
	if job.Id == "" {
		job.Id = jm.uuidGen.NewString()
	}

	meta := &frameModel.MasterMetaKVData{
		ProjectID: tenant.NewProjectInfo(
			req.TenantId,
			req.ProjectId,
		).UniqueID(),
		ID:         job.Id,
		Config:     job.Config,
		StatusCode: frameModel.MasterStatusUninit,
		Ext: frameModel.MasterMetaExt{
			Selectors: selectors,
		},
	}
	switch job.Type {
	case pb.Job_CVSDemo:
		// TODO: check config is valid, refine it later
		extConfig := &cvs.Config{}
		if err := json.Unmarshal(job.Config, extConfig); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to decode config: %v", err)
		}
		meta.Tp = framework.CvsJobMaster
	case pb.Job_DM:
		meta.Tp = framework.DMJobMaster
	case pb.Job_FakeJob:
		meta.Tp = framework.FakeJobMaster
	default:
		return nil, status.Errorf(codes.InvalidArgument, "job type %v is not supported", job.Type)
	}

	// Store job master metadata before creating it.
	// FIXME: note the following two operations are not atomic. We should use
	//  a transaction or an insert statement to avoid inconsistent result.
	if _, err := jm.frameMetaClient.GetJobByID(ctx, job.Id); err == nil {
		return nil, ErrJobAlreadyExists.GenWithStack(&JobAlreadyExistsError{JobID: job.Id})
	}
	if err := metadata.StoreMasterMeta(ctx, jm.frameMetaClient, meta); err != nil {
		return nil, err
	}

	// TODO: Refine me. split the BaseMaster
	defaultMaster, ok := jm.BaseMaster.(interface {
		SetProjectInfo(frameModel.MasterID, tenant.ProjectInfo)
	})
	if ok {
		defaultMaster.SetProjectInfo(meta.ID, tenant.NewProjectInfo(req.TenantId, req.ProjectId))
	} else {
		log.Error("jobmanager don't have the 'SetProjectInfo' interface",
			zap.String("masterID", meta.ID),
			zap.Any("projectInfo", tenant.NewProjectInfo(req.TenantId, req.ProjectId)))
	}

	// CreateWorker here is to create job master actually
	// TODO: use correct worker cost
	workerID, err := jm.BaseMaster.CreateWorkerV2(
		meta.Tp, meta,
		framework.CreateWorkerWithCost(defaultJobMasterCost),
		framework.CreateWorkerWithSelectors(selectors...))
	if err != nil {
		err2 := metadata.DeleteMasterMeta(ctx, jm.frameMetaClient, meta.ID)
		if err2 != nil {
			// TODO: add more GC mechanism if master meta is failed to delete
			log.Error("failed to delete master meta", zap.Error(err2))
		}

		log.Error("create job master met error", zap.Error(err))
		return nil, err
	}

	if workerID != job.Id {
		log.Panic("job id is not equal to worker id of job master", zap.String("job-id", job.Id), zap.String("worker-id", workerID))
	}
	jm.JobFsm.JobDispatched(meta, false /*addFromFailover*/)

	return buildPBJob(meta)
}

func validateCreateJobRequest(req *pb.CreateJobRequest) error {
	if req.Job == nil {
		return status.Error(codes.InvalidArgument, "job must not be nil")
	}
	if req.Job.Id != "" && !jobNameRegex.MatchString(req.Job.Id) {
		return status.Errorf(codes.InvalidArgument, "job id must match %s", jobNameRegex.String())
	}
	if req.Job.Type == pb.Job_TypeUnknown {
		return status.Error(codes.InvalidArgument, "job type must be specified")
	}
	return nil
}

func convertSelectors(req *pb.CreateJobRequest) ([]*label.Selector, error) {
	if len(req.GetJob().Selectors) == 0 {
		return nil, nil
	}

	ret := make([]*label.Selector, 0, len(req.GetJob().Selectors))
	for _, pbSel := range req.Job.Selectors {
		sel, err := schedModel.SelectorFromPB(pbSel)
		if err != nil {
			return nil, err
		}
		if err := sel.Validate(); err != nil {
			return nil, err
		}
		ret = append(ret, sel)
	}
	return ret, nil
}

// ListJobs implements JobManagerServer.ListJobs.
func (jm *JobManagerImpl) ListJobs(ctx context.Context, req *pb.ListJobsRequest) (*pb.ListJobsResponse, error) {
	masterMetas, err := jm.frameMetaClient.QueryJobs(ctx)
	if err != nil {
		return nil, err
	}

	sort.Slice(masterMetas, func(i, j int) bool {
		return masterMetas[i].ID < masterMetas[j].ID
	})

	resp := &pb.ListJobsResponse{
		NextPageToken: req.PageToken,
	}
	firstIdx := sort.Search(len(masterMetas), func(i int) bool {
		return masterMetas[i].ID > req.PageToken
	})
	for i := firstIdx; i < len(masterMetas); i++ {
		if masterMetas[i].Tp == framework.JobManager {
			continue
		}
		job, err := buildPBJob(masterMetas[i])
		if err != nil {
			return nil, err
		}
		resp.NextPageToken = job.Id
		resp.Jobs = append(resp.Jobs, job)
		if req.PageSize > 0 && int32(len(resp.Jobs)) >= req.PageSize {
			break
		}
	}

	return resp, nil
}

func buildPBJob(masterMeta *frameModel.MasterMetaKVData) (*pb.Job, error) {
	var jobType pb.Job_Type
	switch tp := framework.MustConvertWorkerType2JobType(masterMeta.Tp); tp {
	case engineModel.JobTypeCVSDemo:
		jobType = pb.Job_CVSDemo
	case engineModel.JobTypeDM:
		jobType = pb.Job_DM
	case engineModel.JobTypeCDC:
		jobType = pb.Job_CDC
	case engineModel.JobTypeFakeJob:
		jobType = pb.Job_FakeJob
	default:
		return nil, errors.Errorf("job %s has unknown type %v", masterMeta.ID, masterMeta.Tp)
	}

	var jobStatus pb.Job_Status
	switch masterMeta.StatusCode {
	case frameModel.MasterStatusUninit:
		jobStatus = pb.Job_Created
	case frameModel.MasterStatusInit:
		jobStatus = pb.Job_Running
	case frameModel.MasterStatusFinished:
		jobStatus = pb.Job_Finished
	case frameModel.MasterStatusStopped:
		jobStatus = pb.Job_Canceled
	default:
		return nil, errors.Errorf("job %s has unknown type %v", masterMeta.ID, masterMeta.StatusCode)
	}

	return &pb.Job{
		Id:     masterMeta.ID,
		Type:   jobType,
		Status: jobStatus,
		Config: masterMeta.Config,
		Error:  nil, // TODO: Fill error field.
	}, nil
}

// GetJobMasterForwardAddress implements JobManager.GetJobMasterForwardAddress.
func (jm *JobManagerImpl) GetJobMasterForwardAddress(ctx context.Context, jobID string) (string, error) {
	// Always query from database. Master meta in JobFsm may be out of date.
	masterMeta, err := jm.frameMetaClient.GetJobByID(ctx, jobID)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return "", ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: jobID})
		}
		return "", err
	}
	if masterMeta.StatusCode != frameModel.MasterStatusInit || jm.JobFsm.QueryOnlineJob(jobID) == nil {
		return "", ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: jobID})
	}
	return masterMeta.Addr, nil
}

// GetJobStatuses returns the status code of all jobs that are not deleted.
func (jm *JobManagerImpl) GetJobStatuses(
	ctx context.Context,
) (map[frameModel.MasterID]frameModel.MasterStatusCode, error) {
	// BUG? NO filter in the implement
	jobs, err := jm.frameMetaClient.QueryJobs(ctx)
	if err != nil {
		return nil, err
	}

	ret := make(map[frameModel.MasterID]frameModel.MasterStatusCode, len(jobs))
	for _, jobMeta := range jobs {
		ret[jobMeta.ID] = jobMeta.StatusCode
	}
	return ret, nil
}

// NewJobManagerImpl creates a new JobManagerImpl instance
func NewJobManagerImpl(
	dctx *dcontext.Context,
	id frameModel.MasterID,
) (*JobManagerImpl, error) {
	metaCli, err := dctx.Deps().Construct(func(cli pkgOrm.Client) (pkgOrm.Client, error) {
		return cli, nil
	})
	if err != nil {
		return nil, err
	}

	metaClient := metaCli.(pkgOrm.Client)
	cli := metadata.NewMasterMetadataClient(id, metaClient)
	impl := &JobManagerImpl{
		JobFsm:            NewJobFsm(),
		uuidGen:           uuid.NewGenerator(),
		masterMetaClient:  cli,
		clocker:           clock.New(),
		frameMetaClient:   metaClient,
		jobStatusChangeMu: ctxmu.New(),
		notifier:          notifier.NewNotifier[resManager.JobStatusChangeEvent](),
	}
	impl.BaseMaster = framework.NewBaseMaster(
		dctx,
		impl,
		id,
		framework.JobManager,
	)

	// Note the meta data of job manager is not used, it is safe to overwrite it
	// every time a new server master leader is elected. And we always mark the
	// Initialized to true in order to trigger OnMasterRecovered of job manager.
	meta := impl.MasterMeta()
	meta.StatusCode = frameModel.MasterStatusInit
	err = metadata.StoreMasterMeta(dctx, impl.frameMetaClient, meta)
	if err != nil {
		return nil, err
	}
	err = impl.BaseMaster.Init(dctx)
	if err != nil {
		_ = impl.BaseMaster.Close(dctx)
		return nil, err
	}

	return impl, err
}

// InitImpl implements frame.MasterImpl.InitImpl
func (jm *JobManagerImpl) InitImpl(ctx context.Context) error {
	return nil
}

// Tick implements frame.MasterImpl.Tick
func (jm *JobManagerImpl) Tick(ctx context.Context) error {
	filterQuotaError := func(err error) (exceedQuota bool, retErr error) {
		if err == nil {
			return false, nil
		}
		if derrors.ErrMasterConcurrencyExceeded.Equal(err) {
			log.Warn("create worker exceeds quota, retry later", zap.Error(err))
			return true, nil
		}
		return false, err
	}

	err := jm.JobFsm.IterPendingJobs(
		func(job *frameModel.MasterMetaKVData) (string, error) {
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
			func(job *frameModel.MasterMetaKVData) (string, error) {
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

// OnMasterRecovered implements frame.MasterImpl.OnMasterRecovered
func (jm *JobManagerImpl) OnMasterRecovered(ctx context.Context) error {
	jobs, err := jm.masterMetaClient.LoadAllMasters(ctx)
	if err != nil {
		return err
	}

	// TODO: refine me, split the BaseMaster interface
	impl, ok := jm.BaseMaster.(interface {
		InitProjectInfosAfterRecover([]*frameModel.MasterMetaKVData)
	})
	if !ok {
		log.Panic("unfound interface for BaseMaster", zap.String("interface", "InitProjectInfosAfterRecover"))
		return derrors.ErrMasterInterfaceNotFound.GenWithStackByArgs()
	}
	impl.InitProjectInfosAfterRecover(jobs)

	for _, job := range jobs {
		if job.Tp == framework.JobManager {
			continue
		}
		// TODO: filter the job in backend
		if job.StatusCode == frameModel.MasterStatusFinished || job.StatusCode == frameModel.MasterStatusStopped {
			log.Info("skip finished or stopped job", zap.Any("job", job))
			continue
		}
		jm.JobFsm.JobDispatched(job, true /*addFromFailover*/)
		log.Info("recover job, move it to WaitAck job queue", zap.Any("job", job))
	}
	return nil
}

// OnWorkerDispatched implements frame.MasterImpl.OnWorkerDispatched
func (jm *JobManagerImpl) OnWorkerDispatched(worker framework.WorkerHandle, result error) error {
	if result != nil {
		log.Warn("dispatch worker met error", zap.Error(result))
		return jm.JobFsm.JobDispatchFailed(worker)
	}
	return nil
}

// OnWorkerOnline implements frame.MasterImpl.OnWorkerOnline
func (jm *JobManagerImpl) OnWorkerOnline(worker framework.WorkerHandle) error {
	log.Info("on worker online", zap.Any("id", worker.ID()))
	return jm.JobFsm.JobOnline(worker)
}

// OnWorkerOffline implements frame.MasterImpl.OnWorkerOffline
func (jm *JobManagerImpl) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	needFailover := true
	if derrors.ErrWorkerFinish.Equal(reason) {
		log.Info("job master finished", zap.String("id", worker.ID()))
		needFailover = false
	} else if derrors.ErrWorkerStop.Equal(reason) {
		log.Info("job master stopped", zap.String("id", worker.ID()))
		needFailover = false
	} else {
		log.Info("on worker offline", zap.Any("id", worker.ID()), zap.Any("reason", reason))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := worker.GetTombstone().CleanTombstone(ctx); err != nil {
		return err
	}
	jm.JobFsm.JobOffline(worker, needFailover)
	return nil
}

// OnWorkerMessage implements frame.MasterImpl.OnWorkerMessage
func (jm *JobManagerImpl) OnWorkerMessage(worker framework.WorkerHandle, topic p2p.Topic, message interface{}) error {
	log.Info("on worker message", zap.Any("id", worker.ID()), zap.Any("topic", topic), zap.Any("message", message))
	return nil
}

// OnWorkerStatusUpdated implements frame.MasterImpl.OnWorkerStatusUpdated
func (jm *JobManagerImpl) OnWorkerStatusUpdated(worker framework.WorkerHandle, newStatus *frameModel.WorkerStatus) error {
	log.Info("on worker status updated", zap.String("worker-id", worker.ID()), zap.Any("status", newStatus))
	return nil
}

// CloseImpl implements frame.MasterImpl.CloseImpl
func (jm *JobManagerImpl) CloseImpl(ctx context.Context) error {
	jm.notifier.Close()
	return nil
}

// WatchJobStatuses returns a snapshot of job statuses followed by a stream
// of job status changes.
func (jm *JobManagerImpl) WatchJobStatuses(
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
