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
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	pb "github.com/pingcap/tiflow/engine/enginepb"
	"github.com/pingcap/tiflow/engine/executor/cvs"
	"github.com/pingcap/tiflow/engine/framework"
	"github.com/pingcap/tiflow/engine/framework/metadata"
	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	pkgClient "github.com/pingcap/tiflow/engine/pkg/client"
	"github.com/pingcap/tiflow/engine/pkg/clock"
	dcontext "github.com/pingcap/tiflow/engine/pkg/context"
	"github.com/pingcap/tiflow/engine/pkg/ctxmu"
	resManager "github.com/pingcap/tiflow/engine/pkg/externalresource/manager"
	engineHTTPUtil "github.com/pingcap/tiflow/engine/pkg/httputil"
	"github.com/pingcap/tiflow/engine/pkg/notifier"
	pkgOrm "github.com/pingcap/tiflow/engine/pkg/orm"
	"github.com/pingcap/tiflow/engine/pkg/p2p"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
	"github.com/pingcap/tiflow/engine/servermaster/jobop"
	derrors "github.com/pingcap/tiflow/pkg/errors"
	"github.com/pingcap/tiflow/pkg/httputil"
	"github.com/pingcap/tiflow/pkg/notify"
	"github.com/pingcap/tiflow/pkg/uuid"
)

// JobManager defines manager of job master
type JobManager interface {
	framework.Master
	JobStats
	pb.JobManagerServer

	GetJobMasterForwardAddress(ctx context.Context, jobID string) (string, error)
	GetJobStatuses(ctx context.Context) (map[frameModel.MasterID]frameModel.MasterState, error)
	UpdateJobStatus(ctx context.Context, jobID frameModel.MasterID, errMsg string, code frameModel.MasterState) error
	WatchJobStatuses(
		ctx context.Context,
	) (resManager.JobStatusesSnapshot, *notifier.Receiver[resManager.JobStatusChangeEvent], error)
}

const defaultJobMasterCost = 1

const jobOperateInterval = time.Second * 15

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

	masterMetaClient    *metadata.MasterMetadataClient
	uuidGen             uuid.Generator
	clocker             clock.Clock
	frameMetaClient     pkgOrm.Client
	tombstoneCleaned    bool
	jobOperator         jobop.JobOperator
	jobOperatorNotifier *notify.Notifier
	JobBackoffMgr       jobop.BackoffManager

	// jobStatusChangeMu must be taken when we try to create, delete,
	// pause or resume a job.
	// NOTE The concurrency management for the JobManager is not complete
	// yet. We are prioritizing implementing all features.
	// TODO We might add a pending operation queue in the future.
	jobStatusChangeMu *ctxmu.CtxMutex
	notifier          *notifier.Notifier[resManager.JobStatusChangeEvent]
	wg                *errgroup.Group

	// http client for the job detail
	jobHTTPClient engineHTTPUtil.JobHTTPClient
}

// CancelJob implements JobManagerServer.CancelJob.
func (jm *JobManagerImpl) CancelJob(ctx context.Context, req *pb.CancelJobRequest) (*pb.Job, error) {
	meta, err := jm.frameMetaClient.GetJobByID(ctx, req.Id)
	if err != nil {
		if pkgOrm.IsNotFoundError(err) {
			return nil, ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: req.Id})
		}
		return nil, err
	}

	pbJob, err := buildPBJob(meta)
	if err != nil {
		return nil, err
	}
	if pbJob.State == pb.Job_Finished || pbJob.State == pb.Job_Canceled {
		return pbJob, nil
	}

	if err := jm.jobOperator.MarkJobCanceling(ctx, req.Id); err != nil {
		return nil, err
	}
	jm.jobOperatorNotifier.Notify()
	pbJob.State = pb.Job_Canceling
	return pbJob, nil
}

// SendCancelJobMessage implements operateRouter.SendCancelJobMessage
func (jm *JobManagerImpl) SendCancelJobMessage(ctx context.Context, jobID string) error {
	job := jm.JobFsm.QueryOnlineJob(jobID)
	if job == nil {
		if _, err := jm.frameMetaClient.GetJobByID(ctx, jobID); pkgOrm.IsNotFoundError(err) {
			return ErrJobNotFound.GenWithStack(&JobNotFoundError{JobID: jobID})
		}
		return ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: jobID})
	}

	topic := frameModel.WorkerStatusChangeRequestTopic(jm.BaseMaster.MasterID(), job.WorkerHandle().ID())
	msg := &frameModel.StatusChangeRequest{
		SendTime:     jm.clocker.Mono(),
		FromMasterID: jm.BaseMaster.MasterID(),
		Epoch:        jm.BaseMaster.MasterMeta().Epoch,
		ExpectState:  frameModel.WorkerStateStopped,
	}
	handle := job.WorkerHandle().Unwrap()
	if handle == nil {
		return ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: jobID})
	}
	return handle.SendMessage(ctx, topic, msg, true /*nonblocking*/)
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
	if masterMeta.State != frameModel.MasterStateStopped && masterMeta.State != frameModel.MasterStateFinished {
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

func canQueryJobDetail(masterState frameModel.MasterState) bool {
	return masterState == frameModel.MasterStateInit
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

	// if job status is running, forward the request to jobmaster openapi
	if canQueryJobDetail(masterMeta.State) {
		detail, err := jm.jobHTTPClient.GetJobDetail(ctx, masterMeta.Addr, req.Id)
		setDetailToMasterMeta(masterMeta, detail, err)
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
	log.Info("create job", zap.Any("job", req.Job),
		zap.String("tenant_id", req.TenantId), zap.String("project_id", req.ProjectId))

	job := req.Job
	if job.Id == "" {
		job.Id = jm.uuidGen.NewString()
	}

	meta := &frameModel.MasterMeta{
		ProjectID: tenant.NewProjectInfo(
			req.TenantId,
			req.ProjectId,
		).UniqueID(),
		ID:     job.Id,
		Config: job.Config,
		State:  frameModel.MasterStateUninit,
		Ext: frameModel.MasterMetaExt{
			Selectors: selectors,
		},
	}
	switch job.Type {
	case pb.Job_CVSDemo:
		extConfig := &cvs.Config{}
		if err := json.Unmarshal(job.Config, extConfig); err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "failed to decode config: %v", err)
		}
		meta.Type = frameModel.CvsJobMaster
	case pb.Job_DM:
		meta.Type = frameModel.DMJobMaster
	case pb.Job_FakeJob:
		meta.Type = frameModel.FakeJobMaster
	default:
		return nil, status.Errorf(codes.InvalidArgument, "job type %v is not supported", job.Type)
	}

	// create job master metadata before creating it.
	if err := jm.frameMetaClient.InsertJob(ctx, meta); err != nil {
		if pkgOrm.IsDuplicateEntryError(err) {
			return nil, ErrJobAlreadyExists.GenWithStack(&JobAlreadyExistsError{JobID: job.Id})
		}
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
		meta.Type, meta,
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
	var err error
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

	var job *pb.Job
	for i := firstIdx; i < len(masterMetas); i++ {
		if masterMetas[i].Type == frameModel.JobManager {
			continue
		}

		// if job status is running, forward the request to jobmaster openapi
		if canQueryJobDetail(masterMetas[i].State) {
			detail, errJob := jm.jobHTTPClient.GetJobDetail(ctx, masterMetas[i].Addr, masterMetas[i].ID)
			setDetailToMasterMeta(masterMetas[i], detail, errJob)
		}

		job, err = buildPBJob(masterMetas[i])
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

// setDetailToMasterMeta sets the results from GetJobDetail to master meta
func setDetailToMasterMeta(masterMeta *frameModel.MasterMeta, detail []byte, errJob error) {
	if errJob != nil {
		// Currently, we simply ignore 404 error
		if derrors.ErrJobManagerRespStatusCode404.Equal(errJob) {
			log.Warn("get job detail from jobmaster fail", zap.Error(errJob))
			return
		}

		// TODO: deal the response body here after we has normalized the error response format
		log.Error("get job detail from jobmaster fail", zap.Error(errJob))
		// TODO: we should not put the error message here directly
		masterMeta.ErrorMsg = errJob.Error()
	} else if detail != nil {
		masterMeta.Detail = detail
	}
}

func buildPBJob(masterMeta *frameModel.MasterMeta) (*pb.Job, error) {
	var jobType pb.Job_Type
	switch tp := framework.MustConvertWorkerType2JobType(masterMeta.Type); tp {
	case engineModel.JobTypeCVSDemo:
		jobType = pb.Job_CVSDemo
	case engineModel.JobTypeDM:
		jobType = pb.Job_DM
	case engineModel.JobTypeCDC:
		jobType = pb.Job_CDC
	case engineModel.JobTypeFakeJob:
		jobType = pb.Job_FakeJob
	default:
		return nil, errors.Errorf("job %s has unknown type %v", masterMeta.ID, masterMeta.Type)
	}

	var jobStatus pb.Job_State
	switch masterMeta.State {
	case frameModel.MasterStateUninit:
		jobStatus = pb.Job_Created
	case frameModel.MasterStateInit:
		jobStatus = pb.Job_Running
	case frameModel.MasterStateFinished:
		jobStatus = pb.Job_Finished
	case frameModel.MasterStateStopped:
		jobStatus = pb.Job_Canceled
	case frameModel.MasterStateFailed:
		jobStatus = pb.Job_Failed
	default:
		return nil, errors.Errorf("job %s has unknown type %v", masterMeta.ID, masterMeta.State)
	}

	var selectors []*pb.Selector
	for _, sel := range masterMeta.Ext.Selectors {
		pbSel, err := schedModel.SelectorToPB(sel)
		if err != nil {
			return nil, errors.Annotate(err, "buildPBJob")
		}
		selectors = append(selectors, pbSel)
	}
	return &pb.Job{
		Id:     masterMeta.ID,
		Type:   jobType,
		State:  jobStatus,
		Config: masterMeta.Config,
		Detail: masterMeta.Detail,
		Error: &pb.Error{
			Message: masterMeta.ErrorMsg,
		},
		Selectors: selectors,
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
	if masterMeta.State != frameModel.MasterStateInit || jm.JobFsm.QueryOnlineJob(jobID) == nil {
		return "", ErrJobNotRunning.GenWithStack(&JobNotRunningError{JobID: jobID})
	}
	return masterMeta.Addr, nil
}

// GetJobStatuses returns the status code of all jobs that are not deleted.
func (jm *JobManagerImpl) GetJobStatuses(
	ctx context.Context,
) (map[frameModel.MasterID]frameModel.MasterState, error) {
	// BUG? NO filter in the implement
	jobs, err := jm.frameMetaClient.QueryJobs(ctx)
	if err != nil {
		return nil, err
	}

	ret := make(map[frameModel.MasterID]frameModel.MasterState, len(jobs))
	for _, jobMeta := range jobs {
		ret[jobMeta.ID] = jobMeta.State
	}
	return ret, nil
}

// UpdateJobStatus implements JobManager.UpdateJobStatus
func (jm *JobManagerImpl) UpdateJobStatus(
	ctx context.Context, jobID frameModel.MasterID, errMsg string, code frameModel.MasterState,
) error {
	// Note since the job is not online, it is safe to get from metastore and then update
	meta, err := jm.frameMetaClient.GetJobByID(ctx, jobID)
	if err != nil {
		return err
	}
	meta.ErrorMsg = errMsg
	meta.State = code
	return jm.frameMetaClient.UpsertJob(ctx, meta)
}

// NewJobManagerImpl creates a new JobManagerImpl instance
func NewJobManagerImpl(
	dctx *dcontext.Context,
	id frameModel.MasterID,
	backoffConfig *jobop.BackoffConfig,
) (*JobManagerImpl, error) {
	metaCli, err := dctx.Deps().Construct(func(cli pkgOrm.Client) (pkgOrm.Client, error) {
		return cli, nil
	})
	if err != nil {
		return nil, err
	}

	metaClient := metaCli.(pkgOrm.Client)
	cli := metadata.NewMasterMetadataClient(id, metaClient)

	httpCli, err := httputil.NewClient(nil)
	if err != nil {
		return nil, err
	}

	clocker := clock.New()
	impl := &JobManagerImpl{
		JobFsm:              NewJobFsm(),
		uuidGen:             uuid.NewGenerator(),
		masterMetaClient:    cli,
		clocker:             clocker,
		frameMetaClient:     metaClient,
		jobStatusChangeMu:   ctxmu.New(),
		notifier:            notifier.NewNotifier[resManager.JobStatusChangeEvent](),
		jobOperatorNotifier: new(notify.Notifier),
		jobHTTPClient:       engineHTTPUtil.NewJobHTTPClient(httpCli),
		JobBackoffMgr:       jobop.NewBackoffManagerImpl(clocker, backoffConfig),
	}
	impl.BaseMaster = framework.NewBaseMaster(
		dctx,
		impl,
		id,
		frameModel.JobManager,
	)
	impl.jobOperator = jobop.NewJobOperatorImpl(metaClient, impl)
	wg, ctx := errgroup.WithContext(dctx)
	impl.wg = wg

	// Note the meta data of job manager is not used, it is safe to overwrite it
	// every time a new server master leader is elected. And we always mark the
	// Initialized to true in order to trigger OnMasterRecovered of job manager.
	meta := impl.MasterMeta()
	meta.State = frameModel.MasterStateInit
	err = metadata.StoreMasterMeta(ctx, impl.frameMetaClient, meta)
	if err != nil {
		return nil, err
	}
	err = impl.BaseMaster.Init(ctx)
	if err != nil {
		_ = impl.BaseMaster.Close(ctx)
		return nil, err
	}
	impl.bgJobOperatorLoop(ctx)

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
		func(job *frameModel.MasterMeta) (string, error) {
			isJobCanceling := jm.jobOperator.IsJobCanceling(ctx, job.ID)
			if isJobCanceling || jm.JobBackoffMgr.Terminate(job.ID) {
				state := frameModel.MasterStateFailed
				if isJobCanceling {
					state = frameModel.MasterStateStopped
				}
				if err := jm.terminateJob(ctx, job.ErrorMsg, job.ID, state); err != nil {
					return "", err
				}
				return "", derrors.ErrMasterCreateWorkerTerminate.FastGenByArgs()
			}
			if !jm.JobBackoffMgr.Allow(job.ID) {
				return "", derrors.ErrMasterCreateWorkerBackoff.FastGenByArgs()
			}
			return jm.BaseMaster.CreateWorker(
				job.Type, job, defaultJobMasterCost)
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
			func(job *frameModel.MasterMeta) (string, error) {
				return jm.BaseMaster.CreateWorker(
					job.Type, job, defaultJobMasterCost)
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
		InitProjectInfosAfterRecover([]*frameModel.MasterMeta)
	})
	if !ok {
		log.Panic("unfound interface for BaseMaster", zap.String("interface", "InitProjectInfosAfterRecover"))
		return derrors.ErrMasterInterfaceNotFound.GenWithStackByArgs()
	}
	impl.InitProjectInfosAfterRecover(jobs)

	for _, job := range jobs {
		if job.Type == frameModel.JobManager {
			continue
		}
		// TODO: filter the job in backend
		if job.State.IsTerminatedState() {
			log.Info("skip job in terminated status", zap.Any("job", job))
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
		if errIn, ok := pkgClient.ErrCreateWorkerTerminate.Convert(result); ok {
			if err := jm.terminateJob(
				context.Background(), errIn.Details, worker.ID(), frameModel.MasterStateFailed,
			); err != nil {
				return err
			}
			jm.JobFsm.JobOffline(worker, false /* needFailover */)
			return nil
		}
		log.Warn("dispatch worker met error", zap.Error(result))
		jm.JobBackoffMgr.JobFail(worker.ID())
		return jm.JobFsm.JobDispatchFailed(worker)
	}
	return nil
}

// OnWorkerOnline implements frame.MasterImpl.OnWorkerOnline
func (jm *JobManagerImpl) OnWorkerOnline(worker framework.WorkerHandle) error {
	log.Info("on worker online", zap.Any("id", worker.ID()))
	jm.JobBackoffMgr.JobOnline(worker.ID())
	return jm.JobFsm.JobOnline(worker)
}

// OnWorkerOffline implements frame.MasterImpl.OnWorkerOffline
func (jm *JobManagerImpl) OnWorkerOffline(worker framework.WorkerHandle, reason error) error {
	needFailover := true
	if derrors.ErrWorkerFinish.Equal(reason) {
		log.Info("job master finished", zap.String("id", worker.ID()))
		needFailover = false
	} else if derrors.ErrWorkerCancel.Equal(reason) {
		log.Info("job master canceled", zap.String("id", worker.ID()))
		needFailover = false
		jm.jobOperatorNotifier.Notify()
	} else if derrors.ErrWorkerFailed.Equal(reason) {
		log.Info("job master failed permanently", zap.String("id", worker.ID()))
		needFailover = false
	} else {
		log.Info("on worker offline", zap.Any("id", worker.ID()), zap.Any("reason", reason))
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	if err := worker.GetTombstone().CleanTombstone(ctx); err != nil {
		return err
	}
	if needFailover {
		jm.JobBackoffMgr.JobFail(worker.ID())
	} else {
		jm.JobBackoffMgr.JobTerminate(worker.ID())
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
func (jm *JobManagerImpl) CloseImpl(ctx context.Context) {
	jm.notifier.Close()
	jm.jobHTTPClient.Close()
	jm.jobOperatorNotifier.Close()
}

// StopImpl implements frame.MasterImpl.StopImpl
func (jm *JobManagerImpl) StopImpl(ctx context.Context) {
	jm.CloseImpl(ctx)
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

func (jm *JobManagerImpl) bgJobOperatorLoop(ctx context.Context) {
	jm.wg.Go(func() error {
		defer func() {
			log.Info("job manager job operator loop exited")
		}()
		receiver, err := jm.jobOperatorNotifier.NewReceiver(jobOperateInterval)
		if err != nil {
			return err
		}
		defer receiver.Stop()
		for {
			select {
			case <-ctx.Done():
				return errors.Trace(ctx.Err())
			case _, ok := <-receiver.C:
				if !ok {
					return nil
				}
			}
			if err := jm.jobOperator.Tick(ctx); err != nil {
				// error returns from Tick is only caused by metastore error, so
				// only log it and retry later.
				log.Warn("job operator tick with error", zap.Error(err))
			}
		}
	})
}

func (jm *JobManagerImpl) terminateJob(
	ctx context.Context, errMsg string, jobID string, state frameModel.MasterState,
) error {
	log.Info("job master terminated", zap.String("job-id", jobID),
		zap.String("error", errMsg), zap.Any("state", state))
	ctx, cancel := context.WithTimeout(ctx, time.Second*5)
	defer cancel()
	return jm.UpdateJobStatus(ctx, jobID, errMsg, state)
}
