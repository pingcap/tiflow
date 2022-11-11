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

package orm

import (
	"context"
	"database/sql"
	"time"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	engineModel "github.com/pingcap/tiflow/engine/model"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
	metaModel "github.com/pingcap/tiflow/engine/pkg/meta/model"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/pkg/errors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var globalModels = []interface{}{
	&model.ProjectInfo{},
	&model.ProjectOperation{},
	&frameModel.MasterMeta{},
	&frameModel.WorkerStatus{},
	&resModel.ResourceMeta{},
	&model.LogicEpoch{},
	&model.JobOp{},
	&model.Executor{},
}

// TODO: retry and idempotent??
// TODO: split different client to module

type (
	// ResourceMeta is the alias of resModel.ResourceMeta
	ResourceMeta = resModel.ResourceMeta
	// ResourceKey is the alias of resModel.ResourceKey
	ResourceKey = resModel.ResourceKey
)

// TimeRange defines a time range with [start, end] time
type TimeRange struct {
	start time.Time
	end   time.Time
}

// Client defines an interface that has the ability to manage every kind of
// logic abstraction in metastore, including project, project op, job, worker
// and resource
type Client interface {
	metaModel.Client
	// ProjectClient is the interface to operate project.
	ProjectClient
	// ProjectOperationClient is the client to operate project operation.
	ProjectOperationClient
	// JobClient is the interface to operate job info.
	JobClient
	// WorkerClient is the client to operate worker info.
	WorkerClient
	// ResourceClient is the interface to operate resource.
	ResourceClient
	// JobOpClient is the client to operate job operation.
	JobOpClient
	// ExecutorClient is the client to operate executor info.
	ExecutorClient
}

// ProjectClient defines interface that manages project in metastore
type ProjectClient interface {
	CreateProject(ctx context.Context, project *model.ProjectInfo) error
	DeleteProject(ctx context.Context, projectID string) error
	QueryProjects(ctx context.Context) ([]*model.ProjectInfo, error)
	GetProjectByID(ctx context.Context, projectID string) (*model.ProjectInfo, error)
}

// ProjectOperationClient defines interface that manages project operation in metastore
// TODO: support pagination and cursor here
// support `order by time desc limit N`
type ProjectOperationClient interface {
	CreateProjectOperation(ctx context.Context, op *model.ProjectOperation) error
	QueryProjectOperations(ctx context.Context, projectID string) ([]*model.ProjectOperation, error)
	QueryProjectOperationsByTimeRange(ctx context.Context, projectID string, tr TimeRange) ([]*model.ProjectOperation, error)
}

// JobClient defines interface that manages job in metastore
type JobClient interface {
	InsertJob(ctx context.Context, job *frameModel.MasterMeta) error
	UpsertJob(ctx context.Context, job *frameModel.MasterMeta) error
	UpdateJob(ctx context.Context, jobID string, values model.KeyValueMap) error
	DeleteJob(ctx context.Context, jobID string) (Result, error)

	GetJobByID(ctx context.Context, jobID string) (*frameModel.MasterMeta, error)
	QueryJobs(ctx context.Context) ([]*frameModel.MasterMeta, error)
	QueryJobsByProjectID(ctx context.Context, projectID string) ([]*frameModel.MasterMeta, error)
	QueryJobsByState(ctx context.Context, jobID string, state int) ([]*frameModel.MasterMeta, error)
}

// WorkerClient defines interface that manages worker in metastore
type WorkerClient interface {
	UpsertWorker(ctx context.Context, worker *frameModel.WorkerStatus) error
	UpdateWorker(ctx context.Context, worker *frameModel.WorkerStatus) error
	DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error)
	GetWorkerByID(ctx context.Context, masterID string, workerID string) (*frameModel.WorkerStatus, error)
	QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*frameModel.WorkerStatus, error)
	QueryWorkersByState(ctx context.Context, masterID string, state int) ([]*frameModel.WorkerStatus, error)
}

// ResourceClient defines interface that manages resource in metastore
type ResourceClient interface {
	CreateResource(ctx context.Context, resource *ResourceMeta) error
	UpsertResource(ctx context.Context, resource *ResourceMeta) error
	UpdateResource(ctx context.Context, resource *ResourceMeta) error

	GetResourceByID(ctx context.Context, resourceKey ResourceKey) (*ResourceMeta, error)
	QueryResources(ctx context.Context) ([]*ResourceMeta, error)
	QueryResourcesByJobID(ctx context.Context, jobID string) ([]*ResourceMeta, error)
	QueryResourcesByExecutorIDs(ctx context.Context,
		executorID ...engineModel.ExecutorID) ([]*ResourceMeta, error)

	SetGCPendingByJobs(ctx context.Context, jobIDs ...engineModel.JobID) error
	GetOneResourceForGC(ctx context.Context) (*ResourceMeta, error)

	DeleteResource(ctx context.Context, resourceKey ResourceKey) (Result, error)
	DeleteResourcesByTypeAndExecutorIDs(ctx context.Context,
		resType resModel.ResourceType, executorID ...engineModel.ExecutorID) (Result, error)
}

// JobOpClient defines interface that operates job status (upper logic oriented)
type JobOpClient interface {
	SetJobNoop(ctx context.Context, jobID string) (Result, error)
	SetJobCanceling(ctx context.Context, JobID string) (Result, error)
	SetJobCanceled(ctx context.Context, jobID string) (Result, error)
	QueryJobOp(ctx context.Context, jobID string) (*model.JobOp, error)
	QueryJobOpsByStatus(ctx context.Context, op model.JobOpStatus) ([]*model.JobOp, error)
}

// ExecutorClient defines interface that manages executor information in metastore.
type ExecutorClient interface {
	CreateExecutor(ctx context.Context, executor *model.Executor) error
	UpdateExecutor(ctx context.Context, executor *model.Executor) error
	DeleteExecutor(ctx context.Context, executorID engineModel.ExecutorID) error
	QueryExecutors(ctx context.Context) ([]*model.Executor, error)
}

// NewClient return the client to operate framework metastore
func NewClient(cc metaModel.ClientConn) (Client, error) {
	if cc == nil {
		return nil, errors.ErrMetaParamsInvalid.GenWithStackByArgs("input client conn is nil")
	}

	conn, err := cc.GetConn()
	if err != nil {
		return nil, err
	}

	sqlDB, ok := conn.(*sql.DB)
	if !ok {
		return nil, errors.ErrMetaParamsInvalid.GenWithStack("input client conn is not a sql type:%s",
			cc.StoreType())
	}

	return newClient(sqlDB, cc.StoreType())
}

func newClient(db *sql.DB, storeType metaModel.StoreType) (Client, error) {
	ormDB, err := NewGormDB(db, storeType)
	if err != nil {
		return nil, err
	}

	epCli, err := model.NewEpochClient("" /*jobID*/, ormDB)
	if err != nil {
		return nil, err
	}

	return &metaOpsClient{
		db:          ormDB,
		epochClient: epCli,
	}, nil
}

// metaOpsClient is the meta operations client for framework metastore
type metaOpsClient struct {
	// gorm claim to be thread safe
	db          *gorm.DB
	epochClient model.EpochClient
}

func (c *metaOpsClient) Close() error {
	// DON NOT CLOSE the underlying connection
	return nil
}

// ///////////////////////////// Logic Epoch
func (c *metaOpsClient) GenEpoch(ctx context.Context) (frameModel.Epoch, error) {
	return c.epochClient.GenEpoch(ctx)
}

// /////////////////////// Project Operation
// CreateProject insert the model.ProjectInfo
func (c *metaOpsClient) CreateProject(ctx context.Context, project *model.ProjectInfo) error {
	if project == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input project info is nil")
	}
	if err := c.db.WithContext(ctx).
		Create(project).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteProject delete the model.ProjectInfo
func (c *metaOpsClient) DeleteProject(ctx context.Context, projectID string) error {
	if err := c.db.WithContext(ctx).
		Where("id=?", projectID).
		Delete(&model.ProjectInfo{}).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// QueryProject query all projects
func (c *metaOpsClient) QueryProjects(ctx context.Context) ([]*model.ProjectInfo, error) {
	var projects []*model.ProjectInfo
	if err := c.db.WithContext(ctx).
		Find(&projects).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return projects, nil
}

// GetProjectByID query project by projectID
func (c *metaOpsClient) GetProjectByID(ctx context.Context, projectID string) (*model.ProjectInfo, error) {
	var project model.ProjectInfo
	if err := c.db.WithContext(ctx).
		Where("id = ?", projectID).
		First(&project).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return &project, nil
}

// CreateProjectOperation insert the operation
func (c *metaOpsClient) CreateProjectOperation(ctx context.Context, op *model.ProjectOperation) error {
	if op == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input project operation is nil")
	}

	if err := c.db.WithContext(ctx).
		Create(op).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// QueryProjectOperations query all operations of the projectID
func (c *metaOpsClient) QueryProjectOperations(ctx context.Context, projectID string) ([]*model.ProjectOperation, error) {
	var projectOps []*model.ProjectOperation
	if err := c.db.WithContext(ctx).
		Where("project_id = ?", projectID).
		Find(&projectOps).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return projectOps, nil
}

// QueryProjectOperationsByTimeRange query project operation betweem a time range of the projectID
func (c *metaOpsClient) QueryProjectOperationsByTimeRange(ctx context.Context,
	projectID string, tr TimeRange,
) ([]*model.ProjectOperation, error) {
	var projectOps []*model.ProjectOperation
	if err := c.db.WithContext(ctx).
		Where("project_id = ? AND created_at >= ? AND created_at <= ?", projectID, tr.start, tr.end).
		Find(&projectOps).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return projectOps, nil
}

// ///////////////////////////// Job Operation
// InsertJob insert the jobInfo
func (c *metaOpsClient) InsertJob(ctx context.Context, job *frameModel.MasterMeta) error {
	if job == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}

	if err := c.db.WithContext(ctx).Create(job).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// UpsertJob upsert the jobInfo
func (c *metaOpsClient) UpsertJob(ctx context.Context, job *frameModel.MasterMeta) error {
	if job == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns(frameModel.MasterUpdateColumns),
		}).Create(job).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// UpdateJob update the jobInfo
func (c *metaOpsClient) UpdateJob(
	ctx context.Context, jobID string, values model.KeyValueMap,
) error {
	if err := c.db.WithContext(ctx).
		Model(&frameModel.MasterMeta{}).
		Where("id = ?", jobID).
		Updates(values).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteJob delete the specified jobInfo
func (c *metaOpsClient) DeleteJob(ctx context.Context, jobID string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("id = ?", jobID).
		Delete(&frameModel.MasterMeta{})
	if result.Error != nil {
		return nil, errors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetJobByID query job by `jobID`
func (c *metaOpsClient) GetJobByID(ctx context.Context, jobID string) (*frameModel.MasterMeta, error) {
	var job frameModel.MasterMeta
	if err := c.db.WithContext(ctx).
		Where("id = ?", jobID).
		First(&job).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return &job, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobs(ctx context.Context) ([]*frameModel.MasterMeta, error) {
	var jobs []*frameModel.MasterMeta
	if err := c.db.WithContext(ctx).
		Find(&jobs).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobsByProjectID(ctx context.Context, projectID string) ([]*frameModel.MasterMeta, error) {
	var jobs []*frameModel.MasterMeta
	if err := c.db.WithContext(ctx).
		Where("project_id = ?", projectID).
		Find(&jobs).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

// QueryJobsByState query all jobs with `state` of the projectID
func (c *metaOpsClient) QueryJobsByState(ctx context.Context,
	jobID string, state int,
) ([]*frameModel.MasterMeta, error) {
	var jobs []*frameModel.MasterMeta
	if err := c.db.WithContext(ctx).
		Where("project_id = ? AND state = ?", jobID, state).
		Find(&jobs).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

// ///////////////////////////// Worker Operation
// UpsertWorker insert the workerInfo
func (c *metaOpsClient) UpsertWorker(ctx context.Context, worker *frameModel.WorkerStatus) error {
	if worker == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}, {Name: "job_id"}},
			DoUpdates: clause.AssignmentColumns(frameModel.WorkerUpdateColumns),
		}).Create(worker).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

func (c *metaOpsClient) UpdateWorker(ctx context.Context, worker *frameModel.WorkerStatus) error {
	if worker == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	if err := c.db.WithContext(ctx).
		Model(&frameModel.WorkerStatus{}).
		Where("job_id = ? AND id = ?", worker.JobID, worker.ID).
		Updates(worker.Map()).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteWorker delete the specified workInfo
func (c *metaOpsClient) DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("job_id = ? AND id = ?", masterID, workerID).
		Delete(&frameModel.WorkerStatus{})
	if result.Error != nil {
		return nil, errors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetWorkerByID query worker info by workerID
func (c *metaOpsClient) GetWorkerByID(ctx context.Context, masterID string, workerID string) (*frameModel.WorkerStatus, error) {
	var worker frameModel.WorkerStatus
	if err := c.db.WithContext(ctx).
		Where("job_id = ? AND id = ?", masterID, workerID).
		First(&worker).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return &worker, nil
}

// QueryWorkersByMasterID query all workers of masterID
func (c *metaOpsClient) QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*frameModel.WorkerStatus, error) {
	var workers []*frameModel.WorkerStatus
	if err := c.db.WithContext(ctx).
		Where("job_id = ?", masterID).
		Find(&workers).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return workers, nil
}

// QueryWorkersByState query all workers with specified state of masterID
func (c *metaOpsClient) QueryWorkersByState(ctx context.Context, masterID string, state int) ([]*frameModel.WorkerStatus, error) {
	var workers []*frameModel.WorkerStatus
	if err := c.db.WithContext(ctx).
		Where("job_id = ? AND state = ?", masterID, state).
		Find(&workers).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return workers, nil
}

// ///////////////////////////// Resource Operation
// UpsertResource upsert the ResourceMeta
func (c *metaOpsClient) UpsertResource(ctx context.Context, resource *resModel.ResourceMeta) error {
	if resource == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "job_id"}, {Name: "id"}},
			DoUpdates: clause.AssignmentColumns(resModel.ResourceUpdateColumns),
		}).Create(resource).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// CreateResource insert a resource meta.
// Return 'ErrDuplicateResourceID' error if it already exists.
func (c *metaOpsClient) CreateResource(ctx context.Context, resource *resModel.ResourceMeta) error {
	if resource == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}

	err := c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		err := tx.Model(&resModel.ResourceMeta{}).
			Where("job_id = ? AND id = ?", resource.Job, resource.ID).
			Count(&count).Error
		if err != nil {
			return err
		}

		if count > 0 {
			return errors.ErrDuplicateResourceID.GenWithStackByArgs(resource.ID)
		}

		if err := tx.Create(resource).Error; err != nil {
			return errors.ErrMetaOpFail.Wrap(err)
		}
		return nil
	})
	if err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// UpdateResource update the resModel
func (c *metaOpsClient) UpdateResource(ctx context.Context, resource *resModel.ResourceMeta) error {
	if resource == nil {
		return errors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	if err := c.db.WithContext(ctx).
		Model(&resModel.ResourceMeta{}).
		Where("job_id = ? AND id = ?", resource.Job, resource.ID).
		Updates(resource.Map()).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteResource delete the resource meta of specified resourceKey
func (c *metaOpsClient) DeleteResource(ctx context.Context, resourceKey ResourceKey) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("job_id = ? AND id = ?", resourceKey.JobID, resourceKey.ID).
		Delete(&resModel.ResourceMeta{})
	if result.Error != nil {
		return nil, errors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetResourceByID query resource of the resourceKey
func (c *metaOpsClient) GetResourceByID(ctx context.Context, resourceKey ResourceKey) (*resModel.ResourceMeta, error) {
	var resource resModel.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("job_id = ? AND id = ?", resourceKey.JobID, resourceKey.ID).
		First(&resource).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, errors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return &resource, nil
}

// QueryResources get all resource meta
func (c *metaOpsClient) QueryResources(ctx context.Context) ([]*resModel.ResourceMeta, error) {
	var resources []*resModel.ResourceMeta
	if err := c.db.WithContext(ctx).
		Find(&resources).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// QueryResourcesByJobID query all resources of the jobID
func (c *metaOpsClient) QueryResourcesByJobID(ctx context.Context, jobID string) ([]*resModel.ResourceMeta, error) {
	var resources []*resModel.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Find(&resources).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// QueryResourcesByExecutorIDs query all resources of the executorIDs
func (c *metaOpsClient) QueryResourcesByExecutorIDs(
	ctx context.Context, executorIDs ...engineModel.ExecutorID,
) ([]*resModel.ResourceMeta, error) {
	var resources []*resModel.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("executor_id in ?", executorIDs).
		Find(&resources).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// DeleteResourcesByTypeAndExecutorIDs delete a specific type of resources of executorID
func (c *metaOpsClient) DeleteResourcesByTypeAndExecutorIDs(
	ctx context.Context, resType resModel.ResourceType, executorIDs ...engineModel.ExecutorID,
) (Result, error) {
	var result *gorm.DB
	if len(executorIDs) == 1 {
		result = c.db.WithContext(ctx).
			Where("executor_id = ? and id like ?", executorIDs[0], resType.BuildPrefix()+"%").
			Delete(&resModel.ResourceMeta{})
	} else {
		result = c.db.WithContext(ctx).
			Where("executor_id in ? and id like ?", executorIDs, resType.BuildPrefix()+"%").
			Delete(&resModel.ResourceMeta{})
	}
	if result.Error == nil {
		return &ormResult{rowsAffected: result.RowsAffected}, nil
	}

	return nil, errors.ErrMetaOpFail.Wrap(result.Error)
}

// SetGCPendingByJobs set the resourceIDs to the state `waiting to gc`
func (c *metaOpsClient) SetGCPendingByJobs(ctx context.Context, jobIDs ...engineModel.JobID) error {
	err := c.db.WithContext(ctx).
		Model(&resModel.ResourceMeta{}).
		Where("job_id in ?", jobIDs).
		Update("gc_pending", true).Error
	if err == nil {
		return nil
	}
	return errors.ErrMetaOpFail.Wrap(err)
}

// GetOneResourceForGC get one resource ready for gc
func (c *metaOpsClient) GetOneResourceForGC(ctx context.Context) (*resModel.ResourceMeta, error) {
	var ret resModel.ResourceMeta
	err := c.db.WithContext(ctx).
		Order("updated_at asc").
		Where("gc_pending = true").
		First(&ret).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, errors.ErrMetaEntryNotFound.Wrap(err)
		}
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}
	return &ret, nil
}

// SetJobNoop sets a job noop status if a job op record exists and status is
// canceling. This API is used when processing a job operation but the metadata
// of this job is not found (or deleted manually by accident).
func (c *metaOpsClient) SetJobNoop(ctx context.Context, jobID string) (Result, error) {
	result := &ormResult{}
	ops := &model.JobOp{
		Op: model.JobOpStatusNoop,
	}
	exec := c.db.WithContext(ctx).
		Model(&model.JobOp{}).
		Where("job_id = ? AND op = ?", jobID, model.JobOpStatusCanceling).
		Updates(ops.Map())
	if err := exec.Error; err != nil {
		return result, errors.WrapError(errors.ErrMetaOpFail, err)
	}
	result.rowsAffected = exec.RowsAffected
	return result, nil
}

// SetJobCanceling sets a job cancelling status if this op record doesn't exist.
// If a job cancelling op already exists, does nothing.
// If the job is already cancelled, return ErrJobAlreadyCanceled error.
func (c *metaOpsClient) SetJobCanceling(ctx context.Context, jobID string) (Result, error) {
	result := &ormResult{}
	err := c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var count int64
		var ops model.JobOp
		query := tx.Model(&model.JobOp{}).Where("job_id = ?", jobID)
		if err := query.Count(&count).Error; err != nil {
			return errors.WrapError(errors.ErrMetaOpFail, err)
		}
		if count > 0 {
			if err := query.First(&ops).Error; err != nil {
				return errors.WrapError(errors.ErrMetaOpFail, err)
			}
			switch ops.Op {
			case model.JobOpStatusCanceling:
				return nil
			case model.JobOpStatusCanceled:
				return errors.ErrJobAlreadyCanceled.GenWithStackByArgs(jobID)
			default:
			}
		}
		ops = model.JobOp{
			Op:    model.JobOpStatusCanceling,
			JobID: jobID,
		}
		exec := tx.Clauses(
			clause.OnConflict{
				Columns:   []clause.Column{{Name: "job_id"}},
				DoUpdates: clause.AssignmentColumns(model.JobOpUpdateColumns),
			}).Create(&ops)
		if err := exec.Error; err != nil {
			return errors.WrapError(errors.ErrMetaOpFail, err)
		}
		result.rowsAffected = exec.RowsAffected
		return nil
	})
	return result, errors.WrapError(errors.ErrMetaOpFail, err)
}

// SetJobCanceled sets a cancelled status if a cancelling op exists.
//   - If cancelling operation is not found, it can be triggered by unexpected
//     SetJobCanceled don't make any change and return nil error.
//   - If a job is already cancelled, don't make any change and return nil error.
func (c *metaOpsClient) SetJobCanceled(ctx context.Context, jobID string) (Result, error) {
	result := &ormResult{}
	ops := &model.JobOp{
		Op: model.JobOpStatusCanceled,
	}
	exec := c.db.WithContext(ctx).
		Model(&model.JobOp{}).
		Where("job_id = ? AND op = ?", jobID, model.JobOpStatusCanceling).
		Updates(ops.Map())
	if err := exec.Error; err != nil {
		return result, errors.WrapError(errors.ErrMetaOpFail, err)
	}
	result.rowsAffected = exec.RowsAffected
	return result, nil
}

// QueryJobOp queries a JobOp based on jobID
func (c *metaOpsClient) QueryJobOp(
	ctx context.Context, jobID string,
) (*model.JobOp, error) {
	var op *model.JobOp
	err := c.db.WithContext(ctx).Where("job_id = ?", jobID).Find(&op).Error
	if err != nil {
		return nil, err
	}
	return op, nil
}

// QueryJobOpsByStatus query all jobOps with given `op`
func (c *metaOpsClient) QueryJobOpsByStatus(
	ctx context.Context, op model.JobOpStatus,
) ([]*model.JobOp, error) {
	var ops []*model.JobOp
	if err := c.db.WithContext(ctx).
		Where("op = ?", op).
		Find(&ops).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}
	return ops, nil
}

// CreateExecutor creates an executor in the metastore.
func (c *metaOpsClient) CreateExecutor(ctx context.Context, executor *model.Executor) error {
	if err := c.db.WithContext(ctx).
		Create(executor).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// UpdateExecutor updates an executor in the metastore.
func (c *metaOpsClient) UpdateExecutor(ctx context.Context, executor *model.Executor) error {
	if err := c.db.WithContext(ctx).
		Model(&model.Executor{}).
		Where("id = ?", executor.ID).
		Updates(executor.Map()).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// DeleteExecutor deletes an executor in the metastore.
func (c *metaOpsClient) DeleteExecutor(ctx context.Context, executorID engineModel.ExecutorID) error {
	if err := c.db.WithContext(ctx).
		Where("id = ?", executorID).
		Delete(&model.Executor{}).Error; err != nil {
		return errors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// QueryExecutors query all executors in the metastore.
func (c *metaOpsClient) QueryExecutors(ctx context.Context) ([]*model.Executor, error) {
	var executors []*model.Executor
	if err := c.db.WithContext(ctx).
		Find(&executors).Error; err != nil {
		return nil, errors.ErrMetaOpFail.Wrap(err)
	}
	return executors, nil
}

// Result defines a query result interface
type Result interface {
	RowsAffected() int64
}

type ormResult struct {
	rowsAffected int64
}

// RowsAffected return the affected rows of an execution
func (r ormResult) RowsAffected() int64 {
	return r.rowsAffected
}
