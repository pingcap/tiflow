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
	gerrors "errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	libModel "github.com/pingcap/tiflow/engine/lib/model"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
)

var frameMetaModels = []interface{}{
	&model.ProjectInfo{},
	&model.ProjectOperation{},
	&libModel.MasterMetaKVData{},
	&libModel.WorkerStatus{},
	&resourcemeta.ResourceMeta{},
	&model.LogicEpoch{},
}

// TODO: retry and idempotent??
// TODO: context control??

// TimeRange defines a time range with [start, end] time
type TimeRange struct {
	start time.Time
	end   time.Time
}

// Client defines an interface that has the ability to manage every kind of
// logic abstraction in metastore, including project, project op, job, worker
// and resource
type Client interface {
	metaclient.Client
	// project
	ProjectClient
	// project operation
	ProjectOperationClient
	// job info
	JobClient
	// worker status
	WorkerClient
	// resource meta
	ResourceClient

	// Initialize will create all tables for backend operation
	Initialize(ctx context.Context) error
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
	UpsertJob(ctx context.Context, job *libModel.MasterMetaKVData) error
	UpdateJob(ctx context.Context, job *libModel.MasterMetaKVData) error
	DeleteJob(ctx context.Context, jobID string) (Result, error)

	GetJobByID(ctx context.Context, jobID string) (*libModel.MasterMetaKVData, error)
	QueryJobs(ctx context.Context) ([]*libModel.MasterMetaKVData, error)
	QueryJobsByProjectID(ctx context.Context, projectID string) ([]*libModel.MasterMetaKVData, error)
	QueryJobsByStatus(ctx context.Context, jobID string, status int) ([]*libModel.MasterMetaKVData, error)
}

// WorkerClient defines interface that manages worker in metastore
type WorkerClient interface {
	UpsertWorker(ctx context.Context, worker *libModel.WorkerStatus) error
	UpdateWorker(ctx context.Context, worker *libModel.WorkerStatus) error
	DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error)
	GetWorkerByID(ctx context.Context, masterID string, workerID string) (*libModel.WorkerStatus, error)
	QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*libModel.WorkerStatus, error)
	QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]*libModel.WorkerStatus, error)
}

// ResourceClient defines interface that manages resource in metastore
type ResourceClient interface {
	CreateResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error
	UpsertResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error
	UpdateResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error
	DeleteResource(ctx context.Context, resourceID string) (Result, error)
	GetResourceByID(ctx context.Context, resourceID string) (*resourcemeta.ResourceMeta, error)
	QueryResources(ctx context.Context) ([]*resourcemeta.ResourceMeta, error)
	QueryResourcesByJobID(ctx context.Context, jobID string) ([]*resourcemeta.ResourceMeta, error)
	QueryResourcesByExecutorID(ctx context.Context, executorID string) ([]*resourcemeta.ResourceMeta, error)
	SetGCPending(ctx context.Context, ids []resourcemeta.ResourceID) error
	DeleteResourcesByExecutorID(ctx context.Context, executorID string) error
	DeleteResources(ctx context.Context, resourceIDs []string) (Result, error)
	GetOneResourceForGC(ctx context.Context) (*resourcemeta.ResourceMeta, error)
}

// NewClient return the client to operate framework metastore
func NewClient(mc *metaclient.StoreConfigParams) (Client, error) {
	db, err := NewOrmDB(mc)
	if err != nil {
		return nil, err
	}

	return &metaOpsClient{
		db: db,
	}, nil
}

// metaOpsClient is the meta operations client for framework metastore
type metaOpsClient struct {
	// gorm claim to be thread safe
	db *gorm.DB
}

func (c *metaOpsClient) Close() error {
	if c.db != nil {
		impl, err := c.db.DB()
		if err != nil {
			return err
		}
		if impl != nil {
			return cerrors.ErrMetaOpFail.Wrap(impl.Close())
		}
	}

	return nil
}

////////////////////////// Initialize
// Initialize will create all related tables in SQL backend
// TODO: What happen if we upgrade the definition of model when rolling update?
// TODO: need test: change column definition/add column/drop column?
func (c *metaOpsClient) Initialize(ctx context.Context) error {
	if err := c.db.AutoMigrate(frameMetaModels...); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	// check first record in logic_epochs
	return model.InitializeEpoch(ctx, c.db)
}

/////////////////////////////// Logic Epoch
func (c *metaOpsClient) GenEpoch(ctx context.Context) (libModel.Epoch, error) {
	return model.GenEpoch(ctx, c.db)
}

///////////////////////// Project Operation
// CreateProject insert the model.ProjectInfo
func (c *metaOpsClient) CreateProject(ctx context.Context, project *model.ProjectInfo) error {
	if project == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input project info is nil")
	}
	if result := c.db.Create(project); result.Error != nil {
		return cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return nil
}

// DeleteProject delete the model.ProjectInfo
func (c *metaOpsClient) DeleteProject(ctx context.Context, projectID string) error {
	if result := c.db.Where("id=?", projectID).Delete(&model.ProjectInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return nil
}

// QueryProject query all projects
func (c *metaOpsClient) QueryProjects(ctx context.Context) ([]*model.ProjectInfo, error) {
	var projects []*model.ProjectInfo
	if result := c.db.Find(&projects); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return projects, nil
}

// GetProjectByID query project by projectID
func (c *metaOpsClient) GetProjectByID(ctx context.Context, projectID string) (*model.ProjectInfo, error) {
	var project model.ProjectInfo
	if result := c.db.Where("id = ?", projectID).First(&project); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(result.Error)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &project, nil
}

// CreateProjectOperation insert the operation
func (c *metaOpsClient) CreateProjectOperation(ctx context.Context, op *model.ProjectOperation) error {
	if op == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input project operation is nil")
	}

	if result := c.db.Create(op); result.Error != nil {
		return cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return nil
}

// QueryProjectOperations query all operations of the projectID
func (c *metaOpsClient) QueryProjectOperations(ctx context.Context, projectID string) ([]*model.ProjectOperation, error) {
	var projectOps []*model.ProjectOperation
	if result := c.db.Where("project_id = ?", projectID).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return projectOps, nil
}

// QueryProjectOperationsByTimeRange query project operation betweem a time range of the projectID
func (c *metaOpsClient) QueryProjectOperationsByTimeRange(ctx context.Context,
	projectID string, tr TimeRange,
) ([]*model.ProjectOperation, error) {
	var projectOps []*model.ProjectOperation
	if result := c.db.Where("project_id = ? AND created_at >= ? AND created_at <= ?", projectID, tr.start,
		tr.end).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return projectOps, nil
}

/////////////////////////////// Job Operation
// UpsertJob upsert the jobInfo
func (c *metaOpsClient) UpsertJob(ctx context.Context, job *libModel.MasterMetaKVData) error {
	if job == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}

	if err := c.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns(libModel.MasterUpdateColumns),
	}).Create(job).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// UpdateJob update the jobInfo
func (c *metaOpsClient) UpdateJob(ctx context.Context, job *libModel.MasterMetaKVData) error {
	if job == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	// expected SQL: UPDATE xxx SET xxx='xxx', updated_at='2013-11-17 21:34:10' WHERE id=xxx;
	if err := c.db.Model(&libModel.MasterMetaKVData{}).Where("id = ?", job.ID).Updates(job.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteJob delete the specified jobInfo
func (c *metaOpsClient) DeleteJob(ctx context.Context, jobID string) (Result, error) {
	result := c.db.Where("id = ?", jobID).Delete(&libModel.MasterMetaKVData{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetJobByID query job by `jobID`
func (c *metaOpsClient) GetJobByID(ctx context.Context, jobID string) (*libModel.MasterMetaKVData, error) {
	var job libModel.MasterMetaKVData
	if result := c.db.Where("id = ?", jobID).First(&job); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(result.Error)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &job, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobs(ctx context.Context) ([]*libModel.MasterMetaKVData, error) {
	var jobs []*libModel.MasterMetaKVData
	if result := c.db.Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return jobs, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobsByProjectID(ctx context.Context, projectID string) ([]*libModel.MasterMetaKVData, error) {
	var jobs []*libModel.MasterMetaKVData
	if result := c.db.Where("project_id = ?", projectID).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return jobs, nil
}

// QueryJobsByStatus query all jobs with `status` of the projectID
func (c *metaOpsClient) QueryJobsByStatus(ctx context.Context,
	jobID string, status int,
) ([]*libModel.MasterMetaKVData, error) {
	var jobs []*libModel.MasterMetaKVData
	if result := c.db.Where("id = ? AND status = ?", jobID, status).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return jobs, nil
}

/////////////////////////////// Worker Operation
// UpsertWorker insert the workerInfo
func (c *metaOpsClient) UpsertWorker(ctx context.Context, worker *libModel.WorkerStatus) error {
	if worker == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}

	if err := c.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}, {Name: "job_id"}},
		DoUpdates: clause.AssignmentColumns(libModel.WorkerUpdateColumns),
	}).Create(worker).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

func (c *metaOpsClient) UpdateWorker(ctx context.Context, worker *libModel.WorkerStatus) error {
	if worker == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	if err := c.db.Model(&libModel.WorkerStatus{}).Where("job_id = ? AND id = ?", worker.JobID, worker.ID).Updates(worker.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteWorker delete the specified workInfo
func (c *metaOpsClient) DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error) {
	result := c.db.Where("job_id = ? AND id = ?", masterID, workerID).Delete(&libModel.WorkerStatus{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetWorkerByID query worker info by workerID
func (c *metaOpsClient) GetWorkerByID(ctx context.Context, masterID string, workerID string) (*libModel.WorkerStatus, error) {
	var worker libModel.WorkerStatus
	if result := c.db.Where("job_id = ? AND id = ?", masterID,
		workerID).First(&worker); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(result.Error)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &worker, nil
}

// QueryWorkersByMasterID query all workers of masterID
func (c *metaOpsClient) QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*libModel.WorkerStatus, error) {
	var workers []*libModel.WorkerStatus
	if result := c.db.Where("job_id = ?", masterID).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return workers, nil
}

// QueryWorkersByStatus query all workers with specified status of masterID
func (c *metaOpsClient) QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]*libModel.WorkerStatus, error) {
	var workers []*libModel.WorkerStatus
	if result := c.db.Where("job_id = ? AND status = ?", masterID,
		status).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return workers, nil
}

/////////////////////////////// Resource Operation
// UpsertResource upsert the ResourceMeta
func (c *metaOpsClient) UpsertResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error {
	if resource == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}

	if err := c.db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns(resourcemeta.ResourceUpdateColumns),
	}).Create(resource).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

func (c *metaOpsClient) CreateResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error {
	if resource == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}

	err := c.db.Transaction(func(tx *gorm.DB) error {
		var count int64
		err := tx.Model(&resourcemeta.ResourceMeta{}).
			Where("id = ?", resource.ID).
			Count(&count).Error
		if err != nil {
			return err
		}

		if count > 0 {
			return cerrors.ErrDuplicateResourceID.GenWithStackByArgs(resource.ID)
		}

		if err := tx.Create(resource).Error; err != nil {
			return cerrors.ErrMetaOpFail.Wrap(err)
		}
		return nil
	})
	if err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// UpdateResource update the resourcemeta
func (c *metaOpsClient) UpdateResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error {
	if resource == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	if err := c.db.Model(&resourcemeta.ResourceMeta{}).Where("id = ?", resource.ID).Updates(resource.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteResource delete the specified model.libModel.resourcemeta.ResourceMeta
func (c *metaOpsClient) DeleteResource(ctx context.Context, resourceID string) (Result, error) {
	result := c.db.Where("id = ?", resourceID).Delete(&resourcemeta.ResourceMeta{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// DeleteResources delete the specified resources
func (c *metaOpsClient) DeleteResources(ctx context.Context, resourceIDs []string) (Result, error) {
	result := c.db.Where("id in ?", resourceIDs).Delete(&resourcemeta.ResourceMeta{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetResourceByID query resource of the resource_id
func (c *metaOpsClient) GetResourceByID(ctx context.Context, resourceID string) (*resourcemeta.ResourceMeta, error) {
	var resource resourcemeta.ResourceMeta
	if result := c.db.Where("id = ?", resourceID).First(&resource); result.Error != nil {
		if result.Error == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(result.Error)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &resource, nil
}

func (c *metaOpsClient) QueryResources(ctx context.Context) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if result := c.db.Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return resources, nil
}

// QueryResourcesByJobID query all resources of the jobID
func (c *metaOpsClient) QueryResourcesByJobID(ctx context.Context, jobID string) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if result := c.db.Where("job_id = ?", jobID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return resources, nil
}

// QueryResourcesByExecutorID query all resources of the executor_id
func (c *metaOpsClient) QueryResourcesByExecutorID(ctx context.Context, executorID string) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if result := c.db.Where("executor_id = ?", executorID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return resources, nil
}

func (c *metaOpsClient) DeleteResourcesByExecutorID(ctx context.Context, executorID string) error {
	tx := c.db.WithContext(ctx).Where("executor_id = ?", executorID).
		Delete(&resourcemeta.ResourceMeta{})
	if tx.Error != nil {
		return cerrors.ErrMetaOpFail.Wrap(tx.Error)
	}
	return nil
}

func (c *metaOpsClient) SetGCPending(ctx context.Context, ids []resourcemeta.ResourceID) error {
	result := c.db.WithContext(ctx).
		Model(&resourcemeta.ResourceMeta{}).
		Where("id in ?", ids).
		Update("gc_pending", true)
	if result.Error == nil {
		return nil
	}
	return cerrors.ErrMetaOpFail.Wrap(result.Error)
}

func (c *metaOpsClient) GetOneResourceForGC(ctx context.Context) (*resourcemeta.ResourceMeta, error) {
	var ret resourcemeta.ResourceMeta
	result := c.db.WithContext(ctx).
		Order("updated_at asc").
		Where("gc_pending = true").
		First(&ret)
	if result.Error != nil {
		if gerrors.Is(result.Error, gorm.ErrRecordNotFound) {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(result.Error)
		}
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}
	return &ret, nil
}

// Result defines a query result interface
type Result interface {
	RowsAffected() int64
}

type ormResult struct {
	rowsAffected int64
}

func (r ormResult) RowsAffected() int64 {
	return r.rowsAffected
}
