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
	gerrors "errors"
	"fmt"
	"time"

	dmysql "github.com/go-sql-driver/mysql"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	frameModel "github.com/pingcap/tiflow/engine/framework/model"
	cerrors "github.com/pingcap/tiflow/engine/pkg/errors"
	resourcemeta "github.com/pingcap/tiflow/engine/pkg/externalresource/resourcemeta/model"
	"github.com/pingcap/tiflow/engine/pkg/meta/metaclient"
	"github.com/pingcap/tiflow/engine/pkg/orm/model"
	"github.com/pingcap/tiflow/engine/pkg/tenant"
)

var globalModels = []interface{}{
	&model.ProjectInfo{},
	&model.ProjectOperation{},
	&frameModel.MasterMetaKVData{},
	&frameModel.WorkerStatus{},
	&resourcemeta.ResourceMeta{},
	&model.LogicEpoch{},
}

// TODO: retry and idempotent??

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
	UpsertJob(ctx context.Context, job *frameModel.MasterMetaKVData) error
	UpdateJob(ctx context.Context, job *frameModel.MasterMetaKVData) error
	DeleteJob(ctx context.Context, jobID string) (Result, error)

	GetJobByID(ctx context.Context, jobID string) (*frameModel.MasterMetaKVData, error)
	QueryJobs(ctx context.Context) ([]*frameModel.MasterMetaKVData, error)
	QueryJobsByProjectID(ctx context.Context, projectID string) ([]*frameModel.MasterMetaKVData, error)
	QueryJobsByStatus(ctx context.Context, jobID string, status int) ([]*frameModel.MasterMetaKVData, error)
}

// WorkerClient defines interface that manages worker in metastore
type WorkerClient interface {
	UpsertWorker(ctx context.Context, worker *frameModel.WorkerStatus) error
	UpdateWorker(ctx context.Context, worker *frameModel.WorkerStatus) error
	DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error)
	GetWorkerByID(ctx context.Context, masterID string, workerID string) (*frameModel.WorkerStatus, error)
	QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*frameModel.WorkerStatus, error)
	QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]*frameModel.WorkerStatus, error)
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
func NewClient(mc metaclient.StoreConfigParams, conf DBConfig) (Client, error) {
	err := createDatabaseForProject(mc, tenant.FrameProjectInfo.UniqueID(), conf)
	if err != nil {
		return nil, err
	}

	dsn := generateDSNByParams(mc, tenant.FrameProjectInfo.UniqueID(), conf, true)
	sqlDB, err := newSQLDB("mysql", dsn, conf)
	if err != nil {
		return nil, err
	}

	cli, err := newClient(sqlDB)
	if err != nil {
		sqlDB.Close()
	}

	return cli, err
}

// TODO: check the projectID
func createDatabaseForProject(mc metaclient.StoreConfigParams, projectID tenant.ProjectID, conf DBConfig) error {
	dsn := generateDSNByParams(mc, projectID, conf, false)
	log.L().Info("mysql connection", zap.String("dsn", dsn))

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Error(err))
		return cerrors.ErrMetaOpFail.Wrap(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	query := fmt.Sprintf("CREATE DATABASE if not exists %s", projectID)
	_, err = db.ExecContext(ctx, query)
	if err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// generateDSNByParams will use projectID as DBName to achieve isolation.
// Besides, it will add some default mysql params to the dsn
func generateDSNByParams(mc metaclient.StoreConfigParams, projectID tenant.ProjectID,
	conf DBConfig, withDB bool,
) string {
	dsnCfg := dmysql.NewConfig()
	if dsnCfg.Params == nil {
		dsnCfg.Params = make(map[string]string, 1)
	}
	dsnCfg.User = mc.Auth.User
	dsnCfg.Passwd = mc.Auth.Passwd
	dsnCfg.Net = "tcp"
	dsnCfg.Addr = mc.Endpoints[0]
	if withDB {
		dsnCfg.DBName = projectID
	}
	dsnCfg.InterpolateParams = true
	// dsnCfg.MultiStatements = true
	dsnCfg.Params["readTimeout"] = conf.ReadTimeout
	dsnCfg.Params["writeTimeout"] = conf.WriteTimeout
	dsnCfg.Params["timeout"] = conf.DialTimeout
	dsnCfg.Params["parseTime"] = "true"
	// TODO: check for timezone
	dsnCfg.Params["loc"] = "Local"

	// dsn format: [username[:password]@][protocol[(address)]]/
	return dsnCfg.FormatDSN()
}

// newSqlDB return sql.DB for specified driver and dsn
func newSQLDB(driver string, dsn string, conf DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		log.L().Error("open dsn fail", zap.String("dsn", dsn), zap.Any("config", conf), zap.Error(err))
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	db.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(conf.ConnMaxLifeTime)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	db.SetMaxOpenConns(conf.MaxOpenConns)
	return db, nil
}

func newClient(sqlDB *sql.DB) (*metaOpsClient, error) {
	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn:                      sqlDB,
		SkipInitializeWithVersion: false,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		log.L().Error("create gorm client fail", zap.Error(err))
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
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
	impl, err := c.db.DB()
	if err != nil {
		return err
	}
	if impl != nil {
		return cerrors.ErrMetaOpFail.Wrap(impl.Close())
	}

	return nil
}

////////////////////////// Initialize
// Initialize will create all related tables in SQL backend
// TODO: What happen if we upgrade the definition of model when rolling update?
// TODO: need test: change column definition/add column/drop column?
func (c *metaOpsClient) Initialize(ctx context.Context) error {
	failpoint.InjectContext(ctx, "initializedDelay", nil)
	if err := c.db.WithContext(ctx).
		AutoMigrate(globalModels...); err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	// check first record in logic_epochs
	return model.InitializeEpoch(ctx, c.db)
}

/////////////////////////////// Logic Epoch
func (c *metaOpsClient) GenEpoch(ctx context.Context) (frameModel.Epoch, error) {
	failpoint.InjectContext(ctx, "genEpochDelay", nil)
	return model.GenEpoch(ctx, c.db)
}

///////////////////////// Project Operation
// CreateProject insert the model.ProjectInfo
func (c *metaOpsClient) CreateProject(ctx context.Context, project *model.ProjectInfo) error {
	if project == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input project info is nil")
	}
	if err := c.db.WithContext(ctx).
		Create(project).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteProject delete the model.ProjectInfo
func (c *metaOpsClient) DeleteProject(ctx context.Context, projectID string) error {
	if err := c.db.WithContext(ctx).
		Where("id=?", projectID).
		Delete(&model.ProjectInfo{}).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// QueryProject query all projects
func (c *metaOpsClient) QueryProjects(ctx context.Context) ([]*model.ProjectInfo, error) {
	var projects []*model.ProjectInfo
	if err := c.db.WithContext(ctx).
		Find(&projects).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
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
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return &project, nil
}

// CreateProjectOperation insert the operation
func (c *metaOpsClient) CreateProjectOperation(ctx context.Context, op *model.ProjectOperation) error {
	if op == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input project operation is nil")
	}

	if err := c.db.WithContext(ctx).
		Create(op).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// QueryProjectOperations query all operations of the projectID
func (c *metaOpsClient) QueryProjectOperations(ctx context.Context, projectID string) ([]*model.ProjectOperation, error) {
	var projectOps []*model.ProjectOperation
	if err := c.db.WithContext(ctx).
		Where("project_id = ?", projectID).
		Find(&projectOps).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
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
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return projectOps, nil
}

/////////////////////////////// Job Operation
// UpsertJob upsert the jobInfo
func (c *metaOpsClient) UpsertJob(ctx context.Context, job *frameModel.MasterMetaKVData) error {
	if job == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}},
			DoUpdates: clause.AssignmentColumns(frameModel.MasterUpdateColumns),
		}).Create(job).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// UpdateJob update the jobInfo
func (c *metaOpsClient) UpdateJob(ctx context.Context, job *frameModel.MasterMetaKVData) error {
	if job == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input master meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	// expected SQL: UPDATE xxx SET xxx='xxx', updated_at='2013-11-17 21:34:10' WHERE id=xxx;
	if err := c.db.WithContext(ctx).
		Model(&frameModel.MasterMetaKVData{}).
		Where("id = ?", job.ID).
		Updates(job.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteJob delete the specified jobInfo
func (c *metaOpsClient) DeleteJob(ctx context.Context, jobID string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("id = ?", jobID).
		Delete(&frameModel.MasterMetaKVData{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetJobByID query job by `jobID`
func (c *metaOpsClient) GetJobByID(ctx context.Context, jobID string) (*frameModel.MasterMetaKVData, error) {
	var job frameModel.MasterMetaKVData
	if err := c.db.WithContext(ctx).
		Where("id = ?", jobID).
		First(&job).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return &job, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobs(ctx context.Context) ([]*frameModel.MasterMetaKVData, error) {
	var jobs []*frameModel.MasterMetaKVData
	if err := c.db.WithContext(ctx).
		Find(&jobs).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *metaOpsClient) QueryJobsByProjectID(ctx context.Context, projectID string) ([]*frameModel.MasterMetaKVData, error) {
	var jobs []*frameModel.MasterMetaKVData
	if err := c.db.WithContext(ctx).
		Where("project_id = ?", projectID).
		Find(&jobs).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

// QueryJobsByStatus query all jobs with `status` of the projectID
func (c *metaOpsClient) QueryJobsByStatus(ctx context.Context,
	jobID string, status int,
) ([]*frameModel.MasterMetaKVData, error) {
	var jobs []*frameModel.MasterMetaKVData
	if err := c.db.WithContext(ctx).
		Where("id = ? AND status = ?", jobID, status).
		Find(&jobs).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return jobs, nil
}

/////////////////////////////// Worker Operation
// UpsertWorker insert the workerInfo
func (c *metaOpsClient) UpsertWorker(ctx context.Context, worker *frameModel.WorkerStatus) error {
	if worker == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "id"}, {Name: "job_id"}},
			DoUpdates: clause.AssignmentColumns(frameModel.WorkerUpdateColumns),
		}).Create(worker).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

func (c *metaOpsClient) UpdateWorker(ctx context.Context, worker *frameModel.WorkerStatus) error {
	if worker == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input worker meta is nil")
	}
	// we don't use `Save` here to avoid user dealing with the basic model
	if err := c.db.WithContext(ctx).
		Model(&frameModel.WorkerStatus{}).
		Where("job_id = ? AND id = ?", worker.JobID, worker.ID).
		Updates(worker.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteWorker delete the specified workInfo
func (c *metaOpsClient) DeleteWorker(ctx context.Context, masterID string, workerID string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("job_id = ? AND id = ?", masterID, workerID).
		Delete(&frameModel.WorkerStatus{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
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
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return &worker, nil
}

// QueryWorkersByMasterID query all workers of masterID
func (c *metaOpsClient) QueryWorkersByMasterID(ctx context.Context, masterID string) ([]*frameModel.WorkerStatus, error) {
	var workers []*frameModel.WorkerStatus
	if err := c.db.WithContext(ctx).
		Where("job_id = ?", masterID).
		Find(&workers).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return workers, nil
}

// QueryWorkersByStatus query all workers with specified status of masterID
func (c *metaOpsClient) QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]*frameModel.WorkerStatus, error) {
	var workers []*frameModel.WorkerStatus
	if err := c.db.WithContext(ctx).
		Where("job_id = ? AND status = ?", masterID, status).
		Find(&workers).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return workers, nil
}

/////////////////////////////// Resource Operation
// UpsertResource upsert the ResourceMeta
func (c *metaOpsClient) UpsertResource(ctx context.Context, resource *resourcemeta.ResourceMeta) error {
	if resource == nil {
		return cerrors.ErrMetaParamsInvalid.GenWithStackByArgs("input resource meta is nil")
	}

	if err := c.db.WithContext(ctx).
		Clauses(clause.OnConflict{
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

	err := c.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
	if err := c.db.WithContext(ctx).
		Model(&resourcemeta.ResourceMeta{}).
		Where("id = ?", resource.ID).
		Updates(resource.Map()).Error; err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}

	return nil
}

// DeleteResource delete the specified model.frameModel.resourcemeta.ResourceMeta
func (c *metaOpsClient) DeleteResource(ctx context.Context, resourceID string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("id = ?", resourceID).
		Delete(&resourcemeta.ResourceMeta{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// DeleteResources delete the specified resources
func (c *metaOpsClient) DeleteResources(ctx context.Context, resourceIDs []string) (Result, error) {
	result := c.db.WithContext(ctx).
		Where("id in ?", resourceIDs).
		Delete(&resourcemeta.ResourceMeta{})
	if result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(result.Error)
	}

	return &ormResult{rowsAffected: result.RowsAffected}, nil
}

// GetResourceByID query resource of the resource_id
func (c *metaOpsClient) GetResourceByID(ctx context.Context, resourceID string) (*resourcemeta.ResourceMeta, error) {
	var resource resourcemeta.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("id = ?", resourceID).
		First(&resource).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(err)
		}

		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return &resource, nil
}

func (c *metaOpsClient) QueryResources(ctx context.Context) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if err := c.db.WithContext(ctx).
		Find(&resources).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// QueryResourcesByJobID query all resources of the jobID
func (c *metaOpsClient) QueryResourcesByJobID(ctx context.Context, jobID string) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("job_id = ?", jobID).
		Find(&resources).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// QueryResourcesByExecutorID query all resources of the executor_id
func (c *metaOpsClient) QueryResourcesByExecutorID(ctx context.Context, executorID string) ([]*resourcemeta.ResourceMeta, error) {
	var resources []*resourcemeta.ResourceMeta
	if err := c.db.WithContext(ctx).
		Where("executor_id = ?", executorID).
		Find(&resources).Error; err != nil {
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
	}

	return resources, nil
}

// DeleteResourcesByExecutorID delete all the resources of executorID
func (c *metaOpsClient) DeleteResourcesByExecutorID(ctx context.Context, executorID string) error {
	err := c.db.WithContext(ctx).
		Where("executor_id = ?", executorID).
		Delete(&resourcemeta.ResourceMeta{}).Error
	if err != nil {
		return cerrors.ErrMetaOpFail.Wrap(err)
	}
	return nil
}

// SetGCPending set the resourceIDs to the state `waiting to gc`
func (c *metaOpsClient) SetGCPending(ctx context.Context, ids []resourcemeta.ResourceID) error {
	err := c.db.WithContext(ctx).
		Model(&resourcemeta.ResourceMeta{}).
		Where("id in ?", ids).
		Update("gc_pending", true).Error
	if err == nil {
		return nil
	}
	return cerrors.ErrMetaOpFail.Wrap(err)
}

// GetOneResourceForGC get one resource ready for gc
func (c *metaOpsClient) GetOneResourceForGC(ctx context.Context) (*resourcemeta.ResourceMeta, error) {
	var ret resourcemeta.ResourceMeta
	err := c.db.WithContext(ctx).
		Order("updated_at asc").
		Where("gc_pending = true").
		First(&ret).Error
	if err != nil {
		if gerrors.Is(err, gorm.ErrRecordNotFound) {
			return nil, cerrors.ErrMetaEntryNotFound.Wrap(err)
		}
		return nil, cerrors.ErrMetaOpFail.Wrap(err)
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

// RowsAffected return the affected rows of an execution
func (r ormResult) RowsAffected() int64 {
	return r.rowsAffected
}
