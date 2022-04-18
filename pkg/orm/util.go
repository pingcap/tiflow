package metautil

import (
	"context"
	"database/sql"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"

	cerrors "github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/hanfei1991/microcosm/pkg/orm/model"
)

// TODO: retry and idempotent??
// TODO: context control??

const (
	DefaultConnMaxIdleTime = 30 * time.Second
	DefaultConnMaxLifeTime = 12 * time.Hour
	DefaultMaxIdleConns    = 3
	DefaultMaxOpenConns    = 10
)

type TimeRange struct {
	start time.Time
	end   time.Time
}

// refer to: https://pkg.go.dev/database/sql#SetConnMaxIdleTime
type DBConfig struct {
	ConnMaxIdleTime time.Duration
	ConnMaxLifeTime time.Duration
	MaxIdleConns    int
	MaxOpenConns    int
}

// NewSqlDB return sql.DB for specified driver and dsn
// dsn format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
// TODO: using projectID as dbname to construct dsn to achieve isolation
func NewSQLDB(driver, dsn string, conf DBConfig) (*sql.DB, error) {
	db, err := sql.Open(driver, dsn)
	if err != nil {
		return nil, cerrors.ErrMetaNewClientFail.Wrap(err)
	}

	db.SetConnMaxIdleTime(conf.ConnMaxIdleTime)
	db.SetConnMaxLifetime(conf.ConnMaxLifeTime)
	db.SetMaxIdleConns(conf.MaxIdleConns)
	db.SetMaxOpenConns(conf.MaxOpenConns)

	return db, nil
}

// NewMetaOpsClient return the client to operate framework metastore
func NewMetaOpsClient(sqlDB *sql.DB) (*MetaOpsClient, error) {
	if sqlDB == nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs("input db is nil")
	}

	db, err := gorm.Open(mysql.New(mysql.Config{
		Conn: sqlDB,
	}), &gorm.Config{
		SkipDefaultTransaction: true,
		// TODO: logger
	})
	if err != nil {
		return nil, err
	}

	return &MetaOpsClient{
		db: db,
	}, nil
}

// MetaOpsClient is the meta operations client for framework metastore
type MetaOpsClient struct {
	// orm related object
	db *gorm.DB
}

////////////////////////// Initialize
// Initialize will create all related tables in SQL backend
// TODO: What if we change the definition of orm??
func (c *MetaOpsClient) Initialize(ctx context.Context) error {
	if err := c.db.AutoMigrate(&model.ProjectInfo{}, &model.ProjectOperation{}, &model.MasterMeta{},
		&model.WorkerMeta{}, &model.ResourceMeta{}); err != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(err)
	}

	return nil
}

///////////////////////// Project Operation
// AddProject insert the model.ProjectInfo
func (c *MetaOpsClient) AddProject(ctx context.Context, project *model.ProjectInfo) error {
	if project == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("input project info is nil")
	}
	if result := c.db.Create(project); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteProject delete the model.ProjectInfo
func (c *MetaOpsClient) DeleteProject(ctx context.Context, projectID string) error {
	if result := c.db.Where("id=?", projectID).Delete(&model.ProjectInfo{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryProject query all projects
func (c *MetaOpsClient) QueryProjects(ctx context.Context) ([]model.ProjectInfo, error) {
	var projects []model.ProjectInfo
	if result := c.db.Find(&projects); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projects, nil
}

// GetProjectByID query project by projectID
func (c *MetaOpsClient) GetProjectByID(ctx context.Context, projectID string) (*model.ProjectInfo, error) {
	var project model.ProjectInfo
	if result := c.db.Where("id = ?", projectID).First(&project); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &project, nil
}

// AddProjectOperation insert the operation
func (c *MetaOpsClient) AddProjectOperation(ctx context.Context, op *model.ProjectOperation) error {
	if op == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("input project operation is nil")
	}

	if result := c.db.Create(op); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// QueryProjectOperations query all operations of the projectID
func (c *MetaOpsClient) QueryProjectOperations(ctx context.Context, projectID string) ([]model.ProjectOperation, error) {
	var projectOps []model.ProjectOperation
	if result := c.db.Where("project_id = ?", projectID).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projectOps, nil
}

// QueryProjectOperationsByTimeRange query project operation betweem a time range of the projectID
func (c *MetaOpsClient) QueryProjectOperationsByTimeRange(ctx context.Context,
	projectID string, tr TimeRange,
) ([]model.ProjectOperation, error) {
	var projectOps []model.ProjectOperation
	if result := c.db.Where("project_id = ? AND created_at >= ? AND created_at <= ?", projectID, tr.start,
		tr.end).Find(&projectOps); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return projectOps, nil
}

/////////////////////////////// Job Operation
// AddJob insert the jobInfo
func (c *MetaOpsClient) AddJob(ctx context.Context, job *model.MasterMeta) error {
	if job == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("input master meta is nil")
	}

	if result := c.db.Create(job); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// UpdateJob update the jobInfo
func (c *MetaOpsClient) UpdateJob(ctx context.Context, job *model.MasterMeta) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(job); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteJob delete the specified jobInfo
func (c *MetaOpsClient) DeleteJob(ctx context.Context, jobID string) error {
	if result := c.db.Where("id = ?", jobID).Delete(&model.MasterMeta{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// GetJobByID query job by `jobID`
func (c *MetaOpsClient) GetJobByID(ctx context.Context, jobID string) (*model.MasterMeta, error) {
	var job model.MasterMeta
	if result := c.db.Where("id = ?", jobID).First(&job); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &job, nil
}

// QueryJobsByProjectID query all jobs of projectID
func (c *MetaOpsClient) QueryJobsByProjectID(ctx context.Context, projectID string) ([]model.MasterMeta, error) {
	var jobs []model.MasterMeta
	if result := c.db.Where("project_id = ?", projectID).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return jobs, nil
}

// QueryJobsByStatus query all jobs with `status` of the projectID
func (c *MetaOpsClient) QueryJobsByStatus(ctx context.Context,
	jobID string, status int,
) ([]model.MasterMeta, error) {
	var jobs []model.MasterMeta
	if result := c.db.Where("id = ? AND status = ?", jobID, status).Find(&jobs); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return jobs, nil
}

/////////////////////////////// Worker Operation
// AddWorker insert the workerInfo
func (c *MetaOpsClient) AddWorker(ctx context.Context, worker *model.WorkerMeta) error {
	if worker == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("input worker meta is nil")
	}

	if result := c.db.Create(worker); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

func (c *MetaOpsClient) UpdateWorker(ctx context.Context, worker *model.WorkerMeta) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(worker); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteWorker delete the specified workInfo
func (c *MetaOpsClient) DeleteWorker(ctx context.Context, masterID string, workerID string) error {
	if result := c.db.Where("job_id = ? AND id = ?", masterID,
		workerID).Delete(&model.WorkerMeta{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// GetWorkerByID query worker info by workerID
func (c *MetaOpsClient) GetWorkerByID(ctx context.Context, masterID string, workerID string) (*model.WorkerMeta, error) {
	var worker model.WorkerMeta
	if result := c.db.Where("job_id = ? AND id = ?", masterID,
		workerID).First(&worker); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &worker, nil
}

// QueryWorkersByMasterID query all workers of masterID
func (c *MetaOpsClient) QueryWorkersByMasterID(ctx context.Context, masterID string) ([]model.WorkerMeta, error) {
	var workers []model.WorkerMeta
	if result := c.db.Where("job_id = ?", masterID).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return workers, nil
}

// QueryWorkersByStatus query all workers with specified status of masterID
func (c *MetaOpsClient) QueryWorkersByStatus(ctx context.Context, masterID string, status int) ([]model.WorkerMeta, error) {
	var workers []model.WorkerMeta
	if result := c.db.Where("job_id = ? AND status = ?", masterID,
		status).Find(&workers); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return workers, nil
}

/////////////////////////////// Resource Operation
// AddResource insert the model.ResourceMeta
func (c *MetaOpsClient) AddResource(ctx context.Context, resource *model.ResourceMeta) error {
	if resource == nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs("input resource meta is nil")
	}

	if result := c.db.Create(resource); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

func (c *MetaOpsClient) UpdateResource(ctx context.Context, resource *model.ResourceMeta) error {
	// TODO: shall we change the model name?
	if result := c.db.Save(resource); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// DeleteResource delete the specified model.ResourceMeta
func (c *MetaOpsClient) DeleteResource(ctx context.Context, resourceID string) error {
	if result := c.db.Where("id = ?", resourceID).Delete(&model.ResourceMeta{}); result.Error != nil {
		return cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return nil
}

// GetResourceByID query resource of the resource_id
func (c *MetaOpsClient) GetResourceByID(ctx context.Context, resourceID string) (*model.ResourceMeta, error) {
	var resource model.ResourceMeta
	if result := c.db.Where("id = ?", resourceID).
		First(&resource); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return &resource, nil
}

// QueryResourcesByJobID query all resources of the jobID
func (c *MetaOpsClient) QueryResourcesByJobID(ctx context.Context, jobID string) ([]model.ResourceMeta, error) {
	var resources []model.ResourceMeta
	if result := c.db.Where("job_id = ?", jobID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return resources, nil
}

// QueryResourcesByExecutorID query all resources of the executor_id
func (c *MetaOpsClient) QueryResourcesByExecutorID(ctx context.Context, executorID string) ([]model.ResourceMeta, error) {
	var resources []model.ResourceMeta
	if result := c.db.Where("executor_id = ?", executorID).Find(&resources); result.Error != nil {
		return nil, cerrors.ErrMetaOpFail.GenWithStackByArgs(result.Error)
	}

	return resources, nil
}
