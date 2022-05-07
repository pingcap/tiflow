package model

import (
	"path"
	"strings"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	derror "github.com/hanfei1991/microcosm/pkg/errors"
	ormModel "github.com/hanfei1991/microcosm/pkg/orm/model"
	"github.com/hanfei1991/microcosm/pkg/tenant"
)

type (
	WorkerID = string
	// ResourceID should be in the form of `/<type>/<unique-name>`, currently
	// only local type is available.
	ResourceID = string
	JobID      = string
	ExecutorID = model.ExecutorID
)

var ResourceUpdateColumns = []string{
	"updated_at",
	"project_id",
	"id",
	"job_id",
	"worker_id",
	"executor_id",
	"deleted",
}

// ResourceMeta is the records stored in the metastore.
type ResourceMeta struct {
	ormModel.Model
	ProjectID tenant.ProjectID `json:"project-id" gorm:"column:project_id;type:varchar(64) not null"`
	ID        ResourceID       `json:"id" gorm:"column:id;type:varchar(64) not null;uniqueIndex:uidx_id;index:idx_ji,priority:2;index:idx_ei,priority:2"`
	Job       JobID            `json:"job" gorm:"column:job_id;type:varchar(64) not null;index:idx_ji,priority:1"`
	Worker    WorkerID         `json:"worker" gorm:"column:worker_id;type:varchar(64) not null"`
	Executor  ExecutorID       `json:"executor" gorm:"column:executor_id;type:varchar(64) not null;index:idx_ei,priority:1"`
	Deleted   bool             `json:"deleted" gorm:"column:deleted;type:BOOLEAN"`
}

// GetID implements dataset.DataEntry
func (m *ResourceMeta) GetID() string {
	return m.ID
}

// ToQueryResourceResponse converts the ResourceMeta to pb.QueryResourceResponse
func (m *ResourceMeta) ToQueryResourceResponse() *pb.QueryResourceResponse {
	return &pb.QueryResourceResponse{
		CreatorExecutor: string(m.Executor),
		JobId:           m.Job,
		CreatorWorkerId: m.Worker,
	}
}

func (m *ResourceMeta) Map() map[string]interface{} {
	return map[string]interface{}{
		"project_id":  m.ProjectID,
		"id":          m.ID,
		"job_id":      m.Job,
		"worker_id":   m.Worker,
		"executor_id": m.Executor,
		"deleted":     m.Deleted,
	}
}

// GCTodoEntry records a future need for GC'ing a resource.
type GCTodoEntry struct {
	ID           ResourceID `json:"id"`
	Job          JobID      `json:"job"`
	TargetGCTime time.Time  `json:"target_gc_time"`
}

// ResourceType represents the type of the resource
type ResourceType string

const (
	ResourceTypeLocalFile = ResourceType("local")
	ResourceTypeS3        = ResourceType("s3")
)

// ParseResourcePath returns the ResourceType and the path suffix.
func ParseResourcePath(rpath ResourceID) (ResourceType, string, error) {
	if !strings.HasPrefix(rpath, "/") {
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}
	rpath = strings.TrimPrefix(rpath, "/")
	segments := strings.Split(rpath, "/")
	if len(segments) == 0 {
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	var resourceType ResourceType
	switch segments[0] {
	case "local":
		resourceType = ResourceTypeLocalFile
	case "s3":
		resourceType = ResourceTypeS3
	default:
		return "", "", derror.ErrIllegalResourcePath.GenWithStackByArgs(rpath)
	}

	suffix := path.Join(segments[1:]...)
	return resourceType, suffix, nil
}
