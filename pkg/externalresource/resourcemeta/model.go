package resourcemeta

import (
	"time"

	"github.com/hanfei1991/microcosm/model"
)

type (
	WorkerID   = string
	ResourceID = string
	JobID      = string
	ExecutorID = model.ExecutorID
)

// ResourceMeta is the records stored in the metastore.
type ResourceMeta struct {
	ID       ResourceID `json:"id"`
	Job      JobID      `json:"job"`
	Worker   WorkerID   `json:"worker"`
	Executor ExecutorID `json:"executor"`
	Deleted  bool       `json:"deleted"`
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
