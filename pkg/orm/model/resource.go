package model

import (
	"github.com/hanfei1991/microcosm/pb"
)

// ResourceMeta is the records stored in the metastore.
type ResourceMeta struct {
	Model
	ProjectID ProjectID  `gorm:"column:project_id;type:char(36) not null"`
	ID        ResourceID `json:"id" gorm:"column:id;type:char(36) not null;uniqueIndex:uidx_id;index:idx_ji,priority:2;index:idx_ei,priority:2"`
	Job       JobID      `json:"job" gorm:"column:job_id;type:char(36) not null;index:idx_ji,priority:1"`
	Worker    WorkerID   `json:"worker" gorm:"column:worker_id;type:char(36) not null"`
	Executor  ExecutorID `json:"executor" gorm:"column:executor_id;type:char(36) not null;index:idx_ei,priority:1"`
	Deleted   bool       `json:"deleted" gorm:"column:deleted;type:BOOLEAN"`
}

// GetID implements dataset.DataEntry
func (m *ResourceMeta) GetID() string {
	return m.ID
}

// ToQueryResourceResponse converts the ResourceMeta to pb.QueryResourceResponse
func (m *ResourceMeta) ToQueryResourceResponse() *pb.QueryResourceResponse {
	return &pb.QueryResourceResponse{
		CreatorExecutor: m.Executor,
		JobId:           m.Job,
		CreatorWorkerId: m.Worker,
	}
}
