package cluster

import (
	"github.com/hanfei1991/microcosom/model"
	"github.com/hanfei1991/microcosom/pkg/log"
	"go.uber.org/zap"
)

// ResouceManager manages the resources of the clusters.
type ResourceMgr interface {
	GetResourceSnapshot() *ResourceSnapshot
}

// Resource is the min unit of resource that we count.
type ResourceUsage int

type ExecutorResource struct {
	ID model.ExecutorID

	Capacity ResourceUsage
	Reserved ResourceUsage
	Used     ResourceUsage
}

func (e *ExecutorResource) getSnapShot() *ExecutorResource {
	r := &ExecutorResource{
		ID:       e.ID,
		Capacity: e.Capacity,
		Reserved: e.Reserved,
		Used:     e.Used,
	}
	return r
}

// ResourceSnapshot shows the resource usage of every executors.
type ResourceSnapshot struct {
	Executors []*ExecutorResource
}

// GetResourceSnapshot provides the snapshot of current resource usage.
func (r *ExecutorManager) GetResourceSnapshot() *ResourceSnapshot {
	snapshot := &ResourceSnapshot{}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, exec := range r.executors {
		log.L().Logger.Info("executor status", zap.Int32("cap", int32(exec.resource.Capacity)))
		if exec.Status == model.Running && exec.resource.Capacity > exec.resource.Reserved && exec.resource.Capacity > exec.resource.Used {
			snapshot.Executors = append(snapshot.Executors, exec.resource.getSnapShot())
		}
	}
	return snapshot
}
