package resource

import (
	"math/rand"
	"sync"
	"time"

	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pb"
	"github.com/hanfei1991/microcosm/pkg/errors"
	"github.com/pingcap/tiflow/dm/pkg/log"
	"go.uber.org/zap"
)

// CapRescMgr implements ResourceMgr interface, and it uses node capacity as
// alloction algorithm
type CapRescMgr struct {
	mu        sync.Mutex
	r         *rand.Rand // random generator for choosing node
	executors map[model.ExecutorID]*ExecutorResource
}

func NewCapRescMgr() *CapRescMgr {
	return &CapRescMgr{
		r:         rand.New(rand.NewSource(time.Now().UnixNano())),
		executors: make(map[model.ExecutorID]*ExecutorResource),
	}
}

// Register implements RescMgr.Register
func (m *CapRescMgr) Register(id model.ExecutorID, addr string, capacity model.RescUnit) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.executors[id] = &ExecutorResource{
		ID:       id,
		Capacity: capacity,
		Addr:     addr,
	}
	log.L().Info("executor resource is registered",
		zap.String("executor-id", string(id)), zap.Int("capacity", int(capacity)))
}

// Unregister implements RescMgr.Unregister
func (m *CapRescMgr) Unregister(id model.ExecutorID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.executors, id)
	log.L().Info("executor resource is unregistered",
		zap.String("executor-id", string(id)))
}

// Allocate implements RescMgr.Allocate
func (m *CapRescMgr) Allocate(tasks []*pb.ScheduleTask) (bool, *pb.TaskSchedulerResponse) {
	return m.allocateTasksWithNaiveStrategy(tasks)
}

// Update implements RescMgr.Update
func (m *CapRescMgr) Update(id model.ExecutorID, used, reserved model.RescUnit, status model.ExecutorStatus) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	exec, ok := m.executors[id]
	if !ok {
		return errors.ErrUnknownExecutorID.GenWithStackByArgs(id)
	}
	exec.Used = used
	exec.Reserved = reserved
	exec.Status = status
	return nil
}

// getAvailableResource returns resources that are available
func (m *CapRescMgr) getAvailableResource() []*ExecutorResource {
	res := make([]*ExecutorResource, 0)
	for _, exec := range m.executors {
		if exec.Status == model.Running &&
			exec.Capacity > exec.Reserved && exec.Capacity > exec.Used {
			res = append(res, exec)
		}
	}
	return res
}

func (m *CapRescMgr) allocateTasksWithNaiveStrategy(
	tasks []*pb.ScheduleTask,
) (bool, *pb.TaskSchedulerResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[int64]*pb.ScheduleResult)
	resources := m.getAvailableResource()
	if len(resources) == 0 {
		// No resources in this cluster
		return false, nil
	}
	idx := m.r.Intn(len(resources))
	for _, task := range tasks {
		originalIdx := idx
		for {
			exec := resources[idx]
			used := exec.Used
			if exec.Reserved > used {
				used = exec.Reserved
			}
			rest := exec.Capacity - used
			if rest >= model.RescUnit(task.Cost) {
				result[task.GetTask().Id] = &pb.ScheduleResult{
					ExecutorId: string(exec.ID),
					Addr:       exec.Addr,
				}
				break
			}
			idx = (idx + 1) % len(resources)
			if idx == originalIdx {
				return false, nil
			}
		}
	}
	return true, &pb.TaskSchedulerResponse{Schedule: result}
}
