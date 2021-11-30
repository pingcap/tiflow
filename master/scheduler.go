package master

import (
	"github.com/hanfei1991/microcosm/master/cluster"
	"github.com/hanfei1991/microcosm/pb"
)

// TODO: Implement different allocate task logic.
// TODO: Add abstraction for resource allocator.
func (s *Server) allocateTasksWithNaiveStrategy(
	snapshot *cluster.ResourceSnapshot,
	tasks []*pb.ScheduleTask,
) (bool, *pb.TaskSchedulerResponse) {
	var idx int = 0
	result := make(map[int32]*pb.ScheduleResult)
	for _, task := range tasks {
		originalIdx := idx
		for {
			exec := snapshot.Executors[idx]
			used := exec.Used
			if exec.Reserved > used {
				used = exec.Reserved
			}
			rest := exec.Capacity - used
			if rest >= cluster.ResourceUsage(task.Cost) {
				result[task.GetTask().Id] = &pb.ScheduleResult{
					ExecutorId: int32(exec.ID),
				}
				exec.Reserved = exec.Reserved + cluster.ResourceUsage(task.GetCost())
				break
			}
			idx = (idx + 1) % len(snapshot.Executors)
			if idx == originalIdx {
				return false, nil
			}
		}
	}
	return true, &pb.TaskSchedulerResponse{Schedule: result}
}
