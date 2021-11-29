package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/hanfei1991/microcosm/master/cluster"
	"github.com/hanfei1991/microcosm/master/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/autoid"
)

type jobMaster struct {
	*system.Master
	config *Config
}

// BuildBenchmarkJobMaster for benchmark workload.
func BuildBenchmarkJobMaster(rawConfig string, idAllocator *autoid.Allocator, resourceMgr cluster.ResourceMgr, client cluster.ExecutorClient) (*jobMaster, error) {
	config, err := configFromJSON(rawConfig)
	if err != nil {
		return nil, err
	}

	job := &model.Job{
		ID: model.JobID(idAllocator.AllocID()),
	}

	tableTasks := make([]*model.Task, 0)
	for _, addr := range config.Servers {
		tableOp := model.TableReaderOp{
			FlowID:   config.FlowID,
			Addr:     addr,
			TableNum: int32(config.TableNum),
		}
		js, err := json.Marshal(tableOp)
		if err != nil {
			return nil, err
		}
		t := &model.Task{
			FlowID: config.FlowID,
			JobID:  job.ID,
			ID:     model.TaskID(idAllocator.AllocID()),
			Cost:   1,
			Op:     js,
			OpTp:   model.TableReaderType,
		}
		tableTasks = append(tableTasks, t)
	}

	job.Tasks = tableTasks
	hashTasks := make([]*model.Task, 0)
	sinkTasks := make([]*model.Task, 0)
	for i := 1; i <= config.TableNum; i++ {
		hashOp := model.HashOp{
			TableID: int32(i),
		}
		js, err := json.Marshal(hashOp)
		if err != nil {
			return nil, err
		}
		hashTask := &model.Task{
			FlowID: config.FlowID,
			JobID:  job.ID,
			ID:     model.TaskID(idAllocator.AllocID()),
			Cost:   1,
			Op:     js,
			OpTp:   model.HashType,
		}
		hashTasks = append(hashTasks, hashTask)

		sinkOp := model.TableSinkOp{
			File: filepath.Join("/tmp", "dataflow", config.FlowID, fmt.Sprintf("table_%d", i)),
		}
		js, err = json.Marshal(sinkOp)
		if err != nil {
			return nil, err
		}
		sinkTask := &model.Task{
			FlowID: config.FlowID,
			JobID:  job.ID,
			ID:     model.TaskID(idAllocator.AllocID()),
			Cost:   1,
			Op:     js,
			OpTp:   model.TableSinkType,
		}
		sinkTasks = append(sinkTasks, sinkTask)
		connectTwoTask(hashTask, sinkTask)
	}
	for _, trTask := range tableTasks {
		for _, hsTask := range hashTasks {
			connectTwoTask(trTask, hsTask)
		}
	}
	job.Tasks = append(job.Tasks, hashTasks...)
	job.Tasks = append(job.Tasks, sinkTasks...)
	systemJobMaster := system.New(context.Background(), job, resourceMgr, client)
	master := &jobMaster{
		Master: systemJobMaster,
		config: config,
	}
	return master, nil
}

func connectTwoTask(taskA, taskB *model.Task) {
	taskA.Outputs = append(taskA.Outputs, taskB.ID)
	taskB.Inputs = append(taskB.Inputs, taskA.ID)
}
