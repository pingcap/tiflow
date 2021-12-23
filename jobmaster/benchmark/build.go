package benchmark

import (
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/hanfei1991/microcosm/client"
	"github.com/hanfei1991/microcosm/jobmaster/system"
	"github.com/hanfei1991/microcosm/model"
	"github.com/hanfei1991/microcosm/pkg/autoid"
)

// BuildBenchmarkJobMaster for benchmark workload.
func BuildBenchmarkJobMaster(
	rawConfig string,
	jobID model.ID,
	clients *client.Manager,
) (*jobMaster, error) {
	config, err := configFromJSON(rawConfig)
	if err != nil {
		return nil, err
	}

	idAllocator, err := autoid.NewTaskIDAllocator(int32(jobID))
	if err != nil {
		return nil, err
	}
	// build producers
	produceTasks := make([]*model.Task, 0)
	binlogTasks := make([]*model.Task, 0)
	for id := int32(0); id < config.TableNum; id++ {
		produceOp := &model.ProducerOp{
			TableID:      id,
			RecordCnt:    config.RecordCnt,
			DDLFrequency: config.DDLFrequency,
			OutputCnt:    len(config.Servers),
		}
		js, err := json.Marshal(produceOp)
		if err != nil {
			return nil, err
		}
		produceTasks = append(produceTasks, &model.Task{
			FlowID: config.FlowID,
			ID:     model.ID(idAllocator.AllocTaskID()),
			Cost:   1,
			OpTp:   model.ProducerType,
			Op:     js,
		})
	}
	for _, addr := range config.Servers {
		binlogOp := &model.BinlogOp{
			Address: addr,
		}
		js, err := json.Marshal(binlogOp)
		if err != nil {
			return nil, err
		}
		binlogTasks = append(binlogTasks, &model.Task{
			FlowID: config.FlowID,
			ID:     model.ID(idAllocator.AllocTaskID()),
			Cost:   1,
			OpTp:   model.BinlogType,
			Op:     js,
		})
	}

	for _, inputTask := range produceTasks {
		for _, outputTask := range binlogTasks {
			connectTwoTask(inputTask, outputTask)
		}
	}

	tableTasks := make([]*model.Task, 0)
	hashTasks := make([]*model.Task, 0)
	sinkTasks := make([]*model.Task, 0)

	for i, addr := range config.Servers {
		tableOp := model.TableReaderOp{
			FlowID: config.FlowID,
			Addr:   addr,
		}
		js, err := json.Marshal(tableOp)
		if err != nil {
			return nil, err
		}
		tableTask := &model.Task{
			FlowID: config.FlowID,
			ID:     model.ID(idAllocator.AllocTaskID()),
			Cost:   1,
			Op:     js,
			OpTp:   model.TableReaderType,
		}
		tableTasks = append(tableTasks, tableTask)

		hashOp := model.HashOp{}
		js, err = json.Marshal(hashOp)
		if err != nil {
			return nil, err
		}
		hashTask := &model.Task{
			FlowID: config.FlowID,
			ID:     model.ID(idAllocator.AllocTaskID()),
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
			ID:     model.ID(idAllocator.AllocTaskID()),
			Cost:   1,
			Op:     js,
			OpTp:   model.TableSinkType,
		}
		sinkTasks = append(sinkTasks, sinkTask)
		connectTwoTask(tableTask, hashTask)
		connectTwoTask(hashTask, sinkTask)
	}

	systemJobMaster := system.New(jobID, clients)
	master := &jobMaster{
		Master: systemJobMaster,
		config: config,
	}
	master.stage1 = append(master.stage1, produceTasks...)
	master.stage1 = append(master.stage1, binlogTasks...)
	master.stage2 = append(master.stage2, tableTasks...)
	master.stage2 = append(master.stage2, hashTasks...)
	master.stage2 = append(master.stage2, sinkTasks...)
	return master, nil
}

func connectTwoTask(taskA, taskB *model.Task) {
	taskA.Outputs = append(taskA.Outputs, taskB.ID)
	taskB.Inputs = append(taskB.Inputs, taskA.ID)
}
