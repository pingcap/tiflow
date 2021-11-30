package model

import "github.com/hanfei1991/microcosm/pb"

type (
	JobID  int32
	TaskID int32
)

type Job struct {
	ID    JobID
	Tasks []*Task
}

func (j *Job) ToPB() *pb.SubmitBatchTasksRequest {
	req := &pb.SubmitBatchTasksRequest{}
	for _, t := range j.Tasks {
		req.Tasks = append(req.Tasks, t.ToPB())
	}
	return req
}

type Task struct {
	// FlowID is unique for a same dataflow, passed from submitted job
	FlowID string

	ID      TaskID
	JobID   JobID
	Outputs []TaskID
	Inputs  []TaskID

	// TODO: operator or operator tree
	OpTp              OperatorType
	Op                Operator
	Cost              int
	PreferredLocation string
}

func (t *Task) ToPB() *pb.TaskRequest {
	req := &pb.TaskRequest{
		Id:   int32(t.ID),
		Op:   t.Op,
		OpTp: int32(t.OpTp),
	}
	for _, c := range t.Inputs {
		req.Inputs = append(req.Inputs, int32(c))
	}
	for _, c := range t.Outputs {
		req.Outputs = append(req.Outputs, int32(c))
	}
	return req
}

// ToScheduleTaskPB converts a task to a schedule task request
func (t *Task) ToScheduleTaskPB() *pb.ScheduleTask {
	req := &pb.ScheduleTask{
		Task:              t.ToPB(),
		Cost:              int64(t.Cost),
		PreferredLocation: t.PreferredLocation,
	}
	return req
}
