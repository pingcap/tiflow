package model

import "github.com/hanfei1991/microcosom/pb"

type JobID int32
type TaskID int32

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
	ID      TaskID
	JobID   JobID
	Outputs []TaskID
	Inputs  []TaskID

	// TODO: operator or operator tree
	OpTp             OperatorType
	Op               Operator
	Cost             int
	PreferedLocation string
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
