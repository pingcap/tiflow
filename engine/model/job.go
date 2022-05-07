// Copyright 2022 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package model

type (
	// ID is the global identified number for jobs and tasks.
	// For job, the id number is less than int32
	// For task, the id number is a int64-number (job id, task id)
	// that job id is the higher 32-bit and task id is the lower 32-bit.
	ID           int64
	WorkloadType int32
	TaskStatus   int32
)

const (
	Benchmark WorkloadType = iota
	DM
	CDC
)

const (
	Init TaskStatus = iota
	Serving
	Pauseed
	Stopped
)

type JobMaster struct {
	ID          ID           `json:"id"`
	Tp          WorkloadType `json:"type"`
	Config      []byte       `json:"config"`
	MasterAddrs []string     `json:"masters"`
}

type Task struct {
	// FlowID is unique for a same dataflow, passed from submitted job
	FlowID string `json:"flow_id"`

	ID      ID   `json:"id"`
	Outputs []ID `json:"outputs"`
	Inputs  []ID `json:"inputs"`

	// TODO: operator or operator tree
	OpTp              OperatorType `json:"type"`
	Op                Operator     `json:"op"`
	Cost              int          `json:"cost"`
	PreferredLocation string       `json:"location"`

	Exec   ExecutorID `json:"exec"`
	Status TaskStatus
}
