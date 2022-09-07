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

import (
	"bytes"
	"encoding/json"
)

type (
	// MasterID is master id in master worker framework.
	// - It is job manager id when master is job manager and worker is job master.
	// - It is job master id when master is job master and worker is worker.
	MasterID = string
	// WorkerID is worker id in master worker framework.
	// - It is job master id when master is job manager and worker is job master.
	// - It is worker id when master is job master and worker is worker.
	WorkerID = string
	// WorkerType represents task type, such as DM worker, DM master, etc.
	WorkerType int16
	// Epoch is an increasing only value.
	Epoch = int64
)

// Defines all task type
// TODO: Refine me.Currently, when adding a new worker type or job type, we need to modify many code places,
// NOTICE: DO NOT CHANGE the previous worker type
// Modify the comment in model IF you add some new worker type
const (
	JobManager = WorkerType(iota + 1)
	// job master
	CvsJobMaster
	FakeJobMaster
	DMJobMaster
	CdcJobMaster
	// task
	CvsTask
	FakeTask
	DmTask
	CdcTask
	// worker
	WorkerDMDump
	WorkerDMLoad
	WorkerDMSync
	// extend the worker type here
)

var toString = map[WorkerType]string{
	0:             "",
	JobManager:    "JobManager",
	CvsJobMaster:  "CVSJobMaster",
	FakeJobMaster: "FakeJobMaster",
	DMJobMaster:   "DMJobMaster",
	CdcJobMaster:  "CDCJobMaster",
	CvsTask:       "CVSTask",
	FakeTask:      "FakeTask",
	DmTask:        "DMTask",
	CdcTask:       "CDCTask",
	WorkerDMDump:  "DMDumpTask",
	WorkerDMLoad:  "DMLoadTask",
	WorkerDMSync:  "DMSyncTask",
}

var toID = map[string]WorkerType{
	"":              0,
	"JobManager":    JobManager,
	"CVSJobMaster":  CvsJobMaster,
	"FakeJobMaster": FakeJobMaster,
	"DMJobMaster":   DMJobMaster,
	"CDCJobMaster":  CdcJobMaster,
	"CVSTask":       CvsTask,
	"FakeTask":      FakeTask,
	"DMTask":        DmTask,
	"CDCTask":       CdcTask,
	"DMDumpTask":    WorkerDMDump,
	"DMLoadTask":    WorkerDMLoad,
	"DMSyncTask":    WorkerDMSync,
}

// String implements fmt.Stringer interface
func (wt WorkerType) String() string {
	return toString[wt]
}

// MarshalJSON marshals the enum as a quoted json string
func (wt WorkerType) MarshalJSON() ([]byte, error) {
	buffer := bytes.NewBufferString(`"`)
	buffer.WriteString(wt.String())
	buffer.WriteString(`"`)
	return buffer.Bytes(), nil
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (wt *WorkerType) UnmarshalJSON(b []byte) error {
	var j string
	err := json.Unmarshal(b, &j)
	if err != nil {
		return err
	}
	*wt = toID[j]
	return nil
}
