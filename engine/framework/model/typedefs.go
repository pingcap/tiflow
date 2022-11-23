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
	"encoding/json"
	"fmt"

	"github.com/pingcap/tiflow/pkg/errors"
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

var typesStringify = [...]string{
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

var toWorkerType map[string]WorkerType

func init() {
	toWorkerType = make(map[string]WorkerType, len(typesStringify))
	for i, s := range typesStringify {
		toWorkerType[s] = WorkerType(i)
	}
}

// String implements fmt.Stringer interface
func (wt WorkerType) String() string {
	if int(wt) >= len(typesStringify) || wt < 0 {
		return fmt.Sprintf("Unknown WorkerType %d", wt)
	}
	return typesStringify[wt]
}

// MarshalJSON marshals the enum as a quoted json string
func (wt WorkerType) MarshalJSON() ([]byte, error) {
	return json.Marshal(wt.String())
}

// UnmarshalJSON unmashals a quoted json string to the enum value
func (wt *WorkerType) UnmarshalJSON(b []byte) error {
	var (
		j  string
		ok bool
	)
	if err := json.Unmarshal(b, &j); err != nil {
		return err
	}
	*wt, ok = toWorkerType[j]
	if !ok {
		return errors.Errorf("Unknown WorkerType %s", j)
	}
	return nil
}
