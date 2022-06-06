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
	// MasterID is master id in master worker framework.
	// - It is job manager id when master is job manager and worker is job master.
	// - It is job master id when master is job master and worker is worker.
	MasterID = string
	// WorkerID is worker id in master worker framework.
	// - It is job master id when master is job manager and worker is job master.
	// - It is worker id when master is job master and worker is worker.
	WorkerID = string
	// Epoch is an increasing only value.
	Epoch = int64
	// JobType is the type for job.
	JobType = string
	// JobID is the unique identifier for job.
	JobID = string
)
