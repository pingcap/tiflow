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

package s3

import (
	"fmt"

	"github.com/pingcap/tiflow/engine/model"
	"github.com/pingcap/tiflow/engine/pkg/externalresource/internal"
	resModel "github.com/pingcap/tiflow/engine/pkg/externalresource/model"
)

const (
	// dummyJobID is a dummy job ID used for the s3 storage.
	dummyJobID = "dummy-job-%s"
	// DummyWorkerID is a dummy worker ID used for the s3 storage.
	DummyWorkerID = "keep-alive-worker"
	// DummyResourceID is a dummy resource ID used for the s3 storage.
	DummyResourceID = "/s3/dummy"
	// DummyResourceName is a dummy resource name used for the s3 storage.
	DummyResourceName = "dummy"
)

// GetDummyIdent returns a dummy resource ident for testing.
func GetDummyIdent(executorID model.ExecutorID) internal.ResourceIdent {
	return internal.ResourceIdent{
		Name: DummyResourceName,
		ResourceScope: internal.ResourceScope{
			Executor: executorID,
			WorkerID: DummyWorkerID,
		},
	}
}

// GetDummyResourceKey returns a dummy resource key for s3 storage.
func GetDummyResourceKey(executorID model.ExecutorID) resModel.ResourceKey {
	return resModel.ResourceKey{
		JobID: GetDummyJobID(executorID),
		ID:    DummyResourceID,
	}
}

// GetDummyJobID returns a dummy job ID for s3 storage.
func GetDummyJobID(executorID model.ExecutorID) model.JobID {
	return fmt.Sprintf(dummyJobID, executorID)
}
